#include "cluster_entrypoint.h"
#include "storage_node.h"
#include "shard_manager.h"
#include "heartbeat_monitor.h"
#include "async_request_queue.h"
#include "telemetry_collector.h"

#include <iostream>
#include <cstring>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <algorithm>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

constexpr uint32_t PROTOCOL_MAGIC = 0x44424D53;
constexpr size_t MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

ClusterEntrypoint::ClusterEntrypoint(const std::string& host, uint16_t port, int heartbeat_interval_ms)
    : host_(host), port_(port), heartbeat_interval_ms_(heartbeat_interval_ms),
      server_fd_(-1), running_(false), total_requests_(0), total_errors_(0) {
    shard_manager_ = std::make_unique<ShardManager>();
    heartbeat_monitor_ = std::make_unique<HeartbeatMonitor>(heartbeat_interval_ms, 2000,
        [this](const std::string& node_id) { restart_node(node_id); });
    async_queue_ = nullptr;
    telemetry_ = std::make_unique<TelemetryCollector>();
}

ClusterEntrypoint::~ClusterEntrypoint() { stop(); }

bool ClusterEntrypoint::start() {
    if (running_.load()) { log_error("Already running"); return false; }
    
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) { log_error("Socket failed: " + std::string(strerror(errno))); return false; }
    
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    inet_pton(AF_INET, host_.c_str(), &address.sin_addr);
    
    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        log_error("Bind failed"); close(server_fd_); return false;
    }
    if (listen(server_fd_, 100) < 0) { log_error("Listen failed"); close(server_fd_); return false; }
    
    running_.store(true);
    
    async_queue_ = std::make_unique<AsyncRequestQueue>(
        [this](const std::string& q) { auto r = execute_query(q, "async"); if(!r.success) throw std::runtime_error(r.error_message); return r.data; }, 4);
    
    heartbeat_monitor_->start();
    async_queue_->start();
    accept_thread_ = std::thread(&ClusterEntrypoint::accept_loop, this);
    
    log_info("Started on " + host_ + ":" + std::to_string(port_));
    return true;
}

void ClusterEntrypoint::stop(uint32_t) {
    if (!running_.load()) return;
    log_info("Stopping...");
    running_.store(false);
    if (async_queue_) async_queue_->stop(true);
    if (heartbeat_monitor_) heartbeat_monitor_->stop();
    if (server_fd_ >= 0) { shutdown(server_fd_, SHUT_RDWR); close(server_fd_); server_fd_ = -1; }
    if (accept_thread_.joinable()) accept_thread_.join();
    { std::lock_guard<std::mutex> lock(nodes_mutex_); for(auto& p : nodes_) p.second->disconnect(); nodes_.clear(); }
    log_info("Stopped");
}

bool ClusterEntrypoint::add_storage_node(const std::string& node_id, const std::string& host, uint16_t port) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    if (nodes_.count(node_id)) { log_error("Node exists: " + node_id); return false; }
    auto node = std::make_unique<StorageNode>(node_id, host, port);
    shard_manager_->add_node(node_id);
    heartbeat_monitor_->register_node(node_id, node.get());
    nodes_[node_id] = std::move(node);
    log_info("Added node: " + node_id);
    return true;
}

bool ClusterEntrypoint::remove_storage_node(const std::string& node_id) {
    std::unique_ptr<StorageNode> removed;
    { std::lock_guard<std::mutex> lock(nodes_mutex_);
      auto it = nodes_.find(node_id);
      if (it == nodes_.end()) { log_error("Node not found: " + node_id); return false; }
      heartbeat_monitor_->unregister_node(node_id);
      shard_manager_->remove_node(node_id);
      removed = std::move(it->second);
      nodes_.erase(it);
    }
    removed->disconnect();
    log_info("Removed node: " + node_id);
    return true;
}

std::vector<NodeInfo> ClusterEntrypoint::get_nodes_info() const {
    std::vector<NodeInfo> infos;
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(nodes_mutex_));
    for (const auto& p : nodes_) {
        NodeInfo info;
        info.node_id = p.second->get_node_id();
        info.host = p.second->get_host();
        info.port = p.second->get_port();
        info.status = static_cast<NodeStatus>(p.second->get_status());
        info.shard_count = p.second->get_shard_count();
        info.total_requests = p.second->get_total_requests();
        info.failed_requests = p.second->get_failed_requests();
        info.last_heartbeat = p.second->get_last_heartbeat();
        infos.push_back(info);
    }
    return infos;
}

QueryResult ClusterEntrypoint::execute_query(const std::string& query, const std::string& client_id) {
    auto start = std::chrono::steady_clock::now();
    QueryResult result{ "", false, "", 0 };
    try {
        std::string target;
        if (ShardManager::is_global_query(query)) {
            auto nodes = shard_manager_->get_all_nodes();
            if (nodes.empty()) throw std::runtime_error("No nodes");
            target = nodes[0];
        } else {
            std::string tbl = ShardManager::extract_table_name(query);
            target = shard_manager_->get_node_for_key(tbl.empty() ? query : tbl);
        }
        result.data = send_to_node(target, query).data;
        result.success = true;
        result.execution_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        telemetry_->record_request(result.execution_time_ms);
        total_requests_++;
    } catch (const std::exception& e) {
        result.error_message = e.what();
        total_errors_++;
        telemetry_->record_error();
        result.execution_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    }
    return result;
}

std::string ClusterEntrypoint::execute_query_async(const std::string& query, const std::string& client_id) {
    return async_queue_->enqueue(query, client_id, false);
}

QueryResult ClusterEntrypoint::get_async_result(const std::string& guid) {
    auto ar = async_queue_->get_status(guid);
    QueryResult r;
    r.success = (ar.status == AsyncRequestStatus::COMPLETED);
    switch(ar.status) {
        case AsyncRequestStatus::PENDING: r.data = "PENDING"; break;
        case AsyncRequestStatus::RUNNING: r.data = "RUNNING"; break;
        case AsyncRequestStatus::COMPLETED: r.data = ar.result_data; r.execution_time_ms = ar.completed_at_ms - ar.created_at_ms; break;
        case AsyncRequestStatus::FAILED: r.error_message = ar.error_message; break;
    }
    return r;
}

ClusterEntrypoint::ClusterStats ClusterEntrypoint::get_stats() const {
    ClusterStats s{};
    auto ni = get_nodes_info();
    s.total_nodes = ni.size();
    for(const auto& n : ni) if(n.status == NodeStatus::ONLINE) s.online_nodes++;
    s.total_requests = total_requests_.load();
    s.total_errors = total_errors_.load();
    auto ts = telemetry_->get_stats();
    s.current_rps = ts.current_rps;
    s.avg_rps_10min = ts.avg_rps_10min;
    s.max_rps_10min = ts.max_rps_10min;
    s.avg_response_time_10s = ts.avg_response_time_10s;
    s.error_rate_1min = ts.error_rate_1min;
    return s;
}

void ClusterEntrypoint::accept_loop() {
    while (running_.load()) {
        sockaddr_in ca{}; socklen_t cl = sizeof(ca);
        int cfd = accept(server_fd_, (sockaddr*)&ca, &cl);
        if (cfd < 0) { if(errno==EINTR||errno==EAGAIN) continue; if(!running_.load()) break; continue; }
        std::thread([this,cfd](){ handle_client(cfd); }).detach();
    }
}

void ClusterEntrypoint::handle_client(int cfd) {
    auto cleanup = [cfd](){ close(cfd); };
    try {
        uint8_t hdr[21]; 
        if(recv(cfd, hdr, 21, 0) != 21) { cleanup(); return; }
        uint32_t magic; memcpy(&magic, hdr, 4);
        if(magic != PROTOCOL_MAGIC) { cleanup(); return; }
        uint32_t plen; memcpy(&plen, hdr+17, 4);
        if(plen > MAX_MESSAGE_SIZE) { cleanup(); return; }
        std::string query(plen, 0);
        if(plen > 0 && recv(cfd, &query[0], plen, 0) != (ssize_t)plen) { cleanup(); return; }
        
        QueryResult res = execute_query(query, std::to_string(cfd));
        std::string resp = res.success ? res.data : res.error_message;
        uint8_t rtype = res.success ? 0x02 : 0x03;
        
        std::vector<uint8_t> out(21 + resp.size());
        size_t off = 0;
        memcpy(out.data()+off, &magic, 4); off+=4;
        memcpy(out.data()+off, &rtype, 1); off+=1;
        uint32_t rid=0; memcpy(out.data()+off, &rid, 4); off+=4;
        int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        memcpy(out.data()+off, &ts, 8); off+=8;
        uint32_t rlen = resp.size(); memcpy(out.data()+off, &rlen, 4); off+=4;
        if(!resp.empty()) memcpy(out.data()+off, resp.data(), resp.size());
        send(cfd, out.data(), out.size(), MSG_NOSIGNAL);
    } catch(...) { total_errors_++; }
    cleanup();
}

QueryResult ClusterEntrypoint::send_to_node(const std::string& node_id, const std::string& query, int64_t) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    auto it = nodes_.find(node_id);
    if(it == nodes_.end()) throw std::runtime_error("Node not found: "+node_id);
    return QueryResult{ it->second->send_query(query), true, "", 0 };
}

void ClusterEntrypoint::restart_node(const std::string& node_id) {
    log_info("Restarting: " + node_id);
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    auto it = nodes_.find(node_id);
    if(it == nodes_.end()) return;
    it->second->set_status(StorageNode::Status::RESTARTING);
    if(it->second->connect(5000)) { it->second->set_status(StorageNode::Status::ONLINE); log_info("Restarted: "+node_id); }
    else { it->second->set_status(StorageNode::Status::OFFLINE); log_error("Restart failed: "+node_id); }
}

void ClusterEntrypoint::log_info(const std::string& msg) {
    auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S") << " [INFO] " << msg << std::endl;
}
void ClusterEntrypoint::log_error(const std::string& msg) {
    auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cerr << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S") << " [ERROR] " << msg << std::endl;
}
