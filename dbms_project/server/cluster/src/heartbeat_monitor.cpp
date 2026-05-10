#include "heartbeat_monitor.h"
#include "storage_node.h"
#include <chrono>
#include <iostream>

HeartbeatMonitor::HeartbeatMonitor(int interval_ms,
                                   int timeout_ms,
                                   NodeRestartCallback restart_callback)
    : interval_ms_(interval_ms)
    , timeout_ms_(timeout_ms)
    , restart_callback_(restart_callback)
    , running_(false)
    , total_checks_(0)
    , failed_checks_(0)
    , nodes_restarted_(0)
    , last_check_time_(0)
{
}

HeartbeatMonitor::~HeartbeatMonitor() {
    stop();
}

void HeartbeatMonitor::start() {
    if (running_.load()) {
        return;
    }
    
    running_.store(true);
    monitor_thread_ = std::thread(&HeartbeatMonitor::monitoring_loop, this);
}

void HeartbeatMonitor::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
}

void HeartbeatMonitor::register_node(const std::string& node_id, StorageNode* node) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    nodes_[node_id] = node;
}

void HeartbeatMonitor::unregister_node(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    nodes_.erase(node_id);
}

int HeartbeatMonitor::check_all_nodes() {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    
    int problems = 0;
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    
    for (auto& pair : nodes_) {
        const std::string& node_id = pair.first;
        StorageNode* node = pair.second;
        
        total_checks_++;
        
        if (!node->ping(timeout_ms_)) {
            failed_checks_++;
            problems++;
            
            // Узел недоступен - помечаем как OFFLINE
            node->set_status(StorageNode::Status::OFFLINE);
            
            std::cerr << "[Heartbeat] Node " << node_id << " is OFFLINE" << std::endl;
            
            // Запуск перезапуска если есть callback
            if (restart_callback_) {
                trigger_restart(node_id);
            }
        } else {
            node->set_status(StorageNode::Status::ONLINE);
        }
    }
    
    last_check_time_.store(now_ms);
    return problems;
}

bool HeartbeatMonitor::check_node(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    
    total_checks_++;
    
    if (!it->second->ping(timeout_ms_)) {
        failed_checks_++;
        it->second->set_status(StorageNode::Status::OFFLINE);
        return false;
    }
    
    it->second->set_status(StorageNode::Status::ONLINE);
    return true;
}

void HeartbeatMonitor::monitoring_loop() {
    while (running_.load()) {
        check_all_nodes();
        
        // Сон до следующей проверки
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
    }
}

void HeartbeatMonitor::trigger_restart(const std::string& node_id) {
    if (restart_callback_) {
        nodes_restarted_++;
        restart_callback_(node_id);
    }
}

HeartbeatMonitor::HeartbeatStats HeartbeatMonitor::get_stats() const {
    HeartbeatStats stats;
    stats.total_checks = total_checks_.load();
    stats.failed_checks = failed_checks_.load();
    stats.nodes_restarted = nodes_restarted_.load();
    stats.last_check_time = last_check_time_.load();
    return stats;
}
