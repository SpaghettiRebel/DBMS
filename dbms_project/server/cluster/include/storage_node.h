#ifndef STORAGE_NODE_H
#define STORAGE_NODE_H

#include <string>
#include <atomic>
#include <cstdint>
#include <mutex>

/**
 * @brief Представление узла хранения в кластере
 */
class StorageNode {
public:
    enum class Status {
        ONLINE,
        OFFLINE,
        RESTARTING
    };
    
    StorageNode(const std::string& node_id, 
                const std::string& host, 
                uint16_t port);
    
    ~StorageNode() = default;
    
    const std::string& get_node_id() const { return node_id_; }
    const std::string& get_host() const { return host_; }
    uint16_t get_port() const { return port_; }
    
    Status get_status() const { return status_.load(); }
    void set_status(Status status) { status_.store(status); }
    
    bool is_online() const { return status_.load() == Status::ONLINE; }
    
    int get_socket_fd() const { return socket_fd_; }
    void set_socket_fd(int fd) { socket_fd_ = fd; }
    
    uint64_t get_total_requests() const { return total_requests_.load(); }
    void increment_requests() { total_requests_++; }
    
    uint64_t get_failed_requests() const { return failed_requests_.load(); }
    void increment_failed() { failed_requests_++; }
    
    int64_t get_last_heartbeat() const { return last_heartbeat_.load(); }
    void update_heartbeat() { 
        last_heartbeat_.store(get_current_time_ms()); 
    }
    
    size_t get_shard_count() const { return shard_count_; }
    void set_shard_count(size_t count) { shard_count_ = count; }
    
    // Подключение к узлу
    bool connect(int timeout_ms = 5000);
    
    // Отключение от узла
    void disconnect();
    
    // Отправка запроса и получение ответа
    std::string send_query(const std::string& query, int64_t timeout_ms = 30000);
    
    // Проверка доступности (PING)
    bool ping(int64_t timeout_ms = 2000);

private:
    static int64_t get_current_time_ms();
    
    std::string node_id_;
    std::string host_;
    uint16_t port_;
    std::atomic<Status> status_;
    int socket_fd_;
    std::atomic<uint64_t> total_requests_;
    std::atomic<uint64_t> failed_requests_;
    std::atomic<int64_t> last_heartbeat_;
    size_t shard_count_;
    mutable std::mutex mutex_;
};

#endif // STORAGE_NODE_H
