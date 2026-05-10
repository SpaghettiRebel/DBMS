#ifndef CLUSTER_ENTRYPOINT_H
#define CLUSTER_ENTRYPOINT_H

#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <functional>
#include <cstdint>

// Forward declarations
class StorageNode;
class ShardManager;
class HeartbeatMonitor;
class AsyncRequestQueue;
class TelemetryCollector;

/**
 * @brief Результат выполнения запроса
 */
struct QueryResult {
    std::string data;
    bool success;
    std::string error_message;
    int64_t execution_time_ms;
};

/**
 * @brief Статус узла хранения
 */
enum class NodeStatus {
    ONLINE,
    OFFLINE,
    RESTARTING,
    UNKNOWN
};

/**
 * @brief Информация об узле хранения
 */
struct NodeInfo {
    std::string node_id;
    std::string host;
    uint16_t port;
    NodeStatus status;
    size_t shard_count;
    uint64_t total_requests;
    uint64_t failed_requests;
    int64_t last_heartbeat;
};

/**
 * @brief Кластерный балансировщик нагрузки (Entrypoint)
 * 
 * Реализует:
 * - Прием запросов от клиентов
 * - Шардирование данных между Storage-узлами
 * - Динамическое добавление/удаление узлов
 * - Heartbeat мониторинг доступности
 * - Асинхронную обработку запросов
 * - Сбор телеметрии
 */
class ClusterEntrypoint {
public:
    /**
     * @brief Конструктор балансировщика
     * @param host Адрес для прослушивания
     * @param port Порт для прослушивания
     * @param heartbeat_interval_ms Интервал проверки heartbeat (мс)
     */
    ClusterEntrypoint(const std::string& host = "0.0.0.0", 
                      uint16_t port = 8000,
                      int heartbeat_interval_ms = 5000);
    
    ~ClusterEntrypoint();
    
    /**
     * @brief Запуск кластера
     * @return true если запуск успешен
     */
    bool start();
    
    /**
     * @brief Остановка кластера
     * @param timeout_ms Время ожидания завершения
     */
    void stop(uint32_t timeout_ms = 10000);
    
    /**
     * @brief Добавление узла хранения в кластер
     * @param node_id Идентификатор узла
     * @param host Адрес узла
     * @param port Порт узла
     * @return true если добавление успешно
     */
    bool add_storage_node(const std::string& node_id, 
                          const std::string& host, 
                          uint16_t port);
    
    /**
     * @brief Удаление узла хранения из кластера
     * @param node_id Идентификатор узла
     * @return true если удаление успешно
     */
    bool remove_storage_node(const std::string& node_id);
    
    /**
     * @brief Получение информации об узлах
     */
    std::vector<NodeInfo> get_nodes_info() const;
    
    /**
     * @brief Выполнение SQL запроса через кластер
     * @param query SQL запрос
     * @param client_id ID клиента
     * @return Результат выполнения
     */
    QueryResult execute_query(const std::string& query, const std::string& client_id);
    
    /**
     * @brief Асинхронное выполнение запроса
     * @param query SQL запрос
     * @param client_id ID клиента
     * @return GUID запроса для последующего получения результата
     */
    std::string execute_query_async(const std::string& query, const std::string& client_id);
    
    /**
     * @brief Получение статуса асинхронного запроса
     * @param request_guid GUID запроса
     * @return Результат выполнения (если готов) или статус "PENDING"
     */
    QueryResult get_async_result(const std::string& request_guid);
    
    /**
     * @brief Проверка состояния кластера
     */
    bool is_running() const { return running_.load(); }
    
    /**
     * @brief Получение статистики кластера
     */
    struct ClusterStats {
        size_t total_nodes;
        size_t online_nodes;
        uint64_t total_requests;
        uint64_t total_errors;
        double current_rps;
        double avg_rps_10min;
        double max_rps_10min;
        double avg_response_time_10s;
        double error_rate_1min;
    };
    
    ClusterStats get_stats() const;

private:
    /**
     * @brief Основной цикл принятия клиентских подключений
     */
    void accept_loop();
    
    /**
     * @brief Обработка клиентского подключения
     */
    void handle_client(int client_fd);
    
    /**
     * @brief Определение целевого узла для запроса (шардирование)
     */
    std::string select_target_node(const std::string& query);
    
    /**
     * @brief Вычисление хэша для шардирования
     */
    uint32_t compute_shard_hash(const std::string& key);
    
    /**
     * @brief Отправка запроса на Storage-узел
     */
    QueryResult send_to_node(const std::string& node_id, 
                             const std::string& query,
                             int64_t timeout_ms = 30000);
    
    /**
     * @brief Перезапуск недоступного узла
     */
    void restart_node(const std::string& node_id);
    
    /**
     * @brief Логирование событий
     */
    void log_info(const std::string& message);
    void log_error(const std::string& message);

private:
    std::string host_;
    uint16_t port_;
    int heartbeat_interval_ms_;
    
    int server_fd_;
    std::atomic<bool> running_;
    std::thread accept_thread_;
    
    std::unique_ptr<ShardManager> shard_manager_;
    std::unique_ptr<HeartbeatMonitor> heartbeat_monitor_;
    std::unique_ptr<AsyncRequestQueue> async_queue_;
    std::unique_ptr<TelemetryCollector> telemetry_;
    
    std::mutex nodes_mutex_;
    std::unordered_map<std::string, std::unique_ptr<StorageNode>> nodes_;
    
    // Статистика
    std::atomic<uint64_t> total_requests_;
    std::atomic<uint64_t> total_errors_;
};

#endif // CLUSTER_ENTRYPOINT_H
