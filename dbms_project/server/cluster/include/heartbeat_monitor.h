#ifndef HEARTBEAT_MONITOR_H
#define HEARTBEAT_MONITOR_H

#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

// Forward declaration
class StorageNode;

/**
 * @brief Монитор доступности узлов (Heartbeat)
 * 
 * Периодически опрашивает Storage-узлы и автоматически
 * перезапускает недоступные узлы.
 */
class HeartbeatMonitor {
public:
    using NodeRestartCallback = std::function<void(const std::string& node_id)>;
    
    /**
     * @brief Конструктор монитора
     * @param interval_ms Интервал проверки (мс)
     * @param timeout_ms Таймаут ответа на PING (мс)
     * @param restart_callback Callback для перезапуска узла
     */
    HeartbeatMonitor(int interval_ms = 5000,
                     int timeout_ms = 2000,
                     NodeRestartCallback restart_callback = nullptr);
    
    ~HeartbeatMonitor();
    
    /**
     * @brief Запуск монитора
     */
    void start();
    
    /**
     * @brief Остановка монитора
     */
    void stop();
    
    /**
     * @brief Регистрация узла для мониторинга
     * @param node_id Идентификатор узла
     * @param node Ссылка на узел
     */
    void register_node(const std::string& node_id, StorageNode* node);
    
    /**
     * @brief Удаление узла из мониторинга
     * @param node_id Идентификатор узла
     */
    void unregister_node(const std::string& node_id);
    
    /**
     * @brief Проверка состояния всех узлов
     * @return Количество обнаруженных проблем
     */
    int check_all_nodes();
    
    /**
     * @brief Принудительная проверка одного узла
     * @param node_id Идентификатор узла
     * @return true если узел доступен
     */
    bool check_node(const std::string& node_id);
    
    /**
     * @brief Получение статистики
     */
    struct HeartbeatStats {
        uint64_t total_checks;
        uint64_t failed_checks;
        uint64_t nodes_restarted;
        int64_t last_check_time;
    };
    
    HeartbeatStats get_stats() const;

private:
    /**
     * @brief Основной цикл мониторинга
     */
    void monitoring_loop();
    
    /**
     * @brief Перезапуск узла
     * @param node_id Идентификатор узла
     */
    void trigger_restart(const std::string& node_id);
    
    int interval_ms_;
    int timeout_ms_;
    NodeRestartCallback restart_callback_;
    
    std::atomic<bool> running_;
    std::thread monitor_thread_;
    
    std::mutex nodes_mutex_;
    std::unordered_map<std::string, StorageNode*> nodes_;
    
    // Статистика
    std::atomic<uint64_t> total_checks_;
    std::atomic<uint64_t> failed_checks_;
    std::atomic<uint64_t> nodes_restarted_;
    std::atomic<int64_t> last_check_time_;
};

#endif // HEARTBEAT_MONITOR_H
