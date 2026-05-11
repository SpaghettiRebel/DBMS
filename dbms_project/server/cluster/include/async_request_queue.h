#ifndef ASYNC_REQUEST_QUEUE_H
#define ASYNC_REQUEST_QUEUE_H

#include <string>
#include <queue>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>
#include <chrono>
#include <cstdint>

/**
 * @brief Статус асинхронного запроса
 */
enum class AsyncRequestStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED
};

/**
 * @brief Результат асинхронного запроса
 */
struct AsyncRequestResult {
    std::string request_guid;
    AsyncRequestStatus status;
    std::string result_data;
    std::string error_message;
    int64_t created_at_ms;
    int64_t completed_at_ms;
};

/**
 * @brief Элемент очереди запросов
 */
struct QueuedRequest {
    std::string request_guid;
    std::string query;
    std::string client_id;
    int64_t submitted_at_ms;
    bool is_long_running;
};

/**
 * @brief Генерация GUID v4
 */
std::string generate_guid_v4();

/**
 * @brief Асинхронная очередь запросов
 * 
 * Реализует:
 * - Немедленный возврат GUID при отправке длительного запроса
 * - Фоновую обработку запросов
 * - Получение статуса и результата по GUID
 */
class AsyncRequestQueue {
public:
    using QueryExecutor = std::function<std::string(const std::string& query)>;
    
    /**
     * @brief Конструктор очереди
     * @param executor Функция выполнения запросов
     * @param num_workers Количество рабочих потоков
     */
    explicit AsyncRequestQueue(QueryExecutor executor, size_t num_workers = 4);
    
    ~AsyncRequestQueue();
    
    /**
     * @brief Запуск обработки очереди
     */
    void start();
    
    /**
     * @brief Остановка обработки
     * @param wait_for_completion Ждать завершения текущих запросов
     */
    void stop(bool wait_for_completion = true);
    
    /**
     * @brief Добавление запроса в очередь
     * @param query SQL запрос
     * @param client_id ID клиента
     * @param is_long_running Флаг длительной операции
     * @return GUID запроса
     */
    std::string enqueue(const std::string& query, 
                        const std::string& client_id,
                        bool is_long_running = false);
    
    /**
     * @brief Получение статуса запроса
     * @param request_guid GUID запроса
     * @return Результат запроса
     */
    AsyncRequestResult get_status(const std::string& request_guid);
    
    /**
     * @brief Отмена запроса
     * @param request_guid GUID запроса
     * @return true если отмена успешна
     */
    bool cancel(const std::string& request_guid);
    
    /**
     * @brief Получение длины очереди
     */
    size_t queue_size() const;
    
    /**
     * @brief Получение статистики
     */
    struct QueueStats {
        uint64_t total_submitted;
        uint64_t total_completed;
        uint64_t total_failed;
        uint64_t current_queue_size;
        double avg_execution_time_ms;
    };
    
    QueueStats get_stats();

private:
    /**
     * @brief Рабочий поток обработки очереди
     */
    void worker_thread();
    
    /**
     * @brief Проверка является ли запрос длительным
     */
    static bool detect_long_running_query(const std::string& query);
    
    QueryExecutor executor_;
    size_t num_workers_;
    
    std::atomic<bool> running_;
    std::vector<std::thread> workers_;
    
    std::mutex queue_mutex_;
    std::queue<QueuedRequest> request_queue_;
    
    std::mutex results_mutex_;
    std::unordered_map<std::string, AsyncRequestResult> results_;
    
    // Статистика
    std::atomic<uint64_t> total_submitted_;
    std::atomic<uint64_t> total_completed_;
    std::atomic<uint64_t> total_failed_;
    std::atomic<int64_t> total_execution_time_ms_;
};

#endif // ASYNC_REQUEST_QUEUE_H
