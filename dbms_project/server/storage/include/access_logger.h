#ifndef ACCESS_LOGGER_H
#define ACCESS_LOGGER_H

#include <string>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <fstream>
#include <chrono>
#include <cstdint>

namespace dbms {

// Структура записи лога
struct LogEntry {
    std::string client_id;
    std::string request_id;
    std::string query_body;
    int64_t start_time_ms;
    int64_t end_time_ms;
    int status_code;      // 0 = OK, >0 = Error code
    std::string error_msg;
    
    // Форматирование в строку для записи
    std::string to_string() const;
};

class AccessLogger {
public:
    static AccessLogger& instance();
    
    // Инициализация (вызывать один раз при старте)
    void init(const std::string& log_file_path = "access.log");
    
    // Асинхронная запись лога (не блокирует вызывающий поток)
    void log(const LogEntry& entry);
    
    // Принудительная запись всех буферизованных логов на диск
    void flush();
    
    // Остановка фоновой записи и закрытие файла
    void shutdown();
    
    // Получить количество записей в очереди (для телеметрии)
    size_t queue_size() const;
    
    // Получить общее количество записанных логов
    uint64_t total_logged() const;

private:
    AccessLogger() = default;
    ~AccessLogger();
    
    // Запрет копирования
    AccessLogger(const AccessLogger&) = delete;
    AccessLogger& operator=(const AccessLogger&) = delete;
    
    // Фоновый поток записи
    void writer_thread_func();
    
    std::queue<LogEntry> queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::thread writer_thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> total_logged_{0};
    std::ofstream file_;
    std::string log_path_;
    
    // Буфер для пакетной записи
    std::vector<LogEntry> write_buffer_;
    static constexpr size_t BUFFER_SIZE = 100;
    static constexpr auto FLUSH_INTERVAL = std::chrono::milliseconds(500);
};

} // namespace dbms

#endif // ACCESS_LOGGER_H
