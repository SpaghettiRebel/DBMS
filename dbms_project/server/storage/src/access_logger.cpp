#include "access_logger.h"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <ctime>

namespace dbms {

std::string LogEntry::to_string() const {
    std::ostringstream oss;
    
    // Формат времени: YYYY-MM-DD HH:MM:SS.mmm
    auto start_sec = std::chrono::milliseconds(start_time_ms);
    auto start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        start_sec - std::chrono::duration_cast<std::chrono::seconds>(start_sec)
    ).count();
    
    std::time_t start_time_t = std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::time_point(std::chrono::duration_cast<std::chrono::system_clock::duration>(start_sec))
    );
    
    char time_buf[32];
    std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", std::localtime(&start_time_t));
    
    int64_t duration_ms = end_time_ms - start_time_ms;
    
    bool is_err = !(status_code == 0 || status_code == 200 || status_code == 202);

    // Формат: [TIMESTAMP] [CLIENT_ID] [REQUEST_ID] STATUS DURATION_MS "QUERY" [ERROR]
    oss << "[" << time_buf << "." << std::setfill('0') << std::setw(3) << start_ms << "] "
        << "[" << client_id << "] "
        << "[" << request_id << "] "
        << (is_err ? "ERR" : "OK") << "(" << status_code << ") "
        << duration_ms << "ms \"";
    
    // Экранирование кавычек в запросе
    for (char c : query_body) {
        if (c == '"') oss << "\\\"";
        else if (c == '\n') oss << "\\n";
        else if (c == '\r') oss << "\\r";
        else if (c == '\t') oss << "\\t";
        else oss << c;
    }
    
    oss << "\"";
    
    if (is_err && !error_msg.empty()) {
        oss << " ERROR: \"" << error_msg << "\"";
    }
    
    return oss.str();
}

AccessLogger& AccessLogger::instance() {
    static AccessLogger instance;
    return instance;
}

void AccessLogger::init(const std::string& log_file_path) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (running_) return;
    
    log_path_ = log_file_path;
    file_.open(log_path_, std::ios::app);
    if (!file_.is_open()) {
        std::cerr << "[AccessLogger] Warning: Could not open log file: " << log_path_ << std::endl;
    }
    
    running_ = true;
    writer_thread_ = std::thread(&AccessLogger::writer_thread_func, this);
}

AccessLogger::~AccessLogger() {
    shutdown();
}

void AccessLogger::shutdown() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!running_) return;
        running_ = false;
    }
    
    cv_.notify_one();
    
    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
    
    flush();
    if (file_.is_open()) {
        file_.close();
    }
}

void AccessLogger::log(const LogEntry& entry) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push(entry);
        total_logged_++;
    }
    cv_.notify_one();
}

void AccessLogger::flush() {
    std::vector<LogEntry> buffer;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!queue_.empty()) {
            buffer.push_back(queue_.front());
            queue_.pop();
        }
    }
    
    if (file_.is_open() && !buffer.empty()) {
        for (const auto& entry : buffer) {
            file_ << entry.to_string() << "\n";
        }
        file_.flush();
    }
}

void AccessLogger::writer_thread_func() {
    std::vector<LogEntry> buffer;
    buffer.reserve(BUFFER_SIZE);
    
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        
        // Ждем данные или сигнал остановки
        cv_.wait_for(lock, FLUSH_INTERVAL, [this]() {
            return !queue_.empty() || !running_;
        });
        
        // Собираем пакет данных
        while (!queue_.empty() && buffer.size() < BUFFER_SIZE) {
            buffer.push_back(queue_.front());
            queue_.pop();
        }
        
        // Записываем на диск
        if (!buffer.empty() && file_.is_open()) {
            for (const auto& entry : buffer) {
                file_ << entry.to_string() << "\n";
            }
            file_.flush();
            buffer.clear();
        }
        
        // Выход если остановка и очередь пуста
        if (!running_ && queue_.empty()) {
            break;
        }
    }
}

size_t AccessLogger::queue_size() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return queue_.size();
}

uint64_t AccessLogger::total_logged() const {
    return total_logged_.load();
}

} // namespace dbms
