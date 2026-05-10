#include "async_request_queue.h"
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <cctype>

std::string generate_guid_v4() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    static std::uniform_int_distribution<> dis2(8, 11);
    
    std::stringstream ss;
    ss << std::hex;
    
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            ss << '-';
        }
        if (i == 6) {
            ss << dis2(gen);
        } else if (i == 8) {
            ss << (dis(gen) & 0x3) | 0x8;
        } else {
            ss << dis(gen);
        }
    }
    
    return ss.str();
}

AsyncRequestQueue::AsyncRequestQueue(QueryExecutor executor, size_t num_workers)
    : executor_(executor)
    , num_workers_(num_workers)
    , running_(false)
    , total_submitted_(0)
    , total_completed_(0)
    , total_failed_(0)
    , total_execution_time_ms_(0)
{
}

AsyncRequestQueue::~AsyncRequestQueue() {
    stop(true);
}

void AsyncRequestQueue::start() {
    if (running_.load()) {
        return;
    }
    
    running_.store(true);
    
    for (size_t i = 0; i < num_workers_; ++i) {
        workers_.emplace_back(&AsyncRequestQueue::worker_thread, this);
    }
}

void AsyncRequestQueue::stop(bool wait_for_completion) {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    // Уведомление всех ожидающих потоков
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        // Потоки проснутся и увидят running_ = false
    }
    
    if (wait_for_completion) {
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    } else {
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.detach();
            }
        }
    }
    
    workers_.clear();
}

std::string AsyncRequestQueue::enqueue(const std::string& query,
                                        const std::string& client_id,
                                        bool is_long_running) {
    std::string guid = generate_guid_v4();
    
    QueuedRequest request;
    request.request_guid = guid;
    request.query = query;
    request.client_id = client_id;
    request.submitted_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    request.is_long_running = is_long_running || detect_long_running_query(query);
    
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        request_queue_.push(request);
    }
    
    total_submitted_++;
    
    // Инициализация результата как PENDING
    {
        std::lock_guard<std::mutex> lock(results_mutex_);
        AsyncRequestResult result;
        result.request_guid = guid;
        result.status = AsyncRequestStatus::PENDING;
        result.created_at_ms = request.submitted_at_ms;
        results_[guid] = result;
    }
    
    return guid;
}

AsyncRequestResult AsyncRequestQueue::get_status(const std::string& request_guid) {
    std::lock_guard<std::mutex> lock(results_mutex_);
    
    auto it = results_.find(request_guid);
    if (it != results_.end()) {
        return it->second;
    }
    
    // Результат не найден
    AsyncRequestResult not_found;
    not_found.request_guid = request_guid;
    not_found.status = AsyncRequestStatus::FAILED;
    not_found.error_message = "Request not found";
    return not_found;
}

bool AsyncRequestQueue::cancel(const std::string& request_guid) {
    std::lock_guard<std::mutex> lock(results_mutex_);
    
    auto it = results_.find(request_guid);
    if (it != results_.end() && it->second.status == AsyncRequestStatus::PENDING) {
        it->second.status = AsyncRequestStatus::FAILED;
        it->second.error_message = "Cancelled by user";
        return true;
    }
    
    return false;
}

size_t AsyncRequestQueue::queue_size() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return request_queue_.size();
}

AsyncRequestQueue::QueueStats AsyncRequestQueue::get_stats() const {
    QueueStats stats;
    stats.total_submitted = total_submitted_.load();
    stats.total_completed = total_completed_.load();
    stats.total_failed = total_failed_.load();
    
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stats.current_queue_size = request_queue_.size();
    }
    
    uint64_t completed = total_completed_.load();
    if (completed > 0) {
        stats.avg_execution_time_ms = static_cast<double>(total_execution_time_ms_.load()) / completed;
    } else {
        stats.avg_execution_time_ms = 0.0;
    }
    
    return stats;
}

void AsyncRequestQueue::worker_thread() {
    while (running_.load()) {
        QueuedRequest request;
        
        // Извлечение запроса из очереди
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            // Ожидание появления запроса или остановки
            while (request_queue_.empty() && running_.load()) {
                lock.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                lock.lock();
            }
            
            if (!running_.load() && request_queue_.empty()) {
                break;
            }
            
            if (!request_queue_.empty()) {
                request = request_queue_.front();
                request_queue_.pop();
            }
        }
        
        if (request.request_guid.empty()) {
            continue;
        }
        
        // Обновление статуса на RUNNING
        {
            std::lock_guard<std::mutex> lock(results_mutex_);
            auto it = results_.find(request.request_guid);
            if (it != results_.end()) {
                it->second.status = AsyncRequestStatus::RUNNING;
            }
        }
        
        // Выполнение запроса
        auto start_time = std::chrono::steady_clock::now();
        
        try {
            std::string result = executor_(request.query);
            
            auto end_time = std::chrono::steady_clock::now();
            int64_t execution_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time).count();
            
            {
                std::lock_guard<std::mutex> lock(results_mutex_);
                auto it = results_.find(request.request_guid);
                if (it != results_.end()) {
                    it->second.status = AsyncRequestStatus::COMPLETED;
                    it->second.result_data = result;
                    it->second.completed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time.time_since_epoch()).count();
                }
            }
            
            total_completed_++;
            total_execution_time_ms_ += execution_time;
            
        } catch (const std::exception& e) {
            auto end_time = std::chrono::steady_clock::now();
            
            {
                std::lock_guard<std::mutex> lock(results_mutex_);
                auto it = results_.find(request.request_guid);
                if (it != results_.end()) {
                    it->second.status = AsyncRequestStatus::FAILED;
                    it->second.error_message = e.what();
                    it->second.completed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time.time_since_epoch()).count();
                }
            }
            
            total_failed_++;
        }
    }
}

bool AsyncRequestQueue::detect_long_running_query(const std::string& query) {
    // Простая эвристика: запросы с SELECT * без WHERE или с LIKE могут быть долгими
    std::string upper_query;
    upper_query.reserve(query.size());
    for (char c : query) {
        upper_query += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    
    // Проверка на потенциально долгие операции
    if (upper_query.find("SELECT") != std::string::npos) {
        // SELECT без WHERE может быть долгим
        if (upper_query.find("WHERE") == std::string::npos) {
            return true;
        }
        // SELECT с LIKE может использовать полный скан
        if (upper_query.find("LIKE") != std::string::npos) {
            return true;
        }
    }
    
    return false;
}
