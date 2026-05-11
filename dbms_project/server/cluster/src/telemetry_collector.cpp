#include "telemetry_collector.h"
#include <algorithm>

TelemetryCollector::TelemetryCollector()
    : total_requests_(0)
    , total_errors_(0)
{
}

int64_t TelemetryCollector::now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void TelemetryCollector::cleanup_old_data() {
    int64_t now = now_ms();
    
    // Очистка RPS данных (старше 10 минут)
    {
        std::lock_guard<std::mutex> lock(rps_mutex_);
        int64_t cutoff = now - RPS_WINDOW_MS;
        while (!request_timestamps_.empty() && request_timestamps_.front() < cutoff) {
            request_timestamps_.pop_front();
        }
    }
    
    // Очистка данных времени ответа (старше 10 секунд)
    {
        std::lock_guard<std::mutex> lock(time_mutex_);
        int64_t cutoff = now - RESPONSE_TIME_WINDOW_MS;
        while (!response_times_.empty() && response_times_.front().first < cutoff) {
            response_times_.pop_front();
        }
    }
    
    // Очистка данных ошибок (старше 1 минуты)
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        int64_t cutoff = now - ERROR_RATE_WINDOW_MS;
        while (!error_timestamps_.empty() && error_timestamps_.front() < cutoff) {
            error_timestamps_.pop_front();
        }
        while (!all_events_timestamps_.empty() && all_events_timestamps_.front() < cutoff) {
            all_events_timestamps_.pop_front();
        }
    }
}

void TelemetryCollector::record_request(int64_t execution_time_ms) {
    int64_t now = now_ms();
    
    {
        std::lock_guard<std::mutex> lock(rps_mutex_);
        request_timestamps_.push_back(now);
    }
    
    {
        std::lock_guard<std::mutex> lock(time_mutex_);
        response_times_.emplace_back(now, execution_time_ms);
    }
    
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        all_events_timestamps_.push_back(now);
    }
    
    total_requests_++;
    
    // Периодическая очистка
    cleanup_old_data();
}

void TelemetryCollector::record_error() {
    int64_t now = now_ms();
    
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        error_timestamps_.push_back(now);
        all_events_timestamps_.push_back(now);
    }
    
    total_errors_++;
    
    cleanup_old_data();
}

double TelemetryCollector::get_current_rps() const {
    // Текущий RPS - количество запросов за последнюю секунду
    int64_t now = now_ms();
    int64_t one_second_ago = now - 1000;
    
    std::lock_guard<std::mutex> lock(rps_mutex_);
    
    size_t count = 0;
    for (auto it = request_timestamps_.rbegin(); it != request_timestamps_.rend(); ++it) {
        if (*it >= one_second_ago) {
            count++;
        } else {
            break;
        }
    }
    
    return static_cast<double>(count);
}

double TelemetryCollector::get_avg_rps_10min() const {
    std::lock_guard<std::mutex> lock(rps_mutex_);
    
    if (request_timestamps_.empty()) {
        return 0.0;
    }
    
    int64_t now = now_ms();
    int64_t window_start = now - RPS_WINDOW_MS;
    
    size_t count = 0;
    for (const auto& ts : request_timestamps_) {
        if (ts >= window_start) {
            count++;
        }
    }
    
    // Средний RPS = количество запросов / 600 секунд (10 минут)
    return static_cast<double>(count) / 600.0;
}

double TelemetryCollector::get_max_rps_10min() const {
    std::lock_guard<std::mutex> lock(rps_mutex_);
    
    if (request_timestamps_.empty()) {
        return 0.0;
    }
    
    // Вычисление максимального RPS по скользящему окну в 1 секунду
    double max_rps = 0.0;
    
    for (size_t i = 0; i < request_timestamps_.size(); ++i) {
        int64_t window_end = request_timestamps_[i];
        int64_t window_start = window_end - 1000;
        
        size_t count = 0;
        for (size_t j = i; j < request_timestamps_.size(); ++j) {
            if (request_timestamps_[j] <= window_end) {
                count++;
            } else {
                break;
            }
        }
        
        max_rps = std::max(max_rps, static_cast<double>(count));
    }
    
    return max_rps;
}

double TelemetryCollector::get_avg_response_time_10s() const {
    std::lock_guard<std::mutex> lock(time_mutex_);
    
    if (response_times_.empty()) {
        return 0.0;
    }
    
    int64_t total_time = 0;
    for (const auto& pair : response_times_) {
        total_time += pair.second;
    }
    
    return static_cast<double>(total_time) / response_times_.size();
}

double TelemetryCollector::get_error_rate_1min() const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    
    if (all_events_timestamps_.empty()) {
        return 0.0;
    }
    
    int64_t now = now_ms();
    int64_t window_start = now - ERROR_RATE_WINDOW_MS;
    
    size_t total_events = 0;
    size_t error_count = 0;
    
    for (const auto& ts : all_events_timestamps_) {
        if (ts >= window_start) {
            total_events++;
        }
    }
    
    for (const auto& ts : error_timestamps_) {
        if (ts >= window_start) {
            error_count++;
        }
    }
    
    if (total_events == 0) {
        return 0.0;
    }
    
    return (static_cast<double>(error_count) / total_events) * 100.0;
}

TelemetryCollector::TelemetryStats TelemetryCollector::get_stats() const {
    TelemetryStats stats;
    stats.current_rps = get_current_rps();
    stats.avg_rps_10min = get_avg_rps_10min();
    stats.max_rps_10min = get_max_rps_10min();
    stats.avg_response_time_10s = get_avg_response_time_10s();
    stats.error_rate_1min = get_error_rate_1min();
    stats.total_requests = total_requests_.load();
    stats.total_errors = total_errors_.load();
    return stats;
}

void TelemetryCollector::reset() {
    {
        std::lock_guard<std::mutex> lock(rps_mutex_);
        request_timestamps_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(time_mutex_);
        response_times_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        error_timestamps_.clear();
        all_events_timestamps_.clear();
    }
    
    total_requests_.store(0);
    total_errors_.store(0);
}
