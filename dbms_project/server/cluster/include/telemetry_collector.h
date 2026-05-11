#ifndef TELEMETRY_COLLECTOR_H
#define TELEMETRY_COLLECTOR_H

#include <string>
#include <vector>
#include <deque>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <chrono>

/**
 * @brief Сборщик телеметрии производительности
 * 
 * Вычисляет метрики в реальном времени:
 * - Текущий RPS (запросов в секунду)
 * - Средний и максимальный RPS за 10 минут
 * - Среднее время обработки за 10 секунд
 * - Количество ошибок (Error Rate) за последнюю минуту
 */
class TelemetryCollector {
public:
    /**
     * @brief Конструктор сборщика
     */
    TelemetryCollector();
    
    ~TelemetryCollector() = default;
    
    /**
     * @brief Регистрация успешного запроса
     * @param execution_time_ms Время выполнения (мс)
     */
    void record_request(int64_t execution_time_ms);
    
    /**
     * @brief Регистрация ошибки
     */
    void record_error();
    
    /**
     * @brief Получение текущего RPS
     */
    double get_current_rps() const;
    
    /**
     * @brief Получение среднего RPS за 10 минут
     */
    double get_avg_rps_10min() const;
    
    /**
     * @brief Получение максимального RPS за 10 минут
     */
    double get_max_rps_10min() const;
    
    /**
     * @brief Получение среднего времени обработки за 10 секунд
     */
    double get_avg_response_time_10s() const;
    
    /**
     * @brief Получение Error Rate за последнюю минуту
     * @return Процент ошибок (0.0 - 100.0)
     */
    double get_error_rate_1min() const;
    
    /**
     * @brief Получение полной статистики
     */
    struct TelemetryStats {
        double current_rps;
        double avg_rps_10min;
        double max_rps_10min;
        double avg_response_time_10s;
        double error_rate_1min;
        uint64_t total_requests;
        uint64_t total_errors;
    };
    
    TelemetryStats get_stats() const;
    
    /**
     * @brief Сброс статистики
     */
    void reset();

private:
    /**
     * @brief Получение текущего времени в мс
     */
    static int64_t now_ms();
    
    /**
     * @brief Очистка старых данных
     */
    void cleanup_old_data();
    
    // Данные для вычисления RPS (временные метки запросов)
    mutable std::mutex rps_mutex_;
    std::deque<int64_t> request_timestamps_;
    
    // Данные для вычисления времени ответа
    mutable std::mutex time_mutex_;
    std::deque<std::pair<int64_t, int64_t>> response_times_; // (timestamp, time_ms)
    
    // Данные для вычисления Error Rate
    mutable std::mutex error_mutex_;
    std::deque<int64_t> error_timestamps_;
    std::deque<int64_t> all_events_timestamps_; // все события для знаменателя
    
    // Агрегированная статистика
    std::atomic<uint64_t> total_requests_;
    std::atomic<uint64_t> total_errors_;
    
    // Константы
    static constexpr int64_t RPS_WINDOW_MS = 10 * 60 * 1000;  // 10 минут
    static constexpr int64_t RESPONSE_TIME_WINDOW_MS = 10 * 1000;  // 10 секунд
    static constexpr int64_t ERROR_RATE_WINDOW_MS = 60 * 1000;  // 1 минута
};

#endif // TELEMETRY_COLLECTOR_H
