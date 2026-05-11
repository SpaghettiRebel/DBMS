#pragma once

#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <cstdint>

namespace dbms {

// Типы агрегатных функций
enum class AggregateType {
    NONE,
    COUNT,
    SUM,
    AVG
};

// Структура для описания колонки в SELECT (с возможной агрегацией)
struct SelectColumn {
    std::string column_name;      // Имя колонки или "*"
    std::string alias;            // Алиас (AS name)
    AggregateType agg_type;       // Тип агрегации (если есть)
    
    SelectColumn() : agg_type(AggregateType::NONE) {}
};

// Результат выполнения агрегатной функции
struct AggregateResult {
    AggregateType type;
    std::variant<std::monostate, int64_t, double> value;
    uint64_t count_rows; // Для COUNT(*) или общего количества строк

    AggregateResult() : type(AggregateType::NONE), count_rows(0) {}
    
    // Сброс состояния для новой группы
    void reset(AggregateType t) {
        type = t;
        count_rows = 0;
        if (t == AggregateType::SUM || t == AggregateType::AVG) {
            value = 0.0; // Используем double для накопления
        } else if (t == AggregateType::COUNT) {
            value = static_cast<int64_t>(0);
        } else {
            value = std::monostate{};
        }
    }

    // Добавление значения
    void accumulate(const std::variant<std::monostate, int64_t, std::string>& val) {
        count_rows++;
        
        if (std::holds_alternative<std::monostate>(val)) {
            // NULL значение: для COUNT(*) считаем, для COUNT(col) - нет
            if (type == AggregateType::COUNT) {
                // Если это COUNT(*), мы уже увеличили count_rows в начале
                // Если COUNT(col), то NULL не считаем, уменьшаем обратно
                // Логика обработки NULL должна быть на уровне вызова
            }
            return; 
        }

        if (type == AggregateType::COUNT) {
            // COUNT игнорирует NULL, но сюда попадают только не-NULL
            if (std::holds_alternative<int64_t>(value)) {
                std::get<int64_t>(value)++;
            }
        } else if (type == AggregateType::SUM || type == AggregateType::AVG) {
            double num_val = 0.0;
            if (std::holds_alternative<int64_t>(val)) {
                num_val = static_cast<double>(std::get<int64_t>(val));
            } else if (std::holds_alternative<std::string>(val)) {
                // Попытка парсить строку как число? Или ошибка.
                // По ТЗ типы int и string. SUM от строк не определен, считаем 0 или игнорируем.
                try {
                    num_val = std::stod(std::get<std::string>(val));
                } catch (...) {
                    num_val = 0.0;
                }
            }
            
            if (std::holds_alternative<double>(value)) {
                std::get<double>(value) += num_val;
            } else {
                value = num_val;
            }
        }
    }

    // Получение финального результата
    std::variant<std::monostate, int64_t, double> get_final_value() const {
        if (type == AggregateType::COUNT) {
            return std::holds_alternative<int64_t>(value) ? std::get<int64_t>(value) : static_cast<int64_t>(count_rows);
        } else if (type == AggregateType::AVG) {
            if (count_rows == 0) return std::monostate{};
            if (std::holds_alternative<double>(value)) {
                return std::get<double>(value) / static_cast<double>(count_rows);
            }
            return std::monostate{};
        } else if (type == AggregateType::SUM) {
            return value;
        }
        return std::monostate{};
    }
};

} // namespace dbms

// Helper function to convert AggregateType to string
inline std::string aggregate_type_to_string(dbms::AggregateType type) {
    switch (type) {
        case dbms::AggregateType::SUM: return "SUM";
        case dbms::AggregateType::COUNT: return "COUNT";
        case dbms::AggregateType::AVG: return "AVG";
        default: return "NONE";
    }
}
