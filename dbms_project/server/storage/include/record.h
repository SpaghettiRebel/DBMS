#pragma once

#include <string>
#include <variant>
#include <vector>
#include <cstdint>

// Универсальное значение (int, string или NULL)
struct Value {
    bool is_null = false;
    std::variant<int, std::string> data;

    Value() : is_null(true) {}
    
    Value(int v) : is_null(false), data(v) {}
    
    Value(const std::string& v) : is_null(false), data(v) {}
    
    Value(const char* v) : is_null(false), data(std::string(v)) {}

    // Операторы сравнения
    bool operator==(const Value& other) const {
        if (is_null && other.is_null) return true;
        if (is_null || other.is_null) return false;
        return data == other.data;
    }

    bool operator!=(const Value& other) const {
        return !(*this == other);
    }

    bool operator<(const Value& other) const {
        if (is_null || other.is_null) return false;
        if (std::holds_alternative<int>(data) && std::holds_alternative<int>(other.data)) {
            return std::get<int>(data) < std::get<int>(other.data);
        }
        if (std::holds_alternative<std::string>(data) && std::holds_alternative<std::string>(other.data)) {
            return std::get<std::string>(data) < std::get<std::string>(other.data);
        }
        return false;
    }

    bool operator<=(const Value& other) const {
        return *this == other || *this < other;
    }

    bool operator>(const Value& other) const {
        return other < *this;
    }

    bool operator>=(const Value& other) const {
        return *this == other || *this > other;
    }

    // Получение значения как int
    int as_int() const {
        if (is_null) throw std::runtime_error("Value is null");
        if (std::holds_alternative<int>(data)) {
            return std::get<int>(data);
        }
        throw std::runtime_error("Value is not int");
    }

    // Получение значения как string
    const std::string& as_string() const {
        if (is_null) throw std::runtime_error("Value is null");
        if (std::holds_alternative<std::string>(data)) {
            return std::get<std::string>(data);
        }
        throw std::runtime_error("Value is not string");
    }

    // Сериализация в строку для вывода
    std::string to_string() const {
        if (is_null) return "NULL";
        if (std::holds_alternative<int>(data)) {
            return std::to_string(std::get<int>(data));
        }
        return "\"" + std::get<std::string>(data) + "\"";
    }
};

// Запись (строка таблицы)
struct Record {
    std::vector<Value> values;
    uint64_t record_id = 0;  // Уникальный идентификатор записи

    Record() = default;
    
    Record(const std::vector<Value>& vals) : values(vals) {}

    Value get(size_t index) const {
        if (index >= values.size()) {
            return Value();  // Return NULL for out of bounds
        }
        return values[index];
    }

    void set(size_t index, const Value& val) {
        if (index >= values.size()) {
            values.resize(index + 1);
        }
        values[index] = val;
    }

    size_t size() const {
        return values.size();
    }
};
