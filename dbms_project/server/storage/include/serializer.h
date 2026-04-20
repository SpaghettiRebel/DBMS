#include <cstring>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "../shared/QueryPlan.h"
#include "string_pool.h"

using json = nlohmann::json;

class RowSerializer {
public:
    // Сериализация: превращаем список значений в байты согласно схеме таблицы
    static std::vector<char> serialize(const std::vector<ColumnDef>& schema,
        const std::vector<std::string>& target_columns, const std::vector<Value>& values) {
        std::vector<char> buffer;

        std::vector<Value> row_values(schema.size());
        for (size_t i = 0; i < schema.size(); ++i) {
            bool found = false;
            for (size_t j = 0; j < target_columns.size(); ++j) {
                if (schema[i].name == target_columns[j]) {
                    row_values[i] = values[j];
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (schema[i].default_value.has_value()) {
                    row_values[i] = *schema[i].default_value;
                } else {
                    row_values[i].is_null = true;
                }
            }

            if (row_values[i].is_null && (schema[i].is_not_null || schema[i].is_indexed)) {
                throw std::runtime_error("Ошибка: колонка '" + schema[i].name + "' не может быть NULL");
            }
        }

        for (const auto& v : row_values) {
            char null_flag = v.is_null ? 1 : 0;
            buffer.push_back(null_flag);
        }

        for (size_t i = 0; i < schema.size(); ++i) {
            if (row_values[i].is_null) continue;

            if (schema[i].type == DataType::INT) {
                int val = std::get<int>(row_values[i].data);
                append_bytes(buffer, val);
            } else if (schema[i].type == DataType::STRING) {
                // If it's a string, it might be already interned (stored as int/uint32_t)
                if (std::holds_alternative<int>(row_values[i].data)) {
                    uint32_t id = static_cast<uint32_t>(std::get<int>(row_values[i].data));
                    append_bytes(buffer, id);
                } else {
                    const std::string& str = std::get<std::string>(row_values[i].data);
                    uint32_t len = static_cast<uint32_t>(str.size());
                    append_bytes(buffer, len);
                    buffer.insert(buffer.end(), str.begin(), str.end());
                }
            }
        }

        return buffer;
    }

private:
    template <typename T>
    static void append_bytes(std::vector<char>& buffer, T value) {
        char bytes[sizeof(T)];
        std::memcpy(bytes, &value, sizeof(T));
        buffer.insert(buffer.end(), bytes, bytes + sizeof(T));
    }
};

class RowDeserializer {
public:
    static json deserialize(
        const std::vector<ColumnDef>& schema, const char* buffer, size_t& offset, StringPool* pool) {
        json row;
        std::vector<bool> null_bitmap(schema.size());
        for (size_t i = 0; i < schema.size(); ++i) {
            null_bitmap[i] = (buffer[offset++] == 1);
        }

        for (size_t i = 0; i < schema.size(); ++i) {
            if (null_bitmap[i]) {
                row[schema[i].name] = nullptr;
                continue;
            }

            if (schema[i].type == DataType::INT) {
                int val;
                std::memcpy(&val, buffer + offset, sizeof(int));
                offset += sizeof(int);
                row[schema[i].name] = val;
            } else if (schema[i].type == DataType::STRING) {
                uint32_t id;
                std::memcpy(&id, buffer + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                row[schema[i].name] = pool->get(id);
            }
        }
        return row;
    }
};
