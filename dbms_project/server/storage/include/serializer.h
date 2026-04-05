#include <vector>
#include <string>
#include <cstring>
#include <stdexcept>
#include "../shared/QueryPlan.h"

class RowSerializer {
public:
    // Сериализация: превращаем список значений в байты согласно схеме таблицы
    static std::vector<char> serialize(const std::vector<ColumnDef>& schema, 
                                       const std::vector<std::string>& target_columns,
                                       const std::vector<Value>& values) {
        std::vector<char> buffer;
        
        // 1. Создаем карту соответствия: имя колонки -> предоставленное значение
        // Это нужно, так как в INSERT колонки могут идти в любом порядке [cite: 42]
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
            // Если значение не передано, используем DEFAULT или NULL [cite: 42, 75]
            if (!found) {
                if (schema[i].default_value.has_value()) {
                    row_values[i] = *schema[i].default_value;
                } else {
                    row_values[i].is_null = true;
                }
            }

            // Проверка ограничения NOT NULL [cite: 19, 20]
            if (row_values[i].is_null && (schema[i].is_not_null || schema[i].is_indexed)) {
                throw std::runtime_error("Ошибка: колонка '" + schema[i].name + "' не может быть NULL");
            }
        }

        // 2. Записываем Null Bitmap (1 байт на колонку для простоты)
        for (const auto& v : row_values) {
            char null_flag = v.is_null ? 1 : 0;
            buffer.push_back(null_flag);
        }

        // 3. Записываем данные
        for (size_t i = 0; i < schema.size(); ++i) {
            if (row_values[i].is_null) continue;

            if (schema[i].type == DataType::INT) {
                int val = std::get<int>(row_values[i].data);
                append_bytes(buffer, val);
            } 
            else if (schema[i].type == DataType::STRING) {
                const std::string& str = std::get<std::string>(row_values[i].data);
                uint32_t len = static_cast<uint32_t>(str.size());
                append_bytes(buffer, len); // Пишем длину
                buffer.insert(buffer.end(), str.begin(), str.end()); // Пишем саму строку
            }
        }

        return buffer;
    }

private:
    // Вспомогательная функция для записи тривиальных типов (int, uint32_t и т.д.)
    template<typename T>
    static void append_bytes(std::vector<char>& buffer, T value) {
        char bytes[sizeof(T)];
        std::memcpy(bytes, &value, sizeof(T));
        buffer.insert(buffer.end(), bytes, bytes + sizeof(T));
    }
};