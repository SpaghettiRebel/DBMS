#include "schema_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <sys/stat.h>

SchemaManager::SchemaManager(const std::string& root_path) 
    : root_path_(root_path) {
    load_all_schemas();
}

std::string SchemaManager::get_schema_path(const std::string& db_name, 
                                           const std::string& table_name) {
    return root_path_ + "/" + db_name + "/." + table_name + ".schema";
}

void SchemaManager::save_schema(const std::string& db_name, const std::string& table_name,
                                const std::vector<ColumnDef>& columns) {
    std::string path = get_schema_path(db_name, table_name);
    
    // Создаем директорию если не существует
    std::string dir_path = root_path_ + "/" + db_name;
    mkdir(dir_path.c_str(), 0755);
    
    std::ofstream file(path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot create schema file: " + path);
    }
    
    // Запись количества колонок
    uint32_t count = static_cast<uint32_t>(columns.size());
    file.write(reinterpret_cast<const char*>(&count), sizeof(count));
    
    // Запись каждой колонки
    for (const auto& col : columns) {
        uint8_t name_len = static_cast<uint8_t>(col.name.length());
        file.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
        file.write(col.name.c_str(), name_len);
        
        file.write(reinterpret_cast<const char*>(&col.type), sizeof(col.type));
        file.write(reinterpret_cast<const char*>(&col.is_not_null), sizeof(col.is_not_null));
        file.write(reinterpret_cast<const char*>(&col.is_indexed), sizeof(col.is_indexed));
        
        // Запись флага наличия default_value
        bool has_default = col.default_value.has_value();
        file.write(reinterpret_cast<const char*>(&has_default), sizeof(has_default));
        
        if (has_default) {
            // Сериализация значения по умолчанию
            const Value& def_val = col.default_value.value();
            bool is_null = def_val.is_null;
            file.write(reinterpret_cast<const char*>(&is_null), sizeof(is_null));
            if (!is_null) {
                if (std::holds_alternative<int>(def_val.data)) {
                    int64_t val = std::get<int>(def_val.data);
                    file.write(reinterpret_cast<const char*>(&val), sizeof(val));
                } else if (std::holds_alternative<std::string>(def_val.data)) {
                    const std::string& str_val = std::get<std::string>(def_val.data);
                    uint32_t str_len = static_cast<uint32_t>(str_val.length());
                    file.write(reinterpret_cast<const char*>(&str_len), sizeof(str_len));
                    file.write(str_val.c_str(), str_len);
                }
            }
        }
    }
    
    file.close();
}

void SchemaManager::load_schema(const std::string& db_name, const std::string& table_name) {
    std::string path = get_schema_path(db_name, table_name);
    std::ifstream file(path, std::ios::binary);
    
    if (!file.is_open()) {
        return; // Схема не найдена
    }
    
    std::vector<ColumnDef> columns;
    
    // Чтение количества колонок
    uint32_t count;
    file.read(reinterpret_cast<char*>(&count), sizeof(count));
    
    // Чтение каждой колонки
    for (uint32_t i = 0; i < count; ++i) {
        ColumnDef col;
        
        uint8_t name_len;
        file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
        std::string name(name_len, '\0');
        file.read(&name[0], name_len);
        col.name = name;
        
        file.read(reinterpret_cast<char*>(&col.type), sizeof(col.type));
        file.read(reinterpret_cast<char*>(&col.is_not_null), sizeof(col.is_not_null));
        file.read(reinterpret_cast<char*>(&col.is_indexed), sizeof(col.is_indexed));
        
        bool has_default;
        file.read(reinterpret_cast<char*>(&has_default), sizeof(has_default));
        
        if (has_default) {
            bool is_null;
            file.read(reinterpret_cast<char*>(&is_null), sizeof(is_null));
            if (!is_null) {
                if (std::holds_alternative<int>(col.default_value.emplace().data)) {
                    int64_t val;
                    file.read(reinterpret_cast<char*>(&val), sizeof(val));
                    col.default_value->data = static_cast<int>(val);
                } else {
                    col.default_value.emplace();
                    col.default_value->is_null = false;
                    uint32_t str_len;
                    file.read(reinterpret_cast<char*>(&str_len), sizeof(str_len));
                    std::string str_val(str_len, '\0');
                    file.read(&str_val[0], str_len);
                    col.default_value->data = str_val;
                }
            } else {
                col.default_value.emplace();
                col.default_value->is_null = true;
            }
        }
        
        columns.push_back(col);
    }
    
    file.close();
    
    std::string key = db_name + "." + table_name;
    schemas_[key] = columns;
}

void SchemaManager::create_table(const std::string& db_name, const std::string& table_name,
                                 const std::vector<ColumnDef>& columns) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string key = db_name + "." + table_name;
    if (schemas_.find(key) != schemas_.end()) {
        throw std::runtime_error("Table already exists: " + table_name);
    }
    
    save_schema(db_name, table_name, columns);
    schemas_[key] = columns;
}

std::vector<ColumnDef> SchemaManager::get_schema(const std::string& db_name, 
                                                  const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string key = db_name + "." + table_name;
    auto it = schemas_.find(key);
    if (it != schemas_.end()) {
        return it->second;
    }
    
    // Попытка загрузить с диска
    load_schema(db_name, table_name);
    it = schemas_.find(key);
    if (it != schemas_.end()) {
        return it->second;
    }
    
    throw std::runtime_error("Table not found: " + table_name);
}

bool SchemaManager::table_exists(const std::string& db_name, const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string key = db_name + "." + table_name;
    if (schemas_.find(key) != schemas_.end()) {
        return true;
    }
    
    // Проверка на диске
    std::string path = get_schema_path(db_name, table_name);
    std::ifstream file(path);
    return file.good();
}

void SchemaManager::drop_table(const std::string& db_name, const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string key = db_name + "." + table_name;
    schemas_.erase(key);
    
    std::string path = get_schema_path(db_name, table_name);
    remove(path.c_str());
}

bool SchemaManager::check_type(const Value& value, DataType expected_type) {
    if (value.is_null) {
        return true; // NULL допустим для любого типа (проверка NOT_NULL отдельно)
    }
    
    // Проверка типа на основе variant
    if (expected_type == DataType::INT) {
        return std::holds_alternative<int>(value.data);
    } else if (expected_type == DataType::STRING) {
        return std::holds_alternative<std::string>(value.data);
    }
    return false;
}

bool SchemaManager::validate_record(const std::string& db_name, const std::string& table_name,
                                    const std::vector<Value>& values, std::string& error_msg) {
    auto schema = get_schema(db_name, table_name);
    
    if (values.size() != schema.size()) {
        error_msg = "Column count mismatch. Expected " + 
                    std::to_string(schema.size()) + 
                    ", got " + std::to_string(values.size());
        return false;
    }
    
    for (size_t i = 0; i < schema.size(); ++i) {
        const auto& col = schema[i];
        const auto& val = values[i];
        
        // Проверка NOT_NULL
        if (col.is_not_null && val.is_null) {
            error_msg = "Column '" + col.name + "' cannot be NULL";
            return false;
        }
        
        // Проверка типа
        if (!val.is_null && !check_type(val, col.type)) {
            std::string actual_type = val.is_null ? "NULL" : 
                                     (std::holds_alternative<int>(val.data) ? "INT" : "STRING");
            std::string expected_type_str = col.type == DataType::INT ? "INT" : "STRING";
            error_msg = "Type mismatch for column '" + col.name + 
                        "'. Expected " + expected_type_str +
                        ", got " + actual_type;
            return false;
        }
    }
    
    return true;
}

std::vector<std::string> SchemaManager::get_indexed_columns(const std::string& db_name, 
                                                            const std::string& table_name) {
    auto schema = get_schema(db_name, table_name);
    std::vector<std::string> indexed;
    
    for (const auto& col : schema) {
        if (col.is_indexed) {
            indexed.push_back(col.name);
        }
    }
    
    return indexed;
}

void SchemaManager::load_all_schemas() {
    // Рекурсивный обход директорий для поиска схем
    // Упрощенная реализация - загрузка по требованию
}
