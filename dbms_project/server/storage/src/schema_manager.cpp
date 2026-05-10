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
        file.write(reinterpret_cast<const char*>(&col.not_null), sizeof(col.not_null));
        file.write(reinterpret_cast<const char*>(&col.indexed), sizeof(col.indexed));
        file.write(reinterpret_cast<const char*>(&col.has_default), sizeof(col.has_default));
        
        if (col.has_default) {
            // Сериализация значения по умолчанию
            file.write(reinterpret_cast<const char*>(&col.default_value.type), 
                      sizeof(col.default_value.type));
            if (col.default_value.type == DataType::INT) {
                int64_t val = col.default_value.as_int();
                file.write(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (col.default_value.type == DataType::STRING) {
                const std::string& str_val = col.default_value.as_string();
                uint32_t str_len = static_cast<uint32_t>(str_val.length());
                file.write(reinterpret_cast<const char*>(&str_len), sizeof(str_len));
                file.write(str_val.c_str(), str_len);
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
        file.read(reinterpret_cast<char*>(&col.not_null), sizeof(col.not_null));
        file.read(reinterpret_cast<char*>(&col.indexed), sizeof(col.indexed));
        file.read(reinterpret_cast<char*>(&col.has_default), sizeof(col.has_default));
        
        if (col.has_default) {
            DataType val_type;
            file.read(reinterpret_cast<char*>(&val_type), sizeof(val_type));
            
            if (val_type == DataType::INT) {
                int64_t val;
                file.read(reinterpret_cast<char*>(&val), sizeof(val));
                col.default_value = Value(val);
            } else if (val_type == DataType::STRING) {
                uint32_t str_len;
                file.read(reinterpret_cast<char*>(&str_len), sizeof(str_len));
                std::string str_val(str_len, '\0');
                file.read(&str_val[0], str_len);
                col.default_value = Value(str_val);
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
    if (value.type == DataType::NULL_TYPE) {
        return true; // NULL допустим для любого типа (проверка NOT_NULL отдельно)
    }
    return value.type == expected_type;
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
        if (col.not_null && val.type == DataType::NULL_TYPE) {
            error_msg = "Column '" + col.name + "' cannot be NULL";
            return false;
        }
        
        // Проверка типа
        if (val.type != DataType::NULL_TYPE && !check_type(val, col.type)) {
            error_msg = "Type mismatch for column '" + col.name + 
                        "'. Expected " + (col.type == DataType::INT ? "INT" : "STRING") +
                        ", got " + (val.type == DataType::INT ? "INT" : "STRING");
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
        if (col.indexed) {
            indexed.push_back(col.name);
        }
    }
    
    return indexed;
}

void SchemaManager::load_all_schemas() {
    // Рекурсивный обход директорий для поиска схем
    // Упрощенная реализация - загрузка по требованию
}
