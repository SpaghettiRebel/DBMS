#pragma once
#include "table_metadata.h"
#include "../shared/QueryPlan.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <fstream>

// Используем типы из QueryPlan.h (они в глобальном namespace)
using ColumnDef = ::ColumnDef;
using Value = ::Value;
using DataType = ::DataType;

// Alias для схемы таблицы
using Schema = std::vector<ColumnDef>;

// Менеджер схем таблиц
class SchemaManager {
private:
    std::string root_path_;
    std::unordered_map<std::string, std::vector<ColumnDef>> schemas_;
    std::mutex mutex_;
    
    std::string get_schema_path(const std::string& db_name, const std::string& table_name);
    void save_schema(const std::string& db_name, const std::string& table_name, 
                     const std::vector<ColumnDef>& columns);
    void load_schema(const std::string& db_name, const std::string& table_name);

public:
    explicit SchemaManager(const std::string& root_path);
    
    // Создание схемы таблицы
    void create_table(const std::string& db_name, const std::string& table_name,
                      const std::vector<ColumnDef>& columns);
    
    // Получение схемы таблицы
    std::vector<ColumnDef> get_schema(const std::string& db_name, const std::string& table_name);
    
    // Проверка существования таблицы
    bool table_exists(const std::string& db_name, const std::string& table_name);
    
    // Удаление схемы
    void drop_table(const std::string& db_name, const std::string& table_name);
    
    // Валидация записи на соответствие схеме
    bool validate_record(const std::string& db_name, const std::string& table_name,
                         const std::vector<Value>& values, std::string& error_msg);
    
    // Проверка типа значения
    bool check_type(const Value& value, DataType expected_type);
    
    // Получение имен индексированных колонок
    std::vector<std::string> get_indexed_columns(const std::string& db_name, 
                                                  const std::string& table_name);
    
    // Загрузка всех схем при старте
    void load_all_schemas();
};
