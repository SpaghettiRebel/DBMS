#pragma once
#include "../shared/QueryPlan.h"
#include "file_manager.h"
#include "bplus_tree.cpp" // Предположим реализацию здесь
#include <string>
#include <map>
#include <memory>

class Engine {
private:
    std::string root_path; // Путь к "Уровню систем" [cite: 9]
    std::string current_db; // Активная база данных [cite: 35]

    // Вспомогательные методы
    std::string get_table_path(const std::string& table_name);
    void validate_insert(const std::string& table_name, const QueryPlan& plan);

public:
    explicit Engine(std::string root);
    
    // Главный входной узел для выполнения команд
    void execute(const QueryPlan& plan);

    // DDL операции [cite: 36]
    void create_database(const std::string& name);
    void create_table(const QueryPlan& plan);

    // DML операции [cite: 39]
    void insert_record(const QueryPlan& plan);
    std::string select_records(const QueryPlan& plan); // Возвращает JSON строку [cite: 24]
};