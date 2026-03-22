#pragma once
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

// Типы запросов, поддерживаемые СУБД [cite: 30, 36, 39]
enum class QueryType {
    CREATE_DATABASE,
    DROP_DATABASE,
    USE_DATABASE,
    CREATE_TABLE,
    DROP_TABLE,
    INSERT,
    UPDATE,
    DELETE,
    SELECT,
    REVERT  // Для задания 1 (темпоральность) [cite: 54]
};

// Поддерживаемые типы данных
enum class DataType { INT, STRING };

// Операторы сравнения для WHERE [cite: 47, 49, 50]
enum class OpType { EQ, NEQ, LESS, GREATER, LEQ, GEQ, BETWEEN, LIKE };

// Логические операторы (Задание 11) [cite: 79]
enum class LogicalOpType { NONE, AND, OR };

// Агрегатные функции (Задание 12) [cite: 82]
enum class AggregateType { NONE, SUM, COUNT, AVG };

// Универсальное значение (int, string или NULL)
struct Value {
    bool is_null = false;
    std::variant<int, std::string> data;
};

// Описание колонки для CREATE TABLE [cite: 37]
struct ColumnDef {
    std::string name;
    DataType type;
    bool is_not_null = false;            // [cite: 19]
    bool is_indexed = false;             // [cite: 20]
    std::optional<Value> default_value;  // Для задания 10 [cite: 75]
};

// Цель для SELECT (например: SUM(price) AS total) [cite: 44, 45, 82]
struct SelectTarget {
    std::string column_name;
    AggregateType aggregate = AggregateType::NONE;
    std::string alias;
};

struct ConditionNode {
    // Если это листовой узел (конкретное сравнение, например id == 5)
    bool is_leaf = true;

    // Для листа
    std::string left_column;
    OpType op;
    Value right_value;
    std::optional<Value> right_value_between;  // Третье значение для BETWEEN [cite: 49]

    // Для внутренних узлов (AND / OR)
    LogicalOpType logical_op = LogicalOpType::NONE;
    std::unique_ptr<ConditionNode> left_child;
    std::unique_ptr<ConditionNode> right_child;
};

struct QueryPlan {
    QueryType type;
    std::string database_name;
    std::string table_name;

    // --- Для DDL (CREATE TABLE) ---
    std::vector<ColumnDef> columns;

    // --- Для DML (INSERT / UPDATE) ---
    // Список колонок, в которые идет вставка/обновление [cite: 42, 43]
    std::vector<std::string> target_columns;
    // Значения для вставки (или новые значения для UPDATE) [cite: 42, 43]
    std::vector<Value> values;

    // --- Для DML (SELECT) ---
    std::vector<SelectTarget> select_targets;  // Выбираемые колонки [cite: 44]

    // --- Для условий (WHERE) ---
    std::unique_ptr<ConditionNode> where_clause;  // Корень дерева условий [cite: 43, 44]

    // --- Для REVERT ---
    std::string timestamp;  // [cite: 54]
};