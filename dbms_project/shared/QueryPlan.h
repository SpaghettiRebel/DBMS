#pragma once
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#ifdef DELETE
#undef DELETE
#endif
// все sql команды которые поддерживает СУБД
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
    REVERT
};
// типы данных, операторы сравнения, лигические связки, агрегаты
enum class DataType { INT, STRING };

enum class OpType { EQ, NEQ, LESS, GREATER, LEQ, GEQ, BETWEEN, LIKE };

enum class LogicalOpType { NONE, AND, OR };

enum class AggregateType {
    NONE,
    SUM,
    MIN,
    MAX,
    AVG,
    COUNT
};
// хранение данных
struct Value {
    bool is_null = false;
    std::variant<int, std::string> data;
};
// хранит имя колонки, её тип и модификаторы
struct ColumnDef {
    std::string name;
    DataType type;
    bool is_not_null = false;
    bool is_indexed = false;
    std::optional<Value> default_value;
    bool is_unique = false;
    bool is_autoincrement = false;
};
// описывает, что именно мы хотим достать в SELECT
struct SelectTarget {
    std::string column_name;
    AggregateType aggregate = AggregateType::NONE;
    std::string alias;
};
// сложное сравнение WHERE id == 10 AND age > 18
struct ConditionNode {
    bool is_leaf = true;

    std::string left_column;
    OpType op;
    Value right_value;
    std::optional<Value> right_value_between;
    std::string right_column;
    bool is_right_column = false;

    LogicalOpType logical_op = LogicalOpType::NONE;
    std::unique_ptr<ConditionNode> left_child;
    std::unique_ptr<ConditionNode> right_child;
};
// главная анкета
struct QueryPlan {
    QueryType type;
    std::string database_name;
    std::string table_name;

    std::vector<ColumnDef> columns;
    // данные для операций вставки и обновления (INSERT / UPDATE)
    std::vector<std::string> target_columns;
    std::vector<Value> values;
    std::vector<std::vector<Value>> value_rows;
    // данные для операции выборки (SELECT)
    std::vector<SelectTarget> select_targets;
    std::string group_by_column;
    std::string order_by_column;
    bool order_descending = false;

    std::unique_ptr<ConditionNode> where_clause;

    std::string timestamp;
};
