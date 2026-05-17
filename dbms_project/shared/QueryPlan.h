#pragma once
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#ifdef DELETE
#undef DELETE
#endif

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

struct Value {
    bool is_null = false;
    std::variant<int, std::string> data;
};

struct ColumnDef {
    std::string name;
    DataType type;
    bool is_not_null = false;
    bool is_indexed = false;
    std::optional<Value> default_value;
    bool is_unique = false;
    bool is_autoincrement = false;
};

struct SelectTarget {
    std::string column_name;
    AggregateType aggregate = AggregateType::NONE;
    std::string alias;
};

struct ConditionNode {
    bool is_leaf = true;

    std::string left_column;
    OpType op;
    Value right_value;
    std::optional<Value> right_value_between;
    std::string right_column;  // For column-to-column comparisons (empty if right_value is used)
    bool is_right_column = false;  // true if comparing two columns, false if comparing column to literal

    LogicalOpType logical_op = LogicalOpType::NONE;
    std::unique_ptr<ConditionNode> left_child;
    std::unique_ptr<ConditionNode> right_child;
};

struct QueryPlan {
    QueryType type;
    std::string database_name;
    std::string table_name;

    std::vector<ColumnDef> columns;

    std::vector<std::string> target_columns;
    std::vector<Value> values;
    std::vector<std::vector<Value>> value_rows;

    std::vector<SelectTarget> select_targets;
    std::string group_by_column;
    std::string order_by_column;
    bool order_descending = false;

    std::unique_ptr<ConditionNode> where_clause;

    std::string timestamp;
};
