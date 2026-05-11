#pragma once

#include <vector>
#include <string>
#include <memory>
#include <variant>
#include "bplus_tree.h"
#include "../shared/QueryPlan.h"

// Используем типы из QueryPlan.h (они в глобальном namespace)
using Value = ::Value;
using ConditionNode = ::ConditionNode;
using Schema = std::vector<::ColumnDef>;

// Типы предикатов, которые мы умеем оптимизировать
enum class PredicateType {
    NONE,
    EQUALS,          // col == val
    GREATER,         // col > val
    GREATER_EQ,      // col >= val
    LESS,            // col < val
    LESS_EQ,         // col <= val
    BETWEEN,         // col BETWEEN v1 AND v2
    NOT_EQUALS       // col != val (часто менее эффективен для индексов)
};

// Структура, описывающая найденный кандидат на использование индекса
struct IndexCandidate {
    std::string column_name;
    std::string table_name;
    PredicateType type;
    Value search_value;      // Для EQUALS
    Value lower_bound;       // Для RANGE/BETWEEN
    Value upper_bound;       // Для RANGE/BETWEEN
    double selectivity;      // Оценка селективности (0.0 - 1.0)
};

// Стратегии выполнения запроса
enum class ExecutionStrategy {
    FULL_TABLE_SCAN,   // Сканировать всю таблицу
    INDEX_SEEK,        // Точечный поиск по индексу (O(log N))
    INDEX_RANGE_SCAN   // Сканирование диапазона по индексу
};

// План выполнения запроса
struct ExecutionPlan {
    ExecutionStrategy strategy = ExecutionStrategy::FULL_TABLE_SCAN;
    
    // Данные для индексного поиска
    std::string index_path;
    std::string indexed_column;
    PredicateType predicate_type = PredicateType::NONE;
    
    Value search_key;
    Value range_start;
    Value range_end;

    // Статистика
    double estimated_cost = 0.0;
    std::vector<std::string> used_indexes;
};

// Класс оптимизатора запросов
class QueryOptimizer {
public:
    QueryOptimizer(const std::string& db_root_path);

    // Анализ условия WHERE и выбор лучшего плана
    ExecutionPlan analyze(const std::string& table_name, 
                          const ConditionNode* where_clause, 
                          const Schema& schema);

    // Проверка наличия индекса для колонки
    bool has_index(const std::string& table_name, const std::string& column_name) const;

    // Получение пути к файлу индекса
    std::string get_index_path(const std::string& table_name, const std::string& column_name) const;

private:
    std::string db_root_;

    // Сбор всех возможных кандидатов на использование индекса из дерева условий
    void collect_candidates(const ConditionNode* node, 
                            const std::string& table_name, 
                            const Schema& schema, 
                            std::vector<IndexCandidate>& candidates);

    // Оценка селективности предиката (эвристика)
    double estimate_selectivity(const IndexCandidate& cand, const Schema& schema);

    // Выбор лучшей стратегии из кандидатов
    ExecutionPlan choose_best_plan(const std::vector<IndexCandidate>& candidates, 
                                   const Schema& schema);
};
