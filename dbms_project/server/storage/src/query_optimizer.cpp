#include "query_optimizer.h"
#include <algorithm>
#include <cmath>
#include <filesystem>

namespace fs = std::filesystem;

QueryOptimizer::QueryOptimizer(const std::string& db_root_path) 
    : db_root_(db_root_path) {}

bool QueryOptimizer::has_index(const std::string& table_name, const std::string& column_name) const {
    std::string index_file = get_index_path(table_name, column_name);
    return fs::exists(index_file);
}

std::string QueryOptimizer::get_index_path(const std::string& table_name, const std::string& column_name) const {
    // Формат: root/db_name/table_name.idx.column_name
    // Для упрощения предполагаем, что table_name уже содержит путь или мы в контексте БД
    size_t dot_pos = table_name.find('.');
    std::string db_name, tbl_name;
    
    if (dot_pos != std::string::npos) {
        db_name = table_name.substr(0, dot_pos);
        tbl_name = table_name.substr(dot_pos + 1);
    } else {
        // Если база не указана, используем текущую (упрощенно)
        tbl_name = table_name;
        // В реальном приложении нужно брать текущую БД из контекста
        db_name = "default"; 
    }

    return db_root_ + "/" + db_name + "/" + tbl_name + ".idx." + column_name;
}

ExecutionPlan QueryOptimizer::analyze(const std::string& table_name, 
                                      const ConditionNode* where_clause, 
                                      const Schema& schema) {
    ExecutionPlan plan;
    plan.strategy = ExecutionStrategy::FULL_TABLE_SCAN;
    plan.estimated_cost = schema.estimate_row_count(); // Стоимость полного сканирования

    if (!where_clause) {
        return plan;
    }

    std::vector<IndexCandidate> candidates;
    collect_candidates(where_clause, table_name, schema, candidates);

    if (candidates.empty()) {
        return plan;
    }

    // Выбираем лучший кандидат
    // Приоритет: EQUALS > RANGE (BETWEEN, >, <) > остальные
    auto best_it = std::max_element(candidates.begin(), candidates.end(), 
        [](const IndexCandidate& a, const IndexCandidate& b) {
            // Точечный поиск всегда лучше диапазона
            if (a.type == PredicateType::EQUALS && b.type != PredicateType::EQUALS) return false;
            if (a.type != PredicateType::EQUALS && b.type == PredicateType::EQUALS) return true;
            
            // Если тип одинаковый, выбираем более селективный (меньший % строк)
            return a.selectivity > b.selectivity;
        });

    const IndexCandidate& best = *best_it;

    // Проверяем, существует ли физический файл индекса
    if (!has_index(best.table_name, best.column_name)) {
        return plan; // Индекс декларирован, но файла нет (или ошибка)
    }

    // Формируем план использования индекса
    plan.index_path = get_index_path(best.table_name, best.column_name);
    plan.indexed_column = best.column_name;
    plan.predicate_type = best.type;
    plan.estimated_cost = std::log2(schema.estimate_row_count() + 1); // O(log N)

    if (best.type == PredicateType::EQUALS) {
        plan.strategy = ExecutionStrategy::INDEX_SEEK;
        plan.search_key = best.search_value;
    } else if (best.type == PredicateType::BETWEEN || 
               best.type == PredicateType::GREATER || 
               best.type == PredicateType::GREATER_EQ ||
               best.type == PredicateType::LESS || 
               best.type == PredicateType::LESS_EQ) {
        plan.strategy = ExecutionStrategy::INDEX_RANGE_SCAN;
        plan.range_start = best.lower_bound;
        plan.range_end = best.upper_bound;
        
        // Корректировка границ для строгих неравенств
        // (В реальной СУБД тут нужна логика "+epsilon" для чисел или "+1 символ" для строк)
        // Для простоты оставляем как есть, фильтрация дублей будет на этапе чтения записей
    }

    plan.used_indexes.push_back(best.column_name);
    return plan;
}

void QueryOptimizer::collect_candidates(const ConditionNode* node, 
                                        const std::string& table_name, 
                                        const Schema& schema, 
                                        std::vector<IndexCandidate>& candidates) {
    if (!node) return;

    // Рекурсивный обход дерева условий (поддержка AND/OR)
    if (node->type == ConditionNodeType::AND || node->type == ConditionNodeType::OR) {
        if (node->left) collect_candidates(node->left, table_name, schema, candidates);
        if (node->right) collect_candidates(node->right, table_name, schema, candidates);
        return;
    }

    // Обрабатываем листовой предикат
    if (node->type == ConditionNodeType::PREDICATE) {
        std::string col_name = node->left_operand;
        
        // Проверяем, является ли колонка индексированной по схеме
        const ColumnDef* col_def = schema.get_column(col_name);
        if (!col_def || !col_def->is_indexed) {
            return; // Колонка не индексирована
        }

        IndexCandidate cand;
        cand.column_name = col_name;
        cand.table_name = table_name;
        cand.type = PredicateType::NONE;

        // Анализ оператора сравнения
        switch (node->op) {
            case CompareOp::EQ:
                cand.type = PredicateType::EQUALS;
                cand.search_value = node->right_operand;
                break;
            case CompareOp::GT:
                cand.type = PredicateType::GREATER;
                cand.lower_bound = node->right_operand;
                // Верхняя граница - макс возможное значение типа (упрощено)
                cand.upper_bound = Value::max_for_type(col_def->type); 
                break;
            case CompareOp::GE:
                cand.type = PredicateType::GREATER_EQ;
                cand.lower_bound = node->right_operand;
                cand.upper_bound = Value::max_for_type(col_def->type);
                break;
            case CompareOp::LT:
                cand.type = PredicateType::LESS;
                // Нижняя граница - мин возможное
                cand.lower_bound = Value::min_for_type(col_def->type);
                cand.upper_bound = node->right_operand;
                break;
            case CompareOp::LE:
                cand.type = PredicateType::LESS_EQ;
                cand.lower_bound = Value::min_for_type(col_def->type);
                cand.upper_bound = node->right_operand;
                break;
            case CompareOp::BETWEEN:
                cand.type = PredicateType::BETWEEN;
                // right_operand должен быть парой значений или обработан парсером заранее
                // Предполагаем, что парсер разбил BETWEEN на структуру с двумя значениями
                if (node->right_operand.is_pair()) {
                    cand.lower_bound = node->right_operand.get_lower();
                    cand.upper_bound = node->right_operand.get_upper();
                }
                break;
            default:
                // !=, LIKE и другие пока не оптимизируем через индекс (или оптимизируем сложно)
                return;
        }

        if (cand.type != PredicateType::NONE) {
            cand.selectivity = estimate_selectivity(cand, schema);
            candidates.push_back(cand);
        }
    }
}

double QueryOptimizer::estimate_selectivity(const IndexCandidate& cand, const Schema& schema) {
    // Эвристика оценки селективности
    // В реальной СУБД здесь используется гистограмма распределения данных
    
    switch (cand.type) {
        case PredicateType::EQUALS:
            // Для уникальных полей (INDEXED часто уникальны) селективность ~ 1/N
            // Для обычных ~ 1/10 (допущение)
            if (schema.get_column(cand.column_name)->is_unique) {
                return 1.0 / std::max(1.0, (double)schema.estimate_row_count());
            }
            return 0.1; 

        case PredicateType::BETWEEN:
            // Допустим, диапазон покрывает 10-30% данных в среднем
            return 0.2;

        case PredicateType::GREATER:
        case PredicateType::LESS:
        case PredicateType::GREATER_EQ:
        case PredicateType::LESS_EQ:
            // Половина таблицы в среднем
            return 0.5;

        default:
            return 1.0;
    }
}
