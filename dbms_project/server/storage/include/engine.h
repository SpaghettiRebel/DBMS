#pragma once
#include "../shared/QueryPlan.h"
#include "file_manager.h"
#include "bplus_tree.h"
#include "string_pool.h"
#include "journal.h"
#include "table_metadata.h"
#include "wal_manager.h"
#include "binary_file_manager.h"
#include "query_optimizer.h"
#include "schema_manager.h"
#include "aggregates.h"
#include <string>
#include <map>
#include <memory>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <filesystem>

namespace fs = std::filesystem;

// Используем типы из namespace dbms
using dbms::Value;
using dbms::Record;
using dbms::RID;
using dbms::AggregateResult;
using dbms::aggregate_type_to_string;

// Alias для схемы таблицы
using Schema = std::vector<ColumnDef>;

class Engine {
private:
    std::string root_path;
    std::string current_db;
    std::unique_ptr<StringPool> string_pool;
    std::unique_ptr<Journal> journal;
    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<SchemaManager> schema_manager;
    std::unique_ptr<QueryOptimizer> query_optimizer;
    
    // Менеджеры бинарных файлов для открытых таблиц
    std::unordered_map<std::string, std::unique_ptr<BinaryFileManager>> table_managers_;
    std::mutex table_mutex_;
    
    // Активные транзакции
    std::unordered_map<uint64_t, std::string> active_transactions_;
    std::mutex txn_mutex_;
    
    // Кэш B+-деревьев для индексированных колонок
    std::unordered_map<std::string, std::unique_ptr<BPlusTree<Value, RID>>> index_trees_;
    std::mutex index_mutex_;

    std::string get_table_path(const std::string& table_name);
    std::vector<ColumnDef> get_table_schema(const std::string& table_name);
    void update_table_header(const std::string& table_name, const TableHeader& header);
    
    // Вспомогательные методы для работы с WAL
    uint64_t start_transaction();
    void commit_transaction(uint64_t txn_id);
    void log_operation_insert(uint64_t txn_id, const std::string& table, 
                              const pos_t& pos, const std::vector<char>& data);
    void log_operation_update(uint64_t txn_id, const std::string& table,
                              const pos_t& pos, const std::vector<char>& old_data,
                              const std::vector<char>& new_data);
    void log_operation_delete(uint64_t txn_id, const std::string& table,
                              const pos_t& pos, const std::vector<char>& old_data);

    // Методы оптимизированного выполнения запросов
    std::vector<RID> execute_indexed_select(const std::string& table_name, 
                                            const ExecutionPlan& plan);
    std::vector<Record> execute_full_scan(const std::string& table_name, 
                                          const ConditionNode* where_clause);
    
    // Методы для работы с агрегатными функциями
    std::string select_with_aggregates(const fs::path& db_dir,
                                        const QueryPlan& plan,
                                        const Schema& schema,
                                        const TableHeader& h,
                                        const ExecutionPlan& exec_plan);
    
public:
    explicit Engine(std::string root);
    ~Engine();

    void execute(const QueryPlan& plan);

    void create_database(const std::string& name);
    void drop_database(const std::string& name);
    void use_database(const std::string& name);

    void create_table(const QueryPlan& plan);
    void drop_table(const std::string& table_name);

    void insert_record(const QueryPlan& plan);
    std::string select_records(const QueryPlan& plan);
    void update_records(const QueryPlan& plan);
    void delete_records(const QueryPlan& plan);

    void revert(const std::string& table_name, const std::string& timestamp);
    
    // Восстановление из WAL при старте
    void recover_from_wal();
};
