#pragma once
#include "../shared/QueryPlan.h"
#include "file_manager.h"
#include "bplus_tree.h"
#include "string_pool.h"
#include "journal.h"
#include "table_metadata.h"
#include <string>
#include <map>
#include <memory>
#include <vector>

class Engine {
private:
    std::string root_path;
    std::string current_db;
    std::unique_ptr<StringPool> string_pool;
    std::unique_ptr<Journal> journal;

    std::string get_table_path(const std::string& table_name);
    std::vector<ColumnDef> get_table_schema(const std::string& table_name);
    void update_table_header(const std::string& table_name, const TableHeader& header);

public:
    explicit Engine(std::string root);

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
};
