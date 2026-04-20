#include "../include/engine.h"

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>
#include <regex>
#include <sstream>
#include <stdexcept>

#include "../include/serializer.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

struct RecordHeader {
    bool is_deleted;
    uint32_t record_size;
};

class ConditionEvaluator {
public:
    static bool evaluate(const json& row, const ConditionNode* node) {
        if (!node) return true;
        if (node->is_leaf) {
            if (!row.contains(node->left_column)) return false;
            auto val = row[node->left_column];
            if (node->op == OpType::EQ) {
                if (std::holds_alternative<int>(node->right_value.data))
                    return val == std::get<int>(node->right_value.data);
                return val == std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::NEQ) {
                if (std::holds_alternative<int>(node->right_value.data))
                    return val != std::get<int>(node->right_value.data);
                return val != std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::BETWEEN) {
                int i_val = val.get<int>();
                return i_val >= std::get<int>(node->right_value.data) &&
                       i_val < std::get<int>(node->right_value_between->data);
            }
            if (node->op == OpType::LIKE) {
                std::string s_val = val.is_string() ? val.get<std::string>() : std::to_string(val.get<int>());
                std::regex re(std::get<std::string>(node->right_value.data));
                return std::regex_match(s_val, re);
            }
            return true;
        } else {
            if (node->logical_op == LogicalOpType::AND)
                return evaluate(row, node->left_child.get()) && evaluate(row, node->right_child.get());
            if (node->logical_op == LogicalOpType::OR)
                return evaluate(row, node->left_child.get()) || evaluate(row, node->right_child.get());
        }
        return true;
    }
};

Engine::Engine(std::string root) : root_path(std::move(root)) {
    if (!fs::exists(root_path)) fs::create_directories(root_path);
}

void Engine::use_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (!fs::exists(db_path)) throw std::runtime_error("База данных не существует");
    current_db = name;
    string_pool = std::make_unique<StringPool>((db_path / "strings.dat").string());
    journal = std::make_unique<Journal>((db_path / "journal.dat").string());
}

void Engine::execute(const QueryPlan& plan) {
    if (plan.type == QueryType::CREATE_DATABASE)
        create_database(plan.database_name);
    else if (plan.type == QueryType::DROP_DATABASE)
        drop_database(plan.database_name);
    else if (plan.type == QueryType::USE_DATABASE)
        use_database(plan.database_name);
    else if (plan.type == QueryType::CREATE_TABLE)
        create_table(plan);
    else if (plan.type == QueryType::DROP_TABLE)
        drop_table(plan.table_name);
    else if (plan.type == QueryType::INSERT)
        insert_record(plan);
    else if (plan.type == QueryType::SELECT)
        std::cout << select_records(plan) << std::endl;
    else if (plan.type == QueryType::REVERT)
        revert(plan.table_name, plan.timestamp);
    else if (plan.type == QueryType::UPDATE)
        update_records(plan);
    else if (plan.type == QueryType::DELETE)
        delete_records(plan);
    else
        throw std::runtime_error("Операция еще не реализована");
}

void Engine::create_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (fs::exists(db_path)) throw std::runtime_error("База данных уже существует");
    fs::create_directories(db_path);
}

void Engine::drop_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (!fs::exists(db_path)) throw std::runtime_error("База данных не существует");
    fs::remove_all(db_path);
    if (current_db == name) {
        current_db = "";
        string_pool.reset();
        journal.reset();
    }
}

void Engine::create_table(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");
    fs::path table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    Pager pager(table_path.string());
    TableHeader header;
    std::memset(&header, 0, sizeof(header));
    header.magic_number = 0x44424D53;
    header.column_count = static_cast<uint32_t>(plan.columns.size());
    header.last_data_page = 1;
    for (size_t i = 0; i < plan.columns.size(); ++i) {
        const auto& col = plan.columns[i];
        std::strncpy(header.columns[i].name, col.name.c_str(), MAX_NAME_LEN);
        header.columns[i].type = (col.type == DataType::INT) ? 0 : 1;
        header.columns[i].is_not_null = col.is_not_null;
        header.columns[i].is_indexed = col.is_indexed;
    }
    char buffer[PAGE_SIZE] = {0};
    std::memcpy(buffer, &header, sizeof(header));
    pager.write_page(0, buffer);
    pager.allocate_page();
}

void Engine::drop_table(const std::string& table_name) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");
    fs::path table_path = fs::path(root_path) / current_db / (table_name + ".tbl");
    fs::path idx_path = fs::path(root_path) / current_db / (table_name + ".idx");
    if (fs::exists(table_path)) fs::remove(table_path);
    if (fs::exists(idx_path)) fs::remove(idx_path);
}

void Engine::insert_record(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");
    fs::path table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    fs::path idx_path = fs::path(root_path) / current_db / (plan.table_name + ".idx");
    Pager pager(table_path.string());
    Pager idx_pager(idx_path.string());
    char header_buffer[PAGE_SIZE];
    pager.read_page(0, header_buffer);
    TableHeader* header = reinterpret_cast<TableHeader*>(header_buffer);
    auto schema = get_table_schema(plan.table_name);
    auto values_with_ids = plan.values;
    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        for (const auto& col_def : schema) {
            if (col_def.name == plan.target_columns[i]) {
                if (values_with_ids[i].is_null && (col_def.is_not_null || col_def.is_indexed))
                    throw std::runtime_error("NOT NULL violation");
                if (col_def.type == DataType::STRING && !values_with_ids[i].is_null) {
                    values_with_ids[i].data =
                        static_cast<int>(string_pool->intern(std::get<std::string>(values_with_ids[i].data)));
                }
                break;
            }
        }
    }
    auto row_bin = RowSerializer::serialize(schema, plan.target_columns, values_with_ids);
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == schema[i].name && schema[i].type == DataType::INT) {
                    BP_tree<int, pos_t> tree(&idx_pager, header->index_roots[i]);
                    if (tree.contains(std::get<int>(plan.values[j].data))) throw std::runtime_error("Unique violation");
                }
            }
        }
    }
    uint32_t target_p = header->last_data_page;
    char page_buffer[PAGE_SIZE];
    pager.read_page(target_p, page_buffer);
    uint32_t offset = *reinterpret_cast<uint32_t*>(page_buffer);
    if (offset == 0) offset = 4;
    if (offset + sizeof(RecordHeader) + row_bin.size() > PAGE_SIZE) {
        target_p = pager.allocate_page();
        header->last_data_page = target_p;
        pager.read_page(target_p, page_buffer);
        offset = 4;
    }
    RecordHeader rh = {false, static_cast<uint32_t>(row_bin.size())};
    std::memcpy(page_buffer + offset, &rh, sizeof(rh));
    std::memcpy(page_buffer + offset + sizeof(rh), row_bin.data(), row_bin.size());
    pos_t record_pos = {target_p, offset};
    offset += sizeof(rh) + row_bin.size();
    *reinterpret_cast<uint32_t*>(page_buffer) = offset;
    header->row_count++;
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == schema[i].name && schema[i].type == DataType::INT) {
                    BP_tree<int, pos_t> tree(&idx_pager, header->index_roots[i]);
                    tree.insert({std::get<int>(plan.values[j].data), record_pos});
                    header->index_roots[i] = tree.get_root_id();
                }
            }
        }
    }
    pager.write_page(0, header_buffer);
    pager.write_page(target_p, page_buffer);
    JournalEntry je;
    je.op = JournalOp::INSERT;
    je.table_name = plan.table_name;
    je.new_data = row_bin;
    auto now = std::chrono::system_clock::now();
    auto timer = std::chrono::system_clock::to_time_t(now);
    std::tm bt;
    localtime_r(&timer, &bt);
    std::ostringstream oss;
    oss << std::put_time(&bt, "%Y.%m.%d-%H:%M:%S");
    je.timestamp = oss.str();
    journal->log(je);
}

std::string Engine::select_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No DB");
    auto table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    auto idx_path = fs::path(root_path) / current_db / (plan.table_name + ".idx");
    Pager pager(table_path.string());
    Pager idx_pager(idx_path.string());
    char hb[PAGE_SIZE];
    pager.read_page(0, hb);
    TableHeader* h = reinterpret_cast<TableHeader*>(hb);
    auto schema = get_table_schema(plan.table_name);
    json res = json::array();
    if (plan.where_clause && plan.where_clause->is_leaf && plan.where_clause->op == OpType::EQ) {
        for (size_t i = 0; i < schema.size(); ++i) {
            if (schema[i].is_indexed && schema[i].name == plan.where_clause->left_column &&
                schema[i].type == DataType::INT) {
                BP_tree<int, pos_t> tree(&idx_pager, h->index_roots[i]);
                pos_t pos = tree.find(std::get<int>(plan.where_clause->right_value.data));
                if (pos.is_valid()) {
                    char pb[PAGE_SIZE];
                    pager.read_page(pos.page_id, pb);
                    RecordHeader* rh = reinterpret_cast<RecordHeader*>(pb + pos.offset);
                    if (!rh->is_deleted) {
                        size_t doff = pos.offset + sizeof(RecordHeader);
                        res.push_back(RowDeserializer::deserialize(schema, pb, doff, string_pool.get()));
                    }
                }
                return res.dump(4);
            }
        }
    }
    for (uint32_t p = 1; p <= h->last_data_page; ++p) {
        char pb[PAGE_SIZE];
        pager.read_page(p, pb);
        uint32_t coff = *reinterpret_cast<uint32_t*>(pb);
        if (coff <= 4) continue;
        size_t off = 4;
        while (off < coff) {
            RecordHeader* rh = reinterpret_cast<RecordHeader*>(pb + off);
            size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                size_t t_off = doff;
                auto row = RowDeserializer::deserialize(schema, pb, t_off, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) res.push_back(row);
            }
            off += sizeof(RecordHeader) + rh->record_size;
        }
    }
    return res.dump(4);
}

void Engine::delete_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No DB");
    auto table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    Pager pager(table_path.string());
    char hb[PAGE_SIZE];
    pager.read_page(0, hb);
    TableHeader* h = reinterpret_cast<TableHeader*>(hb);
    auto schema = get_table_schema(plan.table_name);
    for (uint32_t p = 1; p <= h->last_data_page; ++p) {
        char pb[PAGE_SIZE];
        pager.read_page(p, pb);
        uint32_t coff = *reinterpret_cast<uint32_t*>(pb);
        if (coff <= 4) continue;
        bool changed = false;
        size_t off = 4;
        while (off < coff) {
            RecordHeader* rh = reinterpret_cast<RecordHeader*>(pb + off);
            size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                size_t t_off = doff;
                auto row = RowDeserializer::deserialize(schema, pb, t_off, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                    rh->is_deleted = true;
                    changed = true;
                    h->row_count--;
                    JournalEntry je;
                    je.op = JournalOp::DELETE;
                    je.table_name = plan.table_name;
                    je.old_data.assign(pb + doff, pb + doff + rh->record_size);
                    journal->log(je);
                }
            }
            off += sizeof(RecordHeader) + rh->record_size;
        }
        if (changed) pager.write_page(p, pb);
    }
    pager.write_page(0, hb);
}

void Engine::update_records(const QueryPlan& plan) {
    delete_records(plan);
    insert_record(plan);
}

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    if (current_db.empty()) throw std::runtime_error("No DB");
    auto schema = get_table_schema(table_name);
    drop_table(table_name);
    QueryPlan cp;
    cp.type = QueryType::CREATE_TABLE;
    cp.table_name = table_name;
    cp.columns = schema;
    create_table(cp);
    auto entries = journal->get_all_entries();
    for (const auto& entry : entries) {
        if (entry.timestamp > timestamp) break;
        if (entry.table_name != table_name) continue;
        if (entry.op == JournalOp::INSERT) {
            auto table_path = fs::path(root_path) / current_db / (table_name + ".tbl");
            Pager pager(table_path.string());
            char hb[PAGE_SIZE];
            pager.read_page(0, hb);
            TableHeader* h = reinterpret_cast<TableHeader*>(hb);
            char pb[PAGE_SIZE];
            pager.read_page(h->last_data_page, pb);
            uint32_t off = *reinterpret_cast<uint32_t*>(pb);
            if (off == 0) off = 4;
            if (off + sizeof(RecordHeader) + entry.new_data.size() > PAGE_SIZE) {
                uint32_t new_p = pager.allocate_page();
                h->last_data_page = new_p;
                pager.read_page(new_p, pb);
                off = 4;
            }
            RecordHeader rh = {false, static_cast<uint32_t>(entry.new_data.size())};
            std::memcpy(pb + off, &rh, sizeof(rh));
            std::memcpy(pb + off + sizeof(rh), entry.new_data.data(), entry.new_data.size());
            off += sizeof(rh) + entry.new_data.size();
            *reinterpret_cast<uint32_t*>(pb) = off;
            h->row_count++;
            pager.write_page(0, hb);
            pager.write_page(h->last_data_page, pb);
        }
    }
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    fs::path table_path = fs::path(root_path) / current_db / (table_name + ".tbl");

    Pager pager(table_path.string());
    char buffer[PAGE_SIZE];
    pager.read_page(0, buffer);
    TableHeader* header = reinterpret_cast<TableHeader*>(buffer);
    std::vector<ColumnDef> schema;

    for (uint32_t i = 0; i < header->column_count; ++i) {
        ColumnDef col;
        col.name = header->columns[i].name;
        col.type = (header->columns[i].type == 0) ? DataType::INT : DataType::STRING;
        col.is_not_null = header->columns[i].is_not_null;
        col.is_indexed = header->columns[i].is_indexed;
        schema.push_back(col);
    }
    return schema;
}
