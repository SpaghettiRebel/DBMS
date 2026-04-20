#include "../include/engine.h"
#include "../include/serializer.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <regex>
#include <fstream>

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
                if (std::holds_alternative<int>(node->right_value.data)) return val == std::get<int>(node->right_value.data);
                return val == std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::BETWEEN) {
                int i_val = val.get<int>();
                return i_val >= std::get<int>(node->right_value.data) && i_val < std::get<int>(node->right_value_between->data);
            }
            if (node->op == OpType::LIKE) {
                std::string s_val = val.is_string() ? val.get<std::string>() : std::to_string(val.get<int>());
                std::regex re(std::get<std::string>(node->right_value.data));
                return std::regex_match(s_val, re);
            }
            return true;
        } else {
            if (node->logical_op == LogicalOpType::AND) return evaluate(row, node->left_child.get()) && evaluate(row, node->right_child.get());
            if (node->logical_op == LogicalOpType::OR) return evaluate(row, node->left_child.get()) || evaluate(row, node->right_child.get());
        }
        return true;
    }
};

void save_metadata(const std::string& path, const TableHeader& header) {
    std::ofstream ofs(path, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(&header), sizeof(header));
}

TableHeader load_metadata(const std::string& path) {
    TableHeader header;
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) throw std::runtime_error("Metadata not found: " + path);
    ifs.read(reinterpret_cast<char*>(&header), sizeof(header));
    return header;
}

Engine::Engine(std::string root) : root_path(std::move(root)) {
    if (!fs::exists(root_path)) fs::create_directories(root_path);
}

void Engine::use_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (!fs::exists(db_path)) throw std::runtime_error("No DB");
    current_db = name;
    string_pool = std::make_unique<StringPool>((db_path / "strings.dat").string());
    journal = std::make_unique<Journal>((db_path / "journal.dat").string());
}

void Engine::execute(const QueryPlan& plan) {
    if (plan.type == QueryType::CREATE_DATABASE) create_database(plan.database_name);
    else if (plan.type == QueryType::DROP_DATABASE) drop_database(plan.database_name);
    else if (plan.type == QueryType::USE_DATABASE) use_database(plan.database_name);
    else if (plan.type == QueryType::CREATE_TABLE) create_table(plan);
    else if (plan.type == QueryType::DROP_TABLE) drop_table(plan.table_name);
    else if (plan.type == QueryType::INSERT) insert_record(plan);
    else if (plan.type == QueryType::SELECT) std::cout << select_records(plan) << std::endl;
    else if (plan.type == QueryType::REVERT) revert(plan.table_name, plan.timestamp);
    else if (plan.type == QueryType::UPDATE) update_records(plan);
    else if (plan.type == QueryType::DELETE) delete_records(plan);
}

void Engine::create_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (fs::exists(db_path)) throw std::runtime_error("DB exists");
    fs::create_directories(db_path);
}

void Engine::drop_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (!fs::exists(db_path)) throw std::runtime_error("No DB");
    fs::remove_all(db_path);
}

void Engine::create_table(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No DB");
    fs::path meta_path = fs::path(root_path) / current_db / (plan.table_name + ".meta");
    fs::path tbl_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    TableHeader h; std::memset(&h, 0, sizeof(h)); h.magic_number = 0x44424D53; h.column_count = plan.columns.size();
    for (size_t i = 0; i < plan.columns.size(); ++i) {
        std::strncpy(h.columns[i].name, plan.columns[i].name.c_str(), MAX_NAME_LEN);
        h.columns[i].type = (plan.columns[i].type == DataType::INT ? 0 : 1);
        h.columns[i].is_not_null = plan.columns[i].is_not_null; h.columns[i].is_indexed = plan.columns[i].is_indexed;
    }
    save_metadata(meta_path.string(), h); Pager p(tbl_path.string()); p.allocate_page();
}

void Engine::drop_table(const std::string& table_name) {
    fs::remove(fs::path(root_path) / current_db / (table_name + ".tbl"));
    fs::remove(fs::path(root_path) / current_db / (table_name + ".meta"));
    fs::remove(fs::path(root_path) / current_db / (table_name + ".idx"));
}

void Engine::insert_record(const QueryPlan& plan) {
    auto meta_path = (fs::path(root_path) / current_db / (plan.table_name + ".meta")).string();
    auto h = load_metadata(meta_path);
    auto schema = get_table_schema(plan.table_name);
    auto values = plan.values;
    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        for (const auto& c : schema) {
            if (c.name == plan.target_columns[i]) {
                if (values[i].is_null && (c.is_not_null || c.is_indexed)) throw std::runtime_error("Constraint violation");
                if (c.type == DataType::STRING && !values[i].is_null) values[i].data = (int)string_pool->intern(std::get<std::string>(values[i].data));
            }
        }
    }
    auto bin = RowSerializer::serialize(schema, plan.target_columns, values);
    Pager p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    char pb[PAGE_SIZE]; p.read_page(h.last_data_page, pb); uint32_t off = *reinterpret_cast<uint32_t*>(pb); if (off == 0) off = 4;
    if (off + sizeof(RecordHeader) + bin.size() > PAGE_SIZE) { h.last_data_page = p.allocate_page(); p.read_page(h.last_data_page, pb); off = 4; }
    RecordHeader rh = {false, (uint32_t)bin.size()}; std::memcpy(pb + off, &rh, sizeof(rh)); std::memcpy(pb + off + sizeof(rh), bin.data(), bin.size());
    pos_t record_pos = {h.last_data_page, off};
    *reinterpret_cast<uint32_t*>(pb) = off + sizeof(rh) + bin.size(); p.write_page(h.last_data_page, pb);

    // Index check and update
    Pager idx_p((fs::path(root_path) / current_db / (plan.table_name + ".idx")).string());
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == schema[i].name && schema[i].type == DataType::INT) {
                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
                    if (tree.contains(std::get<int>(plan.values[j].data))) throw std::runtime_error("Unique violation");
                    tree.insert({std::get<int>(plan.values[j].data), record_pos});
                    h.index_roots[i] = tree.get_root_id();
                }
            }
        }
    }
    h.row_count++; save_metadata(meta_path, h);
    JournalEntry je; je.op = JournalOp::INSERT; je.table_name = plan.table_name; je.new_data = bin;
    auto now = std::chrono::system_clock::now(); auto t = std::chrono::system_clock::to_time_t(now);
    std::tm bt; localtime_r(&t, &bt); std::ostringstream oss; oss << std::put_time(&bt, "%Y.%m.%d-%H:%M:%S"); je.timestamp = oss.str(); journal->log(je);
}

std::string Engine::select_records(const QueryPlan& plan) {
    auto h = load_metadata((fs::path(root_path) / current_db / (plan.table_name + ".meta")).string());
    auto schema = get_table_schema(plan.table_name); Pager p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    json res = json::array();

    // Index optimization
    if (plan.where_clause && plan.where_clause->is_leaf && plan.where_clause->op == OpType::EQ) {
        for (size_t i = 0; i < schema.size(); ++i) {
            if (schema[i].is_indexed && schema[i].name == plan.where_clause->left_column && schema[i].type == DataType::INT) {
                Pager idx_p((fs::path(root_path) / current_db / (plan.table_name + ".idx")).string());
                BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
                auto pos = tree.find(std::get<int>(plan.where_clause->right_value.data));
                if (pos.is_valid()) {
                    char pb[PAGE_SIZE]; p.read_page(pos.page_id, pb);
                    RecordHeader* rh = (RecordHeader*)(pb + pos.offset);
                    if (!rh->is_deleted) { size_t doff = pos.offset + sizeof(RecordHeader); res.push_back(RowDeserializer::deserialize(schema, pb, doff, string_pool.get())); }
                } return res.dump(4);
            }
        }
    }

    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char pb[PAGE_SIZE]; p.read_page(i, pb); uint32_t coff = *reinterpret_cast<uint32_t*>(pb); if (coff <= 4) continue;
        size_t off = 4; while (off < coff) {
            RecordHeader* rh = (RecordHeader*)(pb + off); size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) { size_t toff = doff; auto row = RowDeserializer::deserialize(schema, pb, toff, string_pool.get()); if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) res.push_back(row); }
            off += sizeof(RecordHeader) + rh->record_size;
        }
    } return res.dump(4);
}

void Engine::delete_records(const QueryPlan& plan) {
    auto meta_path = (fs::path(root_path) / current_db / (plan.table_name + ".meta")).string();
    auto h = load_metadata(meta_path);
    auto schema = get_table_schema(plan.table_name); Pager p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char pb[PAGE_SIZE]; p.read_page(i, pb); uint32_t coff = *reinterpret_cast<uint32_t*>(pb); if (coff <= 4) continue;
        bool changed = false; size_t off = 4; while (off < coff) {
            RecordHeader* rh = (RecordHeader*)(pb + off); size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) { size_t toff = doff; auto row = RowDeserializer::deserialize(schema, pb, toff, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                    rh->is_deleted = true; changed = true; h.row_count--;
                    JournalEntry je; je.op = JournalOp::DELETE; je.table_name = plan.table_name; je.old_data.assign(pb + doff, pb + doff + rh->record_size); journal->log(je);
                }
            } off += sizeof(RecordHeader) + rh->record_size;
        } if (changed) p.write_page(i, pb);
    } save_metadata(meta_path, h);
}

void Engine::update_records(const QueryPlan& plan) { delete_records(plan); insert_record(plan); }

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    auto schema = get_table_schema(table_name); drop_table(table_name);
    QueryPlan cp; cp.type = QueryType::CREATE_TABLE; cp.table_name = table_name; cp.columns = schema; create_table(cp);
    for (const auto& e : journal->get_all_entries()) {
        if (e.timestamp > timestamp) break; if (e.table_name != table_name) continue;
        if (e.op == JournalOp::INSERT) {
            auto meta_path = (fs::path(root_path) / current_db / (table_name + ".meta")).string();
            auto h = load_metadata(meta_path);
            Pager p((fs::path(root_path) / current_db / (table_name + ".tbl")).string()); char pb[PAGE_SIZE]; p.read_page(h.last_data_page, pb);
            uint32_t off = *reinterpret_cast<uint32_t*>(pb); if (off == 0) off = 4;
            if (off + sizeof(RecordHeader) + e.new_data.size() > PAGE_SIZE) { h.last_data_page = p.allocate_page(); p.read_page(h.last_data_page, pb); off = 4; }
            RecordHeader rh = {false, (uint32_t)e.new_data.size()}; std::memcpy(pb + off, &rh, sizeof(rh)); std::memcpy(pb + off + sizeof(rh), e.new_data.data(), e.new_data.size());
            *reinterpret_cast<uint32_t*>(pb) = off + sizeof(rh) + e.new_data.size(); p.write_page(h.last_data_page, pb);
            h.row_count++; save_metadata(meta_path, h);
        }
    }
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    auto h = load_metadata((fs::path(root_path) / current_db / (table_name + ".meta")).string());
    std::vector<ColumnDef> s; for (uint32_t i = 0; i < h.column_count; ++i) {
        ColumnDef c; c.name = h.columns[i].name; c.type = (h.columns[i].type == 0 ? DataType::INT : DataType::STRING);
        c.is_not_null = h.columns[i].is_not_null; c.is_indexed = h.columns[i].is_indexed; s.push_back(c);
    } return s;
}
