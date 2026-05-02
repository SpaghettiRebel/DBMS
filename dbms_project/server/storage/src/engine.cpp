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
            if (node->op == OpType::NEQ) {
                if (std::holds_alternative<int>(node->right_value.data)) return val != std::get<int>(node->right_value.data);
                return val != std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::BETWEEN) {
                if (!node->right_value_between) return false;
                if (std::holds_alternative<int>(node->right_value.data)) {
                    int i_val = val.get<int>();
                    return i_val >= std::get<int>(node->right_value.data) && i_val < std::get<int>(node->right_value_between->data);
                }
                std::string s_val = val.get<std::string>();
                return s_val >= std::get<std::string>(node->right_value.data) && s_val < std::get<std::string>(node->right_value_between->data);
            }
            if (node->op == OpType::LIKE) {
                std::string s_val = val.is_string() ? val.get<std::string>() : std::to_string(val.get<int>());
                try {
                    std::regex re(std::get<std::string>(node->right_value.data));
                    return std::regex_match(s_val, re);
                } catch (...) { return false; }
            }
            if (node->op == OpType::LESS) {
                if (std::holds_alternative<int>(node->right_value.data)) return val < std::get<int>(node->right_value.data);
                return val < std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::GREATER) {
                if (std::holds_alternative<int>(node->right_value.data)) return val > std::get<int>(node->right_value.data);
                return val > std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::LEQ) {
                if (std::holds_alternative<int>(node->right_value.data)) return val <= std::get<int>(node->right_value.data);
                return val <= std::get<std::string>(node->right_value.data);
            }
            if (node->op == OpType::GEQ) {
                if (std::holds_alternative<int>(node->right_value.data)) return val >= std::get<int>(node->right_value.data);
                return val >= std::get<std::string>(node->right_value.data);
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
    if (!ofs.write(reinterpret_cast<const char*>(&header), sizeof(header))) throw std::runtime_error("Failed to write metadata");
    ofs.flush();
}

TableHeader load_metadata(const std::string& path) {
    TableHeader header;
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) throw std::runtime_error("Metadata not found: " + path);
    if (!ifs.read(reinterpret_cast<char*>(&header), sizeof(header))) throw std::runtime_error("Failed to read metadata");
    if (header.magic_number != 0x44424D53) throw std::runtime_error("Magic number mismatch: corrupted metadata");
    return header;
}

std::string get_now_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::tm bt; localtime_r(&t, &bt);
    std::ostringstream oss; oss << std::put_time(&bt, "%Y.%m.%d-%H:%M:%S");
    return oss.str();
}

Engine::Engine(std::string root) : root_path(std::move(root)) {
    if (!fs::exists(root_path)) fs::create_directories(root_path);
}

void Engine::use_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (!fs::exists(db_path)) throw std::runtime_error("DB " + name + " not found");
    current_db = name;
    string_pool = std::make_unique<StringPool>((db_path / "strings.dat").string());
    journal = std::make_unique<Journal>((db_path / "journal.dat").string());
}

void Engine::execute(const QueryPlan& plan) {
    try {
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
    } catch (const std::exception& e) { std::cerr << "Engine Error: " << e.what() << std::endl; }
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
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    fs::path meta_path = db_dir / (plan.table_name + ".meta");
    fs::path tbl_path = db_dir / (plan.table_name + ".tbl");
    if (fs::exists(meta_path) || fs::exists(tbl_path)) throw std::runtime_error("Table already exists");
    TableHeader h; std::memset(&h, 0, sizeof(h)); h.magic_number = 0x44424D53; h.column_count = plan.columns.size();
    for (size_t i = 0; i < plan.columns.size(); ++i) {
        std::strncpy(h.columns[i].name, plan.columns[i].name.c_str(), MAX_NAME_LEN);
        h.columns[i].type = (plan.columns[i].type == DataType::INT ? 0 : 1);
        h.columns[i].is_not_null = plan.columns[i].is_not_null; h.columns[i].is_indexed = plan.columns[i].is_indexed;
        if (plan.columns[i].default_value) {
            h.columns[i].has_default = true;
            if (h.columns[i].type == 0) h.columns[i].default_int = std::get<int>(plan.columns[i].default_value->data);
            else h.columns[i].default_string_id = string_pool->intern(std::get<std::string>(plan.columns[i].default_value->data));
        }
    }
    save_metadata(meta_path.string(), h);
    Pager p(tbl_path.string()); p.allocate_page();
}

void Engine::drop_table(const std::string& table_name) {
    fs::path db_dir = fs::path(root_path) / current_db;
    fs::remove(db_dir / (table_name + ".tbl")); fs::remove(db_dir / (table_name + ".meta")); fs::remove(db_dir / (table_name + ".idx"));
}

void Engine::insert_record(const QueryPlan& plan) {
    auto m_path = (fs::path(root_path) / current_db / (plan.table_name + ".meta")).string();
    auto h = load_metadata(m_path); auto schema = get_table_schema(plan.table_name);
    if (plan.target_columns.size() != plan.values.size()) throw std::runtime_error("Target columns/values size mismatch");
    
    std::vector<Value> vals(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) {
        bool found = false;
        for (size_t j = 0; j < plan.target_columns.size(); ++j) {
            if (plan.target_columns[j] == schema[i].name) { vals[i] = plan.values[j]; found = true; break; }
        }
        if (!found) {
            if (h.columns[i].has_default) {
                vals[i].is_null = false;
                if (h.columns[i].type == 0) vals[i].data = h.columns[i].default_int;
                else vals[i].data = (int)h.columns[i].default_string_id;
            } else vals[i].is_null = true;
        }
        if (vals[i].is_null && (h.columns[i].is_not_null || h.columns[i].is_indexed)) throw std::runtime_error("Constraint violation: NULL in NOT_NULL/INDEXED column");
        if (!vals[i].is_null) {
            if (h.columns[i].type == 0 && !std::holds_alternative<int>(vals[i].data)) throw std::runtime_error("Type mismatch for " + std::string(h.columns[i].name));
            if (h.columns[i].type == 1) {
                if (std::holds_alternative<std::string>(vals[i].data)) vals[i].data = (int)string_pool->intern(std::get<std::string>(vals[i].data));
                else if (!std::holds_alternative<int>(vals[i].data)) throw std::runtime_error("Type mismatch for " + std::string(h.columns[i].name));
            }
        }
    }
    auto bin = RowSerializer::serialize(schema, get_all_column_names(schema), vals);
    if (bin.size() + sizeof(RecordHeader) > PAGE_SIZE - 4) throw std::runtime_error("Record too large for page");
    Pager tbl_p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    Pager idx_p((fs::path(root_path) / current_db / (plan.table_name + ".idx")).string());
    for (size_t i = 0; i < h.column_count; ++i) if (h.columns[i].is_indexed) {
        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
        if (tree.contains(std::get<int>(vals[i].data))) throw std::runtime_error("Unique violation on column: " + std::string(h.columns[i].name));
    }
    char pb[PAGE_SIZE]; tbl_p.read_page(h.last_data_page, pb); uint32_t off = *reinterpret_cast<uint32_t*>(pb); if (off == 0) off = 4;
    if (off + bin.size() + sizeof(RecordHeader) > PAGE_SIZE) { h.last_data_page = tbl_p.allocate_page(); tbl_p.read_page(h.last_data_page, pb); off = 4; }
    RecordHeader rh = {false, (uint32_t)bin.size()}; std::memcpy(pb + off, &rh, sizeof(rh)); std::memcpy(pb + off + sizeof(rh), bin.data(), bin.size());
    pos_t record_pos = {h.last_data_page, off}; *reinterpret_cast<uint32_t*>(pb) = off + sizeof(rh) + bin.size(); tbl_p.write_page(h.last_data_page, pb);
    for (size_t i = 0; i < h.column_count; ++i) if (h.columns[i].is_indexed) {
        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
        tree.insert({std::get<int>(vals[i].data), record_pos}); h.index_roots[i] = tree.get_root_id();
    }
    h.row_count++; tbl_p.file.flush(); idx_p.file.flush(); save_metadata(m_path, h);
    JournalEntry je = {JournalOp::INSERT, plan.table_name, get_now_timestamp(), record_pos, {}, bin}; journal->log(je);
}

std::string Engine::select_records(const QueryPlan& plan) {
    auto m_path = (fs::path(root_path) / current_db / (plan.table_name + ".meta")).string();
    auto h = load_metadata(m_path); auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    Pager idx_p((fs::path(root_path) / current_db / (plan.table_name + ".idx")).string()); json res = json::array();
    if (plan.where_clause && plan.where_clause->is_leaf && plan.where_clause->op == OpType::EQ) {
        for (size_t i = 0; i < h.column_count; ++i) if (h.columns[i].is_indexed && h.columns[i].name == plan.where_clause->left_column) {
            int key; if (h.columns[i].type == 0) { if (!std::holds_alternative<int>(plan.where_clause->right_value.data)) continue; key = std::get<int>(plan.where_clause->right_value.data); }
            else { auto sid = string_pool->get_id_if_exists(std::get<std::string>(plan.where_clause->right_value.data)); if (!sid) return res.dump(4); key = (int)*sid; }
            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]); auto it = tree.find(key);
            if (it != tree.end()) {
                pos_t pos = it->second; if (pos.is_valid()) { char pb[PAGE_SIZE]; tbl_p.read_page(pos.page_id, pb); RecordHeader* rh = (RecordHeader*)(pb + pos.offset);
                if (!rh->is_deleted) { size_t doff = pos.offset + sizeof(RecordHeader); res.push_back(RowDeserializer::deserialize(schema, pb, doff, string_pool.get())); } }
            } return res.dump(4);
        }
    }
    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char pb[PAGE_SIZE]; tbl_p.read_page(i, pb); uint32_t coff = *reinterpret_cast<uint32_t*>(pb); if (coff <= 4) continue;
        size_t off = 4; while (off < coff) {
            RecordHeader* rh = (RecordHeader*)(pb + off); size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) { try { size_t toff = doff; auto row = RowDeserializer::deserialize(schema, pb, toff, string_pool.get()); if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) res.push_back(row); } catch(...) {} }
            off += sizeof(RecordHeader) + rh->record_size;
        }
    } return res.dump(4);
}

void Engine::delete_records(const QueryPlan& plan) {
    auto m_path = (fs::path(root_path) / current_db / (plan.table_name + ".meta")).string();
    auto h = load_metadata(m_path); auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    Pager idx_p((fs::path(root_path) / current_db / (plan.table_name + ".idx")).string());
    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char pb[PAGE_SIZE]; tbl_p.read_page(i, pb); uint32_t end_off = *reinterpret_cast<uint32_t*>(pb); if (end_off <= 4) continue;
        bool chg = false; size_t off = 4; while (off < end_off) {
            RecordHeader* rh = (RecordHeader*)(pb + off); size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                try { size_t toff = doff; auto row = RowDeserializer::deserialize(schema, pb, toff, string_pool.get());
                    if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                        rh->is_deleted = true; chg = true; h.row_count--;
                        for (size_t c = 0; c < h.column_count; ++c) if (h.columns[c].is_indexed) { BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]); int k = (h.columns[c].type == 0 ? row[h.columns[c].name].get<int>() : (int)*string_pool->get_id_if_exists(row[h.columns[c].name].get<std::string>())); tree.erase(k); h.index_roots[c] = tree.get_root_id(); }
                        JournalEntry je = {JournalOp::DELETE, plan.table_name, get_now_timestamp(), {i, (uint32_t)off}, std::vector<char>(pb + doff, pb + doff + rh->record_size), {}}; journal->log(je);
                    }
                } catch(...) {}
            } off += sizeof(RecordHeader) + rh->record_size;
        } if (chg) tbl_p.write_page(i, pb);
    } tbl_p.file.flush(); idx_p.file.flush(); save_metadata(m_path, h);
}

void Engine::update_records(const QueryPlan& plan) {
    auto m_path = (fs::path(root_path) / current_db / (plan.table_name + ".meta")).string();
    auto h = load_metadata(m_path); auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((fs::path(root_path) / current_db / (plan.table_name + ".tbl")).string());
    Pager idx_p((fs::path(root_path) / current_db / (plan.table_name + ".idx")).string());
    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char pb[PAGE_SIZE]; tbl_p.read_page(i, pb); uint32_t end_off = *reinterpret_cast<uint32_t*>(pb); if (end_off <= 4) continue;
        bool chg_p = false; size_t off = 4; while (off < end_off) {
            RecordHeader* rh = (RecordHeader*)(pb + off); size_t doff = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                try { size_t toff = doff; auto row = RowDeserializer::deserialize(schema, pb, toff, string_pool.get());
                    if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                        std::vector<char> old_raw(pb + doff, pb + doff + rh->record_size);
                        for (size_t j = 0; j < plan.target_columns.size(); ++j) row[plan.target_columns[j]] = (plan.values[j].is_null ? json(nullptr) : (std::holds_alternative<int>(plan.values[j].data) ? json(std::get<int>(plan.values[j].data)) : json(std::get<std::string>(plan.values[j].data))));
                        std::vector<Value> n_v(schema.size()); for (size_t c = 0; c < schema.size(); ++c) { auto v = row[schema[c].name]; n_v[c].is_null = v.is_null(); if (!v.is_null()) { if (schema[c].type == 0) n_v[c].data = v.get<int>(); else n_v[c].data = (int)string_pool->intern(v.get<std::string>()); } }
                        auto n_bin = RowSerializer::serialize(schema, get_all_column_names(schema), n_v);
                        if (n_bin.size() == rh->record_size) {
                            std::memcpy(pb + doff, n_bin.data(), n_bin.size()); chg_p = true;
                            for (size_t c = 0; c < schema.size(); ++c) if (schema[c].is_indexed) { BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]); int old_k = (schema[c].type == 0 ? RowDeserializer::deserialize(schema, old_raw.data(), (toff=0), string_pool.get())[schema[c].name].get<int>() : (int)*string_pool->get_id_if_exists(RowDeserializer::deserialize(schema, old_raw.data(), (toff=0), string_pool.get())[schema[c].name].get<std::string>())); tree.erase(old_k); tree.insert({std::get<int>(n_v[c].data), {i, (uint32_t)off}}); h.index_roots[c] = tree.get_root_id(); }
                            JournalEntry je = {JournalOp::UPDATE, plan.table_name, get_now_timestamp(), {i, (uint32_t)off}, old_raw, n_bin}; journal->log(je);
                        } else {
                            rh->is_deleted = true; chg_p = true; h.row_count--;
                            JournalEntry je_del = {JournalOp::DELETE, plan.table_name, get_now_timestamp(), {i, (uint32_t)off}, old_raw, {}}; journal->log(je_del);
                            save_metadata(m_path, h); QueryPlan ip = plan; ip.type = QueryType::INSERT; ip.values = n_v; ip.target_columns = get_all_column_names(schema); insert_record(ip); h = load_metadata(m_path);
                        }
                    }
                } catch(...) {}
            } off += sizeof(RecordHeader) + rh->record_size;
        } if (chg_p) tbl_p.write_page(i, pb);
    } tbl_p.file.flush(); idx_p.file.flush(); save_metadata(m_path, h);
}

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    auto entries = journal->get_all_entries(); Pager tbl_p((fs::path(root_path) / current_db / (table_name + ".tbl")).string());
    Pager idx_p((fs::path(root_path) / current_db / (table_name + ".idx")).string()); auto m_path = (fs::path(root_path) / current_db / (table_name + ".meta")).string();
    auto h = load_metadata(m_path); auto schema = get_table_schema(table_name);
    for (int i = (int)entries.size() - 1; i >= 0; --i) {
        const auto& e = entries[i]; if (e.timestamp <= timestamp || e.table_name != table_name) continue;
        char buf[PAGE_SIZE]; tbl_p.read_page(e.record_pos.page_id, buf); RecordHeader* rh = (RecordHeader*)(buf + e.record_pos.offset);
        if (e.op == JournalOp::INSERT) {
            if (!rh->is_deleted) { rh->is_deleted = true; h.row_count--; size_t t=0; auto row = RowDeserializer::deserialize(schema, buf + e.record_pos.offset + sizeof(RecordHeader), t, string_pool.get());
                for (size_t c = 0; c < h.column_count; ++c) if (h.columns[c].is_indexed) { BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]); int k = (h.columns[c].type == 0 ? row[h.columns[c].name].get<int>() : (int)*string_pool->get_id_if_exists(row[h.columns[c].name].get<std::string>())); tree.erase(k); h.index_roots[c] = tree.get_root_id(); }
            }
        } else if (e.op == JournalOp::DELETE) {
            rh->is_deleted = false; h.row_count++; std::memcpy(buf + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size());
            size_t t=0; auto row = RowDeserializer::deserialize(schema, e.old_data.data(), t, string_pool.get());
            for (size_t c = 0; c < h.column_count; ++c) if (h.columns[c].is_indexed) { BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]); int k = (h.columns[c].type == 0 ? row[h.columns[c].name].get<int>() : (int)*string_pool->intern(row[h.columns[c].name].get<std::string>())); tree.insert({k, e.record_pos}); h.index_roots[c] = tree.get_root_id(); }
        } else if (e.op == JournalOp::UPDATE) {
            size_t t=0; auto row_new = RowDeserializer::deserialize(schema, buf + e.record_pos.offset + sizeof(RecordHeader), t, string_pool.get());
            for (size_t c = 0; c < h.column_count; ++c) if (h.columns[c].is_indexed) { BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]); int k_new = (h.columns[c].type == 0 ? row_new[h.columns[c].name].get<int>() : (int)*string_pool->get_id_if_exists(row_new[h.columns[c].name].get<std::string>())); tree.erase(k_new); h.index_roots[c] = tree.get_root_id(); }
            std::memcpy(buf + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size()); rh->record_size = (uint32_t)e.old_data.size();
            t=0; auto row_old = RowDeserializer::deserialize(schema, e.old_data.data(), t, string_pool.get());
            for (size_t c = 0; c < h.column_count; ++c) if (h.columns[c].is_indexed) { BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]); int k_old = (h.columns[c].type == 0 ? row_old[h.columns[c].name].get<int>() : (int)*string_pool->intern(row_old[h.columns[c].name].get<std::string>())); tree.insert({k_old, e.record_pos}); h.index_roots[c] = tree.get_root_id(); }
        } tbl_p.write_page(e.record_pos.page_id, buf);
    } tbl_p.file.flush(); idx_p.file.flush(); save_metadata(m_path, h);
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    auto h = load_metadata((fs::path(root_path) / current_db / (table_name + ".meta")).string());
    std::vector<ColumnDef> s; for (uint32_t i = 0; i < h.column_count; ++i) { ColumnDef c; c.name = h.columns[i].name; c.type = (h.columns[i].type == 0 ? DataType::INT : DataType::STRING); c.is_not_null = h.columns[i].is_not_null; c.is_indexed = h.columns[i].is_indexed; s.push_back(c); } return s;
}
std::vector<std::string> Engine::get_all_column_names(const std::vector<ColumnDef>& schema) { std::vector<std::string> n; for(const auto& c : schema) n.push_back(c.name); return n; }
