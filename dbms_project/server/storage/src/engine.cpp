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
                if (std::holds_alternative<int>(node->right_value.data)) {
                    int i_val = val.get<int>();
                    return i_val >= std::get<int>(node->right_value.data) && i_val < std::get<int>(node->right_value_between->data);
                }
                std::string s_val = val.get<std::string>();
                return s_val >= std::get<std::string>(node->right_value.data) && s_val < std::get<std::string>(node->right_value_between->data);
            }
            if (node->op == OpType::LIKE) {
                std::string s_val = val.is_string() ? val.get<std::string>() : std::to_string(val.get<int>());
                std::regex re(std::get<std::string>(node->right_value.data));
                return std::regex_match(s_val, re);
            }
            if (node->op == OpType::LESS) {
                if (std::holds_alternative<int>(node->right_value.data)) return val < std::get<int>(node->right_value.data);
                return val < std::get<std::string>(node->right_value.data);
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

std::string get_now_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm bt; localtime_r(&t, &bt);
    std::ostringstream oss;
    oss << std::put_time(&bt, "%Y.%m.%d-%H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
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
    if (current_db == name) { current_db = ""; string_pool.reset(); journal.reset(); }
}

void Engine::create_table(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    TableHeader h; std::memset(&h, 0, sizeof(h)); h.magic_number = 0x44424D53;
    h.column_count = static_cast<uint32_t>(plan.columns.size());
    for (size_t i = 0; i < plan.columns.size(); ++i) {
        std::strncpy(h.columns[i].name, plan.columns[i].name.c_str(), MAX_NAME_LEN);
        h.columns[i].type = (plan.columns[i].type == DataType::INT ? 0 : 1);
        h.columns[i].is_not_null = plan.columns[i].is_not_null;
        h.columns[i].is_indexed = plan.columns[i].is_indexed;
    }
    save_metadata((db_dir / (plan.table_name + ".meta")).string(), h);
    Pager p((db_dir / (plan.table_name + ".tbl")).string()); p.allocate_page();
}

void Engine::drop_table(const std::string& table_name) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    fs::remove(db_dir / (table_name + ".tbl"));
    fs::remove(db_dir / (table_name + ".meta"));
    fs::remove(db_dir / (table_name + ".idx"));
}

void Engine::insert_record(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    auto meta_path = (db_dir / (plan.table_name + ".meta")).string();
    auto h = load_metadata(meta_path);
    auto schema = get_table_schema(plan.table_name);
    auto values = plan.values;

    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        for (const auto& col : schema) {
            if (col.name == plan.target_columns[i]) {
                if (values[i].is_null && (col.is_not_null || col.is_indexed))
                    throw std::runtime_error("Constraint violation: NULL in column " + col.name);
                if (col.type == DataType::STRING && !values[i].is_null)
                    values[i].data = (int)string_pool->intern(std::get<std::string>(values[i].data));
                break;
            }
        }
    }

    auto row_bin = RowSerializer::serialize(schema, plan.target_columns, values);
    Pager tbl_p((db_dir / (plan.table_name + ".tbl")).string());
    Pager idx_p((db_dir / (plan.table_name + ".idx")).string());

    for (size_t i = 0; i < h.column_count; ++i) {
        if (h.columns[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == h.columns[i].name) {
                    if (h.columns[i].type == 0) {
                        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
                        if (tree.contains(std::get<int>(plan.values[j].data))) throw std::runtime_error("Unique violation");
                    } else {
                        BP_tree<std::string, pos_t> tree(&idx_p, h.index_roots[i]);
                        if (tree.contains(std::get<std::string>(plan.values[j].data))) throw std::runtime_error("Unique violation");
                    }
                }
            }
        }
    }

    char page_buf[PAGE_SIZE]; tbl_p.read_page(h.last_data_page, page_buf);
    uint32_t offset = *reinterpret_cast<uint32_t*>(page_buf); if (offset == 0) offset = 4;
    if (offset + sizeof(RecordHeader) + row_bin.size() > PAGE_SIZE) {
        h.last_data_page = tbl_p.allocate_page(); tbl_p.read_page(h.last_data_page, page_buf); offset = 4;
    }
    RecordHeader rh = {false, (uint32_t)row_bin.size()};
    std::memcpy(page_buf + offset, &rh, sizeof(rh));
    std::memcpy(page_buf + offset + sizeof(rh), row_bin.data(), row_bin.size());
    pos_t record_pos = {h.last_data_page, offset};
    *reinterpret_cast<uint32_t*>(page_buf) = offset + sizeof(rh) + (uint32_t)row_bin.size();
    tbl_p.write_page(h.last_data_page, page_buf);

    for (size_t i = 0; i < h.column_count; ++i) {
        if (h.columns[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == h.columns[i].name) {
                    if (h.columns[i].type == 0) {
                        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
                        tree.insert({std::get<int>(plan.values[j].data), record_pos});
                        h.index_roots[i] = tree.get_root_id();
                    } else {
                        BP_tree<std::string, pos_t> tree(&idx_p, h.index_roots[i]);
                        tree.insert({std::get<std::string>(plan.values[j].data), record_pos});
                        h.index_roots[i] = tree.get_root_id();
                    }
                }
            }
        }
    }
    h.row_count++; save_metadata(meta_path, h);
    JournalEntry je = {JournalOp::INSERT, plan.table_name, get_now_timestamp(), record_pos, {}, row_bin};
    journal->log(je);
}

std::string Engine::select_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    auto h = load_metadata((db_dir / (plan.table_name + ".meta")).string());
    auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((db_dir / (plan.table_name + ".tbl")).string());
    Pager idx_p((db_dir / (plan.table_name + ".idx")).string());
    json res = json::array();

    if (plan.where_clause && plan.where_clause->is_leaf && plan.where_clause->op == OpType::EQ) {
        for (size_t i = 0; i < h.column_count; ++i) {
            if (h.columns[i].is_indexed && h.columns[i].name == plan.where_clause->left_column) {
                pos_t pos = pos_t::invalid();
                if (h.columns[i].type == 0) {
                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
                    auto it = tree.find(std::get<int>(plan.where_clause->right_value.data));
                    if (it != tree.end()) pos = it->second;
                } else {
                    BP_tree<std::string, pos_t> tree(&idx_p, h.index_roots[i]);
                    auto it = tree.find(std::get<std::string>(plan.where_clause->right_value.data));
                    if (it != tree.end()) pos = it->second;
                }
                if (pos.is_valid()) {
                    char buf[PAGE_SIZE]; tbl_p.read_page(pos.page_id, buf);
                    RecordHeader* rh = (RecordHeader*)(buf + pos.offset);
                    if (!rh->is_deleted) {
                        size_t d_off = pos.offset + sizeof(RecordHeader);
                        res.push_back(RowDeserializer::deserialize(schema, buf, d_off, string_pool.get()));
                    }
                }
                return res.dump(4);
            }
        }
    }

    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char buf[PAGE_SIZE]; tbl_p.read_page(i, buf);
        uint32_t end_off = *reinterpret_cast<uint32_t*>(buf); if (end_off <= 4) continue;
        size_t off = 4;
        while (off < end_off) {
            RecordHeader* rh = (RecordHeader*)(buf + off); size_t data_off = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                size_t t_off = data_off; auto row = RowDeserializer::deserialize(schema, buf, t_off, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) res.push_back(row);
            }
            off += sizeof(RecordHeader) + rh->record_size;
        }
    }
    return res.dump(4);
}

void Engine::delete_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    auto h = load_metadata((db_dir / (plan.table_name + ".meta")).string());
    auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((db_dir / (plan.table_name + ".tbl")).string());
    Pager idx_p((db_dir / (plan.table_name + ".idx")).string());

    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char buf[PAGE_SIZE]; tbl_p.read_page(i, buf);
        uint32_t end_off = *reinterpret_cast<uint32_t*>(buf); if (end_off <= 4) continue;
        bool changed = false; size_t off = 4;
        while (off < end_off) {
            RecordHeader* rh = (RecordHeader*)(buf + off); size_t data_off = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                size_t t_off = data_off; auto row = RowDeserializer::deserialize(schema, buf, t_off, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                    rh->is_deleted = true; changed = true; h.row_count--;
                    for (size_t c = 0; c < h.column_count; ++c) {
                        if (h.columns[c].is_indexed) {
                            if (h.columns[c].type == 0) {
                                BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                                tree.erase(row[h.columns[c].name].get<int>());
                            } else {
                                BP_tree<std::string, pos_t> tree(&idx_p, h.index_roots[c]);
                                tree.erase(row[h.columns[c].name].get<std::string>());
                            }
                        }
                    }
                    std::vector<char> old_data(buf + data_off, buf + data_off + rh->record_size);
                    JournalEntry je = {JournalOp::DELETE, plan.table_name, get_now_timestamp(), {i, (uint32_t)off}, old_data, {}};
                    journal->log(je);
                }
            }
            off += sizeof(RecordHeader) + rh->record_size;
        }
        if (changed) tbl_p.write_page(i, buf);
    }
    save_metadata((db_dir / (plan.table_name + ".meta")).string(), h);
}

void Engine::update_records(const QueryPlan& plan) {
    delete_records(plan);
    insert_record(plan);
}

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    auto entries = journal->get_all_entries();
    Pager tbl_p((fs::path(root_path) / current_db / (table_name + ".tbl")).string());
    auto h_path = (fs::path(root_path) / current_db / (table_name + ".meta")).string();
    auto h = load_metadata(h_path);

    for (int i = (int)entries.size() - 1; i >= 0; --i) {
        const auto& e = entries[i];
        if (e.timestamp <= timestamp) break;
        if (e.table_name != table_name) continue;

        char buf[PAGE_SIZE]; tbl_p.read_page(e.record_pos.page_id, buf);
        RecordHeader* rh = (RecordHeader*)(buf + e.record_pos.offset);

        if (e.op == JournalOp::INSERT) {
            if (!rh->is_deleted) { rh->is_deleted = true; h.row_count--; }
        } else if (e.op == JournalOp::DELETE) {
            rh->is_deleted = false; h.row_count++;
            std::memcpy(buf + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size());
        } else if (e.op == JournalOp::UPDATE) {
            std::memcpy(buf + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size());
            rh->record_size = (uint32_t)e.old_data.size();
        }
        tbl_p.write_page(e.record_pos.page_id, buf);
    }
    save_metadata(h_path, h);
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    auto h = load_metadata((fs::path(root_path) / current_db / (table_name + ".meta")).string());
    std::vector<ColumnDef> s;
    for (uint32_t i = 0; i < h.column_count; ++i) {
        ColumnDef c; c.name = h.columns[i].name; c.type = (h.columns[i].type == 0 ? DataType::INT : DataType::STRING);
        c.is_not_null = h.columns[i].is_not_null; c.is_indexed = h.columns[i].is_indexed;
        s.push_back(c);
    }
    return s;
}
