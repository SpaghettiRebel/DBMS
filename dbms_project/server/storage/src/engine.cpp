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
    ofs.write(reinterpret_cast<const char*>(&header), sizeof(header));
}

TableHeader load_metadata(const std::string& path) {
    TableHeader header;
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) throw std::runtime_error("Metadata not found: " + path);
    if (!ifs.read(reinterpret_cast<char*>(&header), sizeof(header)))
        throw std::runtime_error("Failed to read metadata: " + path);
    if (header.magic_number != 0x44424D53)
        throw std::runtime_error("Corrupted metadata: magic number mismatch in " + path);
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
    } catch (const std::exception& e) {
        std::cerr << "Engine Error: " << e.what() << std::endl;
    }
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
    fs::path meta_path = db_dir / (plan.table_name + ".meta");
    fs::path tbl_path = db_dir / (plan.table_name + ".tbl");
    if (fs::exists(meta_path) || fs::exists(tbl_path))
        throw std::runtime_error("Table already exists");

    TableHeader h; std::memset(&h, 0, sizeof(h)); h.magic_number = 0x44424D53;
    h.column_count = static_cast<uint32_t>(plan.columns.size());
    for (size_t i = 0; i < plan.columns.size(); ++i) {
        std::strncpy(h.columns[i].name, plan.columns[i].name.c_str(), MAX_NAME_LEN);
        h.columns[i].type = (plan.columns[i].type == DataType::INT ? 0 : 1);
        h.columns[i].is_not_null = plan.columns[i].is_not_null;
        h.columns[i].is_indexed = plan.columns[i].is_indexed;
        if (plan.columns[i].default_value.has_value()) {
            h.columns[i].has_default = true;
            if (plan.columns[i].type == DataType::INT) {
                h.columns[i].default_int = std::get<int>(plan.columns[i].default_value->data);
            } else {
                h.columns[i].default_string_id = string_pool->intern(std::get<std::string>(plan.columns[i].default_value->data));
            }
        }
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

    if (plan.target_columns.size() != plan.values.size())
        throw std::runtime_error("Columns and values size mismatch");

    // Check for duplicates and unknown columns
    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        bool found = false;
        for (const auto& col : schema) if (col.name == plan.target_columns[i]) { found = true; break; }
        if (!found) throw std::runtime_error("Unknown column: " + plan.target_columns[i]);
        for (size_t j = i + 1; j < plan.target_columns.size(); ++j)
            if (plan.target_columns[i] == plan.target_columns[j]) throw std::runtime_error("Duplicate column: " + plan.target_columns[i]);
    }

    // 1. Prepare values (handle defaults, NOT NULL, interning)
    std::vector<Value> values(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) {
        bool provided = false;
        for (size_t j = 0; j < plan.target_columns.size(); ++j) {
            if (plan.target_columns[j] == schema[i].name) {
                values[i] = plan.values[j];
                provided = true;
                break;
            }
        }
        if (!provided) {
            if (h.columns[i].has_default) {
                values[i].is_null = false;
                if (h.columns[i].type == 0) values[i].data = h.columns[i].default_int;
                else values[i].data = string_pool->get(h.columns[i].default_string_id);
            } else {
                values[i].is_null = true;
            }
        }

        // NOT NULL check
        if (values[i].is_null && (h.columns[i].is_not_null || h.columns[i].is_indexed))
            throw std::runtime_error("NOT NULL violation on " + schema[i].name);

        // Type validation and String interning
        if (!values[i].is_null) {
            if (h.columns[i].type == 0 && !std::holds_alternative<int>(values[i].data))
                throw std::runtime_error("Type mismatch: expected INT for column " + schema[i].name);
            if (h.columns[i].type == 1) {
                if (!std::holds_alternative<std::string>(values[i].data) && !std::holds_alternative<int>(values[i].data))
                    throw std::runtime_error("Type mismatch: expected STRING for column " + schema[i].name);

                if (std::holds_alternative<std::string>(values[i].data)) {
                    values[i].data = (int)string_pool->intern(std::get<std::string>(values[i].data));
                }
            }
        }
    }

    std::vector<std::string> target_cols;
    for(const auto& c : schema) target_cols.push_back(c.name);
    auto row_bin = RowSerializer::serialize(schema, target_cols, values);
    if (row_bin.size() + sizeof(RecordHeader) > PAGE_SIZE - 4)
        throw std::runtime_error("Record too large");

    Pager tbl_p((db_dir / (plan.table_name + ".tbl")).string());
    Pager idx_p((db_dir / (plan.table_name + ".idx")).string());

    for (size_t i = 0; i < h.column_count; ++i) {
        if (h.columns[i].is_indexed) {
            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
            // Both INT and STRING (via string_id) use int as key
            if (tree.contains(std::get<int>(values[i].data)))
                throw std::runtime_error("Unique violation on " + std::string(h.columns[i].name));
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
            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
            tree.insert({std::get<int>(values[i].data), record_pos});
            h.index_roots[i] = tree.get_root_id();
        }
    }
    tbl_p.file.flush();
    idx_p.file.flush();
    h.row_count++; save_metadata(meta_path, h);
    JournalEntry je = {JournalOp::INSERT, plan.table_name, get_now_timestamp(), record_pos, {}, row_bin};
    journal->log(je);
    // Ensure all changes are flushed to disk
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
                    if (std::holds_alternative<int>(plan.where_clause->right_value.data)) {
                        auto it = tree.find(std::get<int>(plan.where_clause->right_value.data));
                        if (it != tree.end()) pos = it->second;
                    }
                } else {
                    if (std::holds_alternative<std::string>(plan.where_clause->right_value.data)) {
                        auto sid_opt = string_pool->get_id_if_exists(std::get<std::string>(plan.where_clause->right_value.data));
                        if (sid_opt) {
                            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
                            auto it = tree.find((int)*sid_opt);
                            if (it != tree.end()) pos = it->second;
                        }
                    }
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
                            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                            if (h.columns[c].type == 0) {
                                tree.erase(row[h.columns[c].name].get<int>());
                            } else {
                                auto sid_opt = string_pool->get_id_if_exists(row[h.columns[c].name].get<std::string>());
                                if (sid_opt) tree.erase((int)*sid_opt);
                            }
                            h.index_roots[c] = tree.get_root_id();
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
    if (current_db.empty()) throw std::runtime_error("No active DB");
    fs::path db_dir = fs::path(root_path) / current_db;
    auto meta_path = (db_dir / (plan.table_name + ".meta")).string();
    auto h = load_metadata(meta_path);
    auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((db_dir / (plan.table_name + ".tbl")).string());
    Pager idx_p((db_dir / (plan.table_name + ".idx")).string());

    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        char buf[PAGE_SIZE]; tbl_p.read_page(i, buf);
        uint32_t end_off = *reinterpret_cast<uint32_t*>(buf); if (end_off <= 4) continue;
        bool changed_page = false; size_t off = 4;
        while (off < end_off) {
            RecordHeader* rh = (RecordHeader*)(buf + off); size_t data_off = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                size_t t_off = data_off; auto row = RowDeserializer::deserialize(schema, buf, t_off, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                    std::vector<char> old_raw(buf + data_off, buf + data_off + rh->record_size);

                    // Modify row
                    for (size_t col_idx = 0; col_idx < plan.target_columns.size(); ++col_idx) {
                        row[plan.target_columns[col_idx]] = (plan.values[col_idx].is_null ? nullptr :
                            (std::holds_alternative<int>(plan.values[col_idx].data) ?
                                json(std::get<int>(plan.values[col_idx].data)) : json(std::get<std::string>(plan.values[col_idx].data))));
                    }

                    // Re-serialize
                    std::vector<Value> new_vals(schema.size());
                    for (size_t c = 0; c < schema.size(); ++c) {
                        auto val = row[schema[c].name];
                        if (val.is_null()) new_vals[c].is_null = true;
                        else {
                            new_vals[c].is_null = false;
                            if (schema[c].type == 0) new_vals[c].data = val.get<int>();
                            else new_vals[c].data = (int)string_pool->intern(val.get<std::string>());
                        }
                    }

                    std::vector<std::string> all_cols; for(const auto& c : schema) all_cols.push_back(c.name);
                    auto new_bin = RowSerializer::serialize(schema, all_cols, new_vals);

                    if (new_bin.size() == rh->record_size) {
                        // In-place update
                        std::memcpy(buf + data_off, new_bin.data(), new_bin.size());
                        changed_page = true;
                        // Update index if needed (simple: erase old, insert new)
                        for (size_t c = 0; c < schema.size(); ++c) {
                            if (schema[c].is_indexed) {
                                BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                                // Erase old
                                size_t old_toff = 0; auto old_row = RowDeserializer::deserialize(schema, old_raw.data(), old_toff, string_pool.get());
                                if (schema[c].type == 0) tree.erase(old_row[schema[c].name].get<int>());
                                else { auto sid = string_pool->get_id_if_exists(old_row[schema[c].name].get<std::string>()); if(sid) tree.erase((int)*sid); }
                                // Insert new
                                tree.insert({std::get<int>(new_vals[c].data), {i, (uint32_t)off}});
                                h.index_roots[c] = tree.get_root_id();
                            }
                        }
                        JournalEntry je = {JournalOp::UPDATE, plan.table_name, get_now_timestamp(), {i, (uint32_t)off}, old_raw, new_bin};
                        journal->log(je);
                    } else {
                        // Move: mark deleted, insert new
                        rh->is_deleted = true; changed_page = true;
                        h.row_count--;
                        JournalEntry je_del = {JournalOp::DELETE, plan.table_name, get_now_timestamp(), {i, (uint32_t)off}, old_raw, {}};
                        journal->log(je_del);

                        save_metadata(meta_path, h);
                        QueryPlan ip = plan; ip.type = QueryType::INSERT; ip.values = new_vals; ip.target_columns = all_cols;
                        insert_record(ip);
                        h = load_metadata(meta_path); // reload
                    }
                }
            }
            off += sizeof(RecordHeader) + rh->record_size;
        }
        if (changed_page) tbl_p.write_page(i, buf);
    }
    save_metadata(meta_path, h);
}

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    if (current_db.empty()) throw std::runtime_error("No active DB");
    auto entries = journal->get_all_entries();
    Pager tbl_p((fs::path(root_path) / current_db / (table_name + ".tbl")).string());
    Pager idx_p((fs::path(root_path) / current_db / (table_name + ".idx")).string());
    auto h_path = (fs::path(root_path) / current_db / (table_name + ".meta")).string();
    auto h = load_metadata(h_path);
    auto schema = get_table_schema(table_name);

    for (int i = (int)entries.size() - 1; i >= 0; --i) {
        const auto& e = entries[i];
        if (e.timestamp <= timestamp) break;
        if (e.table_name != table_name) continue;

        char buf[PAGE_SIZE]; tbl_p.read_page(e.record_pos.page_id, buf);
        RecordHeader* rh = (RecordHeader*)(buf + e.record_pos.offset);

        if (e.op == JournalOp::INSERT) {
            if (!rh->is_deleted) {
                rh->is_deleted = true; h.row_count--;
                size_t toff = 0; auto row = RowDeserializer::deserialize(schema, buf + e.record_pos.offset + sizeof(RecordHeader), toff, string_pool.get());
                for (size_t c = 0; c < h.column_count; ++c) {
                    if (h.columns[c].is_indexed) {
                        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                        if (h.columns[c].type == 0) tree.erase(row[h.columns[c].name].get<int>());
                        else { auto sid = string_pool->get_id_if_exists(row[h.columns[c].name].get<std::string>()); if(sid) tree.erase((int)*sid); }
                        h.index_roots[c] = tree.get_root_id();
                    }
                }
            }
        } else if (e.op == JournalOp::DELETE) {
            rh->is_deleted = false; h.row_count++;
            std::memcpy(buf + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size());
            size_t toff = 0; auto row = RowDeserializer::deserialize(schema, e.old_data.data(), toff, string_pool.get());
            for (size_t c = 0; c < h.column_count; ++c) {
                if (h.columns[c].is_indexed) {
                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                    if (h.columns[c].type == 0) tree.insert({row[h.columns[c].name].get<int>(), e.record_pos});
                    else tree.insert({(int)string_pool->intern(row[h.columns[c].name].get<std::string>()), e.record_pos});
                    h.index_roots[c] = tree.get_root_id();
                }
            }
        } else if (e.op == JournalOp::UPDATE) {
            size_t toff_new = 0; auto row_new = RowDeserializer::deserialize(schema, buf + e.record_pos.offset + sizeof(RecordHeader), toff_new, string_pool.get());
            for (size_t c = 0; c < h.column_count; ++c) {
                if (h.columns[c].is_indexed) {
                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                    if (h.columns[c].type == 0) tree.erase(row_new[h.columns[c].name].get<int>());
                    else { auto sid = string_pool->get_id_if_exists(row_new[h.columns[c].name].get<std::string>()); if(sid) tree.erase((int)*sid); }
                    h.index_roots[c] = tree.get_root_id();
                }
            }
            std::memcpy(buf + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size());
            rh->record_size = (uint32_t)e.old_data.size();
            size_t toff_old = 0; auto row_old = RowDeserializer::deserialize(schema, e.old_data.data(), toff_old, string_pool.get());
            for (size_t c = 0; c < h.column_count; ++c) {
                if (h.columns[c].is_indexed) {
                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                    if (h.columns[c].type == 0) tree.insert({row_old[h.columns[c].name].get<int>(), e.record_pos});
                    else tree.insert({(int)string_pool->intern(row_old[h.columns[c].name].get<std::string>()), e.record_pos});
                    h.index_roots[c] = tree.get_root_id();
                }
            }
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
