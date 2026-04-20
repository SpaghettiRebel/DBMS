#include "../include/engine.h"

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

#include "../include/serializer.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {
constexpr uint32_t kMagicNumber = 0x44424D53;
constexpr size_t kPageHeaderSize = sizeof(uint32_t);

struct RecordHeader {
    bool is_deleted;
    uint32_t record_size;
};

bool same_pos(const pos_t& a, const pos_t& b) { return a.page_id == b.page_id && a.offset == b.offset; }

bool is_live_record_layout(size_t off, uint32_t end_off, const RecordHeader* rh) {
    if (!rh) return false;
    const size_t header_end = off + sizeof(RecordHeader);
    const size_t payload_end = header_end + rh->record_size;
    return header_end <= end_off && payload_end <= end_off && payload_end <= PAGE_SIZE;
}

std::string get_now_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm bt{};
    localtime_r(&t, &bt);

    std::ostringstream oss;
    oss << std::put_time(&bt, "%Y.%m.%d-%H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

void ensure_parent_dir(const fs::path& path) {
    const auto parent = path.parent_path();
    if (!parent.empty() && !fs::exists(parent)) {
        fs::create_directories(parent);
    }
}

void save_metadata_atomic(const std::string& path, const TableHeader& header) {
    fs::path p(path);
    ensure_parent_dir(p);

    fs::path tmp = p;
    tmp += ".tmp";

    {
        std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
        if (!ofs) throw std::runtime_error("Failed to open metadata temp file: " + tmp.string());

        ofs.write(reinterpret_cast<const char*>(&header), sizeof(header));
        if (!ofs) throw std::runtime_error("Failed to write metadata temp file: " + tmp.string());
        ofs.flush();
        if (!ofs) throw std::runtime_error("Failed to flush metadata temp file: " + tmp.string());
    }

    if (fs::exists(p)) {
        fs::remove(p);
    }

    fs::rename(tmp, p);
}

TableHeader load_metadata_checked(const std::string& path) {
    TableHeader header{};
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) throw std::runtime_error("Metadata not found: " + path);

    if (!ifs.read(reinterpret_cast<char*>(&header), sizeof(header))) {
        throw std::runtime_error("Failed to read metadata: " + path);
    }

    if (header.magic_number != kMagicNumber) {
        throw std::runtime_error("Corrupted metadata: magic number mismatch in " + path);
    }

    const size_t cap = sizeof(header.columns) / sizeof(header.columns[0]);
    if (header.column_count > cap) {
        throw std::runtime_error("Corrupted metadata: column_count out of bounds in " + path);
    }

    return header;
}

std::vector<char> read_page_copy(Pager& pager, uint32_t page_id) {
    std::vector<char> buf(PAGE_SIZE, 0);
    pager.read_page(page_id, buf.data());
    return buf;
}

void write_page_copy(Pager& pager, uint32_t page_id, const std::vector<char>& buf) {
    if (buf.size() != PAGE_SIZE) {
        throw std::runtime_error("Invalid page buffer size");
    }
    pager.write_page(page_id, const_cast<char*>(buf.data()));
}

std::optional<uint32_t> resolve_string_id(StringPool* pool, const std::string& s, bool allow_create) {
    if (!pool) return std::nullopt;
    if (allow_create) {
        return pool->intern(s);
    }
    return pool->get_id_if_exists(s);
}

bool json_to_storage_value(const json& src, bool is_int_type, Value& out, StringPool* pool, bool allow_create_strings) {
    if (src.is_null()) {
        out.is_null = true;
        out.data = 0;
        return true;
    }

    out.is_null = false;

    if (is_int_type) {
        if (!src.is_number_integer()) return false;
        out.data = src.get<int>();
        return true;
    }

    if (src.is_number_integer()) {
        out.data = src.get<int>();
        return true;
    }

    if (src.is_string()) {
        auto id = resolve_string_id(pool, src.get_ref<const std::string&>(), allow_create_strings);
        if (!id.has_value()) return false;
        out.data = static_cast<int>(*id);
        return true;
    }

    return false;
}

bool value_to_json_scalar(const Value& v, json& out) {
    if (v.is_null) {
        out = nullptr;
        return true;
    }

    if (std::holds_alternative<int>(v.data)) {
        out = std::get<int>(v.data);
        return true;
    }

    if (std::holds_alternative<std::string>(v.data)) {
        out = std::get<std::string>(v.data);
        return true;
    }

    return false;
}

bool value_to_storage_key(const Value& v, bool is_int_type, int& key, StringPool* pool, bool allow_create_strings) {
    if (v.is_null) return false;

    if (is_int_type) {
        if (!std::holds_alternative<int>(v.data)) return false;
        key = std::get<int>(v.data);
        return true;
    }

    if (std::holds_alternative<int>(v.data)) {
        key = std::get<int>(v.data);
        return true;
    }

    if (std::holds_alternative<std::string>(v.data)) {
        auto id = resolve_string_id(pool, std::get<std::string>(v.data), allow_create_strings);
        if (!id.has_value()) return false;
        key = static_cast<int>(*id);
        return true;
    }

    return false;
}

bool json_to_storage_key(const json& src, bool is_int_type, int& key, StringPool* pool, bool allow_create_strings) {
    if (src.is_null()) return false;

    if (is_int_type) {
        if (!src.is_number_integer()) return false;
        key = src.get<int>();
        return true;
    }

    if (src.is_number_integer()) {
        key = src.get<int>();
        return true;
    }

    if (src.is_string()) {
        auto id = resolve_string_id(pool, src.get_ref<const std::string&>(), allow_create_strings);
        if (!id.has_value()) return false;
        key = static_cast<int>(*id);
        return true;
    }

    return false;
}

std::unordered_map<std::string, size_t> build_column_index(const std::vector<ColumnDef>& schema) {
    std::unordered_map<std::string, size_t> map;
    map.reserve(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) {
        map.emplace(schema[i].name, i);
    }
    return map;
}

bool build_insert_values(const QueryPlan& plan, const std::vector<ColumnDef>& schema, const TableHeader& h,
    StringPool* pool, std::vector<Value>& out_values) {
    auto col_index = build_column_index(schema);

    if (plan.target_columns.size() != plan.values.size()) {
        throw std::runtime_error("Columns and values size mismatch");
    }

    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        if (!col_index.contains(plan.target_columns[i])) {
            throw std::runtime_error("Unknown column: " + plan.target_columns[i]);
        }
        for (size_t j = i + 1; j < plan.target_columns.size(); ++j) {
            if (plan.target_columns[i] == plan.target_columns[j]) {
                throw std::runtime_error("Duplicate column: " + plan.target_columns[i]);
            }
        }
    }

    out_values.resize(schema.size());

    for (size_t i = 0; i < schema.size(); ++i) {
        bool provided = false;
        for (size_t j = 0; j < plan.target_columns.size(); ++j) {
            if (plan.target_columns[j] == schema[i].name) {
                if (!json_to_storage_value(value_to_json_scalar(plan.values[j]), schema[i].type == DataType::INT,
                        out_values[i], pool, true)) {
                    throw std::runtime_error("Type mismatch in column: " + schema[i].name);
                }
                provided = true;
                break;
            }
        }

        if (!provided) {
            if (h.columns[i].has_default) {
                out_values[i].is_null = false;
                if (h.columns[i].type == 0) {
                    out_values[i].data = h.columns[i].default_int;
                } else {
                    out_values[i].data = static_cast<int>(h.columns[i].default_string_id);
                }
            } else {
                out_values[i].is_null = true;
                out_values[i].data = 0;
            }
        }

        if (out_values[i].is_null && (h.columns[i].is_not_null || h.columns[i].is_indexed)) {
            throw std::runtime_error("NOT NULL violation on " + schema[i].name);
        }

        if (!out_values[i].is_null) {
            if (h.columns[i].type == 0 && !std::holds_alternative<int>(out_values[i].data)) {
                throw std::runtime_error("Type mismatch: expected INT for column " + schema[i].name);
            }

            if (h.columns[i].type == 1) {
                if (!std::holds_alternative<int>(out_values[i].data) &&
                    !std::holds_alternative<std::string>(out_values[i].data)) {
                    throw std::runtime_error("Type mismatch: expected STRING for column " + schema[i].name);
                }

                if (std::holds_alternative<std::string>(out_values[i].data)) {
                    auto id = resolve_string_id(pool, std::get<std::string>(out_values[i].data), true);
                    if (!id.has_value()) throw std::runtime_error("String interning failed");
                    out_values[i].data = static_cast<int>(*id);
                }
            }
        }
    }

    return true;
}

bool build_row_values_from_json(const json& row, const std::vector<ColumnDef>& schema, const TableHeader& h,
    StringPool* pool, std::vector<Value>& out_values, bool allow_create_strings) {
    out_values.resize(schema.size());

    for (size_t i = 0; i < schema.size(); ++i) {
        json cell = row.contains(schema[i].name) ? row.at(schema[i].name) : json(nullptr);
        if (!json_to_storage_value(cell, schema[i].type == DataType::INT, out_values[i], pool, allow_create_strings)) {
            throw std::runtime_error("Type mismatch in column: " + schema[i].name);
        }

        if (out_values[i].is_null && (h.columns[i].is_not_null || h.columns[i].is_indexed)) {
            throw std::runtime_error("NOT NULL violation on " + schema[i].name);
        }
    }

    return true;
}

json payload_to_json_row(const std::vector<char>& payload, const std::vector<ColumnDef>& schema, StringPool* pool) {
    std::vector<char> tmp(payload.size() + kPageHeaderSize, 0);
    if (!payload.empty()) {
        std::memcpy(tmp.data() + kPageHeaderSize, payload.data(), payload.size());
    }
    size_t offset = kPageHeaderSize;
    return RowDeserializer::deserialize(schema, tmp.data(), offset, pool);
}

std::optional<pos_t> find_live_record_by_payload(Pager& tbl_p, const TableHeader& h,
    const std::vector<ColumnDef>& schema, const std::vector<char>& payload, StringPool* pool) {
    for (uint32_t page_id = 0; page_id <= h.last_data_page; ++page_id) {
        std::vector<char> buf(PAGE_SIZE, 0);
        tbl_p.read_page(page_id, buf.data());

        uint32_t end_off = *reinterpret_cast<uint32_t*>(buf.data());
        if (end_off <= kPageHeaderSize || end_off > PAGE_SIZE) continue;

        size_t off = kPageHeaderSize;
        while (off < end_off) {
            if (off + sizeof(RecordHeader) > end_off) break;
            auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + off);
            if (!is_live_record_layout(off, end_off, rh)) break;

            const size_t data_off = off + sizeof(RecordHeader);
            if (!rh->is_deleted && rh->record_size == payload.size()) {
                if (std::memcmp(buf.data() + data_off, payload.data(), payload.size()) == 0) {
                    return pos_t{page_id, static_cast<uint32_t>(off)};
                }
            }

            off += sizeof(RecordHeader) + rh->record_size;
        }
    }

    return std::nullopt;
}

bool tree_contains_key(BP_tree<int, pos_t>& tree, int key) {
    auto it = tree.find(key);
    return it != tree.end();
}

std::optional<pos_t> tree_lookup(BP_tree<int, pos_t>& tree, int key) {
    auto it = tree.find(key);
    if (it == tree.end()) return std::nullopt;
    return it->second;
}

bool index_key_unique_for_pos(BP_tree<int, pos_t>& tree, int key, const pos_t& current_pos) {
    auto it = tree.find(key);
    if (it == tree.end()) return true;
    return same_pos(it->second, current_pos);
}

bool is_valid_page_and_offset(const pos_t& p) { return p.is_valid() && p.offset < PAGE_SIZE; }

}  // namespace

class ConditionEvaluator {
public:
    static bool compare_scalar(const json& lhs, const Value& rhs, OpType op) {
        if (lhs.is_null()) return false;

        if (std::holds_alternative<int>(rhs.data)) {
            if (!lhs.is_number_integer()) return false;
            const int l = lhs.get<int>();
            const int r = std::get<int>(rhs.data);

            switch (op) {
                case OpType::EQ:
                    return l == r;
                case OpType::NEQ:
                    return l != r;
                case OpType::LESS:
                    return l < r;
                case OpType::GREATER:
                    return l > r;
                case OpType::LEQ:
                    return l <= r;
                case OpType::GEQ:
                    return l >= r;
                default:
                    return false;
            }
        }

        if (std::holds_alternative<std::string>(rhs.data)) {
            if (!lhs.is_string()) return false;
            const auto& l = lhs.get_ref<const std::string&>();
            const auto& r = std::get<std::string>(rhs.data);

            switch (op) {
                case OpType::EQ:
                    return l == r;
                case OpType::NEQ:
                    return l != r;
                case OpType::LESS:
                    return l < r;
                case OpType::GREATER:
                    return l > r;
                case OpType::LEQ:
                    return l <= r;
                case OpType::GEQ:
                    return l >= r;
                default:
                    return false;
            }
        }

        return false;
    }

    static bool evaluate(const json& row, const ConditionNode* node) {
        if (!node) return true;

        if (!node->is_leaf) {
            if (node->logical_op == LogicalOpType::AND) {
                return evaluate(row, node->left_child.get()) && evaluate(row, node->right_child.get());
            }
            if (node->logical_op == LogicalOpType::OR) {
                return evaluate(row, node->left_child.get()) || evaluate(row, node->right_child.get());
            }
            return true;
        }

        if (!row.contains(node->left_column)) return false;
        const json& val = row.at(node->left_column);

        switch (node->op) {
            case OpType::EQ:
            case OpType::NEQ:
            case OpType::LESS:
            case OpType::GREATER:
            case OpType::LEQ:
            case OpType::GEQ:
                return compare_scalar(val, node->right_value, node->op);

            case OpType::BETWEEN: {
                if (!node->right_value_between) return false;
                if (val.is_null()) return false;

                if (std::holds_alternative<int>(node->right_value.data)) {
                    if (!val.is_number_integer()) return false;
                    const int v = val.get<int>();
                    const int lo = std::get<int>(node->right_value.data);
                    const int hi = std::get<int>(node->right_value_between->data);
                    return v >= lo && v < hi;
                }

                if (std::holds_alternative<std::string>(node->right_value.data)) {
                    if (!val.is_string()) return false;
                    const auto& v = val.get_ref<const std::string&>();
                    const auto& lo = std::get<std::string>(node->right_value.data);
                    const auto& hi = std::get<std::string>(node->right_value_between->data);
                    return v >= lo && v < hi;
                }

                return false;
            }

            case OpType::LIKE: {
                if (!val.is_string()) return false;
                try {
                    std::regex re(std::get<std::string>(node->right_value.data));
                    return std::regex_match(val.get_ref<const std::string&>(), re);
                } catch (...) {
                    return false;
                }
            }

            default:
                return false;
        }
    }
};

Engine::Engine(std::string root) : root_path(std::move(root)) {
    if (!fs::exists(root_path)) {
        fs::create_directories(root_path);
    }
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
        switch (plan.type) {
            case QueryType::CREATE_DATABASE:
                create_database(plan.database_name);
                break;
            case QueryType::DROP_DATABASE:
                drop_database(plan.database_name);
                break;
            case QueryType::USE_DATABASE:
                use_database(plan.database_name);
                break;
            case QueryType::CREATE_TABLE:
                create_table(plan);
                break;
            case QueryType::DROP_TABLE:
                drop_table(plan.table_name);
                break;
            case QueryType::INSERT:
                insert_record(plan);
                break;
            case QueryType::SELECT:
                std::cout << select_records(plan) << std::endl;
                break;
            case QueryType::REVERT:
                revert(plan.table_name, plan.timestamp);
                break;
            case QueryType::UPDATE:
                update_records(plan);
                break;
            case QueryType::DELETE:
                delete_records(plan);
                break;
        }
    } catch (const std::exception& e) {
        std::cerr << "Engine Error: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Engine Error: unknown failure" << std::endl;
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

    const auto removed = fs::remove_all(db_path);
    if (removed == 0) {
        throw std::runtime_error("Failed to remove DB: " + name);
    }

    if (current_db == name) {
        current_db.clear();
        string_pool.reset();
        journal.reset();
    }
}

void Engine::create_table(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    fs::path db_dir = fs::path(root_path) / current_db;
    fs::path meta_path = db_dir / (plan.table_name + ".meta");
    fs::path tbl_path = db_dir / (plan.table_name + ".tbl");
    fs::path idx_path = db_dir / (plan.table_name + ".idx");

    if (fs::exists(meta_path) || fs::exists(tbl_path) || fs::exists(idx_path)) {
        throw std::runtime_error("Table already exists");
    }

    TableHeader h{};
    std::memset(&h, 0, sizeof(h));
    h.magic_number = kMagicNumber;
    h.column_count = static_cast<uint32_t>(plan.columns.size());

    const size_t cap = sizeof(h.columns) / sizeof(h.columns[0]);
    if (plan.columns.size() > cap) {
        throw std::runtime_error("Too many columns");
    }

    for (size_t i = 0; i < plan.columns.size(); ++i) {
        if (plan.columns[i].name.empty()) {
            throw std::runtime_error("Empty column name");
        }

        std::strncpy(h.columns[i].name, plan.columns[i].name.c_str(), MAX_NAME_LEN - 1);
        h.columns[i].name[MAX_NAME_LEN - 1] = '\0';

        h.columns[i].type = (plan.columns[i].type == DataType::INT ? 0 : 1);
        h.columns[i].is_not_null = plan.columns[i].is_not_null;
        h.columns[i].is_indexed = plan.columns[i].is_indexed;

        if (plan.columns[i].default_value.has_value()) {
            h.columns[i].has_default = true;
            if (plan.columns[i].type == DataType::INT) {
                if (!std::holds_alternative<int>(plan.columns[i].default_value->data)) {
                    throw std::runtime_error("DEFAULT type mismatch for INT column " + plan.columns[i].name);
                }
                h.columns[i].default_int = std::get<int>(plan.columns[i].default_value->data);
            } else {
                if (!std::holds_alternative<std::string>(plan.columns[i].default_value->data)) {
                    throw std::runtime_error("DEFAULT type mismatch for STRING column " + plan.columns[i].name);
                }
                auto id = string_pool->intern(std::get<std::string>(plan.columns[i].default_value->data));
                h.columns[i].default_string_id = id;
            }
        }
    }

    save_metadata_atomic(meta_path.string(), h);

    Pager tbl_p(tbl_path.string());
    tbl_p.allocate_page();

    // ensure idx file exists
    Pager idx_p(idx_path.string());
    (void)idx_p;
}

void Engine::drop_table(const std::string& table_name) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    fs::path db_dir = fs::path(root_path) / current_db;
    fs::path tbl = db_dir / (table_name + ".tbl");
    fs::path meta = db_dir / (table_name + ".meta");
    fs::path idx = db_dir / (table_name + ".idx");

    auto remove_required = [](const fs::path& p) {
        if (fs::exists(p) && !fs::remove(p)) {
            throw std::runtime_error("Failed to remove file: " + p.string());
        }
    };

    remove_required(tbl);
    remove_required(meta);
    remove_required(idx);
}

void Engine::insert_record(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    fs::path db_dir = fs::path(root_path) / current_db;
    const std::string meta_path = (db_dir / (plan.table_name + ".meta")).string();
    const std::string tbl_path = (db_dir / (plan.table_name + ".tbl")).string();
    const std::string idx_path = (db_dir / (plan.table_name + ".idx")).string();

    auto h = load_metadata_checked(meta_path);
    auto schema = get_table_schema(plan.table_name);

    std::vector<Value> values;
    build_insert_values(plan, schema, h, string_pool.get(), values);

    std::vector<std::string> all_cols;
    all_cols.reserve(schema.size());
    for (const auto& c : schema) all_cols.push_back(c.name);

    auto row_bin = RowSerializer::serialize(schema, all_cols, values);
    if (row_bin.size() + sizeof(RecordHeader) > PAGE_SIZE - kPageHeaderSize) {
        throw std::runtime_error("Record too large");
    }

    Pager tbl_p(tbl_path);
    Pager idx_p(idx_path);

    TableHeader h_before = h;
    std::vector<char> last_page_backup;
    uint32_t last_page_id_before = h.last_data_page;
    bool used_new_page = false;

    if (h.last_data_page == 0 && fs::file_size(tbl_path) == 0) {
        // first page is usually allocated during create_table; this is just defensive
        tbl_p.allocate_page();
    }

    last_page_backup = read_page_copy(tbl_p, h.last_data_page);

    // check uniqueness before any write
    for (size_t i = 0; i < h.column_count; ++i) {
        if (!h.columns[i].is_indexed) continue;

        int key = 0;
        if (!value_to_storage_key(values[i], h.columns[i].type == 0, key, string_pool.get(), true)) {
            throw std::runtime_error("Index key conversion failed on " + std::string(h.columns[i].name));
        }

        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
        if (tree_contains_key(tree, key)) {
            throw std::runtime_error("Unique violation on " + std::string(h.columns[i].name));
        }
    }

    pos_t record_pos;
    try {
        std::vector<char> page_buf(PAGE_SIZE, 0);
        tbl_p.read_page(h.last_data_page, page_buf.data());

        uint32_t offset = *reinterpret_cast<uint32_t*>(page_buf.data());
        if (offset == 0) offset = 4;
        if (offset > PAGE_SIZE) throw std::runtime_error("Corrupted page layout");

        if (offset + sizeof(RecordHeader) + row_bin.size() > PAGE_SIZE) {
            h.last_data_page = tbl_p.allocate_page();
            used_new_page = true;
            std::memset(page_buf.data(), 0, PAGE_SIZE);
            offset = 4;
        }

        if (used_new_page) {
            page_buf.assign(PAGE_SIZE, 0);
            tbl_p.read_page(h.last_data_page, page_buf.data());
            offset = 4;
        }

        RecordHeader rh{false, static_cast<uint32_t>(row_bin.size())};
        std::memcpy(page_buf.data() + offset, &rh, sizeof(rh));
        std::memcpy(page_buf.data() + offset + sizeof(rh), row_bin.data(), row_bin.size());

        record_pos = pos_t{h.last_data_page, static_cast<uint32_t>(offset)};
        *reinterpret_cast<uint32_t*>(page_buf.data()) = offset + sizeof(rh) + static_cast<uint32_t>(row_bin.size());
        tbl_p.write_page(h.last_data_page, page_buf.data());

        for (size_t i = 0; i < h.column_count; ++i) {
            if (!h.columns[i].is_indexed) continue;

            int key = 0;
            if (!value_to_storage_key(values[i], h.columns[i].type == 0, key, string_pool.get(), true)) {
                throw std::runtime_error("Index key conversion failed on " + std::string(h.columns[i].name));
            }

            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);
            tree.insert({key, record_pos});
            h.index_roots[i] = tree.get_root_id();
        }

        h.row_count++;
        save_metadata_atomic(meta_path, h);

        if (journal) {
            JournalEntry je{JournalOp::INSERT, plan.table_name, get_now_timestamp(), record_pos, {}, row_bin};
            journal->log(je);
        }

        tbl_p.file.flush();
        idx_p.file.flush();
    } catch (...) {
        // rollback best-effort
        try {
            if (used_new_page) {
                std::vector<char> zero(PAGE_SIZE, 0);
                tbl_p.write_page(h.last_data_page, zero.data());
            } else {
                tbl_p.write_page(last_page_id_before, last_page_backup.data());
            }
        } catch (...) {
        }

        h = h_before;
        try {
            save_metadata_atomic(meta_path, h);
        } catch (...) {
        }
        throw;
    }
}

std::string Engine::select_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    fs::path db_dir = fs::path(root_path) / current_db;
    auto h = load_metadata_checked((db_dir / (plan.table_name + ".meta")).string());
    auto schema = get_table_schema(plan.table_name);
    Pager tbl_p((db_dir / (plan.table_name + ".tbl")).string());
    Pager idx_p((db_dir / (plan.table_name + ".idx")).string());
    json res = json::array();

    // index-accelerated path only for EQ on indexed single column
    if (plan.where_clause && plan.where_clause->is_leaf && plan.where_clause->op == OpType::EQ) {
        for (size_t i = 0; i < h.column_count; ++i) {
            if (!h.columns[i].is_indexed || h.columns[i].name != plan.where_clause->left_column) continue;

            pos_t pos = pos_t::invalid();
            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[i]);

            if (h.columns[i].type == 0) {
                if (std::holds_alternative<int>(plan.where_clause->right_value.data)) {
                    auto found = tree_lookup(tree, std::get<int>(plan.where_clause->right_value.data));
                    if (found.has_value()) pos = *found;
                }
            } else {
                if (std::holds_alternative<std::string>(plan.where_clause->right_value.data)) {
                    auto sid =
                        string_pool->get_id_if_exists(std::get<std::string>(plan.where_clause->right_value.data));
                    if (sid.has_value()) {
                        auto found = tree_lookup(tree, static_cast<int>(*sid));
                        if (found.has_value()) pos = *found;
                    }
                }
            }

            if (is_valid_page_and_offset(pos)) {
                std::vector<char> buf(PAGE_SIZE, 0);
                tbl_p.read_page(pos.page_id, buf.data());

                if (pos.offset + sizeof(RecordHeader) <= PAGE_SIZE) {
                    auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + pos.offset);
                    if (!rh->is_deleted &&
                        is_live_record_layout(pos.offset, *reinterpret_cast<uint32_t*>(buf.data()), rh)) {
                        size_t d_off = pos.offset + sizeof(RecordHeader);
                        res.push_back(RowDeserializer::deserialize(schema, buf.data(), d_off, string_pool.get()));
                    }
                }
            }

            return res.dump(4);
        }
    }

    for (uint32_t i = 0; i <= h.last_data_page; ++i) {
        std::vector<char> buf(PAGE_SIZE, 0);
        tbl_p.read_page(i, buf.data());

        uint32_t end_off = *reinterpret_cast<uint32_t*>(buf.data());
        if (end_off <= kPageHeaderSize || end_off > PAGE_SIZE) continue;

        size_t off = kPageHeaderSize;
        while (off < end_off) {
            if (off + sizeof(RecordHeader) > end_off) break;
            auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + off);
            if (!is_live_record_layout(off, end_off, rh)) break;

            size_t data_off = off + sizeof(RecordHeader);
            if (!rh->is_deleted) {
                auto row = RowDeserializer::deserialize(schema, buf.data(), data_off, string_pool.get());
                if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                    res.push_back(row);
                }
            }

            off += sizeof(RecordHeader) + rh->record_size;
        }
    }

    return res.dump(4);
}

void Engine::delete_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    fs::path db_dir = fs::path(root_path) / current_db;
    const std::string meta_path = (db_dir / (plan.table_name + ".meta")).string();
    const std::string tbl_path = (db_dir / (plan.table_name + ".tbl")).string();
    const std::string idx_path = (db_dir / (plan.table_name + ".idx")).string();

    auto h = load_metadata_checked(meta_path);
    auto schema = get_table_schema(plan.table_name);
    Pager tbl_p(tbl_path);
    Pager idx_p(idx_path);

    TableHeader h_before = h;
    std::vector<JournalEntry> logs;

    try {
        for (uint32_t page_id = 0; page_id <= h.last_data_page; ++page_id) {
            std::vector<char> buf(PAGE_SIZE, 0);
            tbl_p.read_page(page_id, buf.data());

            uint32_t end_off = *reinterpret_cast<uint32_t*>(buf.data());
            if (end_off <= kPageHeaderSize || end_off > PAGE_SIZE) continue;

            bool page_changed = false;
            size_t off = kPageHeaderSize;

            while (off < end_off) {
                if (off + sizeof(RecordHeader) > end_off) break;
                auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + off);
                if (!is_live_record_layout(off, end_off, rh)) break;

                size_t data_off = off + sizeof(RecordHeader);
                if (!rh->is_deleted) {
                    auto row = RowDeserializer::deserialize(schema, buf.data(), data_off, string_pool.get());

                    if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                        std::vector<char> old_payload(buf.begin() + data_off, buf.begin() + data_off + rh->record_size);

                        for (size_t c = 0; c < h.column_count; ++c) {
                            if (!h.columns[c].is_indexed) continue;

                            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                            int key = 0;

                            if (h.columns[c].type == 0) {
                                if (!row.contains(h.columns[c].name) ||
                                    !row.at(h.columns[c].name).is_number_integer()) {
                                    throw std::runtime_error("Corrupted row for indexed INT column");
                                }
                                key = row.at(h.columns[c].name).get<int>();
                            } else {
                                if (!row.contains(h.columns[c].name)) {
                                    throw std::runtime_error("Corrupted row for indexed STRING column");
                                }
                                const auto& cell = row.at(h.columns[c].name);
                                if (cell.is_string()) {
                                    auto sid = string_pool->get_id_if_exists(cell.get_ref<const std::string&>());
                                    if (!sid.has_value())
                                        throw std::runtime_error("String pool corruption during DELETE");
                                    key = static_cast<int>(*sid);
                                } else if (cell.is_number_integer()) {
                                    key = cell.get<int>();
                                } else {
                                    throw std::runtime_error("Corrupted row for indexed STRING column");
                                }
                            }

                            tree.erase(key);
                            h.index_roots[c] = tree.get_root_id();
                        }

                        rh->is_deleted = true;
                        h.row_count--;
                        logs.push_back(JournalEntry{JournalOp::DELETE, plan.table_name, get_now_timestamp(),
                            pos_t{page_id, static_cast<uint32_t>(off)}, old_payload, {}});

                        page_changed = true;
                    }
                }

                off += sizeof(RecordHeader) + rh->record_size;
            }

            if (page_changed) {
                tbl_p.write_page(page_id, buf.data());
            }
        }

        save_metadata_atomic(meta_path, h);

        if (journal) {
            for (const auto& e : logs) journal->log(e);
        }
    } catch (...) {
        h = h_before;
        try {
            save_metadata_atomic(meta_path, h);
        } catch (...) {
        }
        throw;
    }
}

void Engine::update_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    fs::path db_dir = fs::path(root_path) / current_db;
    const std::string meta_path = (db_dir / (plan.table_name + ".meta")).string();
    const std::string tbl_path = (db_dir / (plan.table_name + ".tbl")).string();
    const std::string idx_path = (db_dir / (plan.table_name + ".idx")).string();

    auto h = load_metadata_checked(meta_path);
    auto schema = get_table_schema(plan.table_name);
    Pager tbl_p(tbl_path);
    Pager idx_p(idx_path);

    if (plan.target_columns.size() != plan.values.size()) {
        throw std::runtime_error("Columns and values size mismatch");
    }

    auto col_index = build_column_index(schema);
    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        if (!col_index.contains(plan.target_columns[i])) {
            throw std::runtime_error("Unknown column: " + plan.target_columns[i]);
        }
        for (size_t j = i + 1; j < plan.target_columns.size(); ++j) {
            if (plan.target_columns[i] == plan.target_columns[j]) {
                throw std::runtime_error("Duplicate column: " + plan.target_columns[i]);
            }
        }
    }

    TableHeader h_before = h;
    std::vector<JournalEntry> logs;

    try {
        for (uint32_t page_id = 0; page_id <= h.last_data_page; ++page_id) {
            std::vector<char> buf(PAGE_SIZE, 0);
            tbl_p.read_page(page_id, buf.data());

            uint32_t end_off = *reinterpret_cast<uint32_t*>(buf.data());
            if (end_off <= kPageHeaderSize || end_off > PAGE_SIZE) continue;

            bool page_changed = false;
            size_t off = kPageHeaderSize;

            while (off < end_off) {
                if (off + sizeof(RecordHeader) > end_off) break;
                auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + off);
                if (!is_live_record_layout(off, end_off, rh)) break;

                size_t data_off = off + sizeof(RecordHeader);
                if (!rh->is_deleted) {
                    auto row = RowDeserializer::deserialize(schema, buf.data(), data_off, string_pool.get());

                    if (ConditionEvaluator::evaluate(row, plan.where_clause.get())) {
                        std::vector<char> old_payload(buf.begin() + data_off, buf.begin() + data_off + rh->record_size);

                        json updated_row = row;
                        for (size_t i = 0; i < plan.target_columns.size(); ++i) {
                            updated_row[plan.target_columns[i]] = value_to_json_scalar(plan.values[i]);
                        }

                        std::vector<Value> new_values;
                        build_row_values_from_json(updated_row, schema, h, string_pool.get(), new_values, true);

                        std::vector<std::string> all_cols;
                        all_cols.reserve(schema.size());
                        for (const auto& c : schema) all_cols.push_back(c.name);
                        auto new_payload = RowSerializer::serialize(schema, all_cols, new_values);

                        // Pre-validate indexed uniqueness BEFORE mutating anything
                        for (size_t c = 0; c < h.column_count; ++c) {
                            if (!h.columns[c].is_indexed) continue;

                            int old_key = 0;
                            int new_key = 0;

                            if (!json_to_storage_key(row.at(h.columns[c].name), h.columns[c].type == 0, old_key,
                                    string_pool.get(), false)) {
                                throw std::runtime_error(
                                    "Failed to extract old index key on " + std::string(h.columns[c].name));
                            }
                            if (!value_to_storage_key(
                                    new_values[c], h.columns[c].type == 0, new_key, string_pool.get(), true)) {
                                throw std::runtime_error(
                                    "Failed to extract new index key on " + std::string(h.columns[c].name));
                            }

                            BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                            const pos_t current_pos{page_id, static_cast<uint32_t>(off)};

                            // if key changes, it must be unique; if key same, current row is allowed
                            if (new_key != old_key) {
                                if (!index_key_unique_for_pos(tree, new_key, current_pos)) {
                                    throw std::runtime_error("Unique violation on " + std::string(h.columns[c].name));
                                }
                            }
                        }

                        const bool same_size = (new_payload.size() == rh->record_size);
                        const pos_t old_pos{page_id, static_cast<uint32_t>(off)};
                        pos_t new_pos = old_pos;

                        if (same_size) {
                            // in-place update
                            std::memcpy(buf.data() + data_off, new_payload.data(), new_payload.size());

                            for (size_t c = 0; c < h.column_count; ++c) {
                                if (!h.columns[c].is_indexed) continue;

                                int old_key = 0;
                                int new_key = 0;
                                if (!json_to_storage_key(row.at(h.columns[c].name), h.columns[c].type == 0, old_key,
                                        string_pool.get(), false)) {
                                    throw std::runtime_error(
                                        "Failed to extract old index key on " + std::string(h.columns[c].name));
                                }
                                if (!value_to_storage_key(
                                        new_values[c], h.columns[c].type == 0, new_key, string_pool.get(), true)) {
                                    throw std::runtime_error(
                                        "Failed to extract new index key on " + std::string(h.columns[c].name));
                                }

                                if (old_key != new_key) {
                                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                                    tree.erase(old_key);
                                    tree.insert({new_key, old_pos});
                                    h.index_roots[c] = tree.get_root_id();
                                }
                            }

                            logs.push_back(JournalEntry{JournalOp::UPDATE, plan.table_name, get_now_timestamp(),
                                old_pos, old_payload, new_payload});

                            page_changed = true;
                        } else {
                            // resize: append new row, then tombstone old row
                            std::vector<char> append_backup = read_page_copy(tbl_p, h.last_data_page);
                            uint32_t old_last_page = h.last_data_page;

                            // append new physical record
                            std::vector<char> append_buf(PAGE_SIZE, 0);
                            tbl_p.read_page(h.last_data_page, append_buf.data());
                            uint32_t offset2 = *reinterpret_cast<uint32_t*>(append_buf.data());
                            if (offset2 == 0) offset2 = 4;
                            if (offset2 > PAGE_SIZE) throw std::runtime_error("Corrupted page layout");

                            if (offset2 + sizeof(RecordHeader) + new_payload.size() > PAGE_SIZE) {
                                h.last_data_page = tbl_p.allocate_page();
                                std::memset(append_buf.data(), 0, PAGE_SIZE);
                                tbl_p.read_page(h.last_data_page, append_buf.data());
                                offset2 = 4;
                            }

                            RecordHeader new_rh{false, static_cast<uint32_t>(new_payload.size())};
                            std::memcpy(append_buf.data() + offset2, &new_rh, sizeof(new_rh));
                            std::memcpy(
                                append_buf.data() + offset2 + sizeof(new_rh), new_payload.data(), new_payload.size());
                            new_pos = pos_t{h.last_data_page, static_cast<uint32_t>(offset2)};
                            *reinterpret_cast<uint32_t*>(append_buf.data()) =
                                offset2 + sizeof(new_rh) + static_cast<uint32_t>(new_payload.size());
                            tbl_p.write_page(h.last_data_page, append_buf.data());

                            // update indexes to new position
                            std::vector<std::pair<size_t, std::pair<int, int>>> changed_indexes;
                            try {
                                for (size_t c = 0; c < h.column_count; ++c) {
                                    if (!h.columns[c].is_indexed) continue;

                                    int old_key = 0;
                                    int new_key = 0;
                                    if (!json_to_storage_key(row.at(h.columns[c].name), h.columns[c].type == 0, old_key,
                                            string_pool.get(), false)) {
                                        throw std::runtime_error(
                                            "Failed to extract old index key on " + std::string(h.columns[c].name));
                                    }
                                    if (!value_to_storage_key(
                                            new_values[c], h.columns[c].type == 0, new_key, string_pool.get(), true)) {
                                        throw std::runtime_error(
                                            "Failed to extract new index key on " + std::string(h.columns[c].name));
                                    }

                                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                                    tree.erase(old_key);
                                    tree.insert({new_key, new_pos});
                                    h.index_roots[c] = tree.get_root_id();
                                    changed_indexes.push_back({c, {old_key, new_key}});
                                }

                                // tombstone old row
                                rh->is_deleted = true;
                                page_changed = true;
                                h.row_count--;  // will be compensated by the new live row
                                logs.push_back(JournalEntry{JournalOp::UPDATE, plan.table_name, get_now_timestamp(),
                                    old_pos, old_payload, new_payload});

                                // keep row count unchanged globally
                                h.row_count++;

                                page_changed = true;
                            } catch (...) {
                                // rollback append + indices
                                try {
                                    std::vector<char> zero(PAGE_SIZE, 0);
                                    tbl_p.write_page(new_pos.page_id, zero.data());
                                } catch (...) {
                                }

                                // restore old index state best-effort by reversing operations
                                try {
                                    for (size_t c = 0; c < h.column_count; ++c) {
                                        if (!h.columns[c].is_indexed) continue;
                                        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                                        int old_key = 0;
                                        int new_key = 0;
                                        if (json_to_storage_key(row.at(h.columns[c].name), h.columns[c].type == 0,
                                                old_key, string_pool.get(), false) &&
                                            value_to_storage_key(new_values[c], h.columns[c].type == 0, new_key,
                                                string_pool.get(), true)) {
                                            tree.erase(new_key);
                                            tree.insert({old_key, old_pos});
                                            h.index_roots[c] = tree.get_root_id();
                                        }
                                    }
                                } catch (...) {
                                }

                                h = h_before;
                                save_metadata_atomic(meta_path, h);
                                throw;
                            }
                        }

                        h.row_count = h.row_count;  // keep semantic clarity
                        page_changed = true;
                    }
                }

                off += sizeof(RecordHeader) + rh->record_size;
            }

            if (page_changed) {
                tbl_p.write_page(page_id, buf.data());
            }
        }

        save_metadata_atomic(meta_path, h);

        if (journal) {
            for (const auto& e : logs) journal->log(e);
        }
    } catch (...) {
        h = h_before;
        try {
            save_metadata_atomic(meta_path, h);
        } catch (...) {
        }
        throw;
    }
}

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    if (current_db.empty()) throw std::runtime_error("No active DB");

    auto entries = journal->get_all_entries();
    fs::path db_dir = fs::path(root_path) / current_db;
    const std::string meta_path = (db_dir / (table_name + ".meta")).string();
    const std::string tbl_path = (db_dir / (table_name + ".tbl")).string();
    const std::string idx_path = (db_dir / (table_name + ".idx")).string();

    auto h = load_metadata_checked(meta_path);
    auto schema = get_table_schema(table_name);
    Pager tbl_p(tbl_path);
    Pager idx_p(idx_path);

    TableHeader h_before = h;

    try {
        for (int i = static_cast<int>(entries.size()) - 1; i >= 0; --i) {
            const auto& e = entries[i];
            if (e.timestamp <= timestamp) break;
            if (e.table_name != table_name) continue;
            if (!is_valid_page_and_offset(e.record_pos)) continue;

            if (e.op == JournalOp::INSERT) {
                std::vector<char> buf(PAGE_SIZE, 0);
                tbl_p.read_page(e.record_pos.page_id, buf.data());

                if (e.record_pos.offset + sizeof(RecordHeader) > PAGE_SIZE) continue;
                auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + e.record_pos.offset);
                if (rh->is_deleted) continue;

                auto row = RowDeserializer::deserialize(
                    schema, buf.data(), e.record_pos.offset + sizeof(RecordHeader), string_pool.get());

                for (size_t c = 0; c < h.column_count; ++c) {
                    if (!h.columns[c].is_indexed) continue;

                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                    int key = 0;
                    if (!json_to_storage_key(
                            row.at(h.columns[c].name), h.columns[c].type == 0, key, string_pool.get(), false)) {
                        throw std::runtime_error("String pool corruption during REVERT INSERT");
                    }
                    tree.erase(key);
                    h.index_roots[c] = tree.get_root_id();
                }

                rh->is_deleted = true;
                h.row_count--;
                tbl_p.write_page(e.record_pos.page_id, buf.data());
            } else if (e.op == JournalOp::DELETE) {
                std::vector<char> buf(PAGE_SIZE, 0);
                tbl_p.read_page(e.record_pos.page_id, buf.data());

                if (e.record_pos.offset + sizeof(RecordHeader) > PAGE_SIZE) continue;
                auto* rh = reinterpret_cast<RecordHeader*>(buf.data() + e.record_pos.offset);
                rh->is_deleted = false;
                rh->record_size = static_cast<uint32_t>(e.old_data.size());
                std::memcpy(
                    buf.data() + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(), e.old_data.size());

                auto row = payload_to_json_row(e.old_data, schema, string_pool.get());
                for (size_t c = 0; c < h.column_count; ++c) {
                    if (!h.columns[c].is_indexed) continue;

                    BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);
                    int key = 0;
                    if (!json_to_storage_key(
                            row.at(h.columns[c].name), h.columns[c].type == 0, key, string_pool.get(), false)) {
                        throw std::runtime_error("String pool corruption during REVERT DELETE");
                    }
                    tree.insert({key, e.record_pos});
                    h.index_roots[c] = tree.get_root_id();
                }

                h.row_count++;
                tbl_p.write_page(e.record_pos.page_id, buf.data());
            } else if (e.op == JournalOp::UPDATE) {
                std::vector<char> old_page(PAGE_SIZE, 0);
                tbl_p.read_page(e.record_pos.page_id, old_page.data());

                if (e.record_pos.offset + sizeof(RecordHeader) > PAGE_SIZE) continue;
                auto* old_rh = reinterpret_cast<RecordHeader*>(old_page.data() + e.record_pos.offset);

                if (e.new_data.size() == e.old_data.size()) {
                    auto current_row = RowDeserializer::deserialize(
                        schema, old_page.data(), e.record_pos.offset + sizeof(RecordHeader), string_pool.get());
                    auto old_row = payload_to_json_row(e.old_data, schema, string_pool.get());

                    for (size_t c = 0; c < h.column_count; ++c) {
                        if (!h.columns[c].is_indexed) continue;
                        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);

                        int cur_key = 0;
                        int old_key = 0;
                        if (!json_to_storage_key(current_row.at(h.columns[c].name), h.columns[c].type == 0, cur_key,
                                string_pool.get(), false)) {
                            throw std::runtime_error("String pool corruption during REVERT UPDATE");
                        }
                        if (!json_to_storage_key(old_row.at(h.columns[c].name), h.columns[c].type == 0, old_key,
                                string_pool.get(), false)) {
                            throw std::runtime_error("String pool corruption during REVERT UPDATE");
                        }

                        if (cur_key != old_key) {
                            tree.erase(cur_key);
                            tree.insert({old_key, e.record_pos});
                            h.index_roots[c] = tree.get_root_id();
                        }
                    }

                    std::memcpy(old_page.data() + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(),
                        e.old_data.size());
                    old_rh->record_size = static_cast<uint32_t>(e.old_data.size());
                    tbl_p.write_page(e.record_pos.page_id, old_page.data());
                } else {
                    // resize case: find the live record with new payload and tombstone it
                    auto found_new = find_live_record_by_payload(tbl_p, h, schema, e.new_data, string_pool.get());
                    if (found_new.has_value()) {
                        std::vector<char> new_page(PAGE_SIZE, 0);
                        tbl_p.read_page(found_new->page_id, new_page.data());
                        auto* new_rh = reinterpret_cast<RecordHeader*>(new_page.data() + found_new->offset);
                        new_rh->is_deleted = true;
                        tbl_p.write_page(found_new->page_id, new_page.data());
                    }

                    auto old_row = payload_to_json_row(e.old_data, schema, string_pool.get());
                    auto new_row = payload_to_json_row(e.new_data, schema, string_pool.get());

                    for (size_t c = 0; c < h.column_count; ++c) {
                        if (!h.columns[c].is_indexed) continue;
                        BP_tree<int, pos_t> tree(&idx_p, h.index_roots[c]);

                        int old_key = 0;
                        int new_key = 0;
                        if (!json_to_storage_key(new_row.at(h.columns[c].name), h.columns[c].type == 0, new_key,
                                string_pool.get(), false)) {
                            throw std::runtime_error("String pool corruption during REVERT UPDATE");
                        }
                        if (!json_to_storage_key(old_row.at(h.columns[c].name), h.columns[c].type == 0, old_key,
                                string_pool.get(), false)) {
                            throw std::runtime_error("String pool corruption during REVERT UPDATE");
                        }

                        tree.erase(new_key);
                        tree.insert({old_key, e.record_pos});
                        h.index_roots[c] = tree.get_root_id();
                    }

                    std::memcpy(old_page.data() + e.record_pos.offset + sizeof(RecordHeader), e.old_data.data(),
                        e.old_data.size());
                    old_rh->is_deleted = false;
                    old_rh->record_size = static_cast<uint32_t>(e.old_data.size());
                    tbl_p.write_page(e.record_pos.page_id, old_page.data());
                }
            }
        }

        save_metadata_atomic(meta_path, h);
    } catch (...) {
        h = h_before;
        try {
            save_metadata_atomic(meta_path, h);
        } catch (...) {
        }
        throw;
    }
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    auto h = load_metadata_checked((fs::path(root_path) / current_db / (table_name + ".meta")).string());
    std::vector<ColumnDef> s;
    s.reserve(h.column_count);

    for (uint32_t i = 0; i < h.column_count; ++i) {
        ColumnDef c;
        c.name = h.columns[i].name;
        c.type = (h.columns[i].type == 0 ? DataType::INT : DataType::STRING);
        c.is_not_null = h.columns[i].is_not_null;
        c.is_indexed = h.columns[i].is_indexed;
        s.push_back(c);
    }

    return s;
}