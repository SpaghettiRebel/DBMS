#include "../include/engine.h"
#include "../include/serializer.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace fs = std::filesystem;
using json = nlohmann::json;

Engine::Engine(std::string root) : root_path(std::move(root)) {
    if (!fs::exists(root_path)) {
        fs::create_directories(root_path);
    }
}

void Engine::use_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (!fs::exists(db_path)) throw std::runtime_error("База данных не существует");
    current_db = name;
    string_pool = std::make_unique<StringPool>((db_path / "strings.dat").string());
    journal = std::make_unique<Journal>((db_path / "journal.dat").string());
}

void Engine::execute(const QueryPlan& plan) {
    if (plan.type == QueryType::CREATE_DATABASE) {
        create_database(plan.database_name);
    } else if (plan.type == QueryType::DROP_DATABASE) {
        drop_database(plan.database_name);
    } else if (plan.type == QueryType::USE_DATABASE) {
        use_database(plan.database_name);
    } else if (plan.type == QueryType::CREATE_TABLE) {
        create_table(plan);
    } else if (plan.type == QueryType::DROP_TABLE) {
        drop_table(plan.table_name);
    } else if (plan.type == QueryType::INSERT) {
        insert_record(plan);
    } else if (plan.type == QueryType::SELECT) {
        std::cout << select_records(plan) << std::endl;
    } else if (plan.type == QueryType::REVERT) {
        revert(plan.table_name, plan.timestamp);
    } else if (plan.type == QueryType::UPDATE) {
        update_records(plan);
    } else if (plan.type == QueryType::DELETE) {
        delete_records(plan);
    } else {
        throw std::runtime_error("Операция еще не реализована");
    }
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
    header.first_data_page = 1;
    header.row_count = 0;

    for (size_t i = 0; i < plan.columns.size(); ++i) {
        const auto& col = plan.columns[i];
        std::strncpy(header.columns[i].name, col.name.c_str(), MAX_NAME_LEN);
        header.columns[i].type = (col.type == DataType::INT) ? 0 : 1;
        header.columns[i].is_not_null = col.is_not_null;
        header.columns[i].is_indexed = col.is_indexed;
        
        if (col.default_value.has_value() && col.type == DataType::INT) {
            header.columns[i].has_default = true;
            header.columns[i].default_int = std::get<int>(col.default_value->data);
        }
    }

    char buffer[PAGE_SIZE] = {0};
    std::memcpy(buffer, &header, sizeof(header));
    pager.write_page(0, buffer);
    pager.allocate_page(); 
}

void Engine::drop_table(const std::string& table_name) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");
    fs::path table_path = fs::path(root_path) / current_db / (table_name + ".tbl");
    if (!fs::exists(table_path)) throw std::runtime_error("Таблица не существует");
    fs::remove(table_path);
}

void Engine::insert_record(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");

    fs::path table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    Pager pager(table_path.string());

    char header_buffer[PAGE_SIZE];
    pager.read_page(0, header_buffer);
    TableHeader* header = reinterpret_cast<TableHeader*>(header_buffer);

    std::vector<ColumnDef> schema = get_table_schema(plan.table_name);

    std::vector<Value> values_with_ids = plan.values;
    for (size_t i = 0; i < plan.target_columns.size(); ++i) {
        for (const auto& col_def : schema) {
            if (col_def.name == plan.target_columns[i] && col_def.type == DataType::STRING) {
                if (!std::holds_alternative<std::string>(values_with_ids[i].data)) break;
                std::string s = std::get<std::string>(values_with_ids[i].data);
                uint32_t id = string_pool->intern(s);
                values_with_ids[i].data = static_cast<int>(id);
                break;
            }
        }
    }

    std::vector<char> row_bin = RowSerializer::serialize(schema, plan.target_columns, values_with_ids);

    // Uniqueness check
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == schema[i].name) {
                    if (schema[i].type == DataType::INT) {
                        BP_tree<int, uint64_t> tree;
                        try {
                            if (tree.contains(std::get<int>(plan.values[j].data))) {
                                throw std::runtime_error("Constraint Violation: Unique index on " + schema[i].name);
                            }
                        } catch(const not_implemented&) {}
                    }
                    break;
                }
            }
        }
    }

    JournalEntry j_entry;
    j_entry.op = JournalOp::INSERT;
    j_entry.table_name = plan.table_name;
    j_entry.new_data = row_bin;

    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    auto timer = std::chrono::system_clock::to_time_t(now);
    std::tm bt;
#ifdef _WIN32
    localtime_s(&bt, &timer);
#else
    localtime_r(&timer, &bt);
#endif
    std::ostringstream oss;
    oss << std::put_time(&bt, "%Y.%m.%d-%H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count();
    j_entry.timestamp = oss.str();
    journal->log(j_entry);

    uint32_t target_page_id = header->first_data_page;
    char page_buffer[PAGE_SIZE];
    pager.read_page(target_page_id, page_buffer);

    // Simple free space management: we append records.
    // In a real system, we'd have a header on each page tracking used bytes.
    // For this demonstration, we'll use a fixed row size or just row_count if it were fixed.
    // Since it's variable, we'd need more complex logic.
    // Let's assume we store the current offset in the page itself (first 4 bytes).
    uint32_t current_offset = *reinterpret_cast<uint32_t*>(page_buffer);
    if (current_offset == 0) current_offset = 4; // Start after offset header

    if (current_offset + row_bin.size() > PAGE_SIZE) {
        // Allocate new page if it doesn't fit
        target_page_id = pager.allocate_page();
        pager.read_page(target_page_id, page_buffer);
        current_offset = 4;
    }

    std::memcpy(page_buffer + current_offset, row_bin.data(), row_bin.size());
    uint64_t record_offset = (uint64_t)target_page_id * PAGE_SIZE + current_offset;

    current_offset += row_bin.size();
    *reinterpret_cast<uint32_t*>(page_buffer) = current_offset;

    header->row_count++;
    pager.write_page(0, header_buffer);
    pager.write_page(target_page_id, page_buffer);

    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].is_indexed) {
            for (size_t j = 0; j < plan.target_columns.size(); ++j) {
                if (plan.target_columns[j] == schema[i].name) {
                    if (schema[i].type == DataType::INT) {
                        BP_tree<int, uint64_t> tree;
                        try {
                            tree.insert({std::get<int>(plan.values[j].data), record_offset});
                        } catch(const not_implemented&) {}
                    }
                    break;
                }
            }
        }
    }
}

std::string Engine::select_records(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");

    std::vector<ColumnDef> schema = get_table_schema(plan.table_name);
    fs::path table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    Pager pager(table_path.string());

    char header_buffer[PAGE_SIZE];
    pager.read_page(0, header_buffer);
    TableHeader* header = reinterpret_cast<TableHeader*>(header_buffer);

    json result = json::array();

    // Iterate through data pages (header only tracks the first one,
    // but in a real system we'd know how many there are).
    // For now, let's just scan the first one.
    char page_buffer[PAGE_SIZE];
    pager.read_page(header->first_data_page, page_buffer);

    uint32_t current_offset = *reinterpret_cast<uint32_t*>(page_buffer);
    if (current_offset > 4) {
        size_t offset = 4;
        while (offset < current_offset) {
            result.push_back(RowDeserializer::deserialize(schema, page_buffer, offset, string_pool.get()));
        }
    }

    return result.dump(4);
}

void Engine::update_records(const QueryPlan& plan) {
    // In this simplified version, UPDATE/DELETE are stubs that would
    // find records and either update them in place or mark as deleted.
    throw not_implemented("Engine", "update_records");
}

void Engine::delete_records(const QueryPlan& plan) {
    throw not_implemented("Engine", "delete_records");
}

void Engine::revert(const std::string& table_name, const std::string& timestamp) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");
    auto schema = get_table_schema(table_name);

    // We recreate the table file to clear it [cite: 54]
    drop_table(table_name);
    QueryPlan cp;
    cp.type = QueryType::CREATE_TABLE;
    cp.table_name = table_name;
    cp.columns = schema;
    create_table(cp);

    // Replay journal until the timestamp
    auto entries = journal->get_all_entries();
    for (const auto& entry : entries) {
        if (entry.timestamp > timestamp) break;
        if (entry.table_name != table_name) continue;

        if (entry.op == JournalOp::INSERT) {
            // Low-level insert (bypassing journaling to avoid recursion)
            fs::path table_path = fs::path(root_path) / current_db / (table_name + ".tbl");
            Pager pager(table_path.string());
            char header_buffer[PAGE_SIZE];
            pager.read_page(0, header_buffer);
            TableHeader* header = reinterpret_cast<TableHeader*>(header_buffer);

            char page_buffer[PAGE_SIZE];
            pager.read_page(header->first_data_page, page_buffer);
            uint32_t current_offset = *reinterpret_cast<uint32_t*>(page_buffer);
            if (current_offset == 0) current_offset = 4;

            std::memcpy(page_buffer + current_offset, entry.new_data.data(), entry.new_data.size());
            current_offset += entry.new_data.size();
            *reinterpret_cast<uint32_t*>(page_buffer) = current_offset;

            header->row_count++;
            pager.write_page(0, header_buffer);
            pager.write_page(header->first_data_page, page_buffer);
        }
    }
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    fs::path table_path = fs::path(root_path) / current_db / (table_name + ".tbl");
    Pager pager(table_path.string());
    char buffer[PAGE_SIZE]; pager.read_page(0, buffer);
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
