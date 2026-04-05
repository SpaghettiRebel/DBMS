#include "engine.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <nlohmann/json.hpp> // Рекомендовано для JSON [cite: 24]

namespace fs = std::filesystem;
using json = nlohmann::json;

Engine::Engine(std::string root) : root_path(std::move(root)) {
    if (!fs::exists(root_path)) {
        fs::create_directories(root_path);
    }
}

void Engine::execute(const QueryPlan& plan) {
    switch (plan.type) {
        case QueryType::CREATE_DATABASE:
            create_database(plan.database_name);
            break;
        case QueryType::USE_DATABASE:
            current_db = plan.database_name;
            break;
        case QueryType::CREATE_TABLE:
            create_table(plan);
            break;
        case QueryType::INSERT:
            insert_record(plan);
            break;
        // ... остальные типы
        default:
            throw std::runtime_error("Операция еще не реализована");
    }
}

void Engine::create_database(const std::string& name) {
    fs::path db_path = fs::path(root_path) / name;
    if (fs::exists(db_path)) throw std::runtime_error("База данных уже существует");
    fs::create_directory(db_path); // Уровень БД [cite: 10]
}

// Внутри класса Engine

void Engine::create_table(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");
    
    fs::path table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    Pager pager(table_path.string());

    // 1. Формируем заголовок
    TableHeader header;
    std::memset(&header, 0, sizeof(header));
    header.magic_number = 0x44424D53; // "DBMS"
    header.column_count = static_cast<uint32_t>(plan.columns.size());
    header.first_data_page = 1;

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

    // 2. Пишем заголовок на Page 0
    char buffer[PAGE_SIZE] = {0};
    std::memcpy(buffer, &header, sizeof(header));
    pager.write_page(0, buffer);
    
    // 3. Выделяем первую страницу для будущих данных
    pager.allocate_page(); 
}

std::vector<ColumnDef> Engine::get_table_schema(const std::string& table_name) {
    fs::path table_path = fs::path(root_path) / current_db / (table_name + ".tbl");
    Pager pager(table_path.string());

    char buffer[PAGE_SIZE];
    pager.read_page(0, buffer);

    TableHeader* header = reinterpret_cast<TableHeader*>(buffer);
    if (header->magic_number != 0x44424D53) {
        throw std::runtime_error("Файл поврежден или не является таблицей");
    }

    std::vector<ColumnDef> schema;
    for (uint32_t i = 0; i < header->column_count; ++i) {
        ColumnDef col;
        col.name = header->columns[i].name;
        col.type = (header->columns[i].type == 0) ? DataType::INT : DataType::STRING;
        col.is_not_null = header->columns[i].is_not_null;
        col.is_indexed = header->columns[i].is_indexed;
        
        if (header->columns[i].has_default) {
            Value v;
            v.data = header->columns[i].default_int;
            col.default_value = v;
        }
        schema.push_back(col);
    }
    return schema;
}

void Engine::insert_record(const QueryPlan& plan) {
    if (current_db.empty()) throw std::runtime_error("База данных не выбрана");

    // 1. Получаем схему таблицы (в реальной системе ты должен хранить её в системном каталоге)
    // Для примера предположим, что мы её откуда-то загрузили:
    std::vector<ColumnDef> schema = get_table_schema(plan.table_name);

    // 2. Превращаем данные в байты
    std::vector<char> row_bin = RowSerializer::serialize(schema, plan.target_columns, plan.values);

    // 3. Сохраняем в файл через Pager
    fs::path table_path = fs::path(root_path) / current_db / (plan.table_name + ".tbl");
    Pager pager(table_path.string());
    
    // ВАЖНО: Тебе нужно найти место в файле. 
    // Пока для простоты пишем просто в конец первой страницы.
    char page_buffer[PAGE_SIZE] = {0};
    uint32_t target_page = 0; 
    
    pager.read_page(target_page, page_buffer);
    
    // Логика поиска свободного места на странице (Free Space Management)
    // ... здесь ты копируешь row_bin.data() в page_buffer ...
    
    pager.write_page(target_page, page_buffer);

    // 4. Обновляем B+ дерево для INDEXED полей [cite: 20, 21]
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].is_indexed) {
            // Вставляем значение в bplus_tree.cpp
            // Индекс хранит: Ключ -> (Номер страницы, Смещение на странице)
        }
    }
}

std::string Engine::select_records(const QueryPlan& plan) {
    json result = json::array(); // Вывод в формате JSON [cite: 24]
    
    // Если в WHERE есть INDEXED колонка, используем B+ tree для поиска [cite: 21]
    
    // Пример наполнения для теста:
    // result.push_id({ {"id", 1}, {"name", "Ivan"} });

    return result.dump(4);
}