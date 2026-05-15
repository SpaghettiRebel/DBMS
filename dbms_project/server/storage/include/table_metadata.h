#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <cstring>
#include "file_manager.h"

// Ограничим количество колонок для простоты заголовка
constexpr size_t MAX_COLUMNS = 32;
constexpr size_t MAX_NAME_LEN = 64;

struct ColumnMetadata {
    char name[MAX_NAME_LEN];
    uint8_t type;          // 0 для INT, 1 для STRING
    bool is_not_null;
    bool is_indexed;
    bool is_unique;
    bool is_autoincrement;
    bool has_default;
    int32_t default_int;
    uint32_t default_string_id;
    int64_t next_autoincrement_value;
};

// pos_t определяется в record.h, здесь только forward declaration
struct pos_t;

struct TableHeader {
    uint32_t magic_number;    // Маркер файла СУБД (например, 0x44424D53)
    uint32_t column_count;
    uint32_t row_count;       // Сколько всего записей в таблице
    uint32_t last_data_page;  // Номер последней страницы данных для дозаписи
    uint32_t index_roots[MAX_COLUMNS]; // Корневые страницы для индексов
    ColumnMetadata columns[MAX_COLUMNS];
};
