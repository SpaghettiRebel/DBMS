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
    uint8_t type;          // 0 для INT, 1 для STRING [cite: 7]
    bool is_not_null;      // [cite: 19]
    bool is_indexed;       // [cite: 20]
    bool has_default;      // [cite: 75]
    int32_t default_int;   // Упрощение: храним дефолт только для INT

    // В реальной СУБД дефолтные строки хранятся сложнее,
    // но для начала ограничимся этим.
};

struct pos_t {
    uint32_t page_id;
    uint32_t offset;

    bool is_valid() const { return page_id != 0 || offset != 0; }
    static pos_t invalid() { return {0, 0}; }
};

struct TableHeader {
    uint32_t magic_number;    // Маркер файла СУБД (например, 0x44424D53)
    uint32_t column_count;
    uint32_t row_count;       // Сколько всего записей в таблице
    uint32_t last_data_page;  // Номер последней страницы данных для дозаписи
    uint32_t index_roots[MAX_COLUMNS]; // Корневые страницы для индексов
    ColumnMetadata columns[MAX_COLUMNS];
};