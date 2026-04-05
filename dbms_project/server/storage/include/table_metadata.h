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

struct TableHeader {
    uint32_t magic_number;    // Маркер файла СУБД (например, 0xDEADBEEF)
    uint32_t column_count;
    uint32_t row_count;       // Сколько всего записей в таблице
    uint32_t first_data_page; // Номер первой страницы с данными (обычно 1)
    ColumnMetadata columns[MAX_COLUMNS];
};