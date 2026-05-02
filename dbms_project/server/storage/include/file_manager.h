#pragma once
#include <fstream>
#include <string>
#include <vector>


constexpr size_t PAGE_SIZE = 4096;  // Размер одной страницы в байтах

class Pager {
public:
    std::fstream file;
private:
    std::string filename;
    size_t num_pages;

public:
    // Открывает файл (если нет - создает)
    Pager(const std::string& db_file);
    ~Pager();

    // Читает страницу под номером page_num в переданный буфер
    void read_page(uint32_t page_num, char* buffer);

    // Записывает данные из буфера в страницу под номером page_num
    void write_page(uint32_t page_num, const char* buffer);

    // Добавляет новую пустую страницу в конец файла и возвращает её номер
    uint32_t allocate_page();
};