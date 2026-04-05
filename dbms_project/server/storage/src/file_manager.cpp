#include "file_manager.h"
#include <stdexcept>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

Pager::Pager(const std::string& db_file) : filename(db_file), num_pages(0) {
    // Проверяем наличие директории для реализации иерархии (Система -> БД -> Таблица) [cite: 8, 10]
    fs::path p(filename);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path());
    }

    // Открываем файл в бинарном режиме для чтения и записи 
    file.open(filename, std::ios::in | std::ios::out | std::ios::binary);

    if (!file.is_open()) {
        // Если файла нет, создаем его (out | trunc) и переоткрываем для работы
        file.clear();
        file.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);
        file.close();
        file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!file.is_open()) {
        throw std::runtime_error("Не удалось открыть или создать файл: " + filename);
    }

    // Определяем количество страниц в файле
    file.seekg(0, std::ios::end);
    std::streampos file_size = file.tellg();
    num_pages = static_cast<size_t>(file_size / PAGE_SIZE);
}

Pager::~Pager() {
    if (file.is_open()) {
        file.close();
    }
}

void Pager::read_page(uint32_t page_num, char* buffer) {
    if (page_num >= num_pages) {
        throw std::out_of_range("Попытка чтения несуществующей страницы: " + std::to_string(page_num));
    }

    file.seekg(page_num * PAGE_SIZE, std::ios::beg);
    file.read(buffer, PAGE_SIZE);

    if (file.fail()) {
        throw std::runtime_error("Ошибка при чтении страницы " + std::to_string(page_num));
    }
}

void Pager::write_page(uint32_t page_num, const char* buffer) {
    // В СУБД мы можем писать только в существующие или в первую новую страницу
    file.seekp(page_num * PAGE_SIZE, std::ios::beg);
    file.write(buffer, PAGE_SIZE);

    if (file.fail()) {
        throw std::runtime_error("Ошибка при записи страницы " + std::to_string(page_num));
    }
    
    file.flush(); // Гарантируем сброс данных на диск для надежности 
}

uint32_t Pager::allocate_page() {
    uint32_t new_page_idx = static_cast<uint32_t>(num_pages);
    std::vector<char> empty_buffer(PAGE_SIZE, 0);
    
    write_page(new_page_idx, empty_buffer.data());
    num_pages++;
    return new_page_idx;
}