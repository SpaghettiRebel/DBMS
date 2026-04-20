#include "file_manager.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {
void ensure_parent_dir(const std::string& filename) {
    fs::path p(filename);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path());
    }
}

std::size_t file_page_count(std::fstream& file) {
    file.clear();
    file.seekg(0, std::ios::end);
    std::streampos size = file.tellg();
    if (size < 0) {
        throw std::runtime_error("Failed to determine file size");
    }
    return static_cast<std::size_t>(size) / PAGE_SIZE;
}
}  // namespace

Pager::Pager(const std::string& db_file) : filename(db_file), num_pages(0) {
    ensure_parent_dir(filename);

    file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        {
            std::ofstream create(filename, std::ios::binary | std::ios::trunc);
            if (!create) {
                throw std::runtime_error("Не удалось создать файл: " + filename);
            }
        }

        file.clear();
        file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Не удалось открыть файл: " + filename);
        }
    }

    num_pages = file_page_count(file);
}

Pager::~Pager() {
    if (file.is_open()) {
        file.flush();
        file.close();
    }
}

void Pager::read_page(uint32_t page_num, char* buffer) {
    if (buffer == nullptr) {
        throw std::invalid_argument("Null buffer passed to read_page");
    }

    if (page_num >= num_pages) {
        throw std::out_of_range("Попытка чтения несуществующей страницы: " + std::to_string(page_num));
    }

    file.clear();
    file.seekg(static_cast<std::streamoff>(page_num) * PAGE_SIZE, std::ios::beg);
    if (!file) {
        throw std::runtime_error("Ошибка seekg при чтении страницы " + std::to_string(page_num));
    }

    file.read(buffer, PAGE_SIZE);
    if (!file) {
        throw std::runtime_error("Ошибка при чтении страницы " + std::to_string(page_num));
    }
}

void Pager::write_page(uint32_t page_num, const char* buffer) {
    if (buffer == nullptr) {
        throw std::invalid_argument("Null buffer passed to write_page");
    }

    if (page_num > num_pages) {
        throw std::out_of_range("Попытка записи с пропуском страницы: " + std::to_string(page_num));
    }

    file.clear();
    file.seekp(static_cast<std::streamoff>(page_num) * PAGE_SIZE, std::ios::beg);
    if (!file) {
        throw std::runtime_error("Ошибка seekp при записи страницы " + std::to_string(page_num));
    }

    file.write(buffer, PAGE_SIZE);
    if (!file) {
        throw std::runtime_error("Ошибка при записи страницы " + std::to_string(page_num));
    }

    file.flush();
    if (!file) {
        throw std::runtime_error("Ошибка flush при записи страницы " + std::to_string(page_num));
    }

    if (page_num == num_pages) {
        ++num_pages;
    }
}

uint32_t Pager::allocate_page() {
    std::vector<char> empty_buffer(PAGE_SIZE, 0);
    uint32_t new_page_idx = static_cast<uint32_t>(num_pages);

    write_page(new_page_idx, empty_buffer.data());
    return new_page_idx;
}