#include "binary_file_manager.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

namespace {
// Вспомогательная функция для обеспечения родительских директорий
void ensureParentDir(const std::string& filepath) {
    fs::path p(filepath);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path());
    }
}

// Проверка, существует ли файл
bool fileExists(const std::string& filepath) {
    std::ifstream f(filepath);
    return f.good();
}
}  // namespace

// ============================================================================
// Конструктор / Деструктор
// ============================================================================

BinaryFileManager::BinaryFileManager(const std::string& filepath)
    : filepath_(filepath), page_count_(0), first_free_page_(0) {
    
    ensureParentDir(filepath_);
    
    if (!fileExists(filepath_)) {
        // Создаем новый файл
        initializeFile();
    } else {
        // Загружаем существующий файл
        loadFile();
    }
}

BinaryFileManager::~BinaryFileManager() {
    sync();
    // pager_ автоматически закроется в своем деструкторе
}

BinaryFileManager::BinaryFileManager(BinaryFileManager&& other) noexcept
    : pager_(std::move(other.pager_)),
      filepath_(std::move(other.filepath_)),
      page_count_(other.page_count_),
      first_free_page_(other.first_free_page_) {
    other.page_count_ = 0;
    other.first_free_page_ = 0;
}

BinaryFileManager& BinaryFileManager::operator=(BinaryFileManager&& other) noexcept {
    if (this != &other) {
        sync();
        pager_ = std::move(other.pager_);
        filepath_ = std::move(other.filepath_);
        page_count_ = other.page_count_;
        first_free_page_ = other.first_free_page_;
        other.page_count_ = 0;
        other.first_free_page_ = 0;
    }
    return *this;
}

// ============================================================================
// Инициализация и загрузка файла
// ============================================================================

void BinaryFileManager::initializeFile() {
    pager_ = std::make_unique<Pager>(filepath_);
    
    // Создаем заголовок файла на первой странице
    FileHeader header;
    header.page_header.page_id = 0;
    header.page_header.type = PageType::METADATA;
    header.page_header.timestamp = getCurrentTimestamp();
    header.total_pages = 1;
    header.first_free_page = 0;
    header.created_at = header.page_header.timestamp;
    header.modified_at = header.page_header.timestamp;
    
    // Инициализируем метаданные таблицы нулями
    header.table_meta.magic_number = 0;
    header.table_meta.column_count = 0;
    header.table_meta.row_count = 0;
    header.table_meta.last_data_page = 0;
    for (size_t i = 0; i < MAX_COLUMNS; ++i) {
        header.table_meta.index_roots[i] = 0;
    }
    
    // Записываем заголовок
    std::vector<char> buffer(PAGE_SIZE, 0);
    std::memcpy(buffer.data(), &header, sizeof(FileHeader));
    
    // Вычисляем контрольную сумму для данных после заголовка страницы
    PageHeader* ph = reinterpret_cast<PageHeader*>(buffer.data());
    ph->checksum = ph->compute_checksum(
        buffer.data() + PageHeader::SIZE, 
        sizeof(FileHeader) - PageHeader::SIZE
    );
    
    pager_->write_page(0, buffer.data());
    page_count_ = 1;
    first_free_page_ = 0;
}

void BinaryFileManager::loadFile() {
    pager_ = std::make_unique<Pager>(filepath_);
    
    // Читаем заголовок файла
    std::vector<char> buffer(PAGE_SIZE, 0);
    pager_->read_page(0, buffer.data());
    
    const auto* header = reinterpret_cast<const FileHeader*>(buffer.data());
    
    // Валидация заголовка
    if (!header->page_header.validate()) {
        throw std::runtime_error("Invalid file header: magic number mismatch in " + filepath_);
    }
    
    if (header->page_header.type != PageType::METADATA) {
        throw std::runtime_error("Invalid file header: expected METADATA page type in " + filepath_);
    }
    
    // Восстанавливаем состояние
    page_count_ = header->total_pages;
    first_free_page_ = header->first_free_page;
}

// ============================================================================
// Операции со страницами
// ============================================================================

void BinaryFileManager::readPage(uint32_t page_id, char* buffer) {
    if (!buffer) {
        throw std::invalid_argument("Null buffer passed to readPage");
    }
    
    if (!pageExists(page_id)) {
        throw std::out_of_range("Page does not exist: " + std::to_string(page_id));
    }
    
    pager_->read_page(page_id, buffer);
    
    // Валидация заголовка страницы
    auto* header = reinterpret_cast<PageHeader*>(buffer);
    if (!header->validate()) {
        throw std::runtime_error("Invalid page header at page " + std::to_string(page_id));
    }
    
    // Проверка контрольной суммы
    uint32_t expected_checksum = header->compute_checksum(
        buffer + PageHeader::SIZE,
        PAGE_SIZE - PageHeader::SIZE
    );
    
    if (header->checksum != 0 && header->checksum != expected_checksum) {
        throw std::runtime_error("Checksum mismatch at page " + std::to_string(page_id));
    }
}

void BinaryFileManager::writePage(uint32_t page_id, const char* buffer, 
                                   PageType type, uint32_t data_size) {
    if (!buffer) {
        throw std::invalid_argument("Null buffer passed to writePage");
    }
    
    if (data_size > PAGE_SIZE - PageHeader::SIZE) {
        throw std::invalid_argument("Data size exceeds page capacity");
    }
    
    if (page_id > page_count_) {
        throw std::out_of_range("Invalid page ID: " + std::to_string(page_id));
    }
    
    // Копируем данные в рабочий буфер после заголовка страницы
    std::vector<char> page_buffer(PAGE_SIZE, 0);
    
    // Устанавливаем заголовок страницы в начале буфера
    auto* header = reinterpret_cast<PageHeader*>(page_buffer.data());
    header->magic = BFM_MAGIC;
    header->page_id = page_id;
    header->type = type;
    header->version = BFM_VERSION;
    header->data_size = data_size;
    header->timestamp = getCurrentTimestamp();
    
    // Копируем пользовательские данные после заголовка
    if (data_size > 0) {
        std::memcpy(page_buffer.data() + PageHeader::SIZE, buffer, data_size);
    }
    
    // Вычисляем контрольную сумму данных после заголовка
    header->checksum = header->compute_checksum(
        page_buffer.data() + PageHeader::SIZE,
        PAGE_SIZE - PageHeader::SIZE
    );
    
    // Записываем страницу
    pager_->write_page(page_id, page_buffer.data());
    
    // Обновляем счетчик страниц при необходимости
    if (page_id == page_count_) {
        ++page_count_;
        
        // Обновляем заголовок файла
        FileHeader file_header = readFileHeader();
        file_header.total_pages = page_count_;
        file_header.modified_at = getCurrentTimestamp();
        updateFileHeader(file_header);
    }
}

uint32_t BinaryFileManager::allocatePage(PageType type) {
    uint32_t new_page_id;
    
    // Если есть свободные страницы, используем их
    if (first_free_page_ != 0 && pageExists(first_free_page_)) {
        new_page_id = first_free_page_;
        
        // Читаем заголовок свободной страницы, чтобы получить следующую
        std::vector<char> buffer(PAGE_SIZE, 0);
        pager_->read_page(new_page_id, buffer.data());
        auto* header = reinterpret_cast<PageHeader*>(buffer.data());
        
        // В списке свободных страниц используем page_id как ссылку на следующую
        // Для простоты пока просто помечаем как использованную
        first_free_page_ = 0;  // Упрощенная реализация
        
        // Записываем новую страницу
        std::vector<char> empty_buffer(PAGE_SIZE, 0);
        writePage(new_page_id, empty_buffer.data(), type, 0);
    } else {
        // Выделяем новую страницу в конце файла
        new_page_id = page_count_;
        std::vector<char> empty_buffer(PAGE_SIZE, 0);
        writePage(new_page_id, empty_buffer.data(), type, 0);
    }
    
    return new_page_id;
}

void BinaryFileManager::freePage(uint32_t page_id) {
    if (!pageExists(page_id)) {
        throw std::out_of_range("Cannot free non-existent page: " + std::to_string(page_id));
    }
    
    if (page_id == 0) {
        throw std::invalid_argument("Cannot free file header page");
    }
    
    // Читаем страницу
    std::vector<char> buffer(PAGE_SIZE, 0);
    pager_->read_page(page_id, buffer.data());
    
    // Помечаем как свободную
    auto* header = reinterpret_cast<PageHeader*>(buffer.data());
    header->type = PageType::FREE;
    header->data_size = 0;
    header->timestamp = getCurrentTimestamp();
    
    // В простой реализации сохраняем page_id как ссылку на следующую свободную
    // Это можно улучшить до полноценного списка свободных страниц
    
    // Записываем обратно
    header->checksum = header->compute_checksum(
        buffer.data() + PageHeader::SIZE,
        PAGE_SIZE - PageHeader::SIZE
    );
    
    pager_->write_page(page_id, buffer.data());
    
    // Добавляем в список свободных
    first_free_page_ = page_id;
    
    // Обновляем заголовок файла
    FileHeader file_header = readFileHeader();
    file_header.first_free_page = first_free_page_;
    file_header.modified_at = getCurrentTimestamp();
    updateFileHeader(file_header);
}

// ============================================================================
// Операции с заголовками
// ============================================================================

PageHeader BinaryFileManager::readPageHeader(uint32_t page_id) {
    if (!pageExists(page_id)) {
        throw std::out_of_range("Page does not exist: " + std::to_string(page_id));
    }
    
    std::vector<char> buffer(PageHeader::SIZE, 0);
    pager_->read_page(page_id, buffer.data());
    
    return *reinterpret_cast<PageHeader*>(buffer.data());
}

void BinaryFileManager::updatePageHeader(uint32_t page_id, const PageHeader& header) {
    if (!pageExists(page_id)) {
        throw std::out_of_range("Page does not exist: " + std::to_string(page_id));
    }
    
    // Читаем всю страницу
    std::vector<char> buffer(PAGE_SIZE, 0);
    pager_->read_page(page_id, buffer.data());
    
    // Обновляем только заголовок
    std::memcpy(buffer.data(), &header, PageHeader::SIZE);
    
    // Пересчитываем контрольную сумму
    auto* new_header = reinterpret_cast<PageHeader*>(buffer.data());
    new_header->checksum = new_header->compute_checksum(
        buffer.data() + PageHeader::SIZE,
        PAGE_SIZE - PageHeader::SIZE
    );
    
    // Записываем обратно
    pager_->write_page(page_id, buffer.data());
}

FileHeader BinaryFileManager::readFileHeader() {
    std::vector<char> buffer(PAGE_SIZE, 0);
    pager_->read_page(0, buffer.data());
    
    return *reinterpret_cast<FileHeader*>(buffer.data());
}

void BinaryFileManager::updateFileHeader(const FileHeader& header) {
    std::vector<char> buffer(PAGE_SIZE, 0);
    std::memcpy(buffer.data(), &header, sizeof(FileHeader));
    
    // Обновляем timestamp
    auto* file_header = reinterpret_cast<FileHeader*>(buffer.data());
    file_header->page_header.timestamp = getCurrentTimestamp();
    
    // Пересчитываем контрольную сумму
    file_header->page_header.checksum = file_header->page_header.compute_checksum(
        buffer.data() + PageHeader::SIZE,
        sizeof(FileHeader) - PageHeader::SIZE
    );
    
    // Атомарная запись через временный файл не нужна для одной страницы
    pager_->write_page(0, buffer.data());
}

// ============================================================================
// Утилиты
// ============================================================================

uint32_t BinaryFileManager::getPageCount() const {
    return page_count_;
}

bool BinaryFileManager::pageExists(uint32_t page_id) const {
    return page_id < page_count_;
}

PageType BinaryFileManager::getPageType(uint32_t page_id) const {
    if (!pageExists(page_id)) {
        return PageType::FREE;
    }
    
    std::vector<char> buffer(PageHeader::SIZE, 0);
    try {
        pager_->read_page(page_id, buffer.data());
        auto* header = reinterpret_cast<PageHeader*>(buffer.data());
        if (header->validate()) {
            return header->type;
        }
    } catch (...) {
        // Игнорируем ошибки чтения
    }
    
    return PageType::FREE;
}

bool BinaryFileManager::validatePage(uint32_t page_id) {
    if (!pageExists(page_id)) {
        return false;
    }
    
    try {
        std::vector<char> buffer(PAGE_SIZE, 0);
        pager_->read_page(page_id, buffer.data());
        
        auto* header = reinterpret_cast<PageHeader*>(buffer.data());
        
        // Проверка магического числа и версии
        if (!header->validate()) {
            return false;
        }
        
        // Проверка контрольной суммы
        if (header->checksum != 0) {
            uint32_t expected = header->compute_checksum(
                buffer.data() + PageHeader::SIZE,
                PAGE_SIZE - PageHeader::SIZE
            );
            if (header->checksum != expected) {
                return false;
            }
        }
        
        return true;
    } catch (...) {
        return false;
    }
}

void BinaryFileManager::sync() {
    if (pager_ && pager_->file.is_open()) {
        pager_->file.flush();
        pager_->file.sync();
    }
}

uint32_t BinaryFileManager::calculateChecksum(uint32_t /*page_id*/, 
                                               const char* data, size_t size) {
    PageHeader dummy;
    return dummy.compute_checksum(data, size);
}
