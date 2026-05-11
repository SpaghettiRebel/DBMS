#pragma once
#include "file_manager.h"
#include "table_metadata.h"
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// ============================================================================
// Binary File Manager - Управление бинарными файлами таблиц (.tbl)
// ============================================================================
// Этот класс предоставляет интерфейс для чтения/записи фиксированных блоков 
// (страниц) данных в файлы таблиц, обеспечивая:
// - Атомарную запись страниц
// - Управление заголовками страниц
// - Валидацию целостности данных
// - Поддержку различных типов страниц (данные, индексы, метаданные)
// ============================================================================

constexpr uint32_t BFM_MAGIC = 0x42464D53;  // "BFMS" - Binary File Management System
constexpr size_t BFM_VERSION = 1;

// Типы страниц в файле таблицы
enum class PageType : uint8_t {
    DATA = 0x01,        // Страница с данными записей
    INDEX = 0x02,       // Страница B+-tree индекса
    METADATA = 0x03,    // Страница метаданных таблицы
    FREE = 0xFF         // Свободная/удаленная страница
};

// Заголовок каждой страницы (размещается в начале страницы)
struct __attribute__((packed)) PageHeader {
    uint32_t magic;         // Магическое число для валидации (BFM_MAGIC)
    uint32_t page_id;       // Номер страницы в файле
    PageType type;          // Тип страницы
    uint8_t version;        // Версия формата страницы
    uint16_t flags;         // Флаги (битовая маска)
    uint32_t data_size;     // Размер полезных данных на странице
    uint32_t checksum;      // Контрольная сумма для проверки целостности
    uint64_t timestamp;     // Время последней модификации (Unix timestamp ms)
    
    static constexpr size_t SIZE = 32;  // Реальный размер packed структуры
    
    PageHeader() : magic(BFM_MAGIC), page_id(0), type(PageType::FREE), 
                   version(BFM_VERSION), flags(0), data_size(0), 
                   checksum(0), timestamp(0) {}
    
    // Вычисление простой контрольной суммы (CRC32 можно добавить позже)
    uint32_t compute_checksum(const char* data, size_t size) const {
        uint32_t sum = 0;
        for (size_t i = 0; i < size; ++i) {
            sum = ((sum << 5) + sum) + static_cast<uint8_t>(data[i]);
        }
        return sum;
    }
    
    bool validate() const {
        return magic == BFM_MAGIC && version == BFM_VERSION;
    }
};

// Заголовок файла таблицы (первая страница файла)
struct FileHeader {
    PageHeader page_header;
    uint32_t total_pages;       // Общее количество страниц в файле
    uint32_t first_free_page;   // Номер первой свободной страницы (для reuse)
    uint64_t created_at;        // Время создания файла
    uint64_t modified_at;       // Время последней модификации
    TableHeader table_meta;     // Метаданные таблицы
    
    static constexpr size_t SIZE = PAGE_SIZE;
    
    FileHeader() : total_pages(0), first_free_page(0), created_at(0), modified_at(0) {
        page_header.type = PageType::METADATA;
        page_header.data_size = sizeof(FileHeader) - PageHeader::SIZE;
    }
};

// Менеджер бинарных файлов
class BinaryFileManager {
public:
    // Конструктор - открывает или создает файл таблицы
    explicit BinaryFileManager(const std::string& filepath);
    
    // Деструктор - закрывает файл
    ~BinaryFileManager();
    
    // Запрет копирования
    BinaryFileManager(const BinaryFileManager&) = delete;
    BinaryFileManager& operator=(const BinaryFileManager&) = delete;
    
    // Разрешение перемещения
    BinaryFileManager(BinaryFileManager&& other) noexcept;
    BinaryFileManager& operator=(BinaryFileManager&& other) noexcept;
    
    // =========================================================================
    // Операции со страницами
    // =========================================================================
    
    // Чтение страницы в буфер
    void readPage(uint32_t page_id, char* buffer);
    
    // Запись страницы из буфера (с обновлением заголовка и checksum)
    void writePage(uint32_t page_id, const char* buffer, PageType type, uint32_t data_size);
    
    // Выделение новой страницы (возвращает номер)
    uint32_t allocatePage(PageType type);
    
    // Освобождение страницы (помечает как свободную для повторного использования)
    void freePage(uint32_t page_id);
    
    // =========================================================================
    // Операции с заголовками
    // =========================================================================
    
    // Чтение заголовка страницы
    PageHeader readPageHeader(uint32_t page_id);
    
    // Обновление заголовка страницы
    void updatePageHeader(uint32_t page_id, const PageHeader& header);
    
    // Чтение заголовка файла
    FileHeader readFileHeader();
    
    // Обновление заголовка файла (атомарно)
    void updateFileHeader(const FileHeader& header);
    
    // =========================================================================
    // Утилиты
    // =========================================================================
    
    // Получение размера файла в страницах
    uint32_t getPageCount() const;
    
    // Проверка существования страницы
    bool pageExists(uint32_t page_id) const;
    
    // Получение типа страницы
    PageType getPageType(uint32_t page_id) const;
    
    // Валидация целостности страницы
    bool validatePage(uint32_t page_id);
    
    // Синхронизация данных на диск
    void sync();
    
    // Получение пути к файлу
    const std::string& getFilePath() const { return filepath_; }
    
private:
    std::unique_ptr<Pager> pager_;
    std::string filepath_;
    uint32_t page_count_;
    uint32_t first_free_page_;
    
    // Инициализация нового файла
    void initializeFile();
    
    // Загрузка существующего файла
    void loadFile();
    
    // Вычисление контрольной суммы для страницы
    uint32_t calculateChecksum(uint32_t page_id, const char* data, size_t size);
    
    // Получение текущего времени в миллисекундах
    uint64_t getCurrentTimestamp() const;
};

// ============================================================================
// Inline реализации вспомогательных функций
// ============================================================================

inline uint64_t BinaryFileManager::getCurrentTimestamp() const {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto duration = now.time_since_epoch();
    return duration_cast<milliseconds>(duration).count();
}
