#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <map>
#include <functional>
#include <cstring>
#include <iostream>

// Простая реализация CRC32 для контроля целостности записей
class CRC32 {
public:
    static uint32_t calculate(const void* data, size_t length) {
        static uint32_t table[256];
        static bool tableInitialized = false;

        if (!tableInitialized) {
            for (uint32_t i = 0; i < 256; i++) {
                uint32_t crc = i;
                for (int j = 0; j < 8; j++) {
                    crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
                }
                table[i] = crc;
            }
            tableInitialized = true;
        }

        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < length; i++) {
            crc = (crc >> 8) ^ table[(crc ^ bytes[i]) & 0xFF];
        }
        return crc ^ 0xFFFFFFFF;
    }
};

namespace DBMS {

// Типы операций WAL
enum class WALEntryType : uint8_t {
    OP_INSERT = 1,
    OP_UPDATE = 2,
    OP_DELETE = 3,
    OP_PAGE_WRITE = 4, // Запись целой страницы (для B+Tree узлов)
    OP_TRUNCATE = 5,   // Усечение файла
    OP_CHECKPOINT = 6  // Точка сохранения
};

// Заголовок записи WAL
#pragma pack(push, 1)
struct WALEntryHeader {
    uint64_t lsn;             // Log Sequence Number
    uint64_t transactionId;   // ID транзакции
    uint8_t  type;            // WALEntryType
    uint32_t tableNameLen;    // Длина имени таблицы
    uint64_t offset;          // Смещение в целевом файле
    uint64_t dataSize;        // Размер полезных данных
    uint32_t crc32;           // CRC32 заголовка (без поля crc32)
};
#pragma pack(pop)

struct WALEntry {
    uint64_t lsn;
    uint64_t transactionId;
    WALEntryType type;
    std::string tableName;
    uint64_t offset;
    std::vector<uint8_t> data;
    uint32_t fullRecordCrc; // CRC всей записи (заголовок + данные)
};

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& dbPath)
        : dbPath_(dbPath), currentLsn_(0), currentTxId_(0) {
        walFilePath_ = dbPath_ + "/_system_wal.log";
        openLogFile();
        recover(); // Автоматическое восстановление при старте
    }

    ~WriteAheadLog() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (logFile_.is_open()) {
            logFile_.flush();
            logFile_.close();
        }
    }

    // Начало новой транзакции (возвращает ID)
    uint64_t beginTransaction() {
        std::lock_guard<std::mutex> lock(mutex_);
        currentTxId_++;
        return currentTxId_;
    }

    // Логирование операции записи страницы (используется B+Tree и менеджером страниц)
    bool logPageWrite(uint64_t txId, const std::string& tableName, 
                      uint64_t offset, const void* data, size_t size) {
        return writeEntry(txId, WALEntryType::OP_PAGE_WRITE, tableName, offset, data, size);
    }

    // Логирование операции изменения метаданных или небольших записей
    bool logDataOp(uint64_t txId, WALEntryType type, const std::string& tableName,
                   uint64_t offset, const void* data, size_t size) {
        return writeEntry(txId, type, tableName, offset, data, size);
    }

    // Принудительная синхронизация логов с диском (Critical for Durability)
    void sync() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (logFile_.is_open()) {
            logFile_.flush();
            #ifdef _WIN32
                // Windows specific sync if needed
            #else
                fsync(fileno(logFile_.rdbuf()->pubfile()));
            #endif
        }
    }

    // Точка сохранения: очищает лог, так как все изменения применены
    void checkpoint() {
        std::lock_guard<std::mutex> lock(mutex_);
        // Записываем маркер checkpoint
        WALEntryHeader hdr{};
        hdr.lsn = ++currentLsn_;
        hdr.transactionId = 0;
        hdr.type = static_cast<uint8_t>(WALEntryType::OP_CHECKPOINT);
        hdr.tableNameLen = 0;
        hdr.offset = 0;
        hdr.dataSize = 0;
        hdr.crc32 = CRC32::calculate(&hdr, sizeof(WALEntryHeader) - sizeof(hdr.crc32));
        
        logFile_.write(reinterpret_cast<char*>(&hdr), sizeof(WALEntryHeader));
        logFile_.flush();
        
        // В реальной системе здесь можно было бы создать новый файл лога,
        // а старый удалить или архивировать. Для простоты просто сбрасываем.
        logFile_.close();
        std::remove(walFilePath_.c_str());
        openLogFile();
    }

    // Установка колбэка для применения записей при восстановлении
    using ApplyCallback = std::function<void(const WALEntry&)>;
    void setRecoveryCallback(ApplyCallback cb) {
        recoveryCallback_ = cb;
    }

private:
    std::string dbPath_;
    std::string walFilePath_;
    std::ofstream logFile_;
    std::mutex mutex_;
    uint64_t currentLsn_;
    uint64_t currentTxId_;
    ApplyCallback recoveryCallback_;

    void openLogFile() {
        logFile_.open(walFilePath_, std::ios::binary | std::ios::app);
        if (!logFile_.is_open()) {
            throw std::runtime_error("Failed to open WAL file: " + walFilePath_);
        }
    }

    bool writeEntry(uint64_t txId, WALEntryType type, const std::string& tableName,
                    uint64_t offset, const void* data, size_t size) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!logFile_.is_open()) return false;

        WALEntryHeader hdr{};
        hdr.lsn = ++currentLsn_;
        hdr.transactionId = txId;
        hdr.type = static_cast<uint8_t>(type);
        hdr.tableNameLen = static_cast<uint32_t>(tableName.size());
        hdr.offset = offset;
        hdr.dataSize = static_cast<uint64_t>(size);
        
        // Вычисляем CRC заголовка
        hdr.crc32 = CRC32::calculate(&hdr, sizeof(WALEntryHeader) - sizeof(hdr.crc32));

        // Пишем заголовок
        logFile_.write(reinterpret_cast<char*>(&hdr), sizeof(WALEntryHeader));
        
        // Пишем имя таблицы
        if (!tableName.empty()) {
            logFile_.write(tableName.c_str(), tableName.size());
        }

        // Пишем данные
        if (size > 0 && data != nullptr) {
            logFile_.write(static_cast<const char*>(data), size);
        }

        // Вычисляем и пишем общий CRC записи (опционально, для надежности чтения)
        // В простой реализации достаточно CRC заголовка, но для полной надежности:
        uint32_t fullCrc = CRC32::calculate(data, size);
        logFile_.write(reinterpret_cast<char*>(&fullCrc), sizeof(fullCrc));

        // Немедленная синхронизация для гарантии durability
        logFile_.flush();
        #ifndef _WIN32
            fsync(fileno(logFile_.rdbuf()->pubfile()));
        #endif

        return logFile_.good();
    }

    void recover() {
        if (!std::ifstream(walFilePath_).good()) {
            return; // Файла нет, восстанавливать нечего
        }

        std::ifstream inFile(walFilePath_, std::ios::binary);
        if (!inFile.is_open()) return;

        std::cout << "[WAL] Starting recovery process..." << std::endl;
        int appliedCount = 0;

        while (inFile.peek() != EOF) {
            WALEntryHeader hdr;
            inFile.read(reinterpret_cast<char*>(&hdr), sizeof(WALEntryHeader));
            if (!inFile.good()) break;

            // Проверка CRC заголовка
            uint32_t calculatedHdrCrc = CRC32::calculate(&hdr, sizeof(WALEntryHeader) - sizeof(hdr.crc32));
            if (calculatedHdrCrc != hdr.crc32) {
                std::cerr << "[WAL] Corrupted header detected at LSN " << hdr.lsn << ". Stop recovery." << std::endl;
                break;
            }

            WALEntry entry;
            entry.lsn = hdr.lsn;
            entry.transactionId = hdr.transactionId;
            entry.type = static_cast<WALEntryType>(hdr.type);
            entry.offset = hdr.offset;

            // Чтение имени таблицы
            if (hdr.tableNameLen > 0) {
                std::vector<char> buf(hdr.tableNameLen);
                inFile.read(buf.data(), hdr.tableNameLen);
                entry.tableName.assign(buf.begin(), buf.end());
            }

            // Чтение данных
            if (hdr.dataSize > 0) {
                entry.data.resize(hdr.dataSize);
                inFile.read(reinterpret_cast<char*>(entry.data.data()), hdr.dataSize);
                
                // Чтение и проверка CRC данных
                uint32_t storedDataCrc = 0;
                inFile.read(reinterpret_cast<char*>(&storedDataCrc), sizeof(storedDataCrc));
                uint32_t calcDataCrc = CRC32::calculate(entry.data.data(), entry.data.size());
                
                if (storedDataCrc != calcDataCrc) {
                    std::cerr << "[WAL] Data corruption detected at LSN " << hdr.lsn << ". Skipping." << std::endl;
                    continue; 
                }
            }

            // Применение записи
            if (entry.type == WALEntryType::OP_CHECKPOINT) {
                // Если встретили чекпоинт, значит всё до него уже сохранено в основных файлах.
                // Но в нашей упрощенной модели мы просто идем до конца, 
                // так как чекпоинт очищает файл. Если он тут есть - файл битый или логика сложная.
                continue;
            }

            if (recoveryCallback_) {
                try {
                    recoveryCallback_(entry);
                    appliedCount++;
                } catch (const std::exception& e) {
                    std::cerr << "[WAL] Failed to apply LSN " << hdr.lsn << ": " << e.what() << std::endl;
                }
            }
            
            // Обновляем счетчики, чтобы продолжить логирование с нужного места
            if (hdr.lsn >= currentLsn_) currentLsn_ = hdr.lsn + 1;
            if (hdr.transactionId >= currentTxId_) currentTxId_ = hdr.transactionId;
        }

        std::cout << "[WAL] Recovery complete. Applied " << appliedCount << " entries." << std::endl;
    }
};

} // namespace DBMS
