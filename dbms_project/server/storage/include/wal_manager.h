#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <memory>
#include <mutex>
#include <functional>
#include <unordered_map>
#include "../shared/QueryPlan.h"
#include "table_metadata.h"
#include "record.h"

// ============================================================================
// Write-Ahead Logging (WAL) Manager
// ============================================================================
// Обеспечивает целостность данных (ACID - Durability) путем записи всех 
// изменений в журнал ДО их применения к основным файлам данных.
// При сбое системы данные восстанавливаются из WAL при перезапуске.
// ============================================================================

constexpr uint32_t WAL_MAGIC = 0x57414C4D;  // "WALM"
constexpr size_t WAL_VERSION = 1;
constexpr size_t WAL_BUFFER_SIZE = 4096;

// Типы операций WAL
enum class WALOperationType : uint8_t {
    OP_INSERT = 0x01,
    OP_UPDATE = 0x02,
    OP_DELETE = 0x03,
    OP_PAGE_WRITE = 0x04,
    OP_CHECKPOINT = 0x05,
    OP_REVERT = 0x06
};

// Запись в журнале WAL
struct WALRecord {
    uint64_t lsn;                    // Log Sequence Number - уникальный номер записи
    uint64_t transaction_id;         // ID транзакции
    WALOperationType operation;      // Тип операции
    std::string table_name;          // Имя таблицы
    pos_t record_pos;                // Позиция записи (для INSERT/UPDATE/DELETE)
    std::vector<char> old_data;      // Старые данные (для UPDATE/DELETE)
    std::vector<char> new_data;      // Новые данные (для INSERT/UPDATE)
    uint64_t timestamp;              // Время операции (Unix timestamp ms)
    uint32_t checksum;               // Контрольная сумма записи
    
    WALRecord() : lsn(0), transaction_id(0), operation(WALOperationType::OP_INSERT), 
                  timestamp(0), checksum(0) {}
};

// Callback для восстановления данных из WAL
using WALRecoveryCallback = std::function<void(const WALRecord&)>;

class WriteAheadLog {
public:
    // Конструктор - открывает или создает WAL файл
    explicit WriteAheadLog(const std::string& wal_path);
    
    // Деструктор - закрывает WAL и выполняет checkpoint
    ~WriteAheadLog();
    
    // Запрет копирования
    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;
    
    // =========================================================================
    // Операции журналирования
    // =========================================================================
    
    // Начало новой транзакции (возвращает ID)
    uint64_t beginTransaction();
    
    // Журналирование операции INSERT
    void logInsert(uint64_t txn_id, const std::string& table_name, 
                   const pos_t& pos, const std::vector<char>& data);
    
    // Журналирование операции UPDATE
    void logUpdate(uint64_t txn_id, const std::string& table_name,
                   const pos_t& pos, const std::vector<char>& old_data,
                   const std::vector<char>& new_data);
    
    // Журналирование операции DELETE
    void logDelete(uint64_t txn_id, const std::string& table_name,
                   const pos_t& pos, const std::vector<char>& old_data);
    
    // Журналирование записи страницы
    void logPageWrite(uint64_t txn_id, const std::string& table_name,
                      uint32_t page_id, const std::vector<char>& old_data,
                      const std::vector<char>& new_data);
    
    // Журналирование операции REVERT
    void logRevert(uint64_t txn_id, const std::string& table_name,
                   uint64_t target_timestamp);
    
    // Создание контрольной точки (checkpoint)
    void checkpoint();
    
    // Завершение транзакции (сброс буфера на диск)
    void commitTransaction(uint64_t txn_id);
    
    // =========================================================================
    // Восстановление
    // =========================================================================
    
    // Восстановление состояния из WAL
    // Вызывает callback для каждой записи, которую нужно применить
    void recover(WALRecoveryCallback callback);
    
    // Получение последней LSN
    uint64_t getLastLSN() const { return current_lsn_; }
    
    // Получение количества активных транзакций
    size_t getActiveTransactionCount() const;
    
    // =========================================================================
    // Утилиты
    // =========================================================================
    
    // Принудительная синхронизация WAL на диск
    void sync();
    
    // Очистка старых записей WAL (после checkpoint)
    void truncateBefore(uint64_t lsn);
    
private:
    std::string wal_path_;
    std::fstream file_;
    uint64_t current_lsn_;
    uint64_t last_checkpoint_lsn_;
    std::unordered_map<uint64_t, bool> active_transactions_;
    mutable std::mutex mutex_;
    
    // Буфер для пакетной записи
    std::vector<WALRecord> write_buffer_;
    
    // Вспомогательные функции
    void writeRecord(const WALRecord& record);
    void flushBuffer();
    uint32_t calculateChecksum(const WALRecord& record);
    bool validateRecord(const WALRecord& record);
    uint64_t getCurrentTimestamp() const;
    WALRecord readRecord();
};

// ============================================================================
// Transaction Guard - RAII обертка для автоматического управления транзакциями
// ============================================================================
class WALTransactionGuard {
public:
    explicit WALTransactionGuard(WriteAheadLog& wal) 
        : wal_(wal), txn_id_(wal.beginTransaction()), committed_(false) {}
    
    ~WALTransactionGuard() {
        if (!committed_) {
            // Транзакция не была закоммичена - откат не требуется,
            // так как WAL содержит полные данные для восстановления
        }
    }
    
    uint64_t getTransactionId() const { return txn_id_; }
    
    void commit() {
        wal_.commitTransaction(txn_id_);
        committed_ = true;
    }
    
private:
    WriteAheadLog& wal_;
    uint64_t txn_id_;
    bool committed_;
};
