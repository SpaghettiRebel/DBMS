#include "wal_manager.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <algorithm>

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

// Запись POD-типа в поток
template <typename T>
void writePod(std::ostream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    if (!out) {
        throw std::runtime_error("Failed to write WAL record");
    }
}

// Чтение POD-типа из потока
template <typename T>
bool readPod(std::istream& in, T& value) {
    return static_cast<bool>(in.read(reinterpret_cast<char*>(&value), sizeof(T)));
}

// Чтение строки из потока
bool readString(std::istream& in, std::string& s, uint32_t len) {
    s.assign(len, '\0');
    if (len == 0) return true;
    return static_cast<bool>(in.read(s.data(), len));
}

// Чтение вектора из потока
bool readVector(std::istream& in, std::vector<char>& v, uint32_t len) {
    v.assign(len, 0);
    if (len == 0) return true;
    return static_cast<bool>(in.read(v.data(), len));
}

// Простая хэш-функция для контрольной суммы
uint32_t simpleHash(const uint8_t* data, size_t size) {
    uint32_t hash = 0;
    for (size_t i = 0; i < size; ++i) {
        hash = ((hash << 5) + hash) + data[i];
    }
    return hash;
}

}  // namespace

// ============================================================================
// Конструктор / Деструктор
// ============================================================================

WriteAheadLog::WriteAheadLog(const std::string& wal_path)
    : wal_path_(wal_path), current_lsn_(0), last_checkpoint_lsn_(0) {
    
    ensureParentDir(wal_path_);
    
    // Открываем или создаем файл WAL
    if (!fileExists(wal_path_)) {
        // Создаем новый файл
        {
            std::ofstream create(wal_path_, std::ios::binary | std::ios::trunc);
            if (!create) {
                throw std::runtime_error("Failed to create WAL file: " + wal_path_);
            }
            // Записываем заголовок
            writePod(create, WAL_MAGIC);
            writePod(create, WAL_VERSION);
            writePod(create, current_lsn_);
        }
    }
    
    file_.open(wal_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::ate);
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + wal_path_);
    }
    
    // Читаем заголовок и восстанавливаем LSN
    file_.seekg(0, std::ios::beg);
    uint32_t magic = 0;
    size_t version = 0;
    
    if (!readPod(file_, magic) || magic != WAL_MAGIC) {
        throw std::runtime_error("Invalid WAL file: magic number mismatch");
    }
    
    if (!readPod(file_, version) || version != WAL_VERSION) {
        throw std::runtime_error("Invalid WAL file: version mismatch");
    }
    
    if (!readPod(file_, current_lsn_)) {
        current_lsn_ = 0;
    }
    
    // Переходим в конец файла для записи
    file_.seekp(0, std::ios::end);
}

WriteAheadLog::~WriteAheadLog() {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        flushBuffer();
        checkpoint();
        if (file_.is_open()) {
            file_.flush();
            file_.close();
        }
    } catch (...) {
        // Игнорируем ошибки при закрытии
    }
}

// ============================================================================
// Операции журналирования
// ============================================================================

uint64_t WriteAheadLog::beginTransaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint64_t txn_id = getCurrentTimestamp();
    active_transactions_[txn_id] = true;
    
    return txn_id;
}

void WriteAheadLog::logInsert(uint64_t txn_id, const std::string& table_name,
                               const pos_t& pos, const std::vector<char>& data) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WALRecord record;
    record.lsn = ++current_lsn_;
    record.transaction_id = txn_id;
    record.operation = WALOperationType::OP_INSERT;
    record.table_name = table_name;
    record.record_pos = pos;
    record.new_data = data;
    record.timestamp = getCurrentTimestamp();
    record.checksum = calculateChecksum(record);
    
    write_buffer_.push_back(record);
    
    if (write_buffer_.size() >= WAL_BUFFER_SIZE) {
        flushBuffer();
    }
}

void WriteAheadLog::logUpdate(uint64_t txn_id, const std::string& table_name,
                               const pos_t& pos, const std::vector<char>& old_data,
                               const std::vector<char>& new_data) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WALRecord record;
    record.lsn = ++current_lsn_;
    record.transaction_id = txn_id;
    record.operation = WALOperationType::OP_UPDATE;
    record.table_name = table_name;
    record.record_pos = pos;
    record.old_data = old_data;
    record.new_data = new_data;
    record.timestamp = getCurrentTimestamp();
    record.checksum = calculateChecksum(record);
    
    write_buffer_.push_back(record);
    
    if (write_buffer_.size() >= WAL_BUFFER_SIZE) {
        flushBuffer();
    }
}

void WriteAheadLog::logDelete(uint64_t txn_id, const std::string& table_name,
                               const pos_t& pos, const std::vector<char>& old_data) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WALRecord record;
    record.lsn = ++current_lsn_;
    record.transaction_id = txn_id;
    record.operation = WALOperationType::OP_DELETE;
    record.table_name = table_name;
    record.record_pos = pos;
    record.old_data = old_data;
    record.timestamp = getCurrentTimestamp();
    record.checksum = calculateChecksum(record);
    
    write_buffer_.push_back(record);
    
    if (write_buffer_.size() >= WAL_BUFFER_SIZE) {
        flushBuffer();
    }
}

void WriteAheadLog::logPageWrite(uint64_t txn_id, const std::string& table_name,
                                  uint32_t page_id, const std::vector<char>& old_data,
                                  const std::vector<char>& new_data) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WALRecord record;
    record.lsn = ++current_lsn_;
    record.transaction_id = txn_id;
    record.operation = WALOperationType::OP_PAGE_WRITE;
    record.table_name = table_name;
    record.record_pos = {page_id, 0};  // Используем page_id как часть позиции
    record.old_data = old_data;
    record.new_data = new_data;
    record.timestamp = getCurrentTimestamp();
    record.checksum = calculateChecksum(record);
    
    write_buffer_.push_back(record);
    
    if (write_buffer_.size() >= WAL_BUFFER_SIZE) {
        flushBuffer();
    }
}

void WriteAheadLog::logRevert(uint64_t txn_id, const std::string& table_name,
                               uint64_t target_timestamp) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WALRecord record;
    record.lsn = ++current_lsn_;
    record.transaction_id = txn_id;
    record.operation = WALOperationType::OP_REVERT;
    record.table_name = table_name;
    record.timestamp = target_timestamp;
    record.checksum = calculateChecksum(record);
    
    write_buffer_.push_back(record);
    flushBuffer();  // REVERT должен быть записан немедленно
}

void WriteAheadLog::checkpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Создаем запись checkpoint
    WALRecord record;
    record.lsn = ++current_lsn_;
    record.transaction_id = 0;
    record.operation = WALOperationType::OP_CHECKPOINT;
    record.timestamp = getCurrentTimestamp();
    record.checksum = calculateChecksum(record);
    
    write_buffer_.push_back(record);
    flushBuffer();
    
    last_checkpoint_lsn_ = current_lsn_;
    
    // Обновляем заголовок файла с новой LSN
    file_.seekp(0, std::ios::beg);
    writePod(file_, WAL_MAGIC);
    writePod(file_, WAL_VERSION);
    writePod(file_, current_lsn_);
    file_.flush();
}

void WriteAheadLog::commitTransaction(uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Сбрасываем буфер на диск
    flushBuffer();
    
    // Удаляем транзакцию из активных
    active_transactions_.erase(txn_id);
}

// ============================================================================
// Восстановление
// ============================================================================

void WriteAheadLog::recover(WALRecoveryCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!file_.is_open()) {
        return;
    }
    
    // Сохраняем текущую позицию
    std::streampos current_pos = file_.tellg();
    
    // Переходим в начало (после заголовка)
    file_.seekg(sizeof(uint32_t) + sizeof(size_t) + sizeof(uint64_t), std::ios::beg);
    
    // Читаем все записи и применяем их
    while (true) {
        WALRecord record;
        
        // Читаем LSN
        if (!readPod(file_, record.lsn)) {
            break;  // Конец файла
        }
        
        // Читаем transaction_id
        if (!readPod(file_, record.transaction_id)) {
            break;
        }
        
        // Читаем тип операции
        uint8_t op_type = 0;
        if (!readPod(file_, op_type)) {
            break;
        }
        record.operation = static_cast<WALOperationType>(op_type);
        
        // Читаем имя таблицы
        uint32_t table_len = 0;
        if (!readPod(file_, table_len)) {
            break;
        }
        if (!readString(file_, record.table_name, table_len)) {
            break;
        }
        
        // Читаем позицию записи
        if (!file_.read(reinterpret_cast<char*>(&record.record_pos), sizeof(pos_t))) {
            break;
        }
        
        // Читаем старые данные
        uint32_t old_len = 0;
        if (!readPod(file_, old_len)) {
            break;
        }
        if (!readVector(file_, record.old_data, old_len)) {
            break;
        }
        
        // Читаем новые данные
        uint32_t new_len = 0;
        if (!readPod(file_, new_len)) {
            break;
        }
        if (!readVector(file_, record.new_data, new_len)) {
            break;
        }
        
        // Читаем timestamp
        if (!readPod(file_, record.timestamp)) {
            break;
        }
        
        // Читаем checksum
        if (!readPod(file_, record.checksum)) {
            break;
        }
        
        // Проверяем контрольную сумму
        if (!validateRecord(record)) {
            // Запись повреждена - останавливаем восстановление
            break;
        }
        
        // Вызываем callback для применения записи
        if (callback) {
            callback(record);
        }
    }
    
    // Восстанавливаем позицию
    file_.clear();
    file_.seekg(current_pos, std::ios::beg);
}

size_t WriteAheadLog::getActiveTransactionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_transactions_.size();
}

// ============================================================================
// Утилиты
// ============================================================================

void WriteAheadLog::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    flushBuffer();
    
    if (file_.is_open()) {
        file_.flush();
        file_.sync();
    }
}

void WriteAheadLog::truncateBefore(uint64_t lsn) {
    // В простой реализации просто создаем checkpoint
    // В production можно реализовать сжатие WAL
    checkpoint();
}

void WriteAheadLog::writeRecord(const WALRecord& record) {
    // Записываем все поля записи
    writePod(file_, record.lsn);
    writePod(file_, record.transaction_id);
    writePod(file_, static_cast<uint8_t>(record.operation));
    
    const uint32_t table_len = static_cast<uint32_t>(record.table_name.size());
    writePod(file_, table_len);
    if (table_len > 0) {
        file_.write(record.table_name.data(), table_len);
    }
    
    file_.write(reinterpret_cast<const char*>(&record.record_pos), sizeof(pos_t));
    
    const uint32_t old_len = static_cast<uint32_t>(record.old_data.size());
    writePod(file_, old_len);
    if (old_len > 0) {
        file_.write(record.old_data.data(), old_len);
    }
    
    const uint32_t new_len = static_cast<uint32_t>(record.new_data.size());
    writePod(file_, new_len);
    if (new_len > 0) {
        file_.write(record.new_data.data(), new_len);
    }
    
    writePod(file_, record.timestamp);
    writePod(file_, record.checksum);
    
    if (!file_) {
        throw std::runtime_error("Failed to write WAL record");
    }
}

void WriteAheadLog::flushBuffer() {
    if (write_buffer_.empty()) {
        return;
    }
    
    // Записываем все записи из буфера
    for (const auto& record : write_buffer_) {
        writeRecord(record);
    }
    
    // Принудительно сбрасываем на диск (fsync)
    file_.flush();
    file_.sync();
    
    write_buffer_.clear();
}

uint32_t WriteAheadLog::calculateChecksum(const WALRecord& record) {
    // Собираем данные для хэширования
    std::vector<uint8_t> data;
    
    // Добавляем LSN и transaction_id
    const uint64_t lsn_be = record.lsn;
    const uint64_t txn_be = record.transaction_id;
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&lsn_be), 
                reinterpret_cast<const uint8_t*>(&lsn_be) + sizeof(lsn_be));
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&txn_be), 
                reinterpret_cast<const uint8_t*>(&txn_be) + sizeof(txn_be));
    
    // Добавляем тип операции
    data.push_back(static_cast<uint8_t>(record.operation));
    
    // Добавляем имя таблицы
    data.insert(data.end(), record.table_name.begin(), record.table_name.end());
    
    // Добавляем позицию
    const pos_t pos = record.record_pos;
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&pos), 
                reinterpret_cast<const uint8_t*>(&pos) + sizeof(pos));
    
    // Добавляем старые данные
    data.insert(data.end(), record.old_data.begin(), record.old_data.end());
    
    // Добавляем новые данные
    data.insert(data.end(), record.new_data.begin(), record.new_data.end());
    
    // Добавляем timestamp
    const uint64_t ts_be = record.timestamp;
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&ts_be), 
                reinterpret_cast<const uint8_t*>(&ts_be) + sizeof(ts_be));
    
    return simpleHash(data.data(), data.size());
}

bool WriteAheadLog::validateRecord(const WALRecord& record) {
    if (record.checksum == 0) {
        return true;  // Записи без checksum считаем валидными
    }
    
    uint32_t expected = calculateChecksum(record);
    return record.checksum == expected;
}

uint64_t WriteAheadLog::getCurrentTimestamp() const {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto duration = now.time_since_epoch();
    return duration_cast<milliseconds>(duration).count();
}

WALRecord WriteAheadLog::readRecord() {
    // Эта функция используется внутри recover()
    // Реализация уже там есть
    return WALRecord();
}
