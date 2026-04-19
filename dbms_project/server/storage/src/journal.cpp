#include "../include/journal.h"
#include <chrono>
#include <iomanip>
#include <sstream>

Journal::Journal(const std::string& path) : journal_path(path) {
    file.open(journal_path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!file.is_open()) {
        file.clear();
        file.open(journal_path, std::ios::out | std::ios::binary | std::ios::trunc);
        file.close();
        file.open(journal_path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }
}

Journal::~Journal() {
    if (file.is_open()) file.close();
}

void Journal::log(const JournalEntry& entry) {
    // Формат записи в лог: [TimestampLen][Timestamp][Op][TableLen][Table][OldDataLen][OldData][NewDataLen][NewData]
    uint32_t ts_len = static_cast<uint32_t>(entry.timestamp.size());
    uint32_t table_len = static_cast<uint32_t>(entry.table_name.size());
    uint32_t old_len = static_cast<uint32_t>(entry.old_data.size());
    uint32_t new_len = static_cast<uint32_t>(entry.new_data.size());
    uint8_t op = static_cast<uint8_t>(entry.op);

    file.seekp(0, std::ios::end);
    file.write(reinterpret_cast<char*>(&ts_len), sizeof(ts_len));
    file.write(entry.timestamp.data(), ts_len);
    file.write(reinterpret_cast<char*>(&op), sizeof(op));
    file.write(reinterpret_cast<char*>(&table_len), sizeof(table_len));
    file.write(entry.table_name.data(), table_len);
    file.write(reinterpret_cast<char*>(&old_len), sizeof(old_len));
    file.write(entry.old_data.data(), old_len);
    file.write(reinterpret_cast<char*>(&new_len), sizeof(new_len));
    file.write(entry.new_data.data(), new_len);
    file.flush();
}

std::vector<JournalEntry> Journal::get_all_entries() {
    std::vector<JournalEntry> entries;
    file.seekg(0, std::ios::end);
    std::streampos size = file.tellg();
    file.seekg(0, std::ios::beg);

    while (file.tellg() < size) {
        JournalEntry entry;
        uint32_t ts_len, table_len, old_len, new_len;
        uint8_t op;

        if (!file.read(reinterpret_cast<char*>(&ts_len), sizeof(ts_len))) break;
        entry.timestamp.resize(ts_len);
        file.read(&entry.timestamp[0], ts_len);

        file.read(reinterpret_cast<char*>(&op), sizeof(op));
        entry.op = static_cast<JournalOp>(op);

        file.read(reinterpret_cast<char*>(&table_len), sizeof(table_len));
        entry.table_name.resize(table_len);
        file.read(&entry.table_name[0], table_len);

        file.read(reinterpret_cast<char*>(&old_len), sizeof(old_len));
        entry.old_data.resize(old_len);
        file.read(entry.old_data.data(), old_len);

        file.read(reinterpret_cast<char*>(&new_len), sizeof(new_len));
        entry.new_data.resize(new_len);
        file.read(entry.new_data.data(), new_len);

        entries.push_back(entry);
    }
    return entries;
}
