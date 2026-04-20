#include "../include/journal.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {
template <typename T>
void write_pod(std::ofstream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    if (!out) {
        throw std::runtime_error("Failed to write journal record");
    }
}

template <typename T>
bool read_pod(std::ifstream& in, T& value) {
    return static_cast<bool>(in.read(reinterpret_cast<char*>(&value), sizeof(T)));
}

bool read_bytes(std::ifstream& in, std::string& s, std::uint32_t len) {
    s.assign(len, '\0');
    if (len == 0) return true;
    return static_cast<bool>(in.read(s.data(), len));
}

bool read_bytes(std::ifstream& in, std::vector<char>& v, std::uint32_t len) {
    v.assign(len, 0);
    if (len == 0) return true;
    return static_cast<bool>(in.read(v.data(), len));
}
}  // namespace

Journal::Journal(const std::string& path) : journal_path(path) {
    fs::path p(journal_path);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path());
    }

    file.open(journal_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        {
            std::ofstream create(journal_path, std::ios::binary | std::ios::trunc);
            if (!create) {
                throw std::runtime_error("Failed to create journal file: " + journal_path);
            }
        }

        file.clear();
        file.open(journal_path, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open journal file: " + journal_path);
        }
    }
}

Journal::~Journal() {
    if (file.is_open()) {
        file.flush();
        file.close();
    }
}

void Journal::log(const JournalEntry& entry) {
    if (!file.is_open()) {
        throw std::runtime_error("Journal is not open: " + journal_path);
    }

    file.clear();
    file.seekp(0, std::ios::end);
    if (!file) {
        throw std::runtime_error("Failed to seek journal end: " + journal_path);
    }

    const std::uint32_t ts_len = static_cast<std::uint32_t>(entry.timestamp.size());
    const std::uint32_t table_len = static_cast<std::uint32_t>(entry.table_name.size());
    const std::uint32_t old_len = static_cast<std::uint32_t>(entry.old_data.size());
    const std::uint32_t new_len = static_cast<std::uint32_t>(entry.new_data.size());
    const std::uint8_t op = static_cast<std::uint8_t>(entry.op);

    write_pod(file, ts_len);
    if (ts_len && !file.write(entry.timestamp.data(), ts_len)) {
        throw std::runtime_error("Failed to write journal timestamp");
    }

    write_pod(file, op);

    write_pod(file, table_len);
    if (table_len && !file.write(entry.table_name.data(), table_len)) {
        throw std::runtime_error("Failed to write journal table name");
    }

    file.write(reinterpret_cast<const char*>(&entry.record_pos), sizeof(pos_t));
    if (!file) {
        throw std::runtime_error("Failed to write journal record position");
    }

    write_pod(file, old_len);
    if (old_len && !file.write(entry.old_data.data(), old_len)) {
        throw std::runtime_error("Failed to write journal old_data");
    }

    write_pod(file, new_len);
    if (new_len && !file.write(entry.new_data.data(), new_len)) {
        throw std::runtime_error("Failed to write journal new_data");
    }

    file.flush();
    if (!file) {
        throw std::runtime_error("Failed to flush journal");
    }
}

std::vector<JournalEntry> Journal::get_all_entries() {
    std::vector<JournalEntry> entries;

    if (!file.is_open()) {
        return entries;
    }

    file.clear();
    file.seekg(0, std::ios::beg);
    if (!file) {
        throw std::runtime_error("Failed to seek journal begin: " + journal_path);
    }

    while (true) {
        JournalEntry entry{};
        std::uint32_t ts_len = 0;
        std::uint32_t table_len = 0;
        std::uint32_t old_len = 0;
        std::uint32_t new_len = 0;
        std::uint8_t op = 0;

        std::streampos record_start = file.tellg();

        if (!read_pod(file, ts_len)) {
            break;
        }
        if (!read_bytes(file, entry.timestamp, ts_len)) {
            break;  // partial tail
        }

        if (!read_pod(file, op)) {
            break;
        }
        entry.op = static_cast<JournalOp>(op);

        if (!read_pod(file, table_len)) {
            break;
        }
        if (!read_bytes(file, entry.table_name, table_len)) {
            break;
        }

        if (!file.read(reinterpret_cast<char*>(&entry.record_pos), sizeof(pos_t))) {
            break;
        }

        if (!read_pod(file, old_len)) {
            break;
        }
        if (!read_bytes(file, entry.old_data, old_len)) {
            break;
        }

        if (!read_pod(file, new_len)) {
            break;
        }
        if (!read_bytes(file, entry.new_data, new_len)) {
            break;
        }

        entries.push_back(std::move(entry));

        if (file.fail() && !file.eof()) {
            break;
        }

        (void)record_start;
    }

    return entries;
}