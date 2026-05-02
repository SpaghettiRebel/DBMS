#include "../include/string_pool.h"

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
        throw std::runtime_error("Failed to write string pool");
    }
}

template <typename T>
bool read_pod(std::ifstream& in, T& value) {
    return static_cast<bool>(in.read(reinterpret_cast<char*>(&value), sizeof(T)));
}
}  // namespace

StringPool::StringPool(const std::string& path) : pool_path(path) {
    fs::path p(pool_path);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path());
    }

    file.open(pool_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        {
            std::ofstream create(pool_path, std::ios::binary | std::ios::trunc);
            if (!create) {
                throw std::runtime_error("Failed to create string pool: " + pool_path);
            }
        }

        file.clear();
        file.open(pool_path, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open string pool: " + pool_path);
        }
    }

    load();
}

StringPool::~StringPool() {
    if (file.is_open()) {
        file.flush();
        file.close();
    }
}

void StringPool::load() {
    id_to_str.clear();
    str_to_id.clear();

    if (!file.is_open()) {
        return;
    }

    file.clear();
    file.seekg(0, std::ios::beg);
    if (!file) {
        throw std::runtime_error("Failed to seek string pool begin: " + pool_path);
    }

    while (true) {
        std::uint32_t len = 0;
        if (!read_pod(file, len)) {
            break;
        }

        std::string s(len, '\0');
        if (len != 0) {
            if (!file.read(s.data(), len)) {
                break;  // partial tail
            }
        }

        std::uint32_t id = static_cast<std::uint32_t>(id_to_str.size());
        str_to_id.emplace(s, id);
        id_to_str.push_back(std::move(s));
    }
}

std::uint32_t StringPool::intern(const std::string& str) {
    auto it = str_to_id.find(str);
    if (it != str_to_id.end()) {
        return it->second;
    }

    if (!file.is_open()) {
        throw std::runtime_error("String pool is not open: " + pool_path);
    }

    std::uint32_t id = static_cast<std::uint32_t>(id_to_str.size());

    file.clear();
    file.seekp(0, std::ios::end);
    if (!file) {
        throw std::runtime_error("Failed to seek string pool end: " + pool_path);
    }

    std::uint32_t len = static_cast<std::uint32_t>(str.size());
    write_pod(file, len);
    if (len != 0 && !file.write(str.data(), len)) {
        throw std::runtime_error("Failed to append string to pool");
    }

    file.flush();
    if (!file) {
        throw std::runtime_error("Failed to flush string pool");
    }

    str_to_id.emplace(str, id);
    id_to_str.push_back(str);

    return id;
}

std::string StringPool::get(std::uint32_t id) const {
    if (id >= id_to_str.size()) {
        throw std::out_of_range("Invalid string ID");
    }
    return id_to_str[id];
}

std::optional<std::uint32_t> StringPool::get_id_if_exists(const std::string& str) const {
    auto it = str_to_id.find(str);
    if (it == str_to_id.end()) {
        return std::nullopt;
    }
    return it->second;
}