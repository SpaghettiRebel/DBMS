#include "../include/string_pool.h"
#include <iostream>
#include <cstring>

StringPool::StringPool(const std::string& path) : pool_path(path) {
    file.open(pool_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        file.clear();
        file.open(pool_path, std::ios::out | std::ios::binary | std::ios::trunc);
        file.close();
        file.open(pool_path, std::ios::in | std::ios::out | std::ios::binary);
    }
    load();
}

StringPool::~StringPool() {
    if (file.is_open()) {
        file.close();
    }
}

void StringPool::load() {
    file.seekg(0, std::ios::end);
    std::streampos size = file.tellg();
    file.seekg(0, std::ios::beg);

    while (file.tellg() < size) {
        uint32_t len;
        if (!file.read(reinterpret_cast<char*>(&len), sizeof(len))) break;

        std::string s(len, '\0');
        if (!file.read(&s[0], len)) break;

        uint32_t id = static_cast<uint32_t>(id_to_str.size());
        str_to_id[s] = id;
        id_to_str.push_back(s);
    }
}

uint32_t StringPool::intern(const std::string& str) {
    auto it = str_to_id.find(str);
    if (it != str_to_id.end()) {
        return it->second;
    }

    uint32_t id = static_cast<uint32_t>(id_to_str.size());
    str_to_id[str] = id;
    id_to_str.push_back(str);

    // Persist immediately [cite: 17]
    file.seekp(0, std::ios::end);
    uint32_t len = static_cast<uint32_t>(str.size());
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(str.data(), len);
    file.flush();

    return id;
}

std::string StringPool::get(uint32_t id) const {
    if (id >= id_to_str.size()) {
        throw std::out_of_range("Invalid string ID");
    }
    return id_to_str[id];
}
