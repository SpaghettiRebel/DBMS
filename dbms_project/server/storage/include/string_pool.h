#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <cstdint>
#include <optional>

class StringPool {
private:
    std::string pool_path;
    std::unordered_map<std::string, uint32_t> str_to_id;
    std::vector<std::string> id_to_str;
    std::fstream file;

    void load();

public:
    explicit StringPool(const std::string& path);
    ~StringPool();

    uint32_t intern(const std::string& str);
    std::string get(uint32_t id) const;
    std::optional<uint32_t> get_id_if_exists(const std::string& str) const;
};
