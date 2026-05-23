#include "shard_manager.h"
#include <algorithm>
#include <functional>
#include <cctype>
#include <regex>

// простая хэш-функция (FNV-1a)
static uint32_t fnv1a_hash(const std::string& key) {
    const uint32_t FNV_PRIME = 16777619;
    const uint32_t FNV_OFFSET = 2166136261;
    
    uint32_t hash = FNV_OFFSET;
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }
    return hash;
}

ShardManager::ShardManager(size_t num_virtual_nodes)
    : num_virtual_nodes_(num_virtual_nodes)
{
}
// добавление серверов в кольцо
void ShardManager::add_node(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // проверка наличия узла
    for (const auto& pair : ring_) {
        if (pair.second == node_id) {
            return; // уже существует
        }
    }
    
    // добавление виртуальных узлов
    for (size_t i = 0; i < num_virtual_nodes_; ++i) {
        std::string virtual_key = node_id + "_vn" + std::to_string(i);
        uint32_t hash = hash_key(virtual_key);
        ring_.emplace_back(hash, node_id);
    }
    
    // сортировка кольца по хэшам
    std::sort(ring_.begin(), ring_.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
}
// удаление сервера
void ShardManager::remove_node(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    ring_.erase(
        std::remove_if(ring_.begin(), ring_.end(),
                       [&node_id](const auto& pair) { return pair.second == node_id; }),
        ring_.end()
    );
}
// узнать куда отправить данные или где искать(идем по кольцу до первого сервера если до конца то берем первый сервак в начале)
std::string ShardManager::get_node_for_key(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (ring_.empty()) {
        throw std::runtime_error("No nodes available in the cluster");
    }
    
    uint32_t key_hash = hash_key(key);
    
    // бинарный поиск первого узла с хэшем >= ключа
    auto it = std::lower_bound(ring_.begin(), ring_.end(), key_hash,
                               [](const auto& pair, uint32_t hash) {
                                   return pair.first < hash;
                               });
    
    // если не найдено, берем первый узел
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    
    return it->second;
}
// получить список всех живых серверов
std::vector<std::string> ShardManager::get_all_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::string> nodes;
    std::unordered_map<std::string, bool> seen;
    
    for (const auto& pair : ring_) {
        if (seen.find(pair.second) == seen.end()) {
            nodes.push_back(pair.second);
            seen[pair.second] = true;
        }
    }
    
    return nodes;
}
// узнать, сколько физических компьютеров прямо сейчас держат твою базу данных
size_t ShardManager::get_node_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::unordered_map<std::string, bool> unique_nodes;
    for (const auto& pair : ring_) {
        unique_nodes[pair.second] = true;
    }
    
    return unique_nodes.size();
}
// интерфейс для хэширования
uint32_t ShardManager::hash_key(const std::string& key) const {
    return fnv1a_hash(key);
}
// вытаскивает имя таблицы из запроса
std::string ShardManager::extract_table_name(const std::string& query) {
    // приведение к верхнему регистру для поиска ключевых слов
    std::string upper_query;
    upper_query.reserve(query.size());
    for (char c : query) {
        upper_query += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    std::vector<std::regex> patterns = {
        std::regex(R"(FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?))", std::regex::icase),
        std::regex(R"(INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?))", std::regex::icase),
        std::regex(R"(UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?))", std::regex::icase),
        std::regex(R"(TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?))", std::regex::icase)
    };
    
    for (const auto& pattern : patterns) {
        std::smatch match;
        if (std::regex_search(query, match, pattern) && match.size() > 1) {
            std::string table_name = match[1].str();
            // удаление возможных суффиксов базы данных
            size_t dot_pos = table_name.find('.');
            if (dot_pos != std::string::npos) {
                table_name = table_name.substr(dot_pos + 1);
            }
            return table_name;
        }
    }
    
    return "";
}
// проверяет, является ли запрос глобальной командой администрирования (CREATE DATABASE, DROP DATABASE, USE, REVERT) т.к. они на все кластеры сразу идут
bool ShardManager::is_global_query(const std::string& query) {
    std::string upper_query;
    upper_query.reserve(query.size());
    for (char c : query) {
        upper_query += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    
    // Глобальные запросы (не шардируемые)
    std::vector<std::string> global_keywords = {
        "CREATE DATABASE",
        "DROP DATABASE",
        "USE ",
        "REVERT"
    };
    
    for (const auto& keyword : global_keywords) {
        if (upper_query.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}
