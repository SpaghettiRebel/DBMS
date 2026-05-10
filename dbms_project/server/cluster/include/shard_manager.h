#ifndef SHARD_MANAGER_H
#define SHARD_MANAGER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstdint>

/**
 * @brief Менеджер шардирования данных между узлами
 * 
 * Реализует консистентное хэширование для распределения
 * данных между Storage-узлами кластера.
 */
class ShardManager {
public:
    /**
     * @brief Конструктор менеджера шардирования
     * @param num_virtual_nodes Количество виртуальных узлов на физический
     */
    explicit ShardManager(size_t num_virtual_nodes = 150);
    
    ~ShardManager() = default;
    
    /**
     * @brief Добавление узла в кольцо шардирования
     * @param node_id Идентификатор узла
     */
    void add_node(const std::string& node_id);
    
    /**
     * @brief Удаление узла из кольца шардирования
     * @param node_id Идентификатор узла
     */
    void remove_node(const std::string& node_id);
    
    /**
     * @brief Получение целевого узла для ключа
     * @param key Ключ для хэширования (имя таблицы, ID записи)
     * @return Идентификатор узла
     */
    std::string get_node_for_key(const std::string& key);
    
    /**
     * @brief Получение списка всех узлов
     */
    std::vector<std::string> get_all_nodes() const;
    
    /**
     * @brief Получение количества узлов
     */
    size_t get_node_count() const;
    
    /**
     * @brief Извлечение ключа таблицы из SQL запроса
     * @param query SQL запрос
     * @return Имя таблицы или пустая строка
     */
    static std::string extract_table_name(const std::string& query);
    
    /**
     * @brief Проверка является ли запрос глобальным (не шардируемым)
     * @param query SQL запрос
     * @return true если запрос должен выполняться на всех узлах
     */
    static bool is_global_query(const std::string& query);

private:
    /**
     * @brief Вычисление хэша ключа
     */
    uint32_t hash_key(const std::string& key) const;
    
    size_t num_virtual_nodes_;
    std::vector<std::pair<uint32_t, std::string>> ring_;
    mutable std::mutex mutex_;
};

#endif // SHARD_MANAGER_H
