#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <optional>

namespace dbms::auth {

/**
 * @brief Тип операции для проверки RBAC
 */
enum class Operation {
    READ,            // SELECT
    WRITE,           // INSERT, UPDATE, DELETE
    CREATE_TABLE,    // CREATE TABLE
    DROP_TABLE,      // DROP TABLE
    DROP_DATABASE    // DROP DATABASE
};

/**
 * @brief Набор разрешений для базы данных
 */
struct Permission {
    bool can_read = false;
    bool can_write = false;
    bool can_create_table = false;
    bool can_drop_table = false;
    bool can_drop_database = false;

    bool check(Operation op) const;
};

/**
 * @brief Аккаунт пользователя
 */
struct Account {
    std::string username;
    std::string password_hash;
    std::string salt;
    std::vector<std::string> groups;
    bool is_admin = false;
};

/**
 * @brief Права доступа для конкретной базы данных
 */
struct DatabasePermissions {
    Permission default_permissions;
    std::unordered_map<std::string, Permission> group_permissions;
    std::unordered_map<std::string, Permission> user_permissions;
};

/**
 * @brief Хранилище аккаунтов и разграничение доступа (RBAC)
 *
 * Реализует:
 * - Хранение аккаунтов (пароли = соль + хэш)
 * - Группы пользователей
 * - Права доступа per-database: default, group, user
 * - Алгоритм проверки: personal → group → default
 * - Персистентность в JSON-файле
 */
class AccountStore {
public:
    explicit AccountStore(const std::string& filepath);

    // --- Управление аккаунтами ---
    bool create_account(const std::string& username, const std::string& password, bool is_admin = false);
    bool delete_account(const std::string& username);
    std::optional<Account> authenticate(const std::string& username, const std::string& password) const;
    std::optional<Account> get_account(const std::string& username) const;
    std::vector<Account> list_accounts() const;

    // --- Управление группами ---
    bool add_to_group(const std::string& username, const std::string& group);
    bool remove_from_group(const std::string& username, const std::string& group);

    // --- Управление правами ---
    void set_default_permissions(const std::string& database, const Permission& perm);
    void set_group_permissions(const std::string& database, const std::string& group, const Permission& perm);
    void set_user_permissions(const std::string& database, const std::string& username, const Permission& perm);
    DatabasePermissions get_database_permissions(const std::string& database) const;

    /**
     * @brief Проверка доступа пользователя к операции
     * Алгоритм:
     * 1. admin → всегда разрешено
     * 2. personal permissions → если есть и разрешает → OK
     * 3. group permissions → если хоть одна группа разрешает → OK
     * 4. default permissions → если разрешает → OK
     * 5. Отказ
     */
    bool check_permission(const std::string& username, const std::string& database, Operation op) const;

    // --- Персистентность ---
    void load();
    void save() const;

private:
    void ensure_admin_exists();

    std::string filepath_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Account> accounts_;
    std::unordered_map<std::string, DatabasePermissions> db_permissions_;
};

} // namespace dbms::auth
