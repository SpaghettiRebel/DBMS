#include "account_store.h"
#include "auth.h"

#include <fstream>
#include <iostream>
#include <algorithm>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace dbms::auth {

// --- Permission ---

bool Permission::check(Operation op) const {
    switch (op) {
        case Operation::READ:          return can_read;
        case Operation::WRITE:         return can_write;
        case Operation::CREATE_TABLE:  return can_create_table;
        case Operation::DROP_TABLE:    return can_drop_table;
        case Operation::DROP_DATABASE: return can_drop_database;
    }
    return false;
}

// --- JSON helpers ---

namespace {

json permission_to_json(const Permission& p) {
    return {
        {"can_read",          p.can_read},
        {"can_write",         p.can_write},
        {"can_create_table",  p.can_create_table},
        {"can_drop_table",    p.can_drop_table},
        {"can_drop_database", p.can_drop_database}
    };
}

Permission permission_from_json(const json& j) {
    Permission p;
    if (j.contains("can_read"))          p.can_read          = j["can_read"].get<bool>();
    if (j.contains("can_write"))         p.can_write         = j["can_write"].get<bool>();
    if (j.contains("can_create_table"))  p.can_create_table  = j["can_create_table"].get<bool>();
    if (j.contains("can_drop_table"))    p.can_drop_table    = j["can_drop_table"].get<bool>();
    if (j.contains("can_drop_database")) p.can_drop_database = j["can_drop_database"].get<bool>();
    return p;
}

json account_to_json(const Account& a) {
    return {
        {"username",      a.username},
        {"password_hash", a.password_hash},
        {"salt",          a.salt},
        {"groups",        a.groups},
        {"is_admin",      a.is_admin}
    };
}

Account account_from_json(const json& j) {
    Account a;
    a.username      = j.value("username", "");
    a.password_hash = j.value("password_hash", "");
    a.salt          = j.value("salt", "");
    a.is_admin      = j.value("is_admin", false);
    if (j.contains("groups") && j["groups"].is_array()) {
        for (const auto& g : j["groups"]) {
            a.groups.push_back(g.get<std::string>());
        }
    }
    return a;
}

json db_permissions_to_json(const DatabasePermissions& dp) {
    json result;
    result["default"] = permission_to_json(dp.default_permissions);

    json groups_obj = json::object();
    for (const auto& [group, perm] : dp.group_permissions) {
        groups_obj[group] = permission_to_json(perm);
    }
    result["groups"] = groups_obj;

    json users_obj = json::object();
    for (const auto& [user, perm] : dp.user_permissions) {
        users_obj[user] = permission_to_json(perm);
    }
    result["users"] = users_obj;

    return result;
}

DatabasePermissions db_permissions_from_json(const json& j) {
    DatabasePermissions dp;
    if (j.contains("default")) {
        dp.default_permissions = permission_from_json(j["default"]);
    }
    if (j.contains("groups") && j["groups"].is_object()) {
        for (auto& [key, val] : j["groups"].items()) {
            dp.group_permissions[key] = permission_from_json(val);
        }
    }
    if (j.contains("users") && j["users"].is_object()) {
        for (auto& [key, val] : j["users"].items()) {
            dp.user_permissions[key] = permission_from_json(val);
        }
    }
    return dp;
}

} // anonymous namespace

// --- AccountStore ---

AccountStore::AccountStore(const std::string& filepath) : filepath_(filepath) {
    load();
    ensure_admin_exists();
}

void AccountStore::ensure_admin_exists() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (accounts_.find("admin") == accounts_.end()) {
        Account admin;
        admin.username = "admin";
        admin.salt = generate_salt(16);
        admin.password_hash = hash_password("admin", admin.salt);
        admin.is_admin = true;
        accounts_["admin"] = admin;
        // Сохраняем без блокировки (уже залочен)
        // Вызовем save() потом
    }
    // Всегда сохраняем при первой инициализации
    // (mutex_ уже захвачен, вызываем save внутри через unlock+lock? нет, лучше напрямую)
    try {
        json root;
        json accounts_array = json::array();
        for (const auto& [name, acc] : accounts_) {
            accounts_array.push_back(account_to_json(acc));
        }
        root["accounts"] = accounts_array;

        json perms_obj = json::object();
        for (const auto& [db, dp] : db_permissions_) {
            perms_obj[db] = db_permissions_to_json(dp);
        }
        root["permissions"] = perms_obj;

        std::ofstream ofs(filepath_);
        if (ofs.is_open()) {
            ofs << root.dump(2);
        }
    } catch (...) {
        // ignore save errors during init
    }
}

bool AccountStore::create_account(const std::string& username, const std::string& password, bool is_admin) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (accounts_.find(username) != accounts_.end()) {
        return false; // already exists
    }

    Account acc;
    acc.username = username;
    acc.salt = generate_salt(16);
    acc.password_hash = hash_password(password, acc.salt);
    acc.is_admin = is_admin;
    accounts_[username] = acc;

    // auto-save (unlocked save helper)
    try {
        json root;
        json accounts_array = json::array();
        for (const auto& [name, a] : accounts_) {
            accounts_array.push_back(account_to_json(a));
        }
        root["accounts"] = accounts_array;
        json perms_obj = json::object();
        for (const auto& [db, dp] : db_permissions_) {
            perms_obj[db] = db_permissions_to_json(dp);
        }
        root["permissions"] = perms_obj;
        std::ofstream ofs(filepath_);
        if (ofs.is_open()) ofs << root.dump(2);
    } catch (...) {}

    return true;
}

bool AccountStore::delete_account(const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (username == "admin") return false; // cannot delete admin
    auto it = accounts_.find(username);
    if (it == accounts_.end()) return false;
    accounts_.erase(it);
    // remove user-level permissions
    for (auto& [db, dp] : db_permissions_) {
        dp.user_permissions.erase(username);
    }
    return true;
}

std::optional<Account> AccountStore::authenticate(const std::string& username, const std::string& password) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = accounts_.find(username);
    if (it == accounts_.end()) {
        return std::nullopt;
    }
    if (!verify_password(password, it->second.salt, it->second.password_hash)) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<Account> AccountStore::get_account(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = accounts_.find(username);
    if (it == accounts_.end()) return std::nullopt;
    return it->second;
}

std::vector<Account> AccountStore::list_accounts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Account> result;
    result.reserve(accounts_.size());
    for (const auto& [name, acc] : accounts_) {
        result.push_back(acc);
    }
    return result;
}

bool AccountStore::add_to_group(const std::string& username, const std::string& group) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = accounts_.find(username);
    if (it == accounts_.end()) return false;

    auto& groups = it->second.groups;
    if (std::find(groups.begin(), groups.end(), group) != groups.end()) {
        return true; // already in group
    }
    groups.push_back(group);
    return true;
}

bool AccountStore::remove_from_group(const std::string& username, const std::string& group) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = accounts_.find(username);
    if (it == accounts_.end()) return false;

    auto& groups = it->second.groups;
    auto git = std::find(groups.begin(), groups.end(), group);
    if (git == groups.end()) return false;
    groups.erase(git);
    return true;
}

void AccountStore::set_default_permissions(const std::string& database, const Permission& perm) {
    std::lock_guard<std::mutex> lock(mutex_);
    db_permissions_[database].default_permissions = perm;
}

void AccountStore::set_group_permissions(const std::string& database, const std::string& group, const Permission& perm) {
    std::lock_guard<std::mutex> lock(mutex_);
    db_permissions_[database].group_permissions[group] = perm;
}

void AccountStore::set_user_permissions(const std::string& database, const std::string& username, const Permission& perm) {
    std::lock_guard<std::mutex> lock(mutex_);
    db_permissions_[database].user_permissions[username] = perm;
}

DatabasePermissions AccountStore::get_database_permissions(const std::string& database) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = db_permissions_.find(database);
    if (it == db_permissions_.end()) return {};
    return it->second;
}

bool AccountStore::check_permission(const std::string& username, const std::string& database, Operation op) const {
    std::lock_guard<std::mutex> lock(mutex_);

    // 1. Admin — всегда разрешено
    auto acc_it = accounts_.find(username);
    if (acc_it == accounts_.end()) return false;
    if (acc_it->second.is_admin) return true;

    // Если база не настроена — запрет (безопасный default)
    auto db_it = db_permissions_.find(database);
    if (db_it == db_permissions_.end()) {
        // Нет настроенных прав — используем дефолтные (все запрещено)
        return false;
    }
    const auto& dp = db_it->second;

    // 2. Personal permissions
    auto user_perm_it = dp.user_permissions.find(username);
    if (user_perm_it != dp.user_permissions.end()) {
        if (user_perm_it->second.check(op)) return true;
    }

    // 3. Group permissions — если хоть одна группа разрешает
    for (const auto& group : acc_it->second.groups) {
        auto group_perm_it = dp.group_permissions.find(group);
        if (group_perm_it != dp.group_permissions.end()) {
            if (group_perm_it->second.check(op)) return true;
        }
    }

    // 4. Default permissions
    if (dp.default_permissions.check(op)) return true;

    // 5. Отказ
    return false;
}

void AccountStore::load() {
    std::lock_guard<std::mutex> lock(mutex_);
    accounts_.clear();
    db_permissions_.clear();

    std::ifstream ifs(filepath_);
    if (!ifs.is_open()) {
        return; // файл не существует — будет создан при первом save
    }

    try {
        json root = json::parse(ifs);

        if (root.contains("accounts") && root["accounts"].is_array()) {
            for (const auto& item : root["accounts"]) {
                Account acc = account_from_json(item);
                if (!acc.username.empty()) {
                    accounts_[acc.username] = acc;
                }
            }
        }

        if (root.contains("permissions") && root["permissions"].is_object()) {
            for (auto& [db, val] : root["permissions"].items()) {
                db_permissions_[db] = db_permissions_from_json(val);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[AccountStore] Failed to parse " << filepath_ << ": " << e.what() << std::endl;
    }
}

void AccountStore::save() const {
    std::lock_guard<std::mutex> lock(mutex_);

    json root;

    json accounts_array = json::array();
    for (const auto& [name, acc] : accounts_) {
        accounts_array.push_back(account_to_json(acc));
    }
    root["accounts"] = accounts_array;

    json perms_obj = json::object();
    for (const auto& [db, dp] : db_permissions_) {
        perms_obj[db] = db_permissions_to_json(dp);
    }
    root["permissions"] = perms_obj;

    std::ofstream ofs(filepath_);
    if (!ofs.is_open()) {
        std::cerr << "[AccountStore] Failed to open " << filepath_ << " for writing" << std::endl;
        return;
    }
    ofs << root.dump(2);
}

} // namespace dbms::auth
