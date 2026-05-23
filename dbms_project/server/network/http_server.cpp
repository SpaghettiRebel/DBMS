#include "http_server.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <deque>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef DELETE
#undef DELETE
#endif

#ifdef DEBUG
#undef DEBUG
#endif


#if __has_include(<crow.h>)
#include <crow.h>
#elif __has_include(<crow/crow.h>)
#include <crow/crow.h>
#else
#include "crow.h"
#endif

#include <nlohmann/json.hpp>

#include "auth.h"
#include "account_store.h"
#include "../storage/include/engine.h"
#include "../../shared/QueryPlan.h"

// Cluster module headers
#include "storage_node.h"
#include "shard_manager.h"
#include "heartbeat_monitor.h"
#include "async_request_queue.h"
#include "telemetry_collector.h"
#include "../storage/include/access_logger.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

struct yy_buffer_state;
typedef yy_buffer_state* YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char* yy_str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
int yyparse(void);

extern QueryPlan parsed_query_plan;
void reset_parsed_query_plan();

namespace {

std::optional<QueryPlan> parse_sql_with_bison(const std::string& query_text) {
    static std::mutex parser_mutex;
    const std::lock_guard<std::mutex> lock(parser_mutex);

    reset_parsed_query_plan();
    YY_BUFFER_STATE buffer = yy_scan_string(query_text.c_str());
    if (buffer == nullptr) {
        return std::nullopt;
    }

    const int parse_result = yyparse();
    yy_delete_buffer(buffer);

    if (parse_result != 0) {
        return std::nullopt;
    }

    return std::optional<QueryPlan>(std::move(parsed_query_plan));
}

crow::response json_response(int code, const json& payload) {
    crow::response response;
    response.code = code;
    response.set_header("Content-Type", "application/json");
    response.write(payload.dump());
    return response;
}

json as_array_of_objects(json payload) {
    if (payload.is_array()) {
        bool all_objects = true;
        for (const auto& item : payload) {
            if (!item.is_object()) {
                all_objects = false;
                break;
            }
        }
        if (all_objects) {
            return payload;
        }
    }
    json wrapped = json::array();
    wrapped.push_back({{"result", payload}});
    return wrapped;
}

std::string trim_copy(const std::string& text) {
    std::size_t left = 0;
    while (left < text.size() && std::isspace(static_cast<unsigned char>(text[left])) != 0) {
        ++left;
    }
    std::size_t right = text.size();
    while (right > left && std::isspace(static_cast<unsigned char>(text[right - 1])) != 0) {
        --right;
    }
    return text.substr(left, right - left);
}

std::string upper_copy(std::string text) {
    for (char& ch : text) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return text;
}

bool starts_with_word(const std::string& text, const std::string& word) {
    const std::string trimmed = upper_copy(trim_copy(text));
    if (trimmed.size() < word.size() || trimmed.compare(0, word.size(), word) != 0) {
        return false;
    }
    return trimmed.size() == word.size() || std::isspace(static_cast<unsigned char>(trimmed[word.size()])) != 0;
}

static uint32_t http_fnv1a_hash(const std::string& key) {
    uint32_t hash = 2166136261u;
    for (unsigned char ch : key) {
        hash ^= ch;
        hash *= 16777619u;
    }
    return hash;
}

std::string auth_reason_body(const std::string& reason) {
    json payload = json::array();
    payload.push_back({{"error", reason}});
    return payload.dump();
}

// ============================================================
// Определение типа операции из SQL-запроса для RBAC
// ============================================================
dbms::auth::Operation classify_operation(const std::string& query_text) {
    if (starts_with_word(query_text, "SELECT")) return dbms::auth::Operation::READ;
    if (starts_with_word(query_text, "INSERT")) return dbms::auth::Operation::WRITE;
    if (starts_with_word(query_text, "UPDATE")) return dbms::auth::Operation::WRITE;
    if (starts_with_word(query_text, "DELETE")) return dbms::auth::Operation::WRITE;
    if (starts_with_word(query_text, "REVERT")) return dbms::auth::Operation::WRITE;
    if (starts_with_word(query_text, "CREATE TABLE")) return dbms::auth::Operation::CREATE_TABLE;
    if (starts_with_word(query_text, "DROP TABLE")) return dbms::auth::Operation::DROP_TABLE;
    if (starts_with_word(query_text, "DROP DATABASE")) return dbms::auth::Operation::DROP_DATABASE;
    // USE, CREATE DATABASE — допускаем для всех
    return dbms::auth::Operation::READ;
}

// ============================================================
// Аутентификация запроса через JWT
// ============================================================
std::optional<std::string> authenticate_request(
    const crow::request& request,
    bool require_auth,
    const std::string& jwt_secret,
    crow::response& response) {

    if (!require_auth) {
        return std::string("admin"); // без авторизации — считаем admin
    }

    std::string header = trim_copy(request.get_header_value("Authorization"));
    const std::string prefix = "Bearer ";
    if (header.size() <= prefix.size() || header.compare(0, prefix.size(), prefix) != 0) {
        response.code = 401;
        response.set_header("Content-Type", "application/json");
        response.write(auth_reason_body("missing_or_invalid_authorization_header"));
        return std::nullopt;
    }

    std::string subject;
    const std::string token = trim_copy(header.substr(prefix.size()));
    if (!dbms::auth::validate_jwt(token, jwt_secret, &subject)) {
        response.code = 401;
        response.set_header("Content-Type", "application/json");
        response.write(auth_reason_body("invalid_or_expired_token"));
        return std::nullopt;
    }

    return subject;  // username
}

// ============================================================
// Ответ на запрос
// ============================================================
struct ExecutionResponse {
    int code = 200;
    json payload = json::array();
    std::string node_id;
    int64_t execution_time_ms = 0;
};

// ============================================================
// Кластерный исполнитель — Entrypoint
// Задания 4, 5, 8
// ============================================================
class ClusteredExecutor {
public:
    ClusteredExecutor(const std::string& data_root, std::size_t shard_count,
                      int heartbeat_interval_ms, const std::string& storage_server_path)
        : data_root_(data_root),
          shard_manager_(std::make_unique<ShardManager>()),
          telemetry_(std::make_unique<TelemetryCollector>()),
          storage_server_path_(storage_server_path),
          use_external_nodes_(false)
    {
        // HeartbeatMonitor: при отказе узла — перезапуск
        heartbeat_ = std::make_unique<HeartbeatMonitor>(
            heartbeat_interval_ms, 2000,
            [this](const std::string& node_id) { restart_node(node_id); }
        );

        // Если нет внешних узлов, работаем в in-process режиме (backward compatible)
        fallback_shard_count_ = std::max<std::size_t>(1, shard_count);
    }

    void add_external_node(const std::string& node_id, const std::string& host, uint16_t port) {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        if (external_nodes_.count(node_id)) return;

        auto node = std::make_unique<StorageNode>(node_id, host, port);
        node->connect(5000);
        shard_manager_->add_node(node_id);
        heartbeat_->register_node(node_id, node.get());

        NodeConfig cfg;
        cfg.host = host;
        cfg.port = port;
        cfg.data_root = (fs::path(data_root_) / node_id).string();
        node_configs_[node_id] = cfg;

        external_nodes_[node_id] = std::move(node);
        use_external_nodes_ = true;
    }

    bool remove_external_node(const std::string& node_id) {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        auto it = external_nodes_.find(node_id);
        if (it == external_nodes_.end()) return false;

        heartbeat_->unregister_node(node_id);
        shard_manager_->remove_node(node_id);
        it->second->disconnect();
        external_nodes_.erase(it);
        node_configs_.erase(node_id);

        if (external_nodes_.empty()) {
            use_external_nodes_ = false;
        }
        return true;
    }

    void start_heartbeat() {
        heartbeat_->start();
    }

    void stop_heartbeat() {
        heartbeat_->stop();
    }

    // Инициализация in-process шардов (fallback без внешних узлов)
    void init_inprocess_shards() {
        if (use_external_nodes_) return;
        inprocess_nodes_.clear();
        for (std::size_t i = 0; i < fallback_shard_count_; ++i) {
            auto node = std::make_unique<InProcessNode>();
            node->id = "shard_" + std::to_string(i);
            node->data_root = fallback_shard_count_ == 1 ? data_root_ : (fs::path(data_root_) / node->id).string();
            node->engine = std::make_unique<Engine>(node->data_root);
            node->online = true;
            inprocess_nodes_.push_back(std::move(node));
        }
    }

    ExecutionResponse execute_sql(const std::string& query_text) {
        const auto start = std::chrono::steady_clock::now();
        ExecutionResponse response;

        try {
            if (use_external_nodes_) {
                response = execute_on_cluster(query_text);
            } else {
                response = execute_inprocess(query_text);
            }

            response.execution_time_ms = elapsed_ms(start);
            telemetry_->record_request(response.execution_time_ms);
            if (response.code != 200) {
                telemetry_->record_error();
            }
            return response;
        } catch (const std::exception& ex) {
            response.code = 400;
            response.payload = json::array({{{"error", ex.what()}}});
            response.execution_time_ms = elapsed_ms(start);
            telemetry_->record_request(response.execution_time_ms);
            telemetry_->record_error();
            return response;
        }
    }

    json nodes_json() const {
        json payload = json::array();

        if (use_external_nodes_) {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            for (const auto& [id, node] : external_nodes_) {
                std::string status;
                switch (node->get_status()) {
                    case StorageNode::Status::ONLINE: status = "ONLINE"; break;
                    case StorageNode::Status::OFFLINE: status = "OFFLINE"; break;
                    case StorageNode::Status::RESTARTING: status = "RESTARTING"; break;
                    default: status = "UNKNOWN"; break;
                }
                auto cfg_it = node_configs_.find(id);
                payload.push_back({
                    {"node_id", id},
                    {"host", node->get_host()},
                    {"port", node->get_port()},
                    {"status", status},
                    {"requests", node->get_total_requests()},
                    {"errors", node->get_failed_requests()},
                    {"last_heartbeat_ms", node->get_last_heartbeat()},
                    {"data_root", cfg_it != node_configs_.end() ? cfg_it->second.data_root : ""}
                });
            }
        } else {
            for (const auto& node : inprocess_nodes_) {
                std::lock_guard<std::mutex> lk(node->mutex);
                payload.push_back({
                    {"node_id", node->id},
                    {"status", node->online ? "ONLINE" : "OFFLINE"},
                    {"data_root", node->data_root},
                    {"requests", node->requests},
                    {"errors", node->errors},
                    {"last_heartbeat_ms", now_ms()}
                });
            }
        }
        return payload;
    }

    json stats_json() const {
        auto ts = telemetry_->get_stats();

        std::size_t total_nodes = 0;
        std::size_t online_nodes = 0;

        if (use_external_nodes_) {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            total_nodes = external_nodes_.size();
            for (const auto& [id, node] : external_nodes_) {
                if (node->is_online()) online_nodes++;
            }
        } else {
            total_nodes = inprocess_nodes_.size();
            online_nodes = total_nodes;
        }

        json payload = json::array();
        payload.push_back({
            {"current_rps", ts.current_rps},
            {"avg_rps_10min", ts.avg_rps_10min},
            {"max_rps_10min", ts.max_rps_10min},
            {"avg_response_time_10s_ms", ts.avg_response_time_10s},
            {"error_rate_1min_pct", ts.error_rate_1min},
            {"total_requests", ts.total_requests},
            {"total_errors", ts.total_errors},
            {"total_nodes", total_nodes},
            {"online_nodes", online_nodes},
            {"mode", use_external_nodes_ ? "clustered" : (total_nodes > 1 ? "sharded" : "single-node")}
        });
        return payload;
    }

    json heartbeat_stats_json() const {
        auto hb = heartbeat_->get_stats();
        json payload = json::array();
        payload.push_back({
            {"total_checks", hb.total_checks},
            {"failed_checks", hb.failed_checks},
            {"nodes_restarted", hb.nodes_restarted},
            {"last_check_time_ms", hb.last_check_time}
        });
        return payload;
    }

private:
    // --- In-process mode (fallback) ---
    struct InProcessNode {
        std::string id;
        std::string data_root;
        std::unique_ptr<Engine> engine;
        mutable std::mutex mutex;
        bool online = true;
        uint64_t requests = 0;
        uint64_t errors = 0;
    };

    struct NodeConfig {
        std::string host;
        uint16_t port = 0;
        std::string data_root;
    };

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    static int64_t elapsed_ms(const std::chrono::steady_clock::time_point& start) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    }

    static bool is_cluster_broadcast(const QueryPlan& plan) {
        return plan.type == QueryType::CREATE_DATABASE || plan.type == QueryType::DROP_DATABASE ||
               plan.type == QueryType::USE_DATABASE;
    }

    // --- Cluster mode (external StorageNode processes) ---
    ExecutionResponse execute_on_cluster(const std::string& query_text) {
        ExecutionResponse response;

        // Определяем — broadcast или точечный запрос
        if (ShardManager::is_global_query(query_text)) {
            // Broadcast на все узлы
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            for (auto& [id, node] : external_nodes_) {
                if (!node->is_online()) continue;
                try {
                    std::string result = node->send_query(query_text);
                    // Парсим результат (storage_server возвращает JSON)
                } catch (const std::exception& ex) {
                    response.code = 400;
                    response.payload = json::array({{{"error", std::string("node ") + id + ": " + ex.what()}}});
                    return response;
                }
            }
            response.payload = json::array({{{"status", "ok"}}});
            response.node_id = "broadcast";
            return response;
        }

        // Точечный запрос — маршрутизация по шарду
        std::string table_name = ShardManager::extract_table_name(query_text);
        std::string key = table_name.empty() ? query_text : table_name;
        std::string target_node_id;
        try {
            target_node_id = shard_manager_->get_node_for_key(key);
        } catch (...) {
            response.code = 503;
            response.payload = json::array({{{"error", "no_nodes_available"}}});
            return response;
        }

        std::lock_guard<std::mutex> lock(nodes_mutex_);
        auto it = external_nodes_.find(target_node_id);
        if (it == external_nodes_.end() || !it->second->is_online()) {
            response.code = 503;
            response.payload = json::array({{{"error", "target_node_offline: " + target_node_id}}});
            return response;
        }

        try {
            std::string result = it->second->send_query(query_text);
            response.node_id = target_node_id;
            // storage_server возвращает JSON строку
            try {
                response.payload = as_array_of_objects(json::parse(result));
            } catch (...) {
                // Если не JSON — оборачиваем
                response.payload = json::array({{{"result", result}}});
            }
        } catch (const std::exception& ex) {
            response.code = 400;
            response.payload = json::array({{{"error", ex.what()}}});
        }

        return response;
    }

    // --- In-process mode ---
    ExecutionResponse execute_inprocess(const std::string& query_text) {
        ExecutionResponse response;

        auto route_plan = parse_sql_with_bison(query_text);
        if (!route_plan.has_value()) {
            response.code = 400;
            response.payload = json::array({{{"error", "sql_parse_error"}}});
            return response;
        }

        if (is_cluster_broadcast(*route_plan)) {
            for (auto& node : inprocess_nodes_) {
                ExecutionResponse node_response = execute_on_inprocess_node(*node, query_text);
                if (node_response.code != 200) {
                    return node_response;
                }
            }
            response.payload = json::array({{{"status", "ok"}}});
            response.node_id = "broadcast";
        } else {
            // Шардирование по имени таблицы
            std::string key = route_plan->table_name.empty() ? route_plan->database_name : route_plan->table_name;
            if (key.empty()) key = query_text;
            const std::size_t index = static_cast<std::size_t>(http_fnv1a_hash(key) % inprocess_nodes_.size());
            response = execute_on_inprocess_node(*inprocess_nodes_.at(index), query_text);
        }

        return response;
    }

    ExecutionResponse execute_on_inprocess_node(InProcessNode& node, const std::string& query_text) {
        ExecutionResponse response;
        response.node_id = node.id;

        std::lock_guard<std::mutex> lock(node.mutex);
        node.requests++;

        auto parsed_plan = parse_sql_with_bison(query_text);
        if (!parsed_plan.has_value()) {
            node.errors++;
            response.code = 400;
            response.payload = json::array({{{"error", "sql_parse_error"}}});
            return response;
        }

        try {
            QueryPlan& plan = *parsed_plan;
            node.engine->execute(plan);

            if (plan.type == QueryType::SELECT) {
                const std::string raw_result = node.engine->select_records(plan);
                response.payload = as_array_of_objects(json::parse(raw_result));
            } else {
                response.payload = json::array({{{"status", "ok"}}});
            }
        } catch (const std::exception& ex) {
            node.errors++;
            response.code = 400;
            response.payload = json::array({{{"error", ex.what()}}});
        }

        return response;
    }

    void restart_node(const std::string& node_id) {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        auto it = external_nodes_.find(node_id);
        if (it == external_nodes_.end()) return;

        auto cfg_it = node_configs_.find(node_id);
        if (cfg_it == node_configs_.end()) return;

        it->second->set_status(StorageNode::Status::RESTARTING);

        // Принудительный перезапуск процесса storage_server
        std::string cmd;
        if (!storage_server_path_.empty()) {
            cmd = storage_server_path_ + " --port " + std::to_string(cfg_it->second.port) +
                  " --data-dir " + cfg_it->second.data_root +
                  " --host " + cfg_it->second.host;
#ifdef _WIN32
            cmd = "start /B " + cmd;
#else
            cmd += " &";
#endif
            std::system(cmd.c_str());
            // Даём время запуститься
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }

        // Пытаемся переподключиться
        it->second->disconnect();
        if (it->second->connect(5000)) {
            it->second->set_status(StorageNode::Status::ONLINE);
        } else {
            it->second->set_status(StorageNode::Status::OFFLINE);
        }
    }

    std::string data_root_;
    std::unique_ptr<ShardManager> shard_manager_;
    std::unique_ptr<HeartbeatMonitor> heartbeat_;
    std::unique_ptr<TelemetryCollector> telemetry_;
    std::string storage_server_path_;

    // External cluster nodes
    mutable std::mutex nodes_mutex_;
    std::unordered_map<std::string, std::unique_ptr<StorageNode>> external_nodes_;
    std::unordered_map<std::string, NodeConfig> node_configs_;
    std::atomic<bool> use_external_nodes_;

    // In-process fallback
    std::vector<std::unique_ptr<InProcessNode>> inprocess_nodes_;
    std::size_t fallback_shard_count_ = 1;
};

// ============================================================
// Асинхронная очередь с GUID v4 (задание 6)
// ============================================================
class AsyncQueryQueueV4 {
public:
    AsyncQueryQueueV4(ClusteredExecutor& executor, std::size_t workers) : executor_(executor) {
        // Создаём AsyncRequestQueue с executor lambda
        queue_ = std::make_unique<AsyncRequestQueue>(
            [this](const std::string& query) -> std::string {
                ExecutionResponse resp = executor_.execute_sql(query);
                if (resp.code != 200) {
                    throw std::runtime_error(resp.payload.dump());
                }
                return resp.payload.dump();
            },
            std::max<std::size_t>(1, workers)
        );
        queue_->start();
    }

    ~AsyncQueryQueueV4() {
        if (queue_) queue_->stop(true);
    }

    std::string enqueue(std::string query) {
        return queue_->enqueue(query, "http_client", false);
    }

    json status_json(const std::string& id) {
        auto result = queue_->get_status(id);

        json payload = json::array();
        json row = {{"request_id", result.request_guid}};

        switch (result.status) {
            case AsyncRequestStatus::PENDING:
                row["status"] = "queued";
                break;
            case AsyncRequestStatus::RUNNING:
                row["status"] = "running";
                break;
            case AsyncRequestStatus::COMPLETED: {
                row["status"] = "completed";
                try {
                    row["result"] = json::parse(result.result_data);
                } catch (...) {
                    row["result"] = result.result_data;
                }
                break;
            }
            case AsyncRequestStatus::FAILED:
                row["status"] = "failed";
                row["error"] = result.error_message;
                break;
        }
        payload.push_back(std::move(row));
        return payload;
    }

private:
    ClusteredExecutor& executor_;
    std::unique_ptr<AsyncRequestQueue> queue_;
};

} // anonymous namespace

namespace dbms::network {

int run_http_server(const HttpServerOptions& options) {

    // Инициализация AccessLogger (задание 7)
    dbms::AccessLogger::instance().init("server_access.log");

    crow::App<> app;

    // Кластерный исполнитель
    ClusteredExecutor executor(options.data_root, options.cluster_shards,
                               options.heartbeat_interval_ms, options.storage_server_path);

    // Добавление внешних Storage-узлов из конфигурации (задание 4)
    for (std::size_t i = 0; i < options.storage_nodes.size(); ++i) {
        const auto& sn = options.storage_nodes[i];
        std::string node_id = "node_" + std::to_string(i);
        executor.add_external_node(node_id, sn.host, sn.port);
    }

    // Если нет внешних узлов — инициализируем in-process шарды (fallback)
    if (options.storage_nodes.empty()) {
        executor.init_inprocess_shards();
    }

    // Запуск Heartbeat Monitor (задание 5)
    executor.start_heartbeat();

    // Асинхронная очередь с GUID v4 (задание 6)
    AsyncQueryQueueV4 async_queue(executor, options.async_workers);

    // Хранилище аккаунтов (задание 9)
    dbms::auth::AccountStore account_store(options.accounts_file);

    // ============================================================
    // Health check
    // ============================================================
    CROW_ROUTE(app, "/health").methods(crow::HTTPMethod::Get)([] {
        json payload = json::array();
        payload.push_back({{"status", "ok"}});
        return json_response(200, payload);
    });

    // ============================================================
    // Аутентификация — POST /login (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/login").methods(crow::HTTPMethod::Post)([&options, &account_store](const crow::request& request) {
        try {
            const json body = json::parse(request.body);
            const std::string login = body.value("login", "");
            const std::string password = body.value("password", "");

            auto account = account_store.authenticate(login, password);
            if (!account.has_value()) {
                return json_response(401, json::array({{{"error", "invalid_credentials"}}}));
            }

            // JWT subject = username (роль определяется через RBAC при каждом запросе)
            std::string token = dbms::auth::generate_jwt(account->username, options.jwt_secret);
            json response = json::array();
            response.push_back({
                {"token", token},
                {"username", account->username},
                {"is_admin", account->is_admin},
                {"groups", account->groups}
            });
            return json_response(200, response);
        } catch (...) {
            return json_response(400, json::array({{{"error", "bad_request"}}}));
        }
    });

    // ============================================================
    // Выполнение запросов — POST /query (задания 4, 9, 7)
    // ============================================================
    CROW_ROUTE(app, "/query").methods(crow::HTTPMethod::Post)([&executor, &options, &account_store](const crow::request& request) {
        const int64_t start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        std::string query_text = "[unparsed]";

        crow::response auth_response;
        auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
        if (!username.has_value()) {
            const int64_t end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            dbms::LogEntry entry;
            entry.client_id = "anonymous";
            entry.request_id = "sync";
            entry.query_body = "[unauthorized]";
            entry.start_time_ms = start_time;
            entry.end_time_ms = end_time;
            entry.status_code = auth_response.code;
            entry.error_msg = "unauthorized";
            dbms::AccessLogger::instance().log(entry);
            return auth_response;
        }

        auto log_and_return = [&](int code, const json& payload, const std::string& err_msg = "") {
            const int64_t end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            dbms::LogEntry entry;
            entry.client_id = username.value_or("anonymous");
            entry.request_id = "sync";
            entry.query_body = query_text;
            entry.start_time_ms = start_time;
            entry.end_time_ms = end_time;
            entry.status_code = code;
            entry.error_msg = err_msg;
            dbms::AccessLogger::instance().log(entry);
            return json_response(code, payload);
        };

        try {
            const json request_body = json::parse(request.body);
            if (!request_body.is_object() || !request_body.contains("query") || !request_body["query"].is_string()) {
                json error_payload = json::array();
                error_payload.push_back({{"error", "missing_query_field"}});
                return log_and_return(400, error_payload, "missing_query_field");
            }

            query_text = request_body["query"].get<std::string>();

            // RBAC проверка (задание 9)
            if (options.require_auth) {
                auto op = classify_operation(query_text);
                // Определяем текущую БД (из USE или из запроса)
                std::string current_db = request_body.value("database", "default");
                if (!account_store.check_permission(*username, current_db, op)) {
                    return log_and_return(403, json::array({{{"error", "forbidden_by_rbac"}}}), "forbidden_by_rbac");
                }
            }

            ExecutionResponse response = executor.execute_sql(query_text);
            std::string err_msg = response.code != 200 ? response.payload.dump() : "";
            return log_and_return(response.code, response.payload, err_msg);
        } catch (const std::exception& ex) {
            json error_payload = json::array();
            error_payload.push_back({{"error", ex.what()}});
            return log_and_return(400, error_payload, ex.what());
        } catch (...) {
            json error_payload = json::array();
            error_payload.push_back({{"error", "unknown_error"}});
            return log_and_return(500, error_payload, "unknown_error");
        }
    });

    // ============================================================
    // Асинхронный запрос — POST /query_async (задание 6, GUID v4, 7)
    // ============================================================
    CROW_ROUTE(app, "/query_async").methods(crow::HTTPMethod::Post)([&async_queue, &options, &account_store](const crow::request& request) {
        const int64_t start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        std::string query_text = "[unparsed]";

        crow::response auth_response;
        auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
        if (!username.has_value()) {
            const int64_t end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            dbms::LogEntry entry;
            entry.client_id = "anonymous";
            entry.request_id = "async";
            entry.query_body = "[unauthorized]";
            entry.start_time_ms = start_time;
            entry.end_time_ms = end_time;
            entry.status_code = auth_response.code;
            entry.error_msg = "unauthorized";
            dbms::AccessLogger::instance().log(entry);
            return auth_response;
        }

        auto log_and_return = [&](int code, const json& payload, const std::string& req_id = "", const std::string& err_msg = "") {
            const int64_t end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            dbms::LogEntry entry;
            entry.client_id = username.value_or("anonymous");
            entry.request_id = req_id.empty() ? "async" : req_id;
            entry.query_body = query_text;
            entry.start_time_ms = start_time;
            entry.end_time_ms = end_time;
            entry.status_code = code;
            entry.error_msg = err_msg;
            dbms::AccessLogger::instance().log(entry);
            return json_response(code, payload);
        };

        try {
            const json request_body = json::parse(request.body);
            if (!request_body.is_object() || !request_body.contains("query") || !request_body["query"].is_string()) {
                return log_and_return(400, json::array({{{"error", "missing_query_field"}}}), "", "missing_query_field");
            }
            query_text = request_body["query"].get<std::string>();

            // RBAC проверка
            if (options.require_auth) {
                auto op = classify_operation(query_text);
                std::string current_db = request_body.value("database", "default");
                if (!account_store.check_permission(*username, current_db, op)) {
                    return log_and_return(403, json::array({{{"error", "forbidden_by_rbac"}}}), "", "forbidden_by_rbac");
                }
            }

            const std::string id = async_queue.enqueue(query_text);
            return log_and_return(202, json::array({{{"request_id", id}, {"status", "queued"}}}), id);
        } catch (const std::exception& ex) {
            return log_and_return(400, json::array({{{"error", ex.what()}}}), "", ex.what());
        } catch (...) {
            return log_and_return(400, json::array({{{"error", "bad_request"}}}), "", "bad_request");
        }
    });

    // ============================================================
    // Статус асинхронного запроса — GET /async/<guid>
    // ============================================================
    CROW_ROUTE(app, "/async/<string>").methods(crow::HTTPMethod::Get)(
        [&async_queue, &options](const crow::request& request, const std::string& id) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;
            return json_response(200, async_queue.status_json(id));
        });

    // ============================================================
    // Кластер: список узлов — GET /cluster/nodes (задание 4)
    // ============================================================
    CROW_ROUTE(app, "/cluster/nodes").methods(crow::HTTPMethod::Get)(
        [&executor, &options](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;
            return json_response(200, executor.nodes_json());
        });

    // ============================================================
    // Кластер: статистика — GET /cluster/stats (задание 8, телеметрия)
    // ============================================================
    CROW_ROUTE(app, "/cluster/stats").methods(crow::HTTPMethod::Get)(
        [&executor, &options](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;
            return json_response(200, executor.stats_json());
        });

    // ============================================================
    // Кластер: heartbeat статистика — GET /cluster/heartbeat (задание 5)
    // ============================================================
    CROW_ROUTE(app, "/cluster/heartbeat").methods(crow::HTTPMethod::Get)(
        [&executor, &options](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;
            return json_response(200, executor.heartbeat_stats_json());
        });

    // ============================================================
    // Кластер: добавление узла — POST /cluster/add_node (задание 4)
    // ============================================================
    CROW_ROUTE(app, "/cluster/add_node").methods(crow::HTTPMethod::Post)(
        [&executor, &options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            // Только admin может управлять кластером
            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            try {
                const json body = json::parse(request.body);
                std::string node_id = body.value("node_id", "");
                std::string host = body.value("host", "127.0.0.1");
                uint16_t port = body.value("port", 9000);

                if (node_id.empty()) {
                    return json_response(400, json::array({{{"error", "missing_node_id"}}}));
                }

                executor.add_external_node(node_id, host, port);
                return json_response(200, json::array({{{"status", "node_added"}, {"node_id", node_id}}}));
            } catch (const std::exception& ex) {
                return json_response(400, json::array({{{"error", ex.what()}}}));
            }
        });

    // ============================================================
    // Кластер: удаление узла — POST /cluster/remove_node (задание 4)
    // ============================================================
    CROW_ROUTE(app, "/cluster/remove_node").methods(crow::HTTPMethod::Post)(
        [&executor, &options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            try {
                const json body = json::parse(request.body);
                std::string node_id = body.value("node_id", "");
                if (node_id.empty()) {
                    return json_response(400, json::array({{{"error", "missing_node_id"}}}));
                }
                if (executor.remove_external_node(node_id)) {
                    return json_response(200, json::array({{{"status", "node_removed"}, {"node_id", node_id}}}));
                } else {
                    return json_response(404, json::array({{{"error", "node_not_found"}}}));
                }
            } catch (const std::exception& ex) {
                return json_response(400, json::array({{{"error", ex.what()}}}));
            }
        });

    // ============================================================
    // Аккаунты: создание — POST /accounts/create (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/accounts/create").methods(crow::HTTPMethod::Post)(
        [&options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            try {
                const json body = json::parse(request.body);
                std::string new_username = body.value("username", "");
                std::string new_password = body.value("password", "");
                bool is_admin = body.value("is_admin", false);

                if (new_username.empty() || new_password.empty()) {
                    return json_response(400, json::array({{{"error", "missing_username_or_password"}}}));
                }

                if (account_store.create_account(new_username, new_password, is_admin)) {
                    account_store.save();
                    return json_response(200, json::array({{{"status", "account_created"}, {"username", new_username}}}));
                } else {
                    return json_response(409, json::array({{{"error", "account_already_exists"}}}));
                }
            } catch (const std::exception& ex) {
                return json_response(400, json::array({{{"error", ex.what()}}}));
            }
        });

    // ============================================================
    // Аккаунты: список — GET /accounts (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/accounts").methods(crow::HTTPMethod::Get)(
        [&options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            json accounts_list = json::array();
            for (const auto& acc : account_store.list_accounts()) {
                accounts_list.push_back({
                    {"username", acc.username},
                    {"groups", acc.groups},
                    {"is_admin", acc.is_admin}
                    // Пароли не показываем!
                });
            }
            return json_response(200, accounts_list);
        });

    // ============================================================
    // Группы: добавление — POST /accounts/groups/add (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/accounts/groups/add").methods(crow::HTTPMethod::Post)(
        [&options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            try {
                const json body = json::parse(request.body);
                std::string target = body.value("username", "");
                std::string group = body.value("group", "");
                if (target.empty() || group.empty()) {
                    return json_response(400, json::array({{{"error", "missing_username_or_group"}}}));
                }
                if (account_store.add_to_group(target, group)) {
                    account_store.save();
                    return json_response(200, json::array({{{"status", "added_to_group"}, {"username", target}, {"group", group}}}));
                }
                return json_response(404, json::array({{{"error", "user_not_found"}}}));
            } catch (const std::exception& ex) {
                return json_response(400, json::array({{{"error", ex.what()}}}));
            }
        });

    // ============================================================
    // Группы: удаление — POST /accounts/groups/remove (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/accounts/groups/remove").methods(crow::HTTPMethod::Post)(
        [&options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            try {
                const json body = json::parse(request.body);
                std::string target = body.value("username", "");
                std::string group = body.value("group", "");
                if (target.empty() || group.empty()) {
                    return json_response(400, json::array({{{"error", "missing_username_or_group"}}}));
                }
                if (account_store.remove_from_group(target, group)) {
                    account_store.save();
                    return json_response(200, json::array({{{"status", "removed_from_group"}, {"username", target}, {"group", group}}}));
                }
                return json_response(404, json::array({{{"error", "user_not_found_or_not_in_group"}}}));
            } catch (const std::exception& ex) {
                return json_response(400, json::array({{{"error", ex.what()}}}));
            }
        });

    // ============================================================
    // Права: установка — POST /permissions/set (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/permissions/set").methods(crow::HTTPMethod::Post)(
        [&options, &account_store](const crow::request& request) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            try {
                const json body = json::parse(request.body);
                std::string database = body.value("database", "");
                std::string target_type = body.value("target_type", "");
                std::string target_name = body.value("target_name", "");

                if (database.empty() || target_type.empty()) {
                    return json_response(400, json::array({{{"error", "missing_database_or_target_type"}}}));
                }

                dbms::auth::Permission perm;
                if (body.contains("permissions") && body["permissions"].is_object()) {
                    const auto& p = body["permissions"];
                    perm.can_read          = p.value("read", false);
                    perm.can_write         = p.value("write", false);
                    perm.can_create_table  = p.value("create_table", false);
                    perm.can_drop_table    = p.value("drop_table", false);
                    perm.can_drop_database = p.value("drop_database", false);
                }

                if (target_type == "default") {
                    account_store.set_default_permissions(database, perm);
                } else if (target_type == "group") {
                    if (target_name.empty()) {
                        return json_response(400, json::array({{{"error", "missing_target_name"}}}));
                    }
                    account_store.set_group_permissions(database, target_name, perm);
                } else if (target_type == "user") {
                    if (target_name.empty()) {
                        return json_response(400, json::array({{{"error", "missing_target_name"}}}));
                    }
                    account_store.set_user_permissions(database, target_name, perm);
                } else {
                    return json_response(400, json::array({{{"error", "invalid_target_type"}}}));
                }

                account_store.save();
                return json_response(200, json::array({{{"status", "permissions_set"}, {"database", database}}}));
            } catch (const std::exception& ex) {
                return json_response(400, json::array({{{"error", ex.what()}}}));
            }
        });

    // ============================================================
    // Права: просмотр — GET /permissions/<database> (задание 9)
    // ============================================================
    CROW_ROUTE(app, "/permissions/<string>").methods(crow::HTTPMethod::Get)(
        [&options, &account_store](const crow::request& request, const std::string& database) {
            crow::response auth_response;
            auto username = authenticate_request(request, options.require_auth, options.jwt_secret, auth_response);
            if (!username.has_value()) return auth_response;

            if (options.require_auth) {
                auto acc = account_store.get_account(*username);
                if (!acc || !acc->is_admin) {
                    return json_response(403, json::array({{{"error", "admin_only"}}}));
                }
            }

            auto dp = account_store.get_database_permissions(database);
            auto perm_to_json = [](const dbms::auth::Permission& p) -> json {
                return {
                    {"read", p.can_read},
                    {"write", p.can_write},
                    {"create_table", p.can_create_table},
                    {"drop_table", p.can_drop_table},
                    {"drop_database", p.can_drop_database}
                };
            };

            json result;
            result["database"] = database;
            result["default"] = perm_to_json(dp.default_permissions);

            json groups_obj = json::object();
            for (const auto& [group, perm] : dp.group_permissions) {
                groups_obj[group] = perm_to_json(perm);
            }
            result["groups"] = groups_obj;

            json users_obj = json::object();
            for (const auto& [user, perm] : dp.user_permissions) {
                users_obj[user] = perm_to_json(perm);
            }
            result["users"] = users_obj;

            return json_response(200, json::array({result}));
        });

    // Запуск сервера
    app.bindaddr(options.bind_host).port(options.port).concurrency(1).run();

    executor.stop_heartbeat();
    dbms::AccessLogger::instance().shutdown();
    return 0;
}

int run_http_server(const std::string& data_root, const std::string& bind_host, std::uint16_t port,
    const std::string& jwt_secret) {
    HttpServerOptions options;
    options.data_root = data_root;
    options.bind_host = bind_host;
    options.port = port;
    options.jwt_secret = jwt_secret;
    return run_http_server(options);
}

}
