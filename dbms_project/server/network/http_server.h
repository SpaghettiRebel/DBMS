#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <utility>

namespace dbms::network {

struct StorageNodeConfig {
    std::string host;
    uint16_t port;
};

struct HttpServerOptions {
    std::string data_root;
    std::string bind_host;
    std::uint16_t port = 18080;
    std::string jwt_secret;
    std::size_t cluster_shards = 1;
    std::size_t async_workers = 2;
    bool require_auth = false;

    // Кластерные настройки (задание 4)
    std::vector<StorageNodeConfig> storage_nodes;     // начальные Storage-узлы
    int heartbeat_interval_ms = 5000;                 // интервал heartbeat (задание 5)
    std::string storage_server_path;                  // путь к storage_server для перезапуска

    // RBAC (задание 9)
    std::string accounts_file = "./accounts.json";    // файл хранилища аккаунтов
};

int run_http_server(const HttpServerOptions& options);

int run_http_server(
    const std::string& data_root,
    const std::string& bind_host,
    std::uint16_t port,
    const std::string& jwt_secret);

}
