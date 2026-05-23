#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <iostream>
#include <string>

#include "network/http_server.h"

namespace {

bool read_option(int argc, char* argv[], int& index, const std::string& name, std::string& out) {
    if (std::string(argv[index]) == name && index + 1 < argc) {
        out = argv[index + 1];
        index += 1;
        return true;
    }
    return false;
}

}

int main(int argc, char* argv[]) {
    std::string data_root = "./db_data";
    std::string host = "0.0.0.0";
    std::uint16_t port = 18080;
    std::string jwt_secret = "dev-secret";
    std::size_t cluster_shards = 1;
    std::size_t async_workers = 2;
    bool require_auth = false;
    int heartbeat_interval_ms = 5000;
    std::string storage_server_path;
    std::string accounts_file = "./accounts.json";
    std::vector<dbms::network::StorageNodeConfig> storage_nodes;

    if (const char* env_secret = std::getenv("DBMS_JWT_SECRET"); env_secret != nullptr && *env_secret != '\0') {
        jwt_secret = env_secret;
    }
    if (const char* env_require_auth = std::getenv("DBMS_REQUIRE_AUTH");
        env_require_auth != nullptr && std::string(env_require_auth) == "1") {
        require_auth = true;
    }

    for (int i = 1; i < argc; ++i) {
        std::string value;
        if (read_option(argc, argv, i, "--data-root", value)) {
            data_root = value;
            continue;
        }
        if (read_option(argc, argv, i, "--host", value)) {
            host = value;
            continue;
        }
        if (read_option(argc, argv, i, "--port", value)) {
            port = static_cast<std::uint16_t>(std::stoul(value));
            continue;
        }
        if (read_option(argc, argv, i, "--jwt-secret", value)) {
            jwt_secret = value;
            continue;
        }
        if (read_option(argc, argv, i, "--cluster-shards", value)) {
            cluster_shards = std::max<std::size_t>(1, std::stoul(value));
            continue;
        }
        if (read_option(argc, argv, i, "--async-workers", value)) {
            async_workers = std::max<std::size_t>(1, std::stoul(value));
            continue;
        }
        if (read_option(argc, argv, i, "--heartbeat", value)) {
            heartbeat_interval_ms = std::stoi(value);
            continue;
        }
        if (read_option(argc, argv, i, "--storage-server-path", value)) {
            storage_server_path = value;
            continue;
        }
        if (read_option(argc, argv, i, "--accounts-file", value)) {
            accounts_file = value;
            continue;
        }
        // --storage-node host:port — можно указать несколько раз
        if (read_option(argc, argv, i, "--storage-node", value)) {
            std::size_t colon_pos = value.rfind(':');
            if (colon_pos != std::string::npos) {
                dbms::network::StorageNodeConfig cfg;
                cfg.host = value.substr(0, colon_pos);
                cfg.port = static_cast<uint16_t>(std::stoul(value.substr(colon_pos + 1)));
                storage_nodes.push_back(cfg);
            }
            continue;
        }
        if (std::string(argv[i]) == "--require-auth") {
            require_auth = true;
            continue;
        }
        if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") {
            std::cout << "Usage: " << argv[0]
                      << " [--data-root <path>] [--host <addr>] [--port <num>] [--jwt-secret <secret>]\n"
                      << "                 [--cluster-shards <num>] [--async-workers <num>] [--require-auth]\n"
                      << "                 [--storage-node <host:port>] ... [--heartbeat <ms>]\n"
                      << "                 [--storage-server-path <path>] [--accounts-file <path>]\n\n"
                      << "Options:\n"
                      << "  --data-root <path>          Directory for database files (default: ./db_data)\n"
                      << "  --host <addr>               Host to bind (default: 0.0.0.0)\n"
                      << "  --port <num>                Port to listen (default: 18080)\n"
                      << "  --jwt-secret <secret>       Secret for JWT tokens (default: dev-secret)\n"
                      << "  --cluster-shards <num>      In-process shards (default: 1, used if no --storage-node)\n"
                      << "  --async-workers <num>       Async queue worker threads (default: 2)\n"
                      << "  --require-auth              Require JWT authentication\n"
                      << "  --storage-node <host:port>  External Storage node (can specify multiple times)\n"
                      << "  --heartbeat <ms>            Heartbeat interval in ms (default: 5000)\n"
                      << "  --storage-server-path <p>   Path to storage_server for auto-restart\n"
                      << "  --accounts-file <path>      Path to accounts JSON file (default: ./accounts.json)\n";
            return 0;
        }
    }

    dbms::network::HttpServerOptions options;
    options.data_root = data_root;
    options.bind_host = host;
    options.port = port;
    options.jwt_secret = jwt_secret;
    options.cluster_shards = cluster_shards;
    options.async_workers = async_workers;
    options.require_auth = require_auth;
    options.storage_nodes = storage_nodes;
    options.heartbeat_interval_ms = heartbeat_interval_ms;
    options.storage_server_path = storage_server_path;
    options.accounts_file = accounts_file;
    return dbms::network::run_http_server(options);
}
