#include <cstdint>
#include <cstdlib>
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

    if (const char* env_secret = std::getenv("DBMS_JWT_SECRET"); env_secret != nullptr && *env_secret != '\0') {
        jwt_secret = env_secret;
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
        if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") {
            std::cout << "Usage: " << argv[0]
                      << " [--data-root <path>] [--host <addr>] [--port <num>] [--jwt-secret <secret>]\n";
            return 0;
        }
    }

    return dbms::network::run_http_server(data_root, host, port, jwt_secret);
}
