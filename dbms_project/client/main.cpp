#include <iostream>
#include <string>

#include <httplib.h>
#include <nlohmann/json.hpp>

int main(int argc, char* argv[]) {
    std::string line;
    std::string command_buffer;

    httplib::Client client("127.0.0.1", 8000);

    while (true) {
        std::cout << "dbms> " << std::flush;

        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line == "exit" && command_buffer.empty()) {
            break;
        }

        if (!command_buffer.empty()) {
            command_buffer.push_back('\n');
        }
        command_buffer += line;

        std::size_t last_non_space = command_buffer.find_last_not_of(" \t\r\n");
        if (last_non_space == std::string::npos || command_buffer[last_non_space] != ';') {
            continue;
        }

        nlohmann::json payload = {
            {"query", command_buffer}
        };

        auto res = client.Post("/query", payload.dump(), "application/json");

        if (res && res->status == 200) {
            std::cout << "Server: " << res->body << std::endl;
        } else if (!res) {
            std::cout << "Connection failed." << std::endl;
        } else {
            std::cout << "Server: " << res->body << std::endl;
        }

        command_buffer.clear();
    }

    return 0;
}
