#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <httplib.h>
#include <nlohmann/json.hpp>

namespace {

// убирает пробелы в начале и конце копии строки.
std::string trim_copy(const std::string& text) {
    size_t left = 0;
    while (left < text.size() && std::isspace(static_cast<unsigned char>(text[left]))) {
        ++left;
    }

    size_t right = text.size();
    while (right > left && std::isspace(static_cast<unsigned char>(text[right - 1]))) {
        --right;
    }

    return text.substr(left, right - left);
}

// проверяет, что команда не пустая и заканчивается точкой с запятой.
bool command_complete(const std::string& buffer) {
    const std::string trimmed = trim_copy(buffer);
    return !trimmed.empty() && trimmed.back() == ';';
}

// отправляет sql-запрос на сервер и выводит ответ или ошибку.
bool execute_query(httplib::Client& client, const std::string& query, const std::string& token) {
    nlohmann::json payload = {{"query", query}};
    httplib::Headers headers;
    if (!token.empty()) {
        headers.emplace("Authorization", "Bearer " + token);
    }
    auto res = client.Post("/query", headers, payload.dump(), "application/json");

    if (res && res->status == 200) {
        std::cout << res->body << std::endl;
        return true;
    }

    if (!res) {
        std::cerr << "Connection failed." << std::endl;
        return false;
    }

    std::cerr << "Server: " << res->body << std::endl;
    return false;
}

// разбивает текст скрипта на команды по точке с запятой с учетом строк.
std::vector<std::string> split_script_commands(const std::string& script) {
    std::vector<std::string> commands;
    std::string buffer;
    bool in_string = false;
    bool escaped = false;

    for (char ch : script) {
        buffer.push_back(ch);

        if (in_string) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                in_string = false;
            }
            continue;
        }

        if (ch == '"') {
            in_string = true;
            continue;
        }

        if (ch == ';') {
            std::string command = trim_copy(buffer);
            if (!command.empty()) {
                commands.push_back(std::move(command));
            }
            buffer.clear();
        }
    }

    std::string tail = trim_copy(buffer);
    if (!tail.empty()) {
        commands.push_back(std::move(tail));
    }

    return commands;
}

}

// запускает клиент в режиме скрипта или интерактивном режиме и отправляет команды на сервер.
int main(int argc, char* argv[]) {
    const char* host_env = std::getenv("DBMS_CLIENT_HOST");
    const char* port_env = std::getenv("DBMS_CLIENT_PORT");
    const char* token_env = std::getenv("DBMS_TOKEN");

    const std::string host = host_env != nullptr && *host_env != '\0' ? host_env : "127.0.0.1";
    const int port = port_env != nullptr && *port_env != '\0' ? std::stoi(port_env) : 18080;
    const std::string token = token_env != nullptr && *token_env != '\0' ? token_env : "";

    httplib::Client client(host, port);
    std::string line;
    std::string command_buffer;

    if (argc > 2) {
        std::cerr << "Usage: " << argv[0] << " [script.sql]" << std::endl;
        return 1;
    }

    if (argc == 2) {
        std::ifstream script(argv[1], std::ios::in | std::ios::binary);
        if (!script) {
            std::cerr << "Cannot open script file: " << argv[1] << std::endl;
            return 1;
        }

        std::string text((std::istreambuf_iterator<char>(script)), std::istreambuf_iterator<char>());
        const std::vector<std::string> commands = split_script_commands(text);

        bool all_ok = true;
        for (const auto& command : commands) {
            const std::string trimmed = trim_copy(command);
            if (trimmed.empty()) continue;
            if (trimmed == "exit") break;
            if (!execute_query(client, command, token)) {
                all_ok = false;
            }
        }

        return all_ok ? 0 : 1;
    }

    while (true) {
        std::cout << "dbms> " << std::flush;

        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line == "exit" && trim_copy(command_buffer).empty()) {
            break;
        }

        if (!command_buffer.empty()) {
            command_buffer.push_back('\n');
        }
        command_buffer += line;

        if (!command_complete(command_buffer)) {
            continue;
        }

        execute_query(client, command_buffer, token);
        command_buffer.clear();
    }

    return 0;
}
