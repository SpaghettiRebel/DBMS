#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <httplib.h>
#include <nlohmann/json.hpp>

namespace {

std::string json_cell_to_string(const nlohmann::json& value) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    if (value.is_null()) {
        return "null";
    }
    return value.dump();
}

void print_separator(const std::vector<std::string>& columns, const std::map<std::string, std::size_t>& widths) {
    std::cout << '+';
    for (const auto& column : columns) {
        std::cout << std::string(widths.at(column) + 2, '-') << '+';
    }
    std::cout << '\n';
}

void print_row(const std::vector<std::string>& columns,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::size_t>& widths) {

    std::cout << '|';
    for (const auto& column : columns) {
        const auto it = row.find(column);
        const std::string& value = (it != row.end()) ? it->second : "";
        std::cout << ' ' << value << std::string(widths.at(column) - value.size(), ' ') << " |";
    }
    std::cout << '\n';
}

void print_ascii_table(const std::string& json_response) {
    const nlohmann::json parsed = nlohmann::json::parse(json_response, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_array()) {
        std::cout << json_response << std::endl;
        return;
    }

    if (parsed.empty()) {
        std::cout << "Empty set" << std::endl;
        return;
    }

    std::vector<std::string> columns;
    std::vector<std::map<std::string, std::string>> rows;
    std::map<std::string, std::size_t> widths;

    for (const auto& item : parsed) {
        std::map<std::string, std::string> row;

        if (item.is_object()) {
            for (auto it = item.begin(); it != item.end(); ++it) {
                const std::string key = it.key();
                const std::string value = json_cell_to_string(it.value());

                if (widths.find(key) == widths.end()) {
                    columns.push_back(key);
                    widths[key] = key.size();
                }
                if (value.size() > widths[key]) {
                    widths[key] = value.size();
                }
                row[key] = value;
            }
        } else {
            const std::string key = "value";
            const std::string value = json_cell_to_string(item);
            if (widths.find(key) == widths.end()) {
                columns.push_back(key);
                widths[key] = key.size();
            }
            if (value.size() > widths[key]) {
                widths[key] = value.size();
            }
            row[key] = value;
        }

        rows.push_back(std::move(row));
    }

    if (columns.empty()) {
        std::cout << "Empty set" << std::endl;
        return;
    }

    std::map<std::string, std::string> header_row;
    for (const auto& column : columns) {
        header_row[column] = column;
    }

    print_separator(columns, widths);
    print_row(columns, header_row, widths);
    print_separator(columns, widths);

    for (const auto& row : rows) {
        print_row(columns, row, widths);
    }

    print_separator(columns, widths);
}

}

int main(int argc, char* argv[]) {
    std::string line;
    std::string command_buffer;

    httplib::Client client("127.0.0.1", 18080);

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
            print_ascii_table(res->body);
        } else if (!res) {
            std::cout << "Connection failed." << std::endl;
        } else {
            std::cout << "Server: " << res->body << std::endl;
        }

        command_buffer.clear();
    }

    return 0;
}
