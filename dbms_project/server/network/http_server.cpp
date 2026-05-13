#include "http_server.h"

#include <algorithm>
#include <cctype>
#include <exception>
#include <string>
#include <utility>

#ifdef DELETE
#undef DELETE
#endif

// --- ДОБАВЬ ЭТИ ТРИ СТРОКИ ---
#ifdef DEBUG
#undef DEBUG
#endif
// -----------------------------

#if __has_include(<crow.h>)
#include <crow.h>
#elif __has_include(<crow/crow.h>)
#include <crow/crow.h>
#else
#include "crow.h"
#endif

#include <nlohmann/json.hpp>

#include "auth.h"
#include "../storage/include/engine.h"
#include "../../shared/QueryPlan.h"

using json = nlohmann::json;

namespace {

std::string to_lower_ascii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string trim(std::string value) {
    std::size_t left = 0;
    while (left < value.size() && std::isspace(static_cast<unsigned char>(value[left])) != 0) {
        ++left;
    }
    std::size_t right = value.size();
    while (right > left && std::isspace(static_cast<unsigned char>(value[right - 1])) != 0) {
        --right;
    }
    return value.substr(left, right - left);
}

std::string normalize_identifier(std::string value) {
    value = trim(std::move(value));
    while (!value.empty() && value.back() == ';') {
        value.pop_back();
    }
    return trim(std::move(value));
}

std::string extract_identifier_after_keyword(const std::string& text, const std::string& keyword) {
    const std::string lower_text = to_lower_ascii(text);
    const std::string lower_keyword = to_lower_ascii(keyword);
    const std::size_t pos = lower_text.find(lower_keyword);
    if (pos == std::string::npos) {
        return {};
    }

    std::size_t begin = pos + lower_keyword.size();
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
        ++begin;
    }

    std::size_t end = begin;
    while (end < text.size()) {
        const unsigned char c = static_cast<unsigned char>(text[end]);
        if (std::isalnum(c) == 0 && c != '_') {
            break;
        }
        ++end;
    }

    if (end <= begin) {
        return {};
    }
    return text.substr(begin, end - begin);
}

QueryPlan build_mock_query_plan(const std::string& query_text) {
    QueryPlan plan{};
    plan.type = QueryType::SELECT;

    const std::string normalized = trim(query_text);
    const std::string lower = to_lower_ascii(normalized);

    if (lower.rfind("use ", 0) == 0) {
        plan.type = QueryType::USE_DATABASE;
        plan.database_name = normalize_identifier(normalized.substr(4));
        return plan;
    }

    if (lower.rfind("create database ", 0) == 0) {
        plan.type = QueryType::CREATE_DATABASE;
        plan.database_name = normalize_identifier(normalized.substr(16));
        return plan;
    }

    if (lower.rfind("drop database ", 0) == 0) {
        plan.type = QueryType::DROP_DATABASE;
        plan.database_name = normalize_identifier(normalized.substr(14));
        return plan;
    }

    plan.type = QueryType::SELECT;
    plan.table_name = extract_identifier_after_keyword(normalized, "from");
    if (plan.table_name.empty()) {
        plan.table_name = "__mock_table__";
    }
    return plan;
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

}

namespace dbms::network {

int run_http_server(const std::string& data_root, const std::string& bind_host, std::uint16_t port,
    const std::string& jwt_secret) {

    crow::App<> app;
   // app.get_middleware<dbms::auth::JwtMiddleware>().set_secret(jwt_secret);

    Engine engine(data_root);

    CROW_ROUTE(app, "/health").methods(crow::HTTPMethod::Get)([] {
        json payload = json::array();
        payload.push_back({{"status", "ok"}});
        return json_response(200, payload);
    });

    CROW_ROUTE(app, "/login").methods(crow::HTTPMethod::Post)([&jwt_secret](const crow::request& request) {
        try {
            const json body = json::parse(request.body);
            if (body["login"] == "admin" && body["password"] == "admin") {
                std::string token = dbms::auth::generate_jwt("admin_user", jwt_secret);
                json response = json::array();
                response.push_back({{"token", token}});
                return json_response(200, response);
            }
            return json_response(401, json::array({{"error", "invalid_credentials"}}));
        } catch (...) {
            return json_response(400, json::array({{"error", "bad_request"}}));
        }
    });

    CROW_ROUTE(app, "/query").methods(crow::HTTPMethod::Post)([&engine](const crow::request& request) {
        try {
            const json request_body = json::parse(request.body);
            if (!request_body.is_object() || !request_body.contains("query") || !request_body["query"].is_string()) {
                json error_payload = json::array();
                error_payload.push_back({{"error", "missing_query_field"}});
                return json_response(400, error_payload);
            }

            const std::string query_text = request_body["query"].get<std::string>();
            QueryPlan mock_plan = build_mock_query_plan(query_text);
            engine.execute(mock_plan);

            if (mock_plan.type == QueryType::SELECT) {
                const std::string raw_result = engine.select_records(mock_plan);
                json parsed = json::parse(raw_result);
                return json_response(200, as_array_of_objects(std::move(parsed)));
            }

            json ok_payload = json::array();
            ok_payload.push_back({{"status", "ok"}});
            return json_response(200, ok_payload);
        } catch (const std::exception& ex) {
            json error_payload = json::array();
            error_payload.push_back({{"error", ex.what()}});
            return json_response(400, error_payload);
        } catch (...) {
            json error_payload = json::array();
            error_payload.push_back({{"error", "unknown_error"}});
            return json_response(500, error_payload);
        }
    });

    app.bindaddr(bind_host).port(port).concurrency(1).run();
    return 0;
}

}
