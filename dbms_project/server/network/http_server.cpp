#include "http_server.h"

#include <exception>
#include <mutex>
#include <optional>
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
            std::optional<QueryPlan> parsed_plan = parse_sql_with_bison(query_text);
            if (!parsed_plan.has_value()) {
                json error_payload = json::array();
                error_payload.push_back({{"error", "sql_parse_error"}});
                return json_response(400, error_payload);
            }

            QueryPlan& plan = *parsed_plan;
            engine.execute(plan);

            if (plan.type == QueryType::SELECT) {
                const std::string raw_result = engine.select_records(plan);
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
