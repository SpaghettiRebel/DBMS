#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#ifdef DELETE
#undef DELETE
#endif

#if __has_include(<crow.h>)
#include <crow.h>
#elif __has_include(<crow/crow.h>)
#include <crow/crow.h>
#else
#include "crow.h"
#endif

namespace dbms::auth {

std::string generate_salt(std::size_t size = 16);
std::string hash_password(const std::string& password, const std::string& salt);
bool verify_password(const std::string& password, const std::string& salt, const std::string& expected_hash);
std::string generate_jwt(
    const std::string& subject,
    const std::string& secret,
    std::chrono::seconds ttl = std::chrono::hours(1));
bool validate_jwt(
    const std::string& token,
    const std::string& secret,
    std::string* subject = nullptr);

class JwtMiddleware {
public:
    struct context {};

    JwtMiddleware() = default;

    void set_secret(std::string secret);
    void before_handle(crow::request& req, crow::response& res, context&);
    void after_handle(crow::request&, crow::response&, context&);

private:
    std::string secret_{"dev-secret"};
};

}
