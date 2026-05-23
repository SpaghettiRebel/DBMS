#include "auth.h"

#include <array>
#include <cctype>
#include <cstdint>
#include <random>
#include <sstream>
#include <string_view>
#include <utility>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace {

// переводит число в шестнадцатеричную строку фиксированной длины.
std::string to_hex(uint64_t value) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(16, '0');
    for (int i = 15; i >= 0; --i) {
        out[static_cast<std::size_t>(i)] = kHex[value & 0xF];
        value >>= 4;
    }
    return out;
}

// считает смешанный fnv1a-хэш для строки с заданным seed.
uint64_t fnv1a_mix(std::string_view input, uint64_t seed) {
    uint64_t hash = 1469598103934665603ULL ^ seed;
    for (unsigned char byte : input) {
        hash ^= static_cast<uint64_t>(byte);
        hash *= 1099511628211ULL;
    }
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

// строит компактный хэш данных с учетом ключа.
std::string compact_hash(std::string_view data, std::string_view key) {
    std::string seed_input;
    seed_input.reserve(key.size() + 1 + data.size());
    seed_input.append(key);
    seed_input.push_back(':');
    seed_input.append(data);

    uint64_t h1 = fnv1a_mix(seed_input, 0x9e3779b97f4a7c15ULL);
    uint64_t h2 = fnv1a_mix(seed_input, h1 ^ 0x243f6a8885a308d3ULL);
    uint64_t h3 = fnv1a_mix(seed_input, h2 ^ 0x13198a2e03707344ULL);
    uint64_t h4 = fnv1a_mix(seed_input, h3 ^ 0xa4093822299f31d0ULL);

    for (int round = 0; round < 4096; ++round) {
        h1 = fnv1a_mix(seed_input, h1 ^ static_cast<uint64_t>(round));
        h2 = fnv1a_mix(seed_input, h2 ^ h1);
        h3 = fnv1a_mix(seed_input, h3 ^ h2);
        h4 = fnv1a_mix(seed_input, h4 ^ h3);
    }

    return to_hex(h1) + to_hex(h2) + to_hex(h3) + to_hex(h4);
}

// кодирует строку в base64url без завершающих символов padding.
std::string base64url_encode(std::string_view input) {
    static constexpr char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string output;
    output.reserve(((input.size() + 2) / 3) * 4);

    std::size_t i = 0;
    while (i + 3 <= input.size()) {
        const uint32_t block = (static_cast<uint32_t>(static_cast<unsigned char>(input[i])) << 16) |
                               (static_cast<uint32_t>(static_cast<unsigned char>(input[i + 1])) << 8) |
                               static_cast<uint32_t>(static_cast<unsigned char>(input[i + 2]));
        output.push_back(kTable[(block >> 18) & 0x3F]);
        output.push_back(kTable[(block >> 12) & 0x3F]);
        output.push_back(kTable[(block >> 6) & 0x3F]);
        output.push_back(kTable[block & 0x3F]);
        i += 3;
    }

    const std::size_t remain = input.size() - i;
    if (remain == 1) {
        const uint32_t block = static_cast<uint32_t>(static_cast<unsigned char>(input[i])) << 16;
        output.push_back(kTable[(block >> 18) & 0x3F]);
        output.push_back(kTable[(block >> 12) & 0x3F]);
        output.push_back('=');
        output.push_back('=');
    } else if (remain == 2) {
        const uint32_t block = (static_cast<uint32_t>(static_cast<unsigned char>(input[i])) << 16) |
                               (static_cast<uint32_t>(static_cast<unsigned char>(input[i + 1])) << 8);
        output.push_back(kTable[(block >> 18) & 0x3F]);
        output.push_back(kTable[(block >> 12) & 0x3F]);
        output.push_back(kTable[(block >> 6) & 0x3F]);
        output.push_back('=');
    }

    for (char& c : output) {
        if (c == '+') {
            c = '-';
        } else if (c == '/') {
            c = '_';
        }
    }
    while (!output.empty() && output.back() == '=') {
        output.pop_back();
    }
    return output;
}

// декодирует строку из base64url в исходные байты.
bool base64url_decode(std::string_view input, std::string& output) {
    static const std::array<int8_t, 256> kReverse = [] {
        std::array<int8_t, 256> table{};
        table.fill(-1);
        static constexpr char chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (std::size_t i = 0; chars[i] != '\0'; ++i) {
            table[static_cast<unsigned char>(chars[i])] = static_cast<int8_t>(i);
        }
        return table;
    }();

    std::string normalized(input);
    for (char& c : normalized) {
        if (c == '-') {
            c = '+';
        } else if (c == '_') {
            c = '/';
        }
    }
    while (normalized.size() % 4 != 0) {
        normalized.push_back('=');
    }

    output.clear();
    output.reserve((normalized.size() / 4) * 3);
    for (std::size_t i = 0; i < normalized.size(); i += 4) {
        const char c0 = normalized[i];
        const char c1 = normalized[i + 1];
        const char c2 = normalized[i + 2];
        const char c3 = normalized[i + 3];

        if (kReverse[static_cast<unsigned char>(c0)] < 0 || kReverse[static_cast<unsigned char>(c1)] < 0) {
            return false;
        }
        const uint32_t b0 = static_cast<uint32_t>(kReverse[static_cast<unsigned char>(c0)]);
        const uint32_t b1 = static_cast<uint32_t>(kReverse[static_cast<unsigned char>(c1)]);
        const uint32_t b2 = c2 == '=' ? 0 : static_cast<uint32_t>(kReverse[static_cast<unsigned char>(c2)]);
        const uint32_t b3 = c3 == '=' ? 0 : static_cast<uint32_t>(kReverse[static_cast<unsigned char>(c3)]);

        if ((c2 != '=' && kReverse[static_cast<unsigned char>(c2)] < 0) ||
            (c3 != '=' && kReverse[static_cast<unsigned char>(c3)] < 0)) {
            return false;
        }

        const uint32_t block = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;
        output.push_back(static_cast<char>((block >> 16) & 0xFF));
        if (c2 != '=') {
            output.push_back(static_cast<char>((block >> 8) & 0xFF));
        }
        if (c3 != '=') {
            output.push_back(static_cast<char>(block & 0xFF));
        }
    }
    return true;
}

// создает подпись сообщения на основе секрета.
std::string hmac_like_signature(std::string_view message, std::string_view secret) {
    return compact_hash(message, secret);
}

// формирует json-ответ для ошибки авторизации.
std::string make_unauthorized_body(const std::string& reason) {
    json body = json::array();
    body.push_back({{"error", reason}});
    return body.dump();
}

// убирает пробелы в начале и конце строки.
std::string trim(std::string input) {
    std::size_t left = 0;
    while (left < input.size() && std::isspace(static_cast<unsigned char>(input[left])) != 0) {
        ++left;
    }
    std::size_t right = input.size();
    while (right > left && std::isspace(static_cast<unsigned char>(input[right - 1])) != 0) {
        --right;
    }
    return input.substr(left, right - left);
}

// проверяет, что строка начинается с указанного префикса с учетом регистра.
bool starts_with_case_sensitive(std::string_view value, std::string_view prefix) {
    if (value.size() < prefix.size()) {
        return false;
    }
    return value.compare(0, prefix.size(), prefix) == 0;
}

}

namespace dbms::auth {

// генерирует случайную соль заданного размера.
std::string generate_salt(std::size_t size) {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);

    std::string bytes;
    bytes.resize(size);
    for (std::size_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<char>(dist(gen));
    }
    return base64url_encode(bytes);
}

// хэширует пароль с использованием соли.
std::string hash_password(const std::string& password, const std::string& salt) {
    return compact_hash(password, salt);
}

// проверяет пароль по соли и ожидаемому хэшу.
bool verify_password(const std::string& password, const std::string& salt, const std::string& expected_hash) {
    return hash_password(password, salt) == expected_hash;
}

// создает jwt-токен для пользователя на заданное время жизни.
std::string generate_jwt(const std::string& subject, const std::string& secret, std::chrono::seconds ttl) {
    const auto now = std::chrono::system_clock::now();
    const auto iat = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    const auto exp =
        std::chrono::duration_cast<std::chrono::seconds>((now + ttl).time_since_epoch()).count();

    const std::string header = R"({"alg":"HS256","typ":"JWT"})";
    json payload;
    payload["sub"] = subject;
    payload["iat"] = iat;
    payload["exp"] = exp;

    const std::string encoded_header = base64url_encode(header);
    const std::string encoded_payload = base64url_encode(payload.dump());
    const std::string signing_input = encoded_header + "." + encoded_payload;
    const std::string signature = base64url_encode(hmac_like_signature(signing_input, secret));
    return signing_input + "." + signature;
}

// проверяет jwt-токен и при необходимости возвращает subject.
bool validate_jwt(const std::string& token, const std::string& secret, std::string* subject) {
    const std::size_t first_dot = token.find('.');
    if (first_dot == std::string::npos) {
        return false;
    }
    const std::size_t second_dot = token.find('.', first_dot + 1);
    if (second_dot == std::string::npos || token.find('.', second_dot + 1) != std::string::npos) {
        return false;
    }

    const std::string encoded_header = token.substr(0, first_dot);
    const std::string encoded_payload = token.substr(first_dot + 1, second_dot - first_dot - 1);
    const std::string encoded_signature = token.substr(second_dot + 1);
    const std::string signing_input = encoded_header + "." + encoded_payload;
    const std::string expected_signature = base64url_encode(hmac_like_signature(signing_input, secret));
    if (expected_signature != encoded_signature) {
        return false;
    }

    std::string decoded_payload;
    if (!base64url_decode(encoded_payload, decoded_payload)) {
        return false;
    }

    json payload_json;
    try {
        payload_json = json::parse(decoded_payload);
    } catch (...) {
        return false;
    }

    if (!payload_json.is_object() || !payload_json.contains("exp") || !payload_json["exp"].is_number_integer()) {
        return false;
    }

    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    const auto exp = payload_json["exp"].get<long long>();
    if (now >= exp) {
        return false;
    }

    if (subject != nullptr && payload_json.contains("sub") && payload_json["sub"].is_string()) {
        *subject = payload_json["sub"].get<std::string>();
    }
    return true;
}

// задает секрет для проверки jwt-токенов.
void JwtMiddleware::set_secret(std::string secret) {
    if (!secret.empty()) {
        secret_ = std::move(secret);
    }
}

// проверяет авторизацию перед обработкой защищенных query-запросов.
void JwtMiddleware::before_handle(crow::request& req, crow::response& res, context&) {
    if (!starts_with_case_sensitive(req.url, "/query")) {
        return;
    }

    std::string header = trim(req.get_header_value("Authorization"));
    if (!starts_with_case_sensitive(header, "Bearer ")) {
        res.code = 401;
        res.set_header("Content-Type", "application/json");
        res.write(make_unauthorized_body("missing_or_invalid_authorization_header"));
        res.end();
        return;
    }

    const std::string token = trim(header.substr(7));
    if (token.empty() || !validate_jwt(token, secret_)) {
        res.code = 401;
        res.set_header("Content-Type", "application/json");
        res.write(make_unauthorized_body("invalid_or_expired_token"));
        res.end();
        return;
    }
}

// завершает обработку middleware после ответа.
void JwtMiddleware::after_handle(crow::request&, crow::response&, context&) {}

}
