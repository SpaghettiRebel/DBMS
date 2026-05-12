#pragma once

#include <cstdint>
#include <string>

namespace dbms::network {

int run_http_server(
    const std::string& data_root,
    const std::string& bind_host,
    std::uint16_t port,
    const std::string& jwt_secret);

}
