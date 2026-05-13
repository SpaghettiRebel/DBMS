#include "storage_node.h"
#include <cstring>
#include <chrono>
#include <iostream>
#include <stdexcept>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif
#include <vector>
#include <errno.h>
#include <mutex>

// Константы протокола (должны совпадать с storage_server.cpp)
constexpr uint32_t PROTOCOL_MAGIC = 0xDB000001;
constexpr size_t MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

namespace {

#ifdef _WIN32
bool ensure_winsock_started() {
    static std::once_flag init_flag;
    static bool initialized = false;
    std::call_once(init_flag, []() {
        WSADATA wsa_data{};
        initialized = (WSAStartup(MAKEWORD(2, 2), &wsa_data) == 0);
    });
    return initialized;
}

std::string socket_last_error_message() {
    return std::to_string(WSAGetLastError());
}

void close_socket(int fd) {
    closesocket(static_cast<SOCKET>(fd));
}

int shutdown_socket(int fd) {
    return shutdown(static_cast<SOCKET>(fd), SD_BOTH);
}

int send_flags() {
    return 0;
}
#else
bool ensure_winsock_started() {
    return true;
}

std::string socket_last_error_message() {
    return std::string(strerror(errno));
}

void close_socket(int fd) {
    close(fd);
}

int shutdown_socket(int fd) {
    return shutdown(fd, SHUT_RDWR);
}

int send_flags() {
    return MSG_NOSIGNAL;
}
#endif

}

StorageNode::StorageNode(const std::string& node_id,
                         const std::string& host,
                         uint16_t port)
    : node_id_(node_id)
    , host_(host)
    , port_(port)
    , status_(Status::OFFLINE)
    , socket_fd_(-1)
    , total_requests_(0)
    , failed_requests_(0)
    , last_heartbeat_(0)
    , shard_count_(0)
{
}

int64_t StorageNode::get_current_time_ms() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

bool StorageNode::connect(int timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (socket_fd_ >= 0) {
        return true; // Уже подключен
    }
    
    if (!ensure_winsock_started()) {
        return false;
    }

    int fd = static_cast<int>(socket(AF_INET, SOCK_STREAM, 0));
    if (fd < 0) {
        return false;
    }
    
    // Настройка таймаута
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
#ifdef _WIN32
               reinterpret_cast<const char*>(&tv),
#else
               &tv,
#endif
               sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
#ifdef _WIN32
               reinterpret_cast<const char*>(&tv),
#else
               &tv,
#endif
               sizeof(tv));
    
    // Подключение
    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    
    if (inet_pton(AF_INET, host_.c_str(), &address.sin_addr) <= 0) {
        close_socket(fd);
        return false;
    }
    
    if (::connect(fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        close_socket(fd);
        return false;
    }
    
    socket_fd_ = fd;
    status_ = Status::ONLINE;
    update_heartbeat();
    
    return true;
}

void StorageNode::disconnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (socket_fd_ >= 0) {
        shutdown_socket(socket_fd_);
        close_socket(socket_fd_);
        socket_fd_ = -1;
    }
    
    status_ = Status::OFFLINE;
}

bool StorageNode::ping(int64_t timeout_ms) {
    if (!is_online()) {
        if (!connect(timeout_ms)) {
            return false;
        }
    }
    
    // Формирование PING сообщения
    uint8_t type = 0x04; // MSG_PING
    uint32_t request_id = 0;
    int64_t timestamp = get_current_time_ms();
    uint32_t payload_len = 0;
    
    // Заголовок: [magic:4][type:1][request_id:4][timestamp:8][payload_len:4]
    std::vector<uint8_t> header(21);
    size_t offset = 0;
    
    uint32_t magic = PROTOCOL_MAGIC;
    std::memcpy(header.data() + offset, &magic, 4);
    offset += 4;
    
    std::memcpy(header.data() + offset, &type, 1);
    offset += 1;
    
    std::memcpy(header.data() + offset, &request_id, 4);
    offset += 4;
    
    std::memcpy(header.data() + offset, &timestamp, 8);
    offset += 8;
    
    std::memcpy(header.data() + offset, &payload_len, 4);
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (socket_fd_ < 0) {
        return false;
    }
    
    // Отправка
    int sent = send(socket_fd_,
                    reinterpret_cast<const char*>(header.data()),
                    static_cast<int>(header.size()),
                    send_flags());
    if (sent != static_cast<int>(header.size())) {
        disconnect();
        return false;
    }
    
    // Чтение ответа
    uint8_t response_header[21];
    int received = recv(socket_fd_,
                        reinterpret_cast<char*>(response_header),
                        static_cast<int>(sizeof(response_header)),
                        0);
    if (received != static_cast<int>(sizeof(response_header))) {
        disconnect();
        return false;
    }
    
    // Проверка типа ответа (должен быть PONG = 0x05)
    uint8_t response_type;
    std::memcpy(&response_type, response_header + 4, 1);
    
    if (response_type != 0x05) {
        return false;
    }
    
    update_heartbeat();
    return true;
}

std::string StorageNode::send_query(const std::string& query, int64_t timeout_ms) {
    if (!is_online()) {
        if (!connect(timeout_ms)) {
            failed_requests_++;
            throw std::runtime_error("Failed to connect to node " + node_id_);
        }
    }
    
    // Формирование QUERY сообщения
    uint8_t type = 0x01; // MSG_QUERY
    uint32_t request_id = 1;
    int64_t timestamp = get_current_time_ms();
    uint32_t payload_len = static_cast<uint32_t>(query.size());
    
    // Заголовок + payload
    std::vector<uint8_t> message(21 + payload_len);
    size_t offset = 0;
    
    uint32_t magic = PROTOCOL_MAGIC;
    std::memcpy(message.data() + offset, &magic, 4);
    offset += 4;
    
    std::memcpy(message.data() + offset, &type, 1);
    offset += 1;
    
    std::memcpy(message.data() + offset, &request_id, 4);
    offset += 4;
    
    std::memcpy(message.data() + offset, &timestamp, 8);
    offset += 8;
    
    std::memcpy(message.data() + offset, &payload_len, 4);
    offset += 4;
    
    std::memcpy(message.data() + offset, query.data(), payload_len);
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (socket_fd_ < 0) {
        failed_requests_++;
        throw std::runtime_error("Socket not connected");
    }
    
    // Отправка
    int sent_total = 0;
    while (sent_total < static_cast<int>(message.size())) {
        int sent = send(socket_fd_,
                        reinterpret_cast<const char*>(message.data() + sent_total),
                        static_cast<int>(message.size() - sent_total),
                        send_flags());
        if (sent < 0) {
            if (
#ifdef _WIN32
                WSAGetLastError() == WSAECONNRESET
#else
                errno == EPIPE || errno == ECONNRESET
#endif
            ) {
                disconnect();
            }
            failed_requests_++;
            throw std::runtime_error("Send failed: " + socket_last_error_message());
        }
        sent_total += sent;
    }
    
    // Чтение ответа
    uint8_t response_header[21];
    int received = recv(socket_fd_,
                        reinterpret_cast<char*>(response_header),
                        static_cast<int>(sizeof(response_header)),
                        0);
    if (received != static_cast<int>(sizeof(response_header))) {
        disconnect();
        failed_requests_++;
        throw std::runtime_error("Failed to read response header");
    }
    
    // Парсинг заголовка ответа
    uint32_t resp_magic;
    std::memcpy(&resp_magic, response_header, 4);
    if (resp_magic != PROTOCOL_MAGIC) {
        throw std::runtime_error("Invalid response magic");
    }
    
    uint8_t resp_type;
    std::memcpy(&resp_type, response_header + 4, 1);
    
    uint32_t resp_payload_len;
    std::memcpy(&resp_payload_len, response_header + 17, 4);
    
    if (resp_payload_len > MAX_MESSAGE_SIZE) {
        throw std::runtime_error("Response too large");
    }
    
    // Чтение payload
    std::string response;
    if (resp_payload_len > 0) {
        response.resize(resp_payload_len);
        int payload_received = 0;
        while (payload_received < static_cast<int>(resp_payload_len)) {
            int n = recv(socket_fd_,
                         &response[payload_received],
                         static_cast<int>(resp_payload_len - payload_received),
                         0);
            if (n <= 0) {
                disconnect();
                failed_requests_++;
                throw std::runtime_error("Failed to read response payload");
            }
            payload_received += n;
        }
    }
    
    // Проверка типа ответа
    if (resp_type == 0x03) { // MSG_ERROR
        throw std::runtime_error("Node error: " + response);
    }
    
    total_requests_++;
    return response;
}
