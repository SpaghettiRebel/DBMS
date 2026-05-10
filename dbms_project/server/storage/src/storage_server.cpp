#include "storage_server.h"
#include "engine.h"

#include <iostream>
#include <cstring>
#include <sstream>
#include <chrono>
#include <iomanip>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

// Константы протокола
constexpr uint32_t PROTOCOL_MAGIC = 0xDBMS0001;
constexpr size_t MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10 MB

StorageServer::StorageServer(StorageEngine& engine, 
                             const std::string& host,
                             uint16_t port,
                             int max_connections)
    : engine_(engine)
    , host_(host)
    , port_(port)
    , max_connections_(max_connections)
    , server_fd_(-1)
    , running_(false)
    , total_connections_(0)
    , total_requests_(0)
    , total_errors_(0)
{
}

StorageServer::~StorageServer() {
    stop();
}

bool StorageServer::start() {
    if (running_.load()) {
        log_error("Server is already running");
        return false;
    }

    // Создание сокета
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        log_error("Failed to create socket: " + std::string(strerror(errno)));
        return false;
    }

    // Настройка сокета для повторного использования адреса
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        log_error("Failed to set SO_REUSEADDR: " + std::string(strerror(errno)));
        close(server_fd_);
        return false;
    }

    // Настройка адреса
    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    
    if (inet_pton(AF_INET, host_.c_str(), &address.sin_addr) <= 0) {
        log_error("Invalid address: " + host_);
        close(server_fd_);
        return false;
    }

    // Привязка к адресу
    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        log_error("Bind failed: " + std::string(strerror(errno)));
        close(server_fd_);
        return false;
    }

    // Начало прослушивания
    if (listen(server_fd_, max_connections_) < 0) {
        log_error("Listen failed: " + std::string(strerror(errno)));
        close(server_fd_);
        return false;
    }

    running_.store(true);
    
    // Запуск потока принятия подключений
    accept_thread_ = std::thread(&StorageServer::accept_loop, this);
    
    log_info("Storage server started on " + host_ + ":" + std::to_string(port_));
    return true;
}

void StorageServer::stop(uint32_t timeout_ms) {
    if (!running_.load()) {
        return;
    }

    log_info("Stopping storage server...");
    running_.store(false);

    // Закрытие серверного сокета для прерывания accept()
    if (server_fd_ >= 0) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    // Ожидание завершения потока accept
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    // Закрытие активных подключений
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& pair : active_connections_) {
            if (pair.first >= 0) {
                shutdown(pair.first, SHUT_RDWR);
                close(pair.first);
            }
        }
        active_connections_.clear();
    }

    // Ожидание рабочих потоков
    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    worker_threads_.clear();

    log_info("Storage server stopped");
}

StorageServer::Stats StorageServer::get_stats() const {
    Stats stats;
    stats.total_connections = total_connections_.load();
    stats.total_requests = total_requests_.load();
    stats.total_errors = total_errors_.load();
    
    {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(connections_mutex_));
        stats.active_connections = active_connections_.size();
    }
    
    return stats;
}

void StorageServer::accept_loop() {
    while (running_.load()) {
        struct sockaddr_in client_address;
        socklen_t client_len = sizeof(client_address);
        
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_address, &client_len);
        
        if (client_fd < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }
            if (!running_.load()) {
                break;
            }
            log_error("Accept failed: " + std::string(strerror(errno)));
            continue;
        }

        // Проверка лимита подключений
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            if (static_cast<int>(active_connections_.size()) >= max_connections_) {
                log_error("Max connections reached, rejecting new connection");
                close(client_fd);
                continue;
            }
            active_connections_[client_fd] = true;
        }

        total_connections_++;
        
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_address.sin_addr, client_ip, sizeof(client_ip));
        log_info("New connection from " + std::string(client_ip) + ":" + 
                 std::to_string(ntohs(client_address.sin_port)));

        // Запуск обработки в отдельном потоке
        try {
            worker_threads_.emplace_back(&StorageServer::handle_client, this, client_fd);
        } catch (const std::exception& e) {
            log_error("Failed to create worker thread: " + std::string(e.what()));
            close(client_fd);
            {
                std::lock_guard<std::mutex> lock(connections_mutex_);
                active_connections_.erase(client_fd);
            }
            total_errors_++;
        }
    }
}

void StorageServer::handle_client(int client_fd) {
    auto cleanup = [this, client_fd]() {
        close(client_fd);
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            active_connections_.erase(client_fd);
        }
    };

    try {
        while (running_.load()) {
            StorageMessage msg;
            
            // Чтение сообщения
            if (!read_message(client_fd, msg)) {
                // Клиент закрыл соединение или ошибка чтения
                break;
            }

            total_requests_++;
            
            // Обработка запроса
            process_request(msg, client_fd);
        }
    } catch (const std::exception& e) {
        log_error("Error handling client: " + std::string(e.what()));
        total_errors_++;
        
        // Отправка ошибки клиенту
        StorageMessage error_msg;
        error_msg.type = StorageMessage::Type::MSG_ERROR;
        error_msg.payload = e.what();
        write_message(client_fd, error_msg);
    }

    cleanup();
}

bool StorageServer::read_message(int fd, StorageMessage& msg) {
    // Чтение заголовка: [magic:4][type:1][request_id:4][timestamp:8][payload_len:4]
    constexpr size_t HEADER_SIZE = 4 + 1 + 4 + 8 + 4;
    uint8_t header[HEADER_SIZE];
    
    size_t total_read = 0;
    while (total_read < HEADER_SIZE) {
        ssize_t n = read(fd, header + total_read, HEADER_SIZE - total_read);
        if (n <= 0) {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                return false; // Соединение закрыто или ошибка
            }
            // Ждем данных
            continue;
        }
        total_read += n;
    }

    // Проверка magic number
    uint32_t magic;
    std::memcpy(&magic, header, 4);
    if (magic != PROTOCOL_MAGIC) {
        log_error("Invalid protocol magic number");
        return false;
    }

    // Парсинг заголовка
    std::memcpy(&msg.type, header + 4, 1);
    std::memcpy(&msg.request_id, header + 5, 4);
    std::memcpy(&msg.timestamp, header + 9, 8);
    
    uint32_t payload_len;
    std::memcpy(&payload_len, header + 17, 4);

    if (payload_len > MAX_MESSAGE_SIZE) {
        log_error("Message too large: " + std::to_string(payload_len));
        return false;
    }

    // Чтение payload
    if (payload_len > 0) {
        msg.payload.resize(payload_len);
        size_t total_payload_read = 0;
        while (total_payload_read < payload_len) {
            ssize_t n = read(fd, &msg.payload[total_payload_read], payload_len - total_payload_read);
            if (n <= 0) {
                if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                    return false;
                }
                continue;
            }
            total_payload_read += n;
        }
    }

    return true;
}

bool StorageServer::write_message(int fd, const StorageMessage& msg) {
    // Сериализация сообщения
    auto data = serialize_message(msg);
    
    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t n = send(fd, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (errno == EPIPE) {
                return false; // Клиент закрыл соединение
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                return false;
            }
            continue;
        }
        total_sent += n;
    }
    
    return true;
}

void StorageServer::process_request(const StorageMessage& msg, int client_fd) {
    StorageMessage response;
    response.request_id = msg.request_id;
    response.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    switch (msg.type) {
        case StorageMessage::Type::MSG_PING: {
            response.type = StorageMessage::Type::MSG_PONG;
            response.payload = "OK";
            write_message(client_fd, response);
            break;
        }
        
        case StorageMessage::Type::MSG_SHUTDOWN: {
            log_info("Shutdown request received");
            response.type = StorageMessage::Type::MSG_ACK;
            response.payload = "Shutting down";
            write_message(client_fd, response);
            stop();
            break;
        }
        
        case StorageMessage::Type::MSG_QUERY: {
            try {
                // Выполнение SQL запроса через StorageEngine
                std::string result = engine_.execute_query(msg.payload);
                
                response.type = StorageMessage::Type::MSG_RESPONSE;
                response.payload = result;
                write_message(client_fd, response);
            } catch (const std::exception& e) {
                response.type = StorageMessage::Type::MSG_ERROR;
                response.payload = e.what();
                write_message(client_fd, response);
                total_errors_++;
            }
            break;
        }
        
        default: {
            response.type = StorageMessage::Type::MSG_ERROR;
            response.payload = "Unknown message type";
            write_message(client_fd, response);
            total_errors_++;
            break;
        }
    }
}

std::vector<uint8_t> StorageServer::serialize_message(const StorageMessage& msg) {
    std::vector<uint8_t> data;
    
    // Вычисление размера: [magic:4][type:1][request_id:4][timestamp:8][payload_len:4][payload:N]
    size_t total_size = 4 + 1 + 4 + 8 + 4 + msg.payload.size();
    data.resize(total_size);
    
    size_t offset = 0;
    
    // Magic
    uint32_t magic = PROTOCOL_MAGIC;
    std::memcpy(data.data() + offset, &magic, 4);
    offset += 4;
    
    // Type
    std::memcpy(data.data() + offset, &msg.type, 1);
    offset += 1;
    
    // Request ID
    std::memcpy(data.data() + offset, &msg.request_id, 4);
    offset += 4;
    
    // Timestamp
    std::memcpy(data.data() + offset, &msg.timestamp, 8);
    offset += 8;
    
    // Payload length
    uint32_t payload_len = static_cast<uint32_t>(msg.payload.size());
    std::memcpy(data.data() + offset, &payload_len, 4);
    offset += 4;
    
    // Payload
    if (!msg.payload.empty()) {
        std::memcpy(data.data() + offset, msg.payload.data(), msg.payload.size());
    }
    
    return data;
}

bool StorageServer::deserialize_message(const std::vector<uint8_t>& data, StorageMessage& msg) {
    if (data.size() < 21) { // Минимальный размер заголовка
        return false;
    }
    
    size_t offset = 0;
    
    // Skip magic (already checked in read_message)
    offset += 4;
    
    // Type
    std::memcpy(&msg.type, data.data() + offset, 1);
    offset += 1;
    
    // Request ID
    std::memcpy(&msg.request_id, data.data() + offset, 4);
    offset += 4;
    
    // Timestamp
    std::memcpy(&msg.timestamp, data.data() + offset, 8);
    offset += 8;
    
    // Payload length
    uint32_t payload_len;
    std::memcpy(&payload_len, data.data() + offset, 4);
    offset += 4;
    
    if (offset + payload_len > data.size()) {
        return false;
    }
    
    // Payload
    if (payload_len > 0) {
        msg.payload.assign(data.begin() + offset, data.begin() + offset + payload_len);
    }
    
    return true;
}

void StorageServer::log_info(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
    ss << " [INFO] [StorageServer] " << message;
    std::cout << ss.str() << std::endl;
}

void StorageServer::log_error(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
    ss << " [ERROR] [StorageServer] " << message;
    std::cerr << ss.str() << std::endl;
}
