#include "engine.h"
#include "storage_server.h"

#include <iostream>
#include <csignal>
#include <atomic>

// Глобальный флаг для graceful shutdown
std::atomic<bool> g_shutdown_requested(false);

void signal_handler(int signum) {
    std::cout << "\nReceived signal " << signum << ", shutting down..." << std::endl;
    g_shutdown_requested.store(true);
}

int main(int argc, char* argv[]) {
    // Настройка обработчиков сигналов
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Параметры по умолчанию
    std::string data_dir = "./db_data";
    uint16_t port = 9000;
    std::string host = "0.0.0.0";
    int max_connections = 100;

    // Парсинг аргументов командной строки
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--max-connections" && i + 1 < argc) {
            max_connections = std::stoi(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --data-dir <path>      Directory for database files (default: ./db_data)" << std::endl;
            std::cout << "  --port <port>          Port to listen on (default: 9000)" << std::endl;
            std::cout << "  --host <host>          Host to bind to (default: 0.0.0.0)" << std::endl;
            std::cout << "  --max-connections <n>  Maximum concurrent connections (default: 100)" << std::endl;
            std::cout << "  --help, -h             Show this help message" << std::endl;
            return 0;
        }
    }

    std::cout << "=== DBMS Storage Server ===" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "Listening on: " << host << ":" << port << std::endl;
    std::cout << "Max connections: " << max_connections << std::endl;
    std::cout << std::endl;

    try {
        // Инициализация движка
        StorageEngine engine(data_dir);
        std::cout << "Storage engine initialized successfully" << std::endl;

        // Создание и запуск сервера
        StorageServer server(engine, host, port, max_connections);
        
        if (!server.start()) {
            std::cerr << "Failed to start storage server" << std::endl;
            return 1;
        }

        std::cout << "Storage server is running. Press Ctrl+C to stop." << std::endl;
        std::cout << std::endl;

        // Ожидание сигнала завершения
        while (!g_shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Вывод статистики каждые 10 секунд
            static auto last_stats_time = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_stats_time).count() >= 10) {
                auto stats = server.get_stats();
                std::cout << "[Stats] Connections: " << stats.total_connections 
                          << " (active: " << stats.active_connections << ")"
                          << ", Requests: " << stats.total_requests
                          << ", Errors: " << stats.total_errors << std::endl;
                last_stats_time = now;
            }
        }

        // Graceful shutdown
        std::cout << "Initiating graceful shutdown..." << std::endl;
        server.stop(5000);
        
        std::cout << "Storage server stopped successfully" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
