// считывает настройки (хост, порт), создает объект твоего Диспетчера (ClusterEntrypoint),
// добавляет в него адреса серверов хранения через add_storage_node и запускает его метод start()

#include "cluster_entrypoint.h"
#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>

std::atomic<bool> g_shutdown_requested(false);

void signal_handler(int) {
    std::cout << "\nShutdown requested..." << std::endl;
    g_shutdown_requested.store(true);
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    std::string host = "0.0.0.0";
    uint16_t port = 8000;
    int heartbeat_interval = 5000;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--heartbeat" && i + 1 < argc) {
            heartbeat_interval = std::stoi(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "  --port <p>        Port to listen (default: 8000)\n"
                      << "  --host <h>        Host to bind (default: 0.0.0.0)\n"
                      << "  --heartbeat <ms>  Heartbeat interval ms (default: 5000)\n"
                      << "  --help, -h        Show help\n";
            return 0;
        }
    }
    
    std::cout << "=== DBMS Cluster Entrypoint ===\n"
              << "Listening: " << host << ":" << port << "\n"
              << "Heartbeat: " << heartbeat_interval << "ms\n\n";
    
    try {
        ClusterEntrypoint cluster(host, port, heartbeat_interval);
        
        if (!cluster.start()) {
            std::cerr << "Failed to start cluster\n";
            return 1;
        }
        
        // Добавление тестовых узлов (в реальности они добавляются через API)
        // cluster.add_storage_node("node1", "127.0.0.1", 9001);
        // cluster.add_storage_node("node2", "127.0.0.1", 9002);
        
        std::cout << "Cluster is running. Press Ctrl+C to stop.\n\n";
        
        while (!g_shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            static auto last_stats = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_stats).count() >= 10) {
                auto stats = cluster.get_stats();
                std::cout << "[Stats] Nodes: " << stats.online_nodes << "/" << stats.total_nodes
                          << " | Requests: " << stats.total_requests
                          << " | Errors: " << stats.total_errors
                          << " | RPS: " << stats.current_rps
                          << " | Avg RT: " << stats.avg_response_time_10s << "ms\n";
                last_stats = now;
            }
        }
        
        std::cout << "Shutting down...\n";
        cluster.stop(10000);
        std::cout << "Cluster stopped.\n";
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
