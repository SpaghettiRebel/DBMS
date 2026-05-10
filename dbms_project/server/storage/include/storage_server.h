#ifndef STORAGE_SERVER_H
#define STORAGE_SERVER_H

#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <vector>
#include <functional>
#include <mutex>
#include <unordered_map>

// Forward declaration
class StorageEngine;

/**
 * @brief Протокол сообщений между клиентом/балансировщиком и Storage-узлом
 */
struct StorageMessage {
    enum class Type : uint8_t {
        MSG_QUERY = 0x01,          // SQL запрос
        MSG_RESPONSE = 0x02,       // Ответ с данными
        MSG_ERROR = 0x03,          // Ошибка выполнения
        MSG_PING = 0x04,           // Проверка доступности (Heartbeat)
        MSG_PONG = 0x05,           // Ответ на Ping
        MSG_SHUTDOWN = 0x06,       // Команда завершения работы
        MSG_ACK = 0x07             // Подтверждение получения
    };

    Type type;
    uint32_t request_id;
    std::string payload;
    int64_t timestamp;

    StorageMessage() : type(Type::MSG_QUERY), request_id(0), timestamp(0) {}
};

/**
 * @brief TCP сервер для Storage-узла СУБД
 * 
 * Реализует:
 * - Многопоточную обработку подключений
 * - Бинарный протокол передачи данных
 * - Интеграцию с StorageEngine
 * - Heartbeat механизм
 * - Graceful shutdown
 */
class StorageServer {
public:
    /**
     * @brief Конструктор сервера
     * @param engine Ссылка на экземпляр StorageEngine
     * @param host Адрес для прослушивания (по умолчанию 0.0.0.0)
     * @param port Порт для прослушивания (по умолчанию 9000)
     * @param max_connections Максимальное количество одновременных подключений
     */
    StorageServer(StorageEngine& engine, 
                  const std::string& host = "0.0.0.0", 
                  uint16_t port = 9000,
                  int max_connections = 100);

    /**
     * @brief Деструктор
     */
    ~StorageServer();

    /**
     * @brief Запуск сервера в отдельном потоке
     * @return true если запуск успешен
     */
    bool start();

    /**
     * @brief Остановка сервера
     * @param timeout_ms Время ожидания завершения активных подключений (мс)
     */
    void stop(uint32_t timeout_ms = 5000);

    /**
     * @brief Проверка состояния сервера
     * @return true если сервер запущен
     */
    bool is_running() const { return running_.load(); }

    /**
     * @brief Получение текущего порта
     */
    uint16_t get_port() const { return port_; }

    /**
     * @brief Получение статистики подключений
     */
    struct Stats {
        uint64_t total_connections;
        uint64_t active_connections;
        uint64_t total_requests;
        uint64_t total_errors;
    };
    
    Stats get_stats() const;

private:
    /**
     * @brief Основной цикл принятия подключений
     */
    void accept_loop();

    /**
     * @brief Обработка подключения клиента в отдельном потоке
     * @param client_fd Дескриптор сокета клиента
     */
    void handle_client(int client_fd);

    /**
     * @brief Чтение сообщения от клиента
     * @param fd Дескриптор сокета
     * @param msg Структура для заполнения
     * @return true если сообщение прочитано успешно
     */
    bool read_message(int fd, StorageMessage& msg);

    /**
     * @brief Отправка сообщения клиенту
     * @param fd Дескриптор сокета
     * @param msg Сообщение для отправки
     * @return true если отправка успешна
     */
    bool write_message(int fd, const StorageMessage& msg);

    /**
     * @brief Обработка полученного запроса
     * @param msg Входное сообщение
     * @param client_fd Дескриптор клиента для ответа
     * @return Результат обработки
     */
    void process_request(const StorageMessage& msg, int client_fd);

    /**
     * @brief Сериализация сообщения в бинарный формат
     */
    std::vector<uint8_t> serialize_message(const StorageMessage& msg);

    /**
     * @brief Десериализация сообщения из бинарного формата
     */
    bool deserialize_message(const std::vector<uint8_t>& data, StorageMessage& msg);

    /**
     * @brief Логирование события сервера
     */
    void log_info(const std::string& message);
    void log_error(const std::string& message);

private:
    StorageEngine& engine_;
    std::string host_;
    uint16_t port_;
    int max_connections_;
    
    int server_fd_;
    std::atomic<bool> running_;
    std::thread accept_thread_;
    
    std::vector<std::thread> worker_threads_;
    std::mutex connections_mutex_;
    std::unordered_map<int, bool> active_connections_;
    
    // Статистика
    std::atomic<uint64_t> total_connections_;
    std::atomic<uint64_t> total_requests_;
    std::atomic<uint64_t> total_errors_;
};

#endif // STORAGE_SERVER_H
