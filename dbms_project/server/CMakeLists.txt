cmake_minimum_required(VERSION 3.16)

project(dbms_server LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# Определяем пути к корню и сторонним библиотекам
set(DBMS_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/..)
set(THIRD_PARTY_DIR ${DBMS_ROOT_DIR}/third_party)
set(PARSER_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/parser_generated)

file(MAKE_DIRECTORY ${PARSER_GEN_DIR})

BISON_TARGET(
        DbmsSqlParser
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/grammar.y
        ${PARSER_GEN_DIR}/grammar.tab.cpp
        DEFINES_FILE ${PARSER_GEN_DIR}/grammar.tab.h
)

FLEX_TARGET(
        DbmsSqlLexer
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/lexer.l
        ${PARSER_GEN_DIR}/lex.yy.cpp
)

ADD_FLEX_BISON_DEPENDENCY(DbmsSqlLexer DbmsSqlParser)

# Список исходников ядра (код напарника)
set(STORAGE_CORE_SOURCES
        storage/src/engine.cpp
        storage/src/file_manager.cpp
        storage/src/binary_file_manager.cpp
        storage/src/bplus_tree_persistence.cpp
        storage/src/wal_manager.cpp
        storage/src/journal.cpp
        storage/src/string_pool.cpp
        storage/src/query_optimizer.cpp
        storage/src/schema_manager.cpp
        storage/src/access_logger.cpp
)

# Создаем статическую библиотеку ядра
add_library(dbms_storage_core STATIC ${STORAGE_CORE_SOURCES})

# Добавляем пути поиска заголовков для ядра (включая Boost)
target_include_directories(dbms_storage_core
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/storage/include
        ${DBMS_ROOT_DIR}/shared
        ${THIRD_PARTY_DIR}/boost
        ${THIRD_PARTY_DIR}
)

# Исходники кластерного модуля (без main_cluster.cpp)
set(CLUSTER_MODULE_SOURCES
        cluster/src/storage_node.cpp
        cluster/src/shard_manager.cpp
        cluster/src/heartbeat_monitor.cpp
        cluster/src/async_request_queue.cpp
        cluster/src/telemetry_collector.cpp
)

# Основной исполняемый файл сервера
add_executable(dbms_http_server
        main.cpp
        network/http_server.cpp
        network/auth.cpp
        network/account_store.cpp
        ${CLUSTER_MODULE_SOURCES}
        ${BISON_DbmsSqlParser_OUTPUTS}
        ${FLEX_DbmsSqlLexer_OUTPUTS}
)

# Добавляем пути поиска заголовков для сервера (Crow и всё остальное)
target_include_directories(dbms_http_server
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/network
        ${CMAKE_CURRENT_SOURCE_DIR}/storage/include
        ${CMAKE_CURRENT_SOURCE_DIR}/cluster/include
        ${DBMS_ROOT_DIR}/shared
        ${THIRD_PARTY_DIR}/crow
        ${THIRD_PARTY_DIR}/boost
        ${PARSER_GEN_DIR}
)

# Поиск системных библиотек (потоки)
find_package(Threads REQUIRED)

# Линковка: соединяем сервер с ядром и системными библиотеками
target_link_libraries(dbms_http_server
        PRIVATE
        dbms_storage_core
        Threads::Threads
)

# Если используешь MinGW на Windows, иногда нужна библиотека ws2_32 для работы сети
if(WIN32)
    target_link_libraries(dbms_http_server PRIVATE ws2_32 mswsock)
endif()
