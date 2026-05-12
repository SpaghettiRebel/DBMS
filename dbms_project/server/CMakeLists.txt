cmake_minimum_required(VERSION 3.16)

project(dbms_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(DBMS_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/..)
set(THIRD_PARTY_DIR ${DBMS_ROOT_DIR}/third_party)

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

add_library(dbms_storage_core STATIC ${STORAGE_CORE_SOURCES})

target_include_directories(dbms_storage_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/storage/include
        ${DBMS_ROOT_DIR}/shared
)

add_executable(dbms_http_server
    main.cpp
    network/http_server.cpp
    network/auth.cpp
)

target_include_directories(dbms_http_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/network
        ${CMAKE_CURRENT_SOURCE_DIR}/storage/include
        ${DBMS_ROOT_DIR}/shared
        ${THIRD_PARTY_DIR}
        ${THIRD_PARTY_DIR}/crow
        ${THIRD_PARTY_DIR}/crow/include
        ${THIRD_PARTY_DIR}/Crow
        ${THIRD_PARTY_DIR}/Crow/include
)

if (EXISTS "${THIRD_PARTY_DIR}/crow/CMakeLists.txt")
    add_subdirectory(${THIRD_PARTY_DIR}/crow ${CMAKE_CURRENT_BINARY_DIR}/crow_build)
endif()

find_package(Threads REQUIRED)

target_link_libraries(dbms_http_server
    PRIVATE
        dbms_storage_core
        Threads::Threads
)

if (TARGET Crow::Crow)
    target_link_libraries(dbms_http_server PRIVATE Crow::Crow)
elseif (TARGET crow)
    target_link_libraries(dbms_http_server PRIVATE crow)
endif()
