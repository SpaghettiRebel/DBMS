cmake_minimum_required(VERSION 3.10)
project(DBMS_Project VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Флаги компиляции
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")
set(CMAKE_CXX_FLAGS_DEBUG "-g -DDEBUG")

# Исходники Storage Engine
set(STORAGE_SOURCES
    server/storage/src/engine.cpp
    server/storage/src/file_manager.cpp
    server/storage/src/binary_file_manager.cpp
    server/storage/src/bplus_tree_persistence.cpp
    server/storage/src/wal_manager.cpp
    server/storage/src/journal.cpp
    server/storage/src/string_pool.cpp
    server/storage/src/query_optimizer.cpp
    server/storage/src/schema_manager.cpp
    server/storage/src/access_logger.cpp
    server/storage/src/storage_server.cpp
    server/storage/src/main_storage_server.cpp
)

set(STORAGE_HEADERS
    server/storage/include/engine.h
    server/storage/include/file_manager.h
    server/storage/include/binary_file_manager.h
    server/storage/include/bplus_node.h
    server/storage/include/bplus_tree.h
    server/storage/include/wal_manager.h
    server/storage/include/journal.h
    server/storage/include/string_pool.h
    server/storage/include/table_metadata.h
    server/storage/include/serializer.h
    server/storage/include/query_optimizer.h
    server/storage/include/schema_manager.h
    server/storage/include/access_logger.h
    server/storage/include/storage_server.h
    shared/QueryPlan.h
)

# Storage Server executable
add_executable(storage_server ${STORAGE_SOURCES} ${STORAGE_HEADERS})
target_include_directories(storage_server PRIVATE 
    ${PROJECT_SOURCE_DIR}/server/storage/include
    ${PROJECT_SOURCE_DIR}/shared
)
target_link_libraries(storage_server pthread)

# Клиентское приложение (заглушка)
add_executable(dbms_client client/main.cpp)
target_include_directories(dbms_client PRIVATE ${PROJECT_SOURCE_DIR}/shared)

# Тесты
enable_testing()
add_executable(test_binary_file_manager server/storage/tests/test_binary_file_manager.cpp)
target_include_directories(test_binary_file_manager PRIVATE 
    ${PROJECT_SOURCE_DIR}/server/storage/include
)
target_link_libraries(test_binary_file_manager pthread)

add_test(NAME BinaryFileManagerTest COMMAND test_binary_file_manager)
# Cluster Entrypoint (Sharding)
set(CLUSTER_SOURCES
    server/cluster/src/cluster_entrypoint.cpp
    server/cluster/src/storage_node.cpp
    server/cluster/src/shard_manager.cpp
    server/cluster/src/heartbeat_monitor.cpp
    server/cluster/src/async_request_queue.cpp
    server/cluster/src/telemetry_collector.cpp
    server/cluster/src/main_cluster.cpp
)

set(CLUSTER_HEADERS
    server/cluster/include/cluster_entrypoint.h
    server/cluster/include/storage_node.h
    server/cluster/include/shard_manager.h
    server/cluster/include/heartbeat_monitor.h
    server/cluster/include/async_request_queue.h
    server/cluster/include/telemetry_collector.h
)

add_executable(cluster_entrypoint ${CLUSTER_SOURCES} ${CLUSTER_HEADERS})
target_include_directories(cluster_entrypoint PRIVATE
    ${PROJECT_SOURCE_DIR}/server/cluster/include
    ${PROJECT_SOURCE_DIR}/shared
)
target_link_libraries(cluster_entrypoint pthread)
