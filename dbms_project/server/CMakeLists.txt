cmake_minimum_required(VERSION 3.16)

project(dbms_storage)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# filesystem for older gcc
set(CMAKE_CXX_EXTENSIONS OFF)

file(GLOB STORAGE_HEADERS
    include/*.h
)

file(GLOB STORAGE_SOURCES
    src/*.cpp
)

add_library(dbms_storage STATIC
    ${STORAGE_HEADERS}
    ${STORAGE_SOURCES}
)

target_include_directories(dbms_storage
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Windows
if (WIN32)
    target_compile_definitions(dbms_storage PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

# GCC < 9 may require stdc++fs
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_libraries(dbms_storage stdc++fs)
endif()