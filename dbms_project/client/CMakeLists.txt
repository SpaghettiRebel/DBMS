cmake_minimum_required(VERSION 3.20)
project(dbms_client)

# 1. Сначала создаем исполняемый файл
add_executable(dbms_client main.cpp)

# 2. Теперь указываем, где искать заголовочные файлы
target_include_directories(dbms_client PRIVATE
        ${CMAKE_SOURCE_DIR}/shared
        ${CMAKE_SOURCE_DIR}/third_party
)

# 3. И в конце линкуем библиотеки для Windows
if(WIN32)
    target_compile_definitions(dbms_client PRIVATE
            WINVER=0x0A00
            _WIN32_WINNT=0x0A00
    )
    target_link_libraries(dbms_client PRIVATE ws2_32 mswsock)
endif()
