#include "binary_file_manager.h"
#include <iostream>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;

void test_create_and_open() {
    std::cout << "Test 1: Create and open file..." << std::endl;
    
    const std::string test_file = "/tmp/test_dbms_1.tbl";
    
    // Удаляем файл если существует
    if (fs::exists(test_file)) {
        fs::remove(test_file);
    }
    
    // Создаем новый файл
    {
        BinaryFileManager bfm(test_file);
        assert(bfm.getPageCount() == 1);  // Только заголовок файла
        assert(bfm.pageExists(0));
        assert(!bfm.pageExists(1));
        
        auto header = bfm.readFileHeader();
        assert(header.page_header.type == PageType::METADATA);
        assert(header.page_header.validate());
        assert(header.total_pages == 1);
    }
    
    // Открываем существующий файл
    {
        BinaryFileManager bfm(test_file);
        assert(bfm.getPageCount() == 1);
        auto header = bfm.readFileHeader();
        assert(header.page_header.type == PageType::METADATA);
    }
    
    fs::remove(test_file);
    std::cout << "✓ Test 1 passed" << std::endl;
}

void test_allocate_pages() {
    std::cout << "Test 2: Allocate pages..." << std::endl;
    
    const std::string test_file = "/tmp/test_dbms_2.tbl";
    if (fs::exists(test_file)) fs::remove(test_file);
    
    {
        BinaryFileManager bfm(test_file);
        
        // Выделяем несколько страниц данных
        uint32_t page1 = bfm.allocatePage(PageType::DATA);
        uint32_t page2 = bfm.allocatePage(PageType::DATA);
        uint32_t page3 = bfm.allocatePage(PageType::INDEX);
        
        assert(page1 == 1);
        assert(page2 == 2);
        assert(page3 == 3);
        assert(bfm.getPageCount() == 4);  // 1 заголовок + 3 страницы
        
        // Проверяем типы страниц
        assert(bfm.getPageType(page1) == PageType::DATA);
        assert(bfm.getPageType(page2) == PageType::DATA);
        assert(bfm.getPageType(page3) == PageType::INDEX);
    }
    
    fs::remove(test_file);
    std::cout << "✓ Test 2 passed" << std::endl;
}

void test_read_write_page() {
    std::cout << "Test 3: Read/Write page data..." << std::endl;
    
    const std::string test_file = "/tmp/test_dbms_3.tbl";
    if (fs::exists(test_file)) fs::remove(test_file);
    
    {
        BinaryFileManager bfm(test_file);
        
        // Выделяем страницу
        uint32_t page_id = bfm.allocatePage(PageType::DATA);
        
        // Записываем данные
        const char* test_data = "Hello, DBMS!";
        size_t data_size = strlen(test_data) + 1;
        bfm.writePage(page_id, test_data, PageType::DATA, static_cast<uint32_t>(data_size));
        
        // Читаем данные обратно
        std::vector<char> buffer(PAGE_SIZE, 0);
        bfm.readPage(page_id, buffer.data());
        
        // Данные находятся после заголовка страницы
        const char* read_data = buffer.data() + PageHeader::SIZE;
        assert(strcmp(read_data, test_data) == 0);
        
        // Проверяем заголовок страницы
        PageHeader header = bfm.readPageHeader(page_id);
        assert(header.page_id == page_id);
        assert(header.type == PageType::DATA);
        assert(header.data_size == data_size);
        assert(header.validate());
    }
    
    fs::remove(test_file);
    std::cout << "✓ Test 3 passed" << std::endl;
}

void test_free_page() {
    std::cout << "Test 4: Free page..." << std::endl;
    
    const std::string test_file = "/tmp/test_dbms_4.tbl";
    if (fs::exists(test_file)) fs::remove(test_file);
    
    {
        BinaryFileManager bfm(test_file);
        
        uint32_t page1 = bfm.allocatePage(PageType::DATA);
        uint32_t page2 = bfm.allocatePage(PageType::DATA);
        
        assert(page1 == 1);
        assert(page2 == 2);
        
        // Освобождаем страницу
        bfm.freePage(page1);
        
        // Проверяем тип
        assert(bfm.getPageType(page1) == PageType::FREE);
        
        // Страница должна быть в списке свободных
        auto file_header = bfm.readFileHeader();
        assert(file_header.first_free_page == page1);
    }
    
    fs::remove(test_file);
    std::cout << "✓ Test 4 passed" << std::endl;
}

void test_checksum_validation() {
    std::cout << "Test 5: Checksum validation..." << std::endl;
    
    const std::string test_file = "/tmp/test_dbms_5.tbl";
    if (fs::exists(test_file)) fs::remove(test_file);
    
    {
        BinaryFileManager bfm(test_file);
        
        uint32_t page_id = bfm.allocatePage(PageType::DATA);
        
        const char* test_data = "Checksum test data";
        bfm.writePage(page_id, test_data, PageType::DATA, strlen(test_data) + 1);
        
        // Страница должна проходить валидацию
        assert(bfm.validatePage(page_id));
        
        // Проверяем, что контрольная сумма не нулевая
        PageHeader header = bfm.readPageHeader(page_id);
        assert(header.checksum != 0);
    }
    
    fs::remove(test_file);
    std::cout << "✓ Test 5 passed" << std::endl;
}

void test_timestamp() {
    std::cout << "Test 6: Timestamp tracking..." << std::endl;
    
    const std::string test_file = "/tmp/test_dbms_6.tbl";
    if (fs::exists(test_file)) fs::remove(test_file);
    
    {
        BinaryFileManager bfm(test_file);
        
        uint64_t ts1 = bfm.readFileHeader().page_header.timestamp;
        
        // Небольшая задержка
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        uint32_t page_id = bfm.allocatePage(PageType::DATA);
        const char* data = "test";
        bfm.writePage(page_id, data, PageType::DATA, 5);
        
        uint64_t ts2 = bfm.readPageHeader(page_id).timestamp;
        
        // Время должно увеличиться
        assert(ts2 >= ts1);
    }
    
    fs::remove(test_file);
    std::cout << "✓ Test 6 passed" << std::endl;
}

int main() {
    std::cout << "=== Binary File Manager Tests ===" << std::endl << std::endl;
    
    try {
        test_create_and_open();
        test_allocate_pages();
        test_read_write_page();
        test_free_page();
        test_checksum_validation();
        test_timestamp();
        
        std::cout << std::endl << "=== All tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
