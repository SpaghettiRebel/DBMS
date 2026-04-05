#include "../include/bplus_node.h"
#include "../include/file_manager.h"
#include <cstring>

class BPlusTree {
private:
    Pager& pager;
    uint32_t root_page_id;

    // Создание нового пустого узла
    uint32_t create_node(bool is_leaf) {
        uint32_t new_page = pager.allocate_page();
        char buffer[PAGE_SIZE] = {0};

        BPlusNodeHeader* header = reinterpret_cast<BPlusNodeHeader*>(buffer);

        header->is_leaf = is_leaf;
        header->num_keys = 0;
        header->parent = 0;
        header->next_leaf = 0;

        pager.write_page(new_page, buffer);
        
        return new_page;
    }

public:
    BPlusTree(Pager& p, uint32_t root_id) : pager(p), root_page_id(root_id) {}

    void insert(int32_t key, uint64_t record_offset) {
        // 1. Если дерево пустое, создаем корень-лист
        if (root_page_id == 0) {
            root_page_id = create_node(true);
        }

        // 2. Ищем подходящий лист для вставки
        uint32_t leaf_id = find_leaf(key);
        
        // 3. Вставляем в лист
        insert_into_leaf(leaf_id, key, record_offset);
    }

private:
    void insert_into_leaf(uint32_t leaf_id, int32_t key, uint64_t offset) {
        char buffer[PAGE_SIZE];
        pager.read_page(leaf_id, buffer);
        BPlusNodeHeader* header = reinterpret_cast<BPlusNodeHeader*>(buffer);

        if (header->num_keys < MAX_KEYS_LEAF) {
            // Обычная вставка со сдвигом элементов (как в массиве)
            // ... (реализуй простой insert в отсортированный массив внутри buffer)
            header->num_keys++;
            pager.write_page(leaf_id, buffer);
        } else {
            // МЕСТО ЗАКОНЧИЛОСЬ -> РАЗДЕЛЕНИЕ (SPLIT)
            split_leaf(leaf_id, buffer, key, offset);
        }
    }

    void split_leaf(uint32_t old_leaf_id, char* old_buffer, int32_t key, uint64_t offset) {
        uint32_t new_leaf_id = create_node(true);
        
        // 1. Распределяем ключи: половину оставляем в старом, половину в новом
        // 2. Обновляем связи: old_leaf->next_leaf = new_leaf_id
        // 3. ПОДНИМАЕМ МЕДИАННЫЙ КЛЮЧ В РОДИТЕЛЯ (insert_into_parent)
        
        // ВНИМАНИЕ: Если у листа нет родителя, создаем новый корень (внутренний узел)
    }
};