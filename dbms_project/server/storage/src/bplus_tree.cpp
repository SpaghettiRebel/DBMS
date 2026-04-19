#include "../include/bplus_tree.h"
#include <cstring>
#include <algorithm>

BPlusTree::BPlusTree(Pager& p, uint32_t root_id, bool is_int)
    : pager(p), root_page_id(root_id), is_int_key(is_int) {}

uint32_t BPlusTree::create_node(bool is_leaf) {
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

uint32_t BPlusTree::find_leaf(int32_t key) {
    if (root_page_id == 0) return 0;

    uint32_t current_id = root_page_id;
    char buffer[PAGE_SIZE];

    while (true) {
        pager.read_page(current_id, buffer);
        BPlusNodeHeader* header = reinterpret_cast<BPlusNodeHeader*>(buffer);
        if (header->is_leaf) return current_id;

        // Поиск во внутреннем узле (упрощенно)
        // Внутренний узел: [K1, P1, K2, P2, ..., Kn, Pn, P_last]
        // Для простоты: найдем первого Ki > key, тогда переходим в Pi-1
        // (Это заготовка, полная реализация требует аккуратности с типами)
        current_id = 0; // Временно
        break;
    }
    return current_id;
}

void BPlusTree::insert(int32_t key, uint64_t record_offset) {
    if (root_page_id == 0) {
        root_page_id = create_node(true);
        // Нужно также обновить заголовок таблицы, чтобы сохранить root_page_id
    }

    uint32_t leaf_id = find_leaf(key);
    insert_into_leaf(leaf_id, key, record_offset);
}

void BPlusTree::insert_into_leaf(uint32_t leaf_id, int32_t key, uint64_t offset) {
    char buffer[PAGE_SIZE];
    pager.read_page(leaf_id, buffer);
    BPlusNodeHeader* header = reinterpret_cast<BPlusNodeHeader*>(buffer);

    if (header->num_keys < MAX_KEYS_LEAF) {
        // Простая вставка в отсортированный массив
        int32_t* keys = reinterpret_cast<int32_t*>(buffer + sizeof(BPlusNodeHeader));
        uint64_t* offsets = reinterpret_cast<uint64_t*>(buffer + sizeof(BPlusNodeHeader) + MAX_KEYS_LEAF * sizeof(int32_t));
        
        uint32_t pos = 0;
        while (pos < header->num_keys && keys[pos] < key) pos++;

        for (uint32_t i = header->num_keys; i > pos; i--) {
            keys[i] = keys[i-1];
            offsets[i] = offsets[i-1];
        }

        keys[pos] = key;
        offsets[pos] = offset;
        header->num_keys++;
        pager.write_page(leaf_id, buffer);
    } else {
        // split_leaf(leaf_id, buffer, key, offset);
    }
}

uint64_t BPlusTree::search(int32_t key) {
    uint32_t leaf_id = find_leaf(key);
    if (leaf_id == 0) return -1;

    char buffer[PAGE_SIZE];
    pager.read_page(leaf_id, buffer);
    BPlusNodeHeader* header = reinterpret_cast<BPlusNodeHeader*>(buffer);

    int32_t* keys = reinterpret_cast<int32_t*>(buffer + sizeof(BPlusNodeHeader));
    uint64_t* offsets = reinterpret_cast<uint64_t*>(buffer + sizeof(BPlusNodeHeader) + MAX_KEYS_LEAF * sizeof(int32_t));

    for (uint32_t i = 0; i < header->num_keys; i++) {
        if (keys[i] == key) return offsets[i];
    }
    return -1;
}
