#pragma once
#include "bplus_node.h"
#include "file_manager.h"
#include <string>
#include <variant>

class BPlusTree {
private:
    Pager& pager;
    uint32_t root_page_id;
    bool is_int_key;

    uint32_t create_node(bool is_leaf);
    uint32_t find_leaf(int32_t key);
    void insert_into_leaf(uint32_t leaf_id, int32_t key, uint64_t offset);
    void split_leaf(uint32_t old_leaf_id, char* old_buffer, int32_t key, uint64_t offset);
    // ... другие вспомогательные методы

public:
    BPlusTree(Pager& p, uint32_t root_id, bool is_int);

    void insert(int32_t key, uint64_t record_offset);
    uint64_t search(int32_t key);

    // Для простоты пока реализуем только для INT,
    // для STRING можно использовать хеш или ID из StringPool
};
