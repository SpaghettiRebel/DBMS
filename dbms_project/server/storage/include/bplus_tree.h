#pragma once
#include "file_manager.h"
#include "bplus_node.h"
#include <optional>
#include <vector>
#include <cstring>
#include <algorithm>

/**
 * Persistent B+ Tree implementation.
 * Nodes are stored in 4KB pages via Pager.
 */
template <typename tkey, typename tvalue>
class BP_tree {
public:
    struct Entry {
        tkey key;
        tvalue value;
    };

private:
    Pager* pager;
    uint32_t root_id;

    // Use constants from bplus_node.h
    static constexpr uint32_t MAX_K_INTERNAL = MAX_KEYS_INTERNAL;
    static constexpr uint32_t MAX_K_LEAF = MAX_KEYS_LEAF;

    struct Node {
        BPlusNodeHeader header;
        union {
            struct {
                tkey keys[MAX_K_INTERNAL];
                uint32_t children[MAX_K_INTERNAL + 1];
            } internal;
            struct {
                tkey keys[MAX_K_LEAF];
                tvalue values[MAX_K_LEAF];
            } leaf;
        } body;

        Node() { std::memset(this, 0, sizeof(Node)); }
    };

    static_assert(sizeof(Node) <= PAGE_SIZE, "B+ Tree Node exceeds Page Size");

    void load_node(uint32_t page_id, Node& n) {
        pager->read_page(page_id, reinterpret_cast<char*>(&n));
    }

    void save_node(uint32_t page_id, const Node& n) {
        pager->write_page(page_id, reinterpret_cast<const char*>(&n));
    }

    uint32_t allocate_node(bool is_leaf) {
        uint32_t pid = pager->allocate_page();
        Node n;
        n.header.is_leaf = is_leaf;
        n.header.num_keys = 0;
        n.header.parent = 0;
        n.header.next_leaf = 0;
        save_node(pid, n);
        return pid;
    }

public:
    BP_tree(Pager* p, uint32_t r_id) : pager(p), root_id(r_id) {
        if (root_id == 0) {
            root_id = allocate_node(true);
        }
    }

    uint32_t get_root_id() const { return root_id; }

    std::optional<tvalue> find(tkey key) {
        uint32_t curr_id = root_id;
        while (true) {
            Node n;
            load_node(curr_id, n);
            if (n.header.is_leaf) {
                for (uint32_t i = 0; i < n.header.num_keys; ++i) {
                    if (n.body.leaf.keys[i] == key) return n.body.leaf.values[i];
                }
                return std::nullopt;
            } else {
                uint32_t i = 0;
                while (i < n.header.num_keys && key >= n.body.internal.keys[i]) {
                    i++;
                }
                curr_id = n.body.internal.children[i];
            }
        }
    }

    void insert(const std::pair<tkey, tvalue>& data) {
        insert_recursive(root_id, data.first, data.second);
    }

    void erase(tkey key) {
        erase_recursive(root_id, key);
    }

private:
    void insert_recursive(uint32_t node_id, tkey key, tvalue value) {
        Node n;
        load_node(node_id, n);
        if (n.header.is_leaf) {
            if (n.header.num_keys < MAX_K_LEAF) {
                uint32_t i = n.header.num_keys;
                while (i > 0 && n.body.leaf.keys[i-1] > key) {
                    n.body.leaf.keys[i] = n.body.leaf.keys[i-1];
                    n.body.leaf.values[i] = n.body.leaf.values[i-1];
                    i--;
                }
                n.body.leaf.keys[i] = key;
                n.body.leaf.values[i] = value;
                n.header.num_keys++;
                save_node(node_id, n);
            } else {
                split_leaf(node_id, n, key, value);
            }
        } else {
            uint32_t i = 0;
            while (i < n.header.num_keys && key >= n.body.internal.keys[i]) {
                i++;
            }
            insert_recursive(n.body.internal.children[i], key, value);
        }
    }

    void split_leaf(uint32_t node_id, Node& n, tkey key, tvalue value) {
        uint32_t new_id = allocate_node(true);
        Node newNode;
        load_node(new_id, newNode);

        std::vector<std::pair<tkey, tvalue>> temp;
        for(uint32_t i=0; i<n.header.num_keys; ++i) temp.push_back({n.body.leaf.keys[i], n.body.leaf.values[i]});
        temp.push_back({key, value});
        std::sort(temp.begin(), temp.end(), [](auto& a, auto& b){ return a.first < b.first; });

        uint32_t mid = temp.size() / 2;
        n.header.num_keys = mid;
        for(uint32_t i=0; i<mid; ++i) {
            n.body.leaf.keys[i] = temp[i].first;
            n.body.leaf.values[i] = temp[i].second;
        }

        newNode.header.num_keys = temp.size() - mid;
        for(uint32_t i=mid; i<temp.size(); ++i) {
            newNode.body.leaf.keys[i-mid] = temp[i].first;
            newNode.body.leaf.values[i-mid] = temp[i].second;
        }

        newNode.header.next_leaf = n.header.next_leaf;
        n.header.next_leaf = new_id;

        save_node(node_id, n);
        save_node(new_id, newNode);

        update_parent(node_id, newNode.body.leaf.keys[0], new_id);
    }

    void update_parent(uint32_t old_id, tkey new_key, uint32_t new_id) {
        Node child;
        load_node(old_id, child);
        if (old_id == root_id) {
            uint32_t new_root = allocate_node(false);
            Node nr;
            load_node(new_root, nr);
            nr.header.num_keys = 1;
            nr.body.internal.keys[0] = new_key;
            nr.body.internal.children[0] = old_id;
            nr.body.internal.children[1] = new_id;
            save_node(new_root, nr);
            root_id = new_root;

            child.header.parent = root_id;
            save_node(old_id, child);

            Node n_new;
            load_node(new_id, n_new);
            n_new.header.parent = root_id;
            save_node(new_id, n_new);
        } else {
            uint32_t parent_id = child.header.parent;
            Node p;
            load_node(parent_id, p);

            if (p.header.num_keys < MAX_K_INTERNAL) {
                uint32_t i = p.header.num_keys;
                while (i > 0 && p.body.internal.keys[i-1] > new_key) {
                    p.body.internal.keys[i] = p.body.internal.keys[i-1];
                    p.body.internal.children[i+1] = p.body.internal.children[i];
                    i--;
                }
                p.body.internal.keys[i] = new_key;
                p.body.internal.children[i+1] = new_id;
                p.header.num_keys++;
                save_node(parent_id, p);

                Node n_new;
                load_node(new_id, n_new);
                n_new.header.parent = parent_id;
                save_node(new_id, n_new);
            } else {
                split_internal(parent_id, p, new_key, new_id);
            }
        }
    }

    void split_internal(uint32_t node_id, Node& n, tkey key, uint32_t child_id) {
        uint32_t next_id = allocate_node(false);
        Node next;
        load_node(next_id, next);

        struct IntEntry { tkey k; uint32_t c; };
        std::vector<IntEntry> temp;
        for(uint32_t i=0; i<n.header.num_keys; ++i) temp.push_back({n.body.internal.keys[i], n.body.internal.children[i+1]});
        temp.push_back({key, child_id});
        std::sort(temp.begin(), temp.end(), [](auto& a, auto& b){ return a.k < b.k; });

        uint32_t mid = temp.size() / 2;
        tkey push_up = temp[mid].k;

        n.header.num_keys = mid;
        for(uint32_t i=0; i<mid; ++i) {
            n.body.internal.keys[i] = temp[i].k;
            n.body.internal.children[i+1] = temp[i].c;
        }

        next.header.num_keys = temp.size() - mid - 1;
        next.body.internal.children[0] = temp[mid].c;
        for(uint32_t i=mid+1; i<temp.size(); ++i) {
            next.body.internal.keys[i-mid-1] = temp[i].k;
            next.body.internal.children[i-mid] = temp[i].c;
        }

        save_node(node_id, n);
        save_node(next_id, next);

        for(uint32_t i=0; i<=next.header.num_keys; ++i) {
            Node c;
            load_node(next.body.internal.children[i], c);
            c.header.parent = next_id;
            save_node(next.body.internal.children[i], c);
        }

        update_parent(node_id, push_up, next_id);
    }

    void erase_recursive(uint32_t node_id, tkey key) {
        Node n;
        load_node(node_id, n);
        if (n.header.is_leaf) {
            bool found = false;
            for (uint32_t i = 0; i < n.header.num_keys; ++i) {
                if (n.body.leaf.keys[i] == key) {
                    for (uint32_t j = i; j < n.header.num_keys - 1; ++j) {
                        n.body.leaf.keys[j] = n.body.leaf.keys[j+1];
                        n.body.leaf.values[j] = n.body.leaf.values[j+1];
                    }
                    n.header.num_keys--;
                    found = true;
                    break;
                }
            }
            if (found) save_node(node_id, n);
        } else {
            uint32_t i = 0;
            while (i < n.header.num_keys && key >= n.body.internal.keys[i]) {
                i++;
            }
            erase_recursive(n.body.internal.children[i], key);
        }
    }
};
