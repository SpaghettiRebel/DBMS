#include <iterator>
#include <utility>
#include <vector>
#include <boost/container/static_vector.hpp>
#include <concepts>
#include <stack>
#include "pp_allocator.h"
#include "associative_container.h"
#include "not_implemented.h"
#include "file_manager.h"
#include "table_metadata.h"
#include <initializer_list>
#include <cstring>

#ifndef SYS_PROG_B_PLUS_TREE_H
#define SYS_PROG_B_PLUS_TREE_H

// Persistent B+ Tree Node structure for disk storage
struct DiskBPlusNodeHeader {
    bool is_leaf;
    uint32_t num_keys;
    uint32_t parent;
    uint32_t next_leaf;
};

template <typename tkey, typename tvalue, typename compare = std::less<tkey>, std::size_t t = 10>
class BP_tree final : private compare {
public:
    using tree_data_type = std::pair<tkey, tvalue>;
    using tree_data_type_const = std::pair<const tkey, tvalue>;
    using value_type = tree_data_type_const;

private:
    static constexpr const size_t maximum_keys_in_node = 2 * t - 1;

    Pager* _pager;
    uint32_t _root_page_id;

    inline bool compare_keys(const tkey& lhs, const tkey& rhs) const {
        return compare::operator()(lhs, rhs);
    }

public:
    BP_tree(Pager* pager, uint32_t root_id) : _pager(pager), _root_page_id(root_id) {}

    uint32_t get_root_id() const { return _root_page_id; }

    tvalue find(const tkey& key) {
        if (_root_page_id == 0) return tvalue(); // Should be some invalid pos_t

        uint32_t curr_id = _root_page_id;
        char buffer[PAGE_SIZE];

        while (true) {
            _pager->read_page(curr_id, buffer);
            DiskBPlusNodeHeader* header = reinterpret_cast<DiskBPlusNodeHeader*>(buffer);

            if (header->is_leaf) {
                // Search in leaf
                tkey* keys = reinterpret_cast<tkey*>(buffer + sizeof(DiskBPlusNodeHeader));
                tvalue* values = reinterpret_cast<tvalue*>(buffer + sizeof(DiskBPlusNodeHeader) + maximum_keys_in_node * sizeof(tkey));
                for (uint32_t i = 0; i < header->num_keys; ++i) {
                    if (keys[i] == key) return values[i];
                }
                break;
            } else {
                // Search in internal
                tkey* keys = reinterpret_cast<tkey*>(buffer + sizeof(DiskBPlusNodeHeader));
                uint32_t* pointers = reinterpret_cast<uint32_t*>(buffer + sizeof(DiskBPlusNodeHeader) + maximum_keys_in_node * sizeof(tkey));

                uint32_t i = 0;
                while (i < header->num_keys && compare_keys(keys[i], key)) i++;
                curr_id = pointers[i];
            }
        }
        return tvalue();
    }

    void insert(const tree_data_type& data) {
        if (_root_page_id == 0) {
            _root_page_id = _pager->allocate_page();
            char buffer[PAGE_SIZE] = {0};
            DiskBPlusNodeHeader* header = reinterpret_cast<DiskBPlusNodeHeader*>(buffer);
            header->is_leaf = true;
            header->num_keys = 1;

            tkey* keys = reinterpret_cast<tkey*>(buffer + sizeof(DiskBPlusNodeHeader));
            tvalue* values = reinterpret_cast<tvalue*>(buffer + sizeof(DiskBPlusNodeHeader) + maximum_keys_in_node * sizeof(tkey));
            keys[0] = data.first;
            values[0] = data.second;

            _pager->write_page(_root_page_id, buffer);
            return;
        }
        // Full split-based insert would go here
    }

    bool contains(const tkey& key) {
        pos_t p = find(key);
        return p.is_valid();
    }
};

#endif
