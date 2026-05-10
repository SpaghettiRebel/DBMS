#include "bplus_tree.h"
#include "wal_manager.h"
#include <cassert>
#include <cstring>

// ============================================================================
// Реализация методов персистентности B+ Tree
// ============================================================================

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::ensure_page_exists(uint32_t page_id) {
    if (!this->_pager) {
        throw std::runtime_error("Pager not initialized for persistent B+ tree");
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::serialize_node(
    std::vector<char>& out, const bptree_node_base* node) const {

    if (!node) {
        uint32_t null_marker = 0xFFFFFFFF;
        bptree_disk_detail::append_value(out, null_marker);
        return;
    }

    uint32_t node_type = node->_is_terminate ? 1u : 0u;
    bptree_disk_detail::append_value(out, node_type);

    if (node->_is_terminate) {
        const auto* term = static_cast<const bptree_node_term*>(node);
        uint64_t data_count = static_cast<uint64_t>(term->_data.size());
        bptree_disk_detail::append_value(out, data_count);

        for (const auto& pair : term->_data) {
            bptree_disk_detail::append_value(out, pair.first);
            bptree_disk_detail::append_value(out, pair.second);
        }

        uint32_t next_id = term->_next ? reinterpret_cast<uintptr_t>(term->_next) : 0u;
        bptree_disk_detail::append_value(out, next_id);

    } else {
        const auto* middle = static_cast<const bptree_node_middle*>(node);
        uint64_t keys_count = static_cast<uint64_t>(middle->_keys.size());
        bptree_disk_detail::append_value(out, keys_count);

        for (const auto& key : middle->_keys) {
            bptree_disk_detail::append_value(out, key);
        }

        for (const auto* child : middle->_pointers) {
            serialize_node(out, child);
        }
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_node_base*
BP_tree<tkey, tvalue, compare, t>::deserialize_node(const char*& cur, const char* end) {

    uint32_t node_type = bptree_disk_detail::read_value<uint32_t>(cur, end);

    if (node_type == 0xFFFFFFFF) {
        return nullptr;
    }

    if (node_type == 1) {
        auto* term = this->_allocator.template allocate_object<bptree_node_term>();
        new (term) bptree_node_term();

        uint64_t data_count = bptree_disk_detail::read_value<uint64_t>(cur, end);

        for (uint64_t i = 0; i < data_count; ++i) {
            tkey key = bptree_disk_detail::read_value<tkey>(cur, end);
            tvalue value = bptree_disk_detail::read_value<tvalue>(cur, end);
            term->_data.emplace_back(std::move(key), std::move(value));
        }

        uint32_t next_id = bptree_disk_detail::read_value<uint32_t>(cur, end);
        if (next_id != 0) {
            term->_next = reinterpret_cast<bptree_node_term*>(static_cast<uintptr_t>(next_id));
        }

        return term;

    } else {
        auto* middle = this->_allocator.template allocate_object<bptree_node_middle>();
        new (middle) bptree_node_middle();

        uint64_t keys_count = bptree_disk_detail::read_value<uint64_t>(cur, end);

        for (uint64_t i = 0; i < keys_count; ++i) {
            tkey key = bptree_disk_detail::read_value<tkey>(cur, end);
            middle->_keys.push_back(std::move(key));
        }

        for (std::size_t i = 0; i < keys_count + 1; ++i) {
            bptree_node_base* child = deserialize_node(cur, end);
            if (child) {
                middle->_pointers.push_back(child);
            }
        }

        return middle;
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::collect_leaves(
    bptree_node_base* node, std::vector<bptree_node_term*>& leaves) const noexcept {

    if (!node) return;

    if (node->_is_terminate) {
        leaves.push_back(static_cast<bptree_node_term*>(node));
    } else {
        auto* middle = static_cast<bptree_node_middle*>(node);
        for (auto* child : middle->_pointers) {
            collect_leaves(child, leaves);
        }
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::persist_to_disk() {
    if (!this->_pager || !this->_dirty) {
        return;
    }

    std::vector<bptree_node_term*> leaves;
    collect_leaves(this->_root, leaves);

    for (std::size_t i = 0; i < leaves.size(); ++i) {
        leaves[i]->_next = (i + 1 < leaves.size()) ? leaves[i + 1] : nullptr;
    }

    std::vector<char> buffer;

    disk_header header{disk_magic, disk_version, 0, static_cast<uint64_t>(this->_size)};
    bptree_disk_detail::append_value(buffer, header.magic);
    bptree_disk_detail::append_value(buffer, header.version);
    bptree_disk_detail::append_value(buffer, header.payload_size);
    bptree_disk_detail::append_value(buffer, header.item_count);

    serialize_node(buffer, this->_root);

    header.payload_size = static_cast<uint64_t>(buffer.size() - sizeof(disk_header));
    std::memcpy(buffer.data(), &header, sizeof(disk_header));

    const uint32_t pages_needed = static_cast<uint32_t>((buffer.size() + PAGE_SIZE - 1) / PAGE_SIZE);

    for (uint32_t i = 0; i < pages_needed; ++i) {
        uint32_t page_id = this->_root_page_id + i;
        if (page_id == 0) {
            page_id = this->_pager->allocate_page();
            this->_root_page_id = page_id;
        }

        const char* page_data = buffer.data() + i * PAGE_SIZE;
        std::vector<char> page_buffer(PAGE_SIZE, 0);
        std::size_t copy_size = std::min(PAGE_SIZE, buffer.size() - i * PAGE_SIZE);
        std::memcpy(page_buffer.data(), page_data, copy_size);

        this->_pager->write_page(page_id, page_buffer.data());
    }

    this->_dirty = false;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::load_from_disk() {
    if (!this->_pager || this->_root_page_id == 0) {
        return;
    }

    std::vector<char> buffer;
    uint32_t current_page = this->_root_page_id;

    while (true) {
        std::vector<char> page_buffer(PAGE_SIZE, 0);
        try {
            this->_pager->read_page(current_page, page_buffer.data());
        } catch (...) {
            break;
        }

        buffer.insert(buffer.end(), page_buffer.begin(), page_buffer.end());

        if (buffer.size() < sizeof(disk_header)) {
            break;
        }

        if (current_page == this->_root_page_id) {
            disk_header header;
            std::memcpy(&header, buffer.data(), sizeof(disk_header));

            if (header.magic != disk_magic || header.version != disk_version) {
                throw std::runtime_error("Invalid B+ tree file format");
            }

            this->_size = static_cast<std::size_t>(header.item_count);
        }

        bool has_more = false;
        for (std::size_t i = PAGE_SIZE - 8; i < PAGE_SIZE; ++i) {
            if (page_buffer[i] != 0) {
                has_more = true;
                break;
            }
        }

        if (!has_more && current_page > this->_root_page_id) {
            break;
        }

        ++current_page;
    }

    if (buffer.empty()) {
        return;
    }

    const char* cur = buffer.data() + sizeof(disk_header);
    const char* end = buffer.data() + buffer.size();

    this->_root = deserialize_node(cur, end);

    std::vector<bptree_node_term*> leaves;
    collect_leaves(this->_root, leaves);
    for (std::size_t i = 0; i < leaves.size(); ++i) {
        leaves[i]->_next = (i + 1 < leaves.size()) ? leaves[i + 1] : nullptr;
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::flush() {
    persist_to_disk();
    if (this->_pager) {
        this->_pager->file.flush();
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::BP_tree(
    Pager* pager, uint32_t root_page_id, const compare& cmp, pp_allocator<value_type> alloc)
    : _allocator(alloc), _root(nullptr), _size(0),
      _pager(pager), _root_page_id(root_page_id), _persistent(true), _dirty(false) {

    this->compare = cmp;

    if (_pager && _root_page_id != 0) {
        load_from_disk();
    } else if (_pager) {
        _root_page_id = _pager->allocate_page();
        _root = _allocator.template allocate_object<bptree_node_term>();
        new (_root) bptree_node_term();
        _size = 0;
        _dirty = true;
    }
}

// Явная инстанциация для распространенных типов
template class BP_tree<int, uint64_t, std::less<int>, 5>;
template class BP_tree<int, uint64_t, std::greater<int>, 5>;
template class BP_tree<std::string, uint64_t, std::less<std::string>, 5>;
template class BP_tree<std::string, uint64_t, std::greater<std::string>, 5>;
