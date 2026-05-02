#include <associative_container.h>
#include <not_implemented.h>
#include <pp_allocator.h>

#include <algorithm>
#include <boost/container/static_vector.hpp>
#include <concepts>
#include <initializer_list>
#include <iterator>
#include <stack>
#include <utility>
#include <vector>

#ifndef SYS_PROG_B_PLUS_TREE_H
#define SYS_PROG_B_PLUS_TREE_H

template <typename tkey, typename tvalue, comparator<tkey> compare = std::less<tkey>, std::size_t t = 5>
class BP_tree final : private compare  // EBCO
{
public:
    using tree_data_type = std::pair<tkey, tvalue>;
    using tree_data_type_const = std::pair<const tkey, tvalue>;
    using value_type = tree_data_type_const;

private:
    static constexpr const size_t minimum_keys_in_node = t - 1;
    static constexpr const size_t maximum_keys_in_node = 2 * t - 1;

    // region comparators declaration

    inline bool compare_keys(const tkey& lhs, const tkey& rhs) const;
    inline bool compare_pairs(const tree_data_type& lhs, const tree_data_type& rhs) const;

    // endregion comparators declaration

    struct bptree_node_base {
        bool _is_terminate;

        bptree_node_base() noexcept;
        virtual ~bptree_node_base() = default;
    };

    struct bptree_node_term : public bptree_node_base {
        bptree_node_term* _next;

        boost::container::static_vector<tree_data_type, maximum_keys_in_node + 1> _data;
        bptree_node_term() noexcept;
    };

    struct bptree_node_middle : public bptree_node_base {
        boost::container::static_vector<tkey, maximum_keys_in_node + 1> _keys;
        boost::container::static_vector<bptree_node_base*, maximum_keys_in_node + 2> _pointers;
        bptree_node_middle() noexcept;
    };

    pp_allocator<value_type> _allocator;
    bptree_node_base* _root;
    size_t _size;

    pp_allocator<value_type> get_allocator() const noexcept;

public:
    // region constructors declaration

    explicit BP_tree(const compare& cmp = compare(), pp_allocator<value_type> = pp_allocator<value_type>());

    explicit BP_tree(pp_allocator<value_type> alloc, const compare& comp = compare());

    template <input_iterator_for_pair<tkey, tvalue> iterator>
    explicit BP_tree(iterator begin, iterator end, const compare& cmp = compare(),
        pp_allocator<value_type> = pp_allocator<value_type>());

    BP_tree(std::initializer_list<std::pair<tkey, tvalue>> data, const compare& cmp = compare(),
        pp_allocator<value_type> = pp_allocator<value_type>());

    // endregion constructors declaration

    // region five declaration

    BP_tree(const BP_tree& other);

    BP_tree(BP_tree&& other) noexcept;

    BP_tree& operator=(const BP_tree& other);

    BP_tree& operator=(BP_tree&& other) noexcept;

    ~BP_tree() noexcept;

    // endregion five declaration

private:
    void split_node(bptree_node_middle* parent, size_t index);
    void split_root();
    void clear(bptree_node_base* node);

    bptree_node_base* erase(bptree_node_base* node, const tkey& key, bool& erased);
    void balance_after_erase(bptree_node_middle* parent, size_t index);

public:
    // region iterators declaration

    class bptree_iterator;
    class bptree_const_iterator;

    class bptree_iterator final {
        bptree_node_term* _node;
        size_t _index;

    public:
        using value_type = tree_data_type_const;
        using reference = value_type&;
        using pointer = value_type*;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = bptree_iterator;

        friend class BP_tree;
        friend class bptree_const_iterator;

        reference operator*() const noexcept;
        pointer operator->() const noexcept;

        self& operator++();
        self operator++(int);

        bool operator==(const self& other) const noexcept;
        bool operator!=(const self& other) const noexcept;

        size_t current_node_keys_count() const noexcept;
        size_t index() const noexcept;

        explicit bptree_iterator(bptree_node_term* node = nullptr, size_t index = 0);
    };

    class bptree_const_iterator final {
        const bptree_node_term* _node;
        size_t _index;

    public:
        using value_type = tree_data_type_const;
        using reference = const value_type&;
        using pointer = const value_type*;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = bptree_const_iterator;

        friend class BP_tree;
        friend class bptree_iterator;

        bptree_const_iterator(const bptree_iterator& it) noexcept;

        reference operator*() const noexcept;
        pointer operator->() const noexcept;

        self& operator++();
        self operator++(int);

        bool operator==(const self& other) const noexcept;
        bool operator!=(const self& other) const noexcept;

        size_t current_node_keys_count() const noexcept;
        size_t index() const noexcept;

        explicit bptree_const_iterator(const bptree_node_term* node = nullptr, size_t index = 0);
    };

    friend class bptree_iterator;
    friend class bptree_const_iterator;

    // endregion iterators declaration

    // region element access declaration

    /*
     * Returns a reference to the mapped value of the element with specified key. If no such element exists, an
     * exception of type std::out_of_range is thrown.
     */
    tvalue& at(const tkey&);
    const tvalue& at(const tkey&) const;

    /*
     * If key not exists, makes default initialization of value
     */
    tvalue& operator[](const tkey& key);
    tvalue& operator[](tkey&& key);

    // endregion element access declaration
    // region iterator begins declaration

    bptree_iterator begin();
    bptree_iterator end();

    bptree_const_iterator begin() const;
    bptree_const_iterator end() const;

    bptree_const_iterator cbegin() const;
    bptree_const_iterator cend() const;

    // endregion iterator begins declaration

    // region lookup declaration

    size_t size() const noexcept;
    bool empty() const noexcept;

    /*
     * Returns end() if not exist
     */

    bptree_iterator find(const tkey& key);
    bptree_const_iterator find(const tkey& key) const;

    bptree_iterator lower_bound(const tkey& key);
    bptree_const_iterator lower_bound(const tkey& key) const;

    bptree_iterator upper_bound(const tkey& key);
    bptree_const_iterator upper_bound(const tkey& key) const;

    bool contains(const tkey& key) const;

    // endregion lookup declaration

    // region modifiers declaration

    void clear() noexcept;

    /*
     * Does nothing if key exists, delegates to emplace.
     * Second return value is true, when inserted
     */
    std::pair<bptree_iterator, bool> insert(const tree_data_type& data);
    std::pair<bptree_iterator, bool> insert(tree_data_type&& data);

    template <typename... Args>
    std::pair<bptree_iterator, bool> emplace(Args&&... args);

    /*
     * Updates value if key exists, delegates to emplace.
     */
    bptree_iterator insert_or_assign(const tree_data_type& data);
    bptree_iterator insert_or_assign(tree_data_type&& data);

    template <typename... Args>
    bptree_iterator emplace_or_assign(Args&&... args);

    /*
     * Return iterator to node next ro removed or end() if key not exists
     */
    bptree_iterator erase(bptree_iterator pos);
    bptree_iterator erase(bptree_const_iterator pos);

    bptree_iterator erase(bptree_iterator beg, bptree_iterator en);
    bptree_iterator erase(bptree_const_iterator beg, bptree_const_iterator en);

    bptree_iterator erase(const tkey& key);

    // endregion modifiers declaration
};

template <std::input_iterator iterator,
    comparator<typename std::iterator_traits<iterator>::value_type::first_type> compare =
        std::less<typename std::iterator_traits<iterator>::value_type::first_type>,
    std::size_t t = 5, typename U>
BP_tree(iterator begin, iterator end, const compare& cmp = compare(), pp_allocator<U> = pp_allocator<U>())
    -> BP_tree<typename std::iterator_traits<iterator>::value_type::first_type,
        typename std::iterator_traits<iterator>::value_type::second_type, compare, t>;

template <typename tkey, typename tvalue, comparator<tkey> compare = std::less<tkey>, std::size_t t = 5, typename U>
BP_tree(std::initializer_list<std::pair<tkey, tvalue>> data, const compare& cmp = compare(),
    pp_allocator<U> = pp_allocator<U>()) -> BP_tree<tkey, tvalue, compare, t>;

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::compare_pairs(
    const BP_tree::tree_data_type& lhs, const BP_tree::tree_data_type& rhs) const {
    return compare_keys(lhs.first, rhs.first);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_node_base::bptree_node_base() noexcept : _is_terminate(false) {}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_node_term::bptree_node_term() noexcept {
    this->_is_terminate = true;
    _next = nullptr;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_node_middle::bptree_node_middle() noexcept {
    this->_is_terminate = false;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
pp_allocator<typename BP_tree<tkey, tvalue, compare, t>::value_type> BP_tree<tkey, tvalue, compare, t>::get_allocator()
    const noexcept {
    return _allocator;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator::reference
BP_tree<tkey, tvalue, compare, t>::bptree_iterator::operator*() const noexcept {
    return reinterpret_cast<reference>(_node->_data[_index]);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator::pointer
BP_tree<tkey, tvalue, compare, t>::bptree_iterator ::operator->() const noexcept {
    return reinterpret_cast<pointer>(&_node->_data[_index]);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator::self&
BP_tree<tkey, tvalue, compare, t>::bptree_iterator::operator++() {
    _index++;
    if (_index >= _node->_data.size()) {
        _node = _node->_next;
        _index = 0;
    }
    return *this;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator::self
BP_tree<tkey, tvalue, compare, t>::bptree_iterator::operator++(int) {
    self tmp = *this;
    ++(*this);
    return tmp;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::bptree_iterator::operator==(const self& other) const noexcept {
    return _node == other._node && _index == other._index;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::bptree_iterator::operator!=(const self& other) const noexcept {
    return !(*this == other);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
size_t BP_tree<tkey, tvalue, compare, t>::bptree_iterator::current_node_keys_count() const noexcept {
    return _node ? _node->_data.size() : 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
size_t BP_tree<tkey, tvalue, compare, t>::bptree_iterator::index() const noexcept {
    return (_node != nullptr) ? _index : 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_iterator::bptree_iterator(bptree_node_term* node, size_t index)
    : _node(node), _index(index) {}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::bptree_const_iterator(const bptree_iterator& it) noexcept
    : _node(it._node), _index(it._index) {}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::reference
BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::operator*() const noexcept {
    return reinterpret_cast<reference>(_node->_data[_index]);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::pointer
BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::operator->() const noexcept {
    return reinterpret_cast<pointer>(&_node->_data[_index]);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::self&
BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::operator++() {
    if (_node == nullptr) return *this;

    _index++;

    if (_index >= _node->_data.size()) {
        _node = _node->_next;
        _index = 0;
    }

    return *this;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::self
BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::operator++(int) {
    bptree_const_iterator tmp = *this;
    ++(*this);
    return tmp;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::operator==(const self& other) const noexcept {
    return _node == other._node && _index == other._index;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::operator!=(const self& other) const noexcept {
    return !(*this == other);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
size_t BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::current_node_keys_count() const noexcept {
    return _node ? _node->_data.size() : 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
size_t BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::index() const noexcept {
    return (_node != nullptr) ? _index : 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator::bptree_const_iterator(
    const bptree_node_term* node, size_t index)
    : _node(node), _index(index) {}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
tvalue& BP_tree<tkey, tvalue, compare, t>::at(const tkey& key) {
    auto it = find(key);
    if (it == end()) throw std::out_of_range("Key not found");
    return it->second;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
const tvalue& BP_tree<tkey, tvalue, compare, t>::at(const tkey& key) const {
    auto it = find(key);
    if (it == end()) throw std::out_of_range("Key not found");
    return it->second;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
tvalue& BP_tree<tkey, tvalue, compare, t>::operator[](const tkey& key) {
    auto res = emplace(key, tvalue());
    return res.first->second;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
tvalue& BP_tree<tkey, tvalue, compare, t>::operator[](tkey&& key) {
    auto res = emplace(std::move(key), tvalue());
    return res.first->second;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
std::pair<typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator, bool> BP_tree<tkey, tvalue, compare, t>::insert(
    const tree_data_type& data) {
    return emplace(data.first, data.second);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::compare_keys(const tkey& lhs, const tkey& rhs) const {
    return compare::operator()(lhs, rhs);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::BP_tree(const compare& cmp, pp_allocator<value_type> alloc)
    : compare(cmp), _allocator(alloc), _root(nullptr), _size(0) {}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::BP_tree(pp_allocator<value_type> alloc, const compare& cmp)
    : compare(cmp), _allocator(alloc), _root(nullptr), _size(0) {}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
template <input_iterator_for_pair<tkey, tvalue> iterator>
BP_tree<tkey, tvalue, compare, t>::BP_tree(
    iterator begin, iterator end, const compare& cmp, pp_allocator<value_type> alloc)
    : compare(cmp), _allocator(alloc), _root(nullptr), _size(0) {
    for (; begin != end; ++begin) {
        emplace(begin->first, begin->second);
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::BP_tree(
    std::initializer_list<std::pair<tkey, tvalue>> data, const compare& cmp, pp_allocator<value_type> alloc)
    : compare(cmp), _allocator(alloc), _root(nullptr), _size(0) {
    for (auto const& item : data) {
        emplace(item.first, item.second);
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::BP_tree(const BP_tree& other)
    : compare(other), _allocator(other._allocator), _root(nullptr), _size(0) {
    for (auto const& item : other) {
        emplace(item.first, item.second);
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::BP_tree(BP_tree&& other) noexcept
    : compare(std::move(other)), _allocator(std::move(other._allocator)), _root(other._root), _size(other._size) {
    other._root = nullptr;
    other._size = 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>& BP_tree<tkey, tvalue, compare, t>::operator=(const BP_tree& other) {
    if (this != &other) {
        clear();
        compare::operator=(other);
        _allocator = other._allocator;
        for (auto const& item : other) {
            emplace(item.first, item.second);
        }
    }
    return *this;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>& BP_tree<tkey, tvalue, compare, t>::operator=(BP_tree&& other) noexcept {
    if (this != &other) {
        clear();
        compare::operator=(std::move(other));
        _allocator = std::move(other._allocator);
        _root = other._root;
        _size = other._size;
        other._root = nullptr;
        other._size = 0;
    }
    return *this;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::~BP_tree() noexcept {
    clear();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::begin() {
    bptree_node_base* curr = _root;
    if (curr == nullptr) return end();
    while (!curr->_is_terminate) {
        if (static_cast<bptree_node_middle*>(curr)->_pointers.empty()) return end();
        curr = static_cast<bptree_node_middle*>(curr)->_pointers[0];
    }
    return bptree_iterator(static_cast<bptree_node_term*>(curr), 0);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::end() {
    return bptree_iterator(nullptr, 0);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::begin() const {
    bptree_node_base* curr = _root;
    if (curr == nullptr) return end();
    while (!curr->_is_terminate) {
        if (static_cast<bptree_node_middle*>(curr)->_pointers.empty()) return end();
        curr = static_cast<bptree_node_middle*>(curr)->_pointers[0];
    }
    return bptree_const_iterator(static_cast<bptree_node_term*>(curr), 0);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::end() const {
    return bptree_const_iterator(nullptr, 0);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::cbegin() const {
    return begin();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::cend() const {
    return end();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
size_t BP_tree<tkey, tvalue, compare, t>::size() const noexcept {
    return _size;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::empty() const noexcept {
    return _size == 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::find(const tkey& key) {
    auto it = lower_bound(key);
    if (it != end() && !compare_keys(key, it->first)) return it;
    return end();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::find(
    const tkey& key) const {
    auto it = lower_bound(key);
    if (it != end() && !compare_keys(key, it->first)) return it;
    return end();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::lower_bound(
    const tkey& key) {
    bptree_node_base* curr = _root;
    if (curr == nullptr) return end();
    while (!curr->_is_terminate) {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(curr);
        auto it = std::lower_bound(middle->_keys.begin(), middle->_keys.end(), key,
            [this](const tkey& lhs, const tkey& rhs) { return compare_keys(lhs, rhs); });
        size_t idx = std::distance(middle->_keys.begin(), it);
        curr = middle->_pointers[idx];
    }
    bptree_node_term* term = static_cast<bptree_node_term*>(curr);
    auto it = std::lower_bound(term->_data.begin(), term->_data.end(), key,
        [this](const tree_data_type& lhs, const tkey& rhs) { return compare_keys(lhs.first, rhs); });
    if (it == term->_data.end()) {
        if (term->_next != nullptr) return bptree_iterator(term->_next, 0);
        return end();
    }
    return bptree_iterator(term, std::distance(term->_data.begin(), it));
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::lower_bound(
    const tkey& key) const {
    bptree_node_base* curr = _root;
    if (curr == nullptr) return end();
    while (!curr->_is_terminate) {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(curr);
        auto it = std::lower_bound(middle->_keys.begin(), middle->_keys.end(), key,
            [this](const tkey& lhs, const tkey& rhs) { return compare_keys(lhs, rhs); });
        size_t idx = std::distance(middle->_keys.begin(), it);
        curr = middle->_pointers[idx];
    }
    bptree_node_term* term = static_cast<bptree_node_term*>(curr);
    auto it = std::lower_bound(term->_data.begin(), term->_data.end(), key,
        [this](const tree_data_type& lhs, const tkey& rhs) { return compare_keys(lhs.first, rhs); });
    if (it == term->_data.end()) {
        if (term->_next != nullptr) return bptree_const_iterator(term->_next, 0);
        return end();
    }
    return bptree_const_iterator(term, std::distance(term->_data.begin(), it));
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::upper_bound(
    const tkey& key) {
    bptree_node_base* curr = _root;

    if (curr == nullptr) return end();

    while (!curr->_is_terminate) {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(curr);

        auto it = std::upper_bound(middle->_keys.begin(), middle->_keys.end(), key,
            [this](const tkey& lhs, const tkey& rhs) { return compare_keys(lhs, rhs); });

        size_t idx = std::distance(middle->_keys.begin(), it);
        curr = middle->_pointers[idx];
    }

    bptree_node_term* term = static_cast<bptree_node_term*>(curr);

    auto it = std::upper_bound(term->_data.begin(), term->_data.end(), key,
        [this](const tkey& lhs, const tree_data_type& rhs) { return compare_keys(lhs, rhs.first); });

    if (it == term->_data.end()) {
        if (term->_next != nullptr) return bptree_iterator(term->_next, 0);
        return end();
    }

    return bptree_iterator(term, std::distance(term->_data.begin(), it));
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_const_iterator BP_tree<tkey, tvalue, compare, t>::upper_bound(
    const tkey& key) const {
    bptree_node_base* curr = _root;

    if (curr == nullptr) return end();

    while (!curr->_is_terminate) {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(curr);

        auto it = std::upper_bound(middle->_keys.begin(), middle->_keys.end(), key,
            [this](const tkey& lhs, const tkey& rhs) { return compare_keys(lhs, rhs); });

        size_t idx = std::distance(middle->_keys.begin(), it);
        curr = middle->_pointers[idx];
    }

    bptree_node_term* term = static_cast<bptree_node_term*>(curr);

    auto it = std::upper_bound(term->_data.begin(), term->_data.end(), key,
        [this](const tkey& lhs, const tree_data_type& rhs) { return compare_keys(lhs, rhs.first); });

    if (it == term->_data.end()) {
        if (term->_next != nullptr) return bptree_const_iterator(term->_next, 0);
        return end();
    }

    return bptree_const_iterator(term, std::distance(term->_data.begin(), it));
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
bool BP_tree<tkey, tvalue, compare, t>::contains(const tkey& key) const {
    return find(key) != end();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::clear() noexcept {
    clear(_root);
    _root = nullptr;
    _size = 0;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::clear(bptree_node_base* node) {
    if (node == nullptr) return;

    if (node->_is_terminate) {
        _allocator.template delete_object<bptree_node_term>(static_cast<bptree_node_term*>(node));
    } else {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(node);

        for (auto child : middle->_pointers) {
            clear(child);
        }

        _allocator.template delete_object<bptree_node_middle>(middle);
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::split_root() {
    bptree_node_middle* new_root = _allocator.template new_object<bptree_node_middle>();

    new_root->_pointers.push_back(_root);
    split_node(new_root, 0);
    _root = new_root;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::split_node(bptree_node_middle* parent, size_t index) {
    bptree_node_base* node_to_split = parent->_pointers[index];

    if (node_to_split->_is_terminate) {
        bptree_node_term* term = static_cast<bptree_node_term*>(node_to_split);
        bptree_node_term* new_term = _allocator.template new_object<bptree_node_term>();

        size_t mid = term->_data.size() / 2;

        new_term->_data.assign(
            std::make_move_iterator(term->_data.begin() + mid), std::make_move_iterator(term->_data.end()));

        term->_data.erase(term->_data.begin() + mid, term->_data.end());

        new_term->_next = term->_next;
        term->_next = new_term;

        parent->_keys.insert(parent->_keys.begin() + index, new_term->_data.front().first);
        parent->_pointers.insert(parent->_pointers.begin() + index + 1, new_term);
        return;
    }

    bptree_node_middle* middle = static_cast<bptree_node_middle*>(node_to_split);
    bptree_node_middle* new_middle = _allocator.template new_object<bptree_node_middle>();

    const size_t mid = middle->_keys.size() / 2;
    tkey push_up = std::move(middle->_keys[mid]);

    new_middle->_keys.assign(
        std::make_move_iterator(middle->_keys.begin() + mid + 1), std::make_move_iterator(middle->_keys.end()));

    new_middle->_pointers.assign(middle->_pointers.begin() + mid + 1, middle->_pointers.end());

    middle->_keys.erase(middle->_keys.begin() + mid, middle->_keys.end());
    middle->_pointers.erase(middle->_pointers.begin() + mid + 1, middle->_pointers.end());

    parent->_keys.insert(parent->_keys.begin() + index, std::move(push_up));
    parent->_pointers.insert(parent->_pointers.begin() + index + 1, new_middle);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
std::pair<typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator, bool> BP_tree<tkey, tvalue, compare, t>::insert(
    tree_data_type&& data) {
    return emplace(std::move(data.first), std::move(data.second));
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
template <typename... Args>
std::pair<typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator, bool> BP_tree<tkey, tvalue, compare, t>::emplace(
    Args&&... args) {
    tree_data_type pair(std::forward<Args>(args)...);

    if (_root == nullptr) {
        _root = _allocator.template new_object<bptree_node_term>();
        static_cast<bptree_node_term*>(_root)->_data.push_back(std::move(pair));
        _size = 1;

        return {bptree_iterator(static_cast<bptree_node_term*>(_root), 0), true};
    }

    if (!_root->_is_terminate && static_cast<bptree_node_middle*>(_root)->_keys.size() == maximum_keys_in_node) {
        split_root();
    } else if (_root->_is_terminate && static_cast<bptree_node_term*>(_root)->_data.size() == maximum_keys_in_node) {
        split_root();
    }

    bptree_node_base* curr = _root;
    bptree_node_middle* parent = nullptr;

    while (!curr->_is_terminate) {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(curr);

        auto it = std::lower_bound(middle->_keys.begin(), middle->_keys.end(), pair.first,
            [this](const tkey& lhs, const tkey& rhs) { return compare_keys(lhs, rhs); });
        size_t idx = std::distance(middle->_keys.begin(), it);

        bptree_node_base* child = middle->_pointers[idx];

        if (child->_is_terminate) {
            if (static_cast<bptree_node_term*>(child)->_data.size() == maximum_keys_in_node) {
                split_node(middle, idx);

                if (!compare_keys(pair.first, middle->_keys[idx])) {
                    idx++;
                }

                child = middle->_pointers[idx];
            }
        } else {
            if (static_cast<bptree_node_middle*>(child)->_keys.size() == maximum_keys_in_node) {
                split_node(middle, idx);

                if (compare_keys(middle->_keys[idx], pair.first)) {
                    idx++;
                }

                child = middle->_pointers[idx];
            }
        }

        parent = middle;
        curr = child;
    }

    bptree_node_term* term = static_cast<bptree_node_term*>(curr);
    auto it = std::lower_bound(term->_data.begin(), term->_data.end(), pair,
        [this](const tree_data_type& lhs, const tree_data_type& rhs) { return compare_keys(lhs.first, rhs.first); });

    if (it != term->_data.end() && !compare_keys(pair.first, it->first)) {
        return {bptree_iterator(term, std::distance(term->_data.begin(), it)), false};
    }

    size_t idx = std::distance(term->_data.begin(), it);
    term->_data.insert(it, std::move(pair));
    _size++;

    return {bptree_iterator(term, idx), true};
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::insert_or_assign(
    const tree_data_type& data) {
    return emplace_or_assign(data.first, data.second);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::insert_or_assign(
    tree_data_type&& data) {
    return emplace_or_assign(std::move(data.first), std::move(data.second));
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
template <typename... Args>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::emplace_or_assign(
    Args&&... args) {
    tree_data_type pair(std::forward<Args>(args)...);
    auto it = find(pair.first);

    if (it != end()) {
        it->second = std::move(pair.second);
        return it;
    }

    return emplace(std::move(pair.first), std::move(pair.second)).first;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::erase(const tkey& key) {
    bool erased = false;
    _root = erase(_root, key, erased);

    if (erased) {
        _size--;

        if (_root != nullptr && !_root->_is_terminate && static_cast<bptree_node_middle*>(_root)->_keys.empty()) {
            bptree_node_base* new_root = static_cast<bptree_node_middle*>(_root)->_pointers[0];
            _allocator.template delete_object<bptree_node_middle>(static_cast<bptree_node_middle*>(_root));
            _root = new_root;
        }

        return lower_bound(key);
    }

    return end();
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::erase(
    bptree_iterator pos) {
    if (pos == end()) return end();

    return erase(pos->first);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::erase(
    bptree_const_iterator pos) {
    if (pos == end()) return end();

    return erase(pos->first);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::erase(
    bptree_iterator beg, bptree_iterator en) {
    while (beg != en) {
        beg = erase(beg);
    }
    return beg;
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
typename BP_tree<tkey, tvalue, compare, t>::bptree_iterator BP_tree<tkey, tvalue, compare, t>::erase(
    bptree_const_iterator beg, bptree_const_iterator en) {
    while (beg != en) {
        beg = erase(beg);
    }
    return bptree_iterator(const_cast<bptree_node_term*>(beg._node), beg._index);
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
BP_tree<tkey, tvalue, compare, t>::bptree_node_base* BP_tree<tkey, tvalue, compare, t>::erase(
    bptree_node_base* node, const tkey& key, bool& erased) {
    if (node == nullptr) return nullptr;

    if (node->_is_terminate) {
        bptree_node_term* term = static_cast<bptree_node_term*>(node);
        auto it = std::lower_bound(term->_data.begin(), term->_data.end(), key,
            [this](const tree_data_type& lhs, const tkey& rhs) { return compare_keys(lhs.first, rhs); });

        if (it != term->_data.end() && !compare_keys(key, it->first)) {
            term->_data.erase(it);
            erased = true;
        }
        return term;
    } else {
        bptree_node_middle* middle = static_cast<bptree_node_middle*>(node);
        auto it = std::lower_bound(middle->_keys.begin(), middle->_keys.end(), key,
            [this](const tkey& lhs, const tkey& rhs) { return compare_keys(lhs, rhs); });
        size_t idx = std::distance(middle->_keys.begin(), it);

        middle->_pointers[idx] = erase(middle->_pointers[idx], key, erased);

        if (erased) {
            bptree_node_base* child = middle->_pointers[idx];
            bool underflow = false;
            if (child == nullptr) {
                underflow = true;
            } else if (child->_is_terminate) {
                if (static_cast<bptree_node_term*>(child)->_data.size() < minimum_keys_in_node) underflow = true;
            } else {
                if (static_cast<bptree_node_middle*>(child)->_keys.size() < minimum_keys_in_node) underflow = true;
            }

            if (underflow) {
                balance_after_erase(middle, idx);
            }

            for (size_t i = 0; i < middle->_keys.size(); ++i) {
                bptree_node_base* right_child = middle->_pointers[i + 1];
                while (!right_child->_is_terminate)
                    right_child = static_cast<bptree_node_middle*>(right_child)->_pointers[0];
                middle->_keys[i] = static_cast<bptree_node_term*>(right_child)->_data.front().first;
            }
        }
        return middle;
    }
}

template <typename tkey, typename tvalue, comparator<tkey> compare, std::size_t t>
void BP_tree<tkey, tvalue, compare, t>::balance_after_erase(bptree_node_middle* parent, size_t index) {
    bptree_node_base* curr = parent->_pointers[index];

    // пробуем занять у левого соседа
    if (index > 0) {
        bptree_node_base* left = parent->_pointers[index - 1];

        // если левый это лист
        if (left->_is_terminate) {
            bptree_node_term* l_term = static_cast<bptree_node_term*>(left);
            bptree_node_term* c_term = static_cast<bptree_node_term*>(curr);

            // если у левого есть лишнее то берем
            if (l_term->_data.size() > minimum_keys_in_node) {
                // если текущего нет создаем
                if (c_term == nullptr) {
                    c_term = _allocator.template new_object<bptree_node_term>();
                    parent->_pointers[index] = c_term;
                    l_term->_next = c_term;
                }

                // переносим элемент слева направо
                c_term->_data.insert(c_term->_data.begin(), std::move(l_term->_data.back()));
                l_term->_data.pop_back();
                parent->_keys[index - 1] = c_term->_data.front().first;

                return;
            }
        } else {
            // если левый это внутренний узел
            bptree_node_middle* l_mid = static_cast<bptree_node_middle*>(left);
            bptree_node_middle* c_mid = static_cast<bptree_node_middle*>(curr);

            // если можно занять ключ
            if (l_mid->_keys.size() > minimum_keys_in_node) {
                // тянем ключ через родителя
                c_mid->_keys.insert(c_mid->_keys.begin(), std::move(parent->_keys[index - 1]));
                parent->_keys[index - 1] = std::move(l_mid->_keys.back());
                l_mid->_keys.pop_back();
                c_mid->_pointers.insert(c_mid->_pointers.begin(), l_mid->_pointers.back());
                l_mid->_pointers.pop_back();

                return;
            }
        }
    }

    // если слева не получилось пробуем справа
    if (index < parent->_keys.size()) {
        bptree_node_base* right = parent->_pointers[index + 1];

        // если правый это лист
        if (right->_is_terminate) {
            bptree_node_term* r_term = static_cast<bptree_node_term*>(right);
            bptree_node_term* c_term = static_cast<bptree_node_term*>(curr);

            // если у правого есть лишнее берем
            if (r_term->_data.size() > minimum_keys_in_node) {
                // если текущего нет создаем и вшиваем в список
                if (c_term == nullptr) {
                    c_term = _allocator.template new_object<bptree_node_term>();
                    parent->_pointers[index] = c_term;

                    if (index > 0) static_cast<bptree_node_term*>(parent->_pointers[index - 1])->_next = c_term;

                    c_term->_next = r_term;
                }

                // переносим элемент справа налево
                c_term->_data.push_back(std::move(r_term->_data.front()));
                r_term->_data.erase(r_term->_data.begin());
                parent->_keys[index] = r_term->_data.front().first;

                return;
            }
        } else {
            // если правый это внутренний узел
            bptree_node_middle* r_mid = static_cast<bptree_node_middle*>(right);
            bptree_node_middle* c_mid = static_cast<bptree_node_middle*>(curr);

            // если можно занять ключ
            if (r_mid->_keys.size() > minimum_keys_in_node) {
                // тянем ключ через родителя
                c_mid->_keys.push_back(std::move(parent->_keys[index]));
                parent->_keys[index] = std::move(r_mid->_keys.front());
                r_mid->_keys.erase(r_mid->_keys.begin());
                c_mid->_pointers.push_back(r_mid->_pointers.front());
                r_mid->_pointers.erase(r_mid->_pointers.begin());
                return;
            }
        }
    }

    // если занять не получилось сливаемся с левым
    if (index > 0) {
        bptree_node_base* left = parent->_pointers[index - 1];

        // если это листья
        if (left->_is_terminate) {
            bptree_node_term* l_term = static_cast<bptree_node_term*>(left);
            bptree_node_term* c_term = static_cast<bptree_node_term*>(curr);

            // переносим все в левый и удаляем текущий
            if (c_term != nullptr) {
                l_term->_data.insert(l_term->_data.end(), std::make_move_iterator(c_term->_data.begin()),
                    std::make_move_iterator(c_term->_data.end()));
                l_term->_next = c_term->_next;
                _allocator.template delete_object<bptree_node_term>(c_term);
            }

            // чистим родителя
            parent->_keys.erase(parent->_keys.begin() + index - 1);
            parent->_pointers.erase(parent->_pointers.begin() + index);
        } else {
            // если это внутренние узлы
            bptree_node_middle* l_mid = static_cast<bptree_node_middle*>(left);
            bptree_node_middle* c_mid = static_cast<bptree_node_middle*>(curr);

            // склеиваем узлы
            l_mid->_keys.push_back(std::move(parent->_keys[index - 1]));
            l_mid->_keys.insert(l_mid->_keys.end(), std::make_move_iterator(c_mid->_keys.begin()),
                std::make_move_iterator(c_mid->_keys.end()));
            l_mid->_pointers.insert(l_mid->_pointers.end(), c_mid->_pointers.begin(), c_mid->_pointers.end());

            _allocator.template delete_object<bptree_node_middle>(c_mid);

            parent->_keys.erase(parent->_keys.begin() + index - 1);
            parent->_pointers.erase(parent->_pointers.begin() + index);
        }
    } else {
        // если нет левого сливаемся с правым
        bptree_node_base* right = parent->_pointers[index + 1];

        // если это листья
        if (right->_is_terminate) {
            bptree_node_term* r_term = static_cast<bptree_node_term*>(right);
            bptree_node_term* c_term = static_cast<bptree_node_term*>(curr);

            // переносим все из правого в текущий
            if (c_term != nullptr) {
                c_term->_data.insert(c_term->_data.end(), std::make_move_iterator(r_term->_data.begin()),
                    std::make_move_iterator(r_term->_data.end()));
                c_term->_next = r_term->_next;

                _allocator.template delete_object<bptree_node_term>(r_term);
                parent->_keys.erase(parent->_keys.begin() + index);
                parent->_pointers.erase(parent->_pointers.begin() + index + 1);
            } else {
                // просто подставляем правый вместо текущего
                parent->_pointers[index] = r_term;
                parent->_keys.erase(parent->_keys.begin() + index);
                parent->_pointers.erase(parent->_pointers.begin() + index + 1);
            }
        } else {
            // если это внутренние узлы
            bptree_node_middle* r_mid = static_cast<bptree_node_middle*>(right);
            bptree_node_middle* c_mid = static_cast<bptree_node_middle*>(curr);

            // склеиваем текущий и правый
            c_mid->_keys.push_back(std::move(parent->_keys[index]));
            c_mid->_keys.insert(c_mid->_keys.end(), std::make_move_iterator(r_mid->_keys.begin()),
                std::make_move_iterator(r_mid->_keys.end()));
            c_mid->_pointers.insert(c_mid->_pointers.end(), r_mid->_pointers.begin(), r_mid->_pointers.end());

            _allocator.template delete_object<bptree_node_middle>(r_mid);

            parent->_keys.erase(parent->_keys.begin() + index);
            parent->_pointers.erase(parent->_pointers.begin() + index + 1);
        }
    }
}

#endif
