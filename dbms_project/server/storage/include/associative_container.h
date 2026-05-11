#pragma once
#include <concepts>
#include <iterator>
#include <utility>
#include <functional>

// Concept for comparator - works with std::less, std::greater, etc.
// Supports both old-style comparators with member types and simple callable comparators
template<typename T, typename Key>
concept comparator = requires(T comp, const Key& a, const Key& b) {
    { comp(a, b) } -> std::convertible_to<bool>;
};

// Simplified concept for input iterator that produces pairs
template<typename Iter, typename Key, typename Value>
concept input_iterator_for_pair = std::input_iterator<Iter> && 
    requires(Iter it) {
        { *it } -> std::convertible_to<std::pair<Key, Value>>;
    };
