#pragma once
#include <memory>

template <typename T>
class pp_allocator : public std::allocator<T> {
public:
    using std::allocator<T>::allocator;
};

template <typename T, typename Key>
concept input_iterator_for_pair = std::input_iterator<T>;  // Simplified for the prototype

template <typename T>
concept comparator =
    requires(T a, const typename T::first_argument_type& b, const typename T::second_argument_type& c) {
        { a(b, c) } -> std::convertible_to<bool>;
    };
