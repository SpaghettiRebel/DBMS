#ifndef ASSOCIATIVE_CONTAINER_H
#define ASSOCIATIVE_CONTAINER_H

#include <iostream>
#include <vector>

template <typename compare, typename tkey>
concept comparator = requires(const compare c, const tkey& lhs, const tkey& rhs) {
    { c(lhs, rhs) } -> std::same_as<bool>;
} && std::copyable<compare> && std::default_initializable<compare>;

template <typename f_iter, typename tkey, typename tval>
concept input_iterator_for_pair =
    std::input_iterator<f_iter> &&
    std::same_as<typename std::iterator_traits<f_iter>::value_type, std::pair<tkey, tval>>;

#endif