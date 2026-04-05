#pragma once
#include <cstdint>
#include "file_manager.h"

// Исправлено название файла: bplus_node.h
struct BPlusNodeHeader {
    bool is_leaf;       // true - лист, false - внутренний узел 
    uint32_t num_keys;  // Текущее количество ключей в узле
    uint32_t parent;    // Номер страницы родителя (0 для корня)
    uint32_t next_leaf; // Для листьев: связь в односвязный список для range-запросов [cite: 49]
};

// Константы для B+ дерева (для ключей типа INT)
// Размер метаданных узла
constexpr size_t NODE_HEADER_SIZE = sizeof(BPlusNodeHeader);
// Размер полезной нагрузки страницы
constexpr size_t NODE_BODY_SIZE = PAGE_SIZE - NODE_HEADER_SIZE;

// Расчет порядка дерева (сколько ключей влезет в одну страницу)
// В листе: [Key (4)] [Offset (8)]
// Во внутреннем узле: [Key (4)] [PageID (4)] + 1 лишний PageID
constexpr uint32_t MAX_KEYS_LEAF = NODE_BODY_SIZE / (sizeof(int32_t) + sizeof(uint64_t));
constexpr uint32_t MAX_KEYS_INTERNAL = (NODE_BODY_SIZE - sizeof(uint32_t)) / (sizeof(int32_t) + sizeof(uint32_t));

// Структура узла в памяти (для удобного наложения на буфер Pager)
struct BPlusNode {
    BPlusNodeHeader header;
    char data[NODE_BODY_SIZE]; // Сюда будем копировать ключи и указатели через memcpy
};