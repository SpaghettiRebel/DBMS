#pragma once

#include "../shared/QueryPlan.h"
#include <string>
#include <vector>
#include <cstdint>

// Alias для удобства (используем глобальный namespace)
using Value = ::Value;
using Record = std::vector<Value>;

// Позиция в файле (page_id, offset)
struct pos_t {
    uint32_t page_id;
    uint32_t offset;
    
    pos_t() : page_id(0), offset(0) {}
    pos_t(uint32_t p, uint32_t o) : page_id(p), offset(o) {}
    
    bool is_valid() const { return page_id != 0 || offset != 0; }
    
    bool operator==(const pos_t& other) const {
        return page_id == other.page_id && offset == other.offset;
    }
    
    bool operator!=(const pos_t& other) const {
        return !(*this == other);
    }
};

// RID - Record ID (алиас на pos_t)
using RID = pos_t;
