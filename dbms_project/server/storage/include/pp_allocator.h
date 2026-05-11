#pragma once
#include <memory>
#include <type_traits>
#include <cstddef>

// Полиморфный аллокатор для B+ дерева
// Выделяет память для любых типов, а не только для T
class pp_allocator_base {
public:
    virtual ~pp_allocator_base() = default;
    
    template<typename U, typename... Args>
    U* new_object(Args&&... args) {
        void* ptr = allocate_raw(sizeof(U), alignof(U));
        try {
            ::new (ptr) U(std::forward<Args>(args)...);
        } catch (...) {
            deallocate_raw(ptr, sizeof(U), alignof(U));
            throw;
        }
        return static_cast<U*>(ptr);
    }
    
    template<typename U>
    void delete_object(U* ptr) {
        if (ptr) {
            ptr->~U();
            deallocate_raw(ptr, sizeof(U), alignof(U));
        }
    }
    
protected:
    virtual void* allocate_raw(std::size_t size, std::size_t align) = 0;
    virtual void deallocate_raw(void* ptr, std::size_t size, std::size_t align) = 0;
};

template <typename T>
class pp_allocator : public pp_allocator_base {
public:
    using value_type = T;
    
    pp_allocator() : pool_(nullptr), pool_size_(0), pool_used_(0) {}
    
    ~pp_allocator() override {
        if (pool_) {
            std::free(pool_);
        }
    }
    
    T* allocate(std::size_t n) {
        return static_cast<T*>(allocate_raw(n * sizeof(T), alignof(T)));
    }
    
    void deallocate(T* ptr, std::size_t n) {
        deallocate_raw(ptr, n * sizeof(T), alignof(T));
    }
    
protected:
    void* allocate_raw(std::size_t size, std::size_t align) override {
        // Выравниваем размер и позицию
        std::size_t aligned_size = (size + align - 1) & ~(align - 1);
        
        // Проверяем, есть ли место в пуле
        if (pool_used_ + aligned_size > pool_size_) {
            // Увеличиваем пул
            std::size_t new_size = pool_size_ + std::max(aligned_size, static_cast<std::size_t>(4096));
            pool_ = static_cast<char*>(std::realloc(pool_, new_size));
            if (!pool_) {
                throw std::bad_alloc();
            }
            pool_size_ = new_size;
        }
        
        void* result = pool_ + pool_used_;
        pool_used_ += aligned_size;
        return result;
    }
    
    void deallocate_raw(void* ptr, std::size_t size, std::size_t align) override {
        // Для простоты не освобождаем память сразу, она будет переиспользована
        // В реальном использовании можно реализовать более сложную логику
        (void)ptr; (void)size; (void)align;
    }
    
private:
    char* pool_;
    std::size_t pool_size_;
    std::size_t pool_used_;
};
