#pragma once

#include <atomic>

template<typename T>
struct pointer_indirection {
    T*  pointer;
    std::atomic<long> atomic_counter;

    T* get_ptr() {
        long current_counter = LOAD_META_INLINE(atomic_counter);
        if(!(current_counter < 0)) {
            while (!atomic_counter.compare_exchange_weak(current_counter, current_counter+1, std::memory_order_acq_rel)) {
                if(current_counter < 0) {
                    while(current_counter == -1) {
                        current_counter = LOAD_META_INLINE(atomic_counter);
                    }
                    break;
                }
            }
        } else if(current_counter == -1) {
            while(current_counter == -1) {
                current_counter = LOAD_META_INLINE(atomic_counter);
            }
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        return pointer;
    }

    void deregister_reference() {
        long current_counter = LOAD_META_INLINE(atomic_counter);
        while (!atomic_counter.compare_exchange_weak(current_counter, current_counter-1, std::memory_order_acq_rel));
    }

    void exchange_ptr(T* new_ptr) {
        long expected = 0l;
        while(!atomic_counter.compare_exchange_weak(expected, -1l, std::memory_order_acq_rel));
        pointer = new_ptr;
        std::atomic_thread_fence(std::memory_order_release);
        expected = -1l;
        if(!atomic_counter.compare_exchange_weak(expected, -2l, std::memory_order_acq_rel)) {
            INVALID_BRANCH_TAKEN
        }
    }
};