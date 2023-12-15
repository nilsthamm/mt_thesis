#pragma once

#include <algorithm>
#include <atomic>
#include <iostream>
#include <stdalign.h>
#include <stdexcept>
#include <thread>
#include <climits>


#define META_DATA_BIT_SIZE 64

#define META_DATA_MAX_VALUE UINT64_MAX

#define CACHE_LINE_SIZE 64

#define PMEM_CACHE_LINE_SIZE 256

// Defines how many bits of the _version_meta_data bitset of the node are used for other purposes than the bitmap indicating the branch validity
// 8 bits as a version number (even versions indicate no lock is beeing hold, uneven vice versa)

/*********** Basic *****************/



#define MAX_VERSION CHAR_MAX



#define UNLOCK_BIT_MASK ULONG_MAX<<1

#define META_DATA_TYPE size_t

#define BRANCH_VERSION_TYPE size_t

#define LOCKING_DEBUG false

#ifdef NDEBUG
#if LOCKING_DEBUG
#pragma message("WARNING: LOCKING_DEBUG enabled")
#endif
#endif

#define ACQUIRE_META_ATOMIC_WAIT_COMMANDS(ATOMIC_META_DATA, WAIT_COMMANDS)  {   \
    META_DATA_TYPE value_var___ = LOAD_META_RELAXED_INLINE(ATOMIC_META_DATA);   \
    value_var___ = value_var___&UNLOCK_BIT_MASK;                                \
    long counter = 0;\
    while(!ATOMIC_META_DATA.compare_exchange_weak(value_var___, value_var___+1, std::memory_order_acq_rel)){ \
        if(LOCKING_DEBUG){counter++;if(counter==10000000) { std::stringstream stream; stream << "Possible deadlock at " << __FILE__ << ":" << __LINE__; std::cerr << stream.str()<<std::endl;}}\
        WAIT_COMMANDS                                                           \
        value_var___ &= UNLOCK_BIT_MASK;                                        \
    }                                                                           \
    std::atomic_thread_fence(std::memory_order_acquire);                        \
}

#define ACQUIRE_META_ATOMIC(ATOMIC_META_DATA)  ACQUIRE_META_ATOMIC_WAIT_COMMANDS(ATOMIC_META_DATA, ;)

#define IS_LOCKED(VERSION_NUMBER) (VERSION_NUMBER&1)

#define STORE_META(META_ATOMIC, META_VALUE) META_ATOMIC.store(META_VALUE, std::memory_order_release);

#define STORE_META_RELAXED(META_ATOMIC, META_VALUE) META_ATOMIC.store(META_VALUE, std::memory_order_relaxed);

#define LOAD_META(META_ATOMIC, META_VALUE) META_VALUE = META_ATOMIC.load(std::memory_order_acquire);

#define LOAD_META_INLINE(META_ATOMIC) META_ATOMIC.load(std::memory_order_acquire)

#define LOAD_META_RELAXED_INLINE(META_ATOMIC) META_ATOMIC.load(std::memory_order_relaxed)

#define RELEASE_META_DATA_LOCK_WITH_INCREASE(ATOMIC_META) { \
    std::atomic_thread_fence(std::memory_order_release);    \
    auto const meta_value__ = ATOMIC_META.load(std::memory_order_relaxed); \
    if(LOCKING_DEBUG && !IS_LOCKED(meta_value__)) { std::stringstream stream; stream << "Releasing unlocked meta (" << meta_value__ << "; ID: " << static_cast<void*>(&ATOMIC_META) << "; Thread: " << std::this_thread::get_id() << ") at " << __FILE__ << ":" << __LINE__; std::cerr << stream.str()<<std::endl;} \
    STORE_META(ATOMIC_META, meta_value__+1)}

#define INCREASE_ATOMIC_COUNTER(ATOMIC_COUNTER) { \
    META_DATA_TYPE temp_counter___ = LOAD_META_INLINE(ATOMIC_COUNTER); \
    while(!_processing_counter.compare_exchange_weak(temp_counter___, temp_counter___ + 1, std::memory_order_acq_rel)) { \
        temp_counter___ = LOAD_META_INLINE(ATOMIC_COUNTER); \
    } \
}

#define DECREAS_ATOMIC_COUNTER(ATOMIC_COUNTER) { \
    META_DATA_TYPE temp_counter___ = LOAD_META_INLINE(ATOMIC_COUNTER); \
    while(!_processing_counter.compare_exchange_weak(temp_counter___, temp_counter___ - 1, std::memory_order_acq_rel)) { \
        temp_counter___ = LOAD_META_INLINE(ATOMIC_COUNTER); \
    } \
}

#define RELEASE_META_DATA_LOCK(ATOMIC_META) { \
    PearTreeAllocatorSingleton::sfence();     \
    auto const meta_value__ = ATOMIC_META.load(std::memory_order_relaxed); \
    /**if(!IS_LOCKED(meta_value__)) { std::stringstream stream; stream << "Releasing unlocked meta (" << meta_value__ << "; ID: " << static_cast<void*>(&ATOMIC_META) << "; Thread: " << std::this_thread::get_id() << ") at " << __FILE__ << ":" << __LINE__; std::cerr << stream.str()<<std::endl;} **/\
    STORE_META(ATOMIC_META,meta_value__&UNLOCK_BIT_MASK)                   \
}


#define LOAD_META_CONST(META_ATOMIC, META_VALUE) META_DATA_TYPE const META_VALUE = META_ATOMIC.load(std::memory_order_acquire);

#define LOAD_META_CONST_RELAXED(META_ATOMIC, META_VALUE) META_DATA_TYPE const META_VALUE = META_ATOMIC.load(std::memory_order_relaxed);

#define xstr_macro(a) str_macro(a)
#define str_macro(a) #a

#include "helper_macros.hpp"
#include "pear_maros.hpp"
#include "basic_macros.hpp"