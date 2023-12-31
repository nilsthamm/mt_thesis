#pragma once



#define BASIC_META_DATA_VERSION_BIT_SIZE 8

#define BASIC_META_DATA_GLOBAL_VERSION_BIT_SIZE 7
#define BASIC_META_DATA_RESERVED_BITS BASIC_META_DATA_VERSION_BIT_SIZE + BASIC_META_DATA_GLOBAL_VERSION_BIT_SIZE
static_assert(BASIC_META_DATA_VERSION_BIT_SIZE + BASIC_META_DATA_GLOBAL_VERSION_BIT_SIZE == BASIC_META_DATA_RESERVED_BITS);

#define BASIC_META_DATA_RESERVED_BITS BASIC_META_DATA_VERSION_BIT_SIZE + BASIC_META_DATA_GLOBAL_VERSION_BIT_SIZE


#define BASIC_META_DATA_MASK_BIT_SET UINT64_MAX << BASIC_META_DATA_RESERVED_BITS

#define BASIC_META_DATA_MASK_VERSION (UINT64_MAX >> (META_DATA_BIT_SIZE-BASIC_META_DATA_VERSION_BIT_SIZE))

#define BASIC_META_DATA_MASK_GLOBAL_VERSION ((UINT64_MAX >> (META_DATA_BIT_SIZE-BASIC_META_DATA_GLOBAL_VERSION_BIT_SIZE))<<BASIC_META_DATA_VERSION_BIT_SIZE)

#define BASIC_SET_GLOBAL_VERSION(META_LONG, GLOBAL_VERSION) (META_LONG&(~BASIC_META_DATA_MASK_GLOBAL_VERSION))+(GLOBAL_VERSION<<BASIC_META_DATA_VERSION_BIT_SIZE)

#define BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(ATOMIC_META_DATA, VALUE_VAR, WAIT_COMMANDS, GLOBAL_VERSION)  \
    LOAD_META(ATOMIC_META_DATA, VALUE_VAR) \
    VALUE_VAR = (BASIC_SET_GLOBAL_VERSION(VALUE_VAR,GLOBAL_VERSION))&UNLOCK_BIT_MASK;                                                               \
    while(!ATOMIC_META_DATA.compare_exchange_weak(VALUE_VAR, VALUE_VAR+1, std::memory_order_acq_rel)){ \
        VALUE_VAR &= UNLOCK_BIT_MASK;                                                           \
        WAIT_COMMANDS                                                   \
    }                                                                                         \
    VALUE_VAR += 1;

#define BASIC_ACQUIRE_META_ATOMIC_VALUE(ATOMIC_META, VALUE_VAR, GLOBAL_VERSION) BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(ATOMIC_META, VALUE_VAR, ;, GLOBAL_VERSION)

#define BASIC_ACQUIRE_META_ATOMIC_WAIT_COMMANDS(ATOMIC_META, WAIT_COMMANDS, GLOBAL_VERSION) { \
    META_DATA_TYPE meta_value__; \
    BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(ATOMIC_META, meta_value__, WAIT_COMMANDS, GLOBAL_VERSION)     \
}

#define BASIC_ACQUIRE_META_ATOMIC(ATOMIC_META, GLOBAL_VERSION) { \
    META_DATA_TYPE meta_value__; \
    BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(ATOMIC_META, meta_value__, ;, GLOBAL_VERSION)     \
}

#define BASIC_CHECK_BIT_MAP(MAP_ULONG_VALUE,INDEX)  MAP_ULONG_VALUE & ((unsigned long) 1 << (INDEX))

#define BASIC_GET_VERSION(META_LONG) static_cast<u_char>(META_LONG & BASIC_META_DATA_MASK_VERSION)

#define BASIC_GET_VERSION_UNLOCKED(META_LONG) BASIC_GET_VERSION((META_LONG&UNLOCK_BIT_MASK))

#define BASIC_GET_GLOBAL_VERSION(META_LONG) static_cast<u_char>((META_LONG&BASIC_META_DATA_MASK_GLOBAL_VERSION)>>BASIC_META_DATA_VERSION_BIT_SIZE)

#define BASIC_SET_VERSION_INLINE(NEW_VERSION, META_LONG) (META_LONG&(~BASIC_META_DATA_MASK_VERSION))+NEW_VERSION

#define BASIC_SET_VERSION(NEW_VERSION, META_LONG) META_LONG = BASIC_SET_VERSION_INLINE(NEW_VERSION, META_LONG) ;

#define BASIC_LOAD_VERSION(ATOMIC_META) BASIC_GET_VERSION(ATOMIC_META.load(std::memory_order_acquire))


#define BASIC_RELEASE_META_DATA_LOCK(ATOMIC_META_DATA) { \
    auto const meta_value__ = ATOMIC_META_DATA.load(std::memory_order_relaxed); \
    STORE_META(ATOMIC_META_DATA, (meta_value__&(~BASIC_META_DATA_MASK_VERSION))+std::max(BASIC_GET_VERSION((meta_value__+1)),(u_char)4))}

#define BASIC_RELEASE_META_DATA_LOCK_WITH_VALUE(ATOMIC_META_DATA, META_VALUE) STORE_META(ATOMIC_META_DATA, (META_VALUE&(~BASIC_META_DATA_MASK_VERSION))+std::max(BASIC_GET_VERSION((META_VALUE+1)),(u_char)4))

