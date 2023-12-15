#pragma once

#define CPUS_SOCKET_TWO 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71

#define PEAR_OPTIMAL_NODE_SIZE 38,12
#define PEAR_MAX_NODE_SIZE 62,24
#define PEAR_SMALL_NODE_SIZE 30,8

// 13650 nodes = 100 MB DRAM
#define PEAR_OPTIMAL_DRAM_COUNT 13650

#define PEAR_IS_REORGANIZING(NODE_META_VALUE) (((NODE_META_VALUE&(PEAR_NODE_LOCAL_VERSION_BIT_MASK))&~(PEAR_NODE_VERSION_RESERVED_BITS_MASK))==0b0)

#define PEAR_SPLIT_PARENT_STARTED_BIT_MASK 0b11

#define PEAR_SPLIT_PARENT_STARTED(NODE_META_VALUE) ((NODE_META_VALUE&PEAR_SPLIT_PARENT_STARTED_BIT_MASK)==0b11)

#define PEAR_NODE_VERSION_RESERVED_BITS_MASK 0b11

#define PEAR_MIN_BRANCH_UNRESERVED_VERSION 2

#define PEAR_MIN_NODE_UNRESERVED_VERSION 4

#define PEAR_GLOBAL_VERSION_BIT_SIZE 32

#define PEAR_GLOBAL_VERSION_TYPE uint32_t

#define PEAR_BRANCH_META_DATA_VERSION_BIT_SIZE 32

#define PEAR_NODE_LOCAL_VERSION_BIT_SIZE 32

#define PEAR_NODE_VERSION_TYPE uint32_t

#define PEAR_UNSIGNED_NODE_VERSION_TYPE int32_t

#define PEAR_LEVEL_TYPE uint32_t

#define PEAR_BRANCH_VERSION_TYPE uint32_t

#define PEAR_QUERY_QUEUE_ENTRY_TYPE search_queue_entry<node_type *>

#define PEAR_QUERY_QUEUE_TYPE std::deque<PEAR_QUERY_QUEUE_ENTRY_TYPE>

#define PEAR_QUERY_DRAM_QUEUE_ENTRY_TYPE search_queue_entry<std::shared_ptr<typename node_type::pointer_indirection_type>>

#define PEAR_QUERY_DRAM_QUEUE_TYPE std::deque<PEAR_QUERY_DRAM_QUEUE_ENTRY_TYPE>

#define PEAR_NODE_LOCAL_VERSION_BIT_MASK (META_DATA_MAX_VALUE>>PEAR_GLOBAL_VERSION_BIT_SIZE)

#define PEAR_BRANCH_LOCAL_VERSION_BIT_MASK (META_DATA_MAX_VALUE>>PEAR_GLOBAL_VERSION_BIT_SIZE)

#define PEAR_GET_NODE_VERSION_INLINE(NODE_VERSION_META_VALUE) static_cast<PEAR_NODE_VERSION_TYPE>(NODE_VERSION_META_VALUE&PEAR_NODE_LOCAL_VERSION_BIT_MASK)

#define PEAR_SET_NODE_VERSION_INLINE(NODE_META_VALUE, NEW_VERSION) ((META_DATA_TYPE)(NODE_META_VALUE&~PEAR_NODE_LOCAL_VERSION_BIT_MASK)+(META_DATA_TYPE)static_cast<PEAR_NODE_VERSION_TYPE>(NEW_VERSION))

#define PEAR_SET_NODE_VERSIONS_INLINE(LOCAL_VERSION, GLOBAL_VERSION) (((META_DATA_TYPE)GLOBAL_VERSION<<PEAR_NODE_LOCAL_VERSION_BIT_SIZE)+(META_DATA_TYPE)static_cast<PEAR_NODE_VERSION_TYPE>(LOCAL_VERSION))

#define PEAR_GET_BRANCH_VERSION_INLINE(BRANCH_META_VALUE) static_cast<PEAR_BRANCH_VERSION_TYPE>(BRANCH_META_VALUE&PEAR_BRANCH_LOCAL_VERSION_BIT_MASK)

#define PEAR_GET_BRANCH_VERSION_UNLOCKED_INLINE(BRANCH_META_VALUE) (PEAR_GET_BRANCH_VERSION_INLINE(BRANCH_META_VALUE)&UNLOCK_BIT_MASK)

#define PEAR_RELEASE_BRANCH_LOCK_WITH_VERSION(BRANCH_ATOMIC, NEW_VERSION) {  \
    std::atomic_thread_fence(std::memory_order_release);                     \
    auto const meta_value__ = BRANCH_ATOMIC.load(std::memory_order_relaxed); \
    if(LOCKING_DEBUG && !IS_LOCKED(meta_value__)) { std::stringstream stream; stream << "Releasing unlocked meta (" << meta_value__ << "; ID: " << static_cast<void*>(&BRANCH_ATOMIC) << "; Thread: " << std::this_thread::get_id() << ") at " << __FILE__ << ":" << __LINE__; std::cerr << stream.str()<<std::endl;} \
    STORE_META(BRANCH_ATOMIC,LOAD_META_RELAXED_INLINE(BRANCH_ATOMIC)&~PEAR_BRANCH_LOCAL_VERSION_BIT_MASK+(NEW_VERSION&UNLOCK_BIT_MASK));                                                                                                                                                               \
}

#define PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(NODE_ATOMIC) { \
    std::atomic_thread_fence(std::memory_order_release);    \
    auto const meta_value__ = NODE_ATOMIC.load(std::memory_order_relaxed); \
    if(LOCKING_DEBUG && !IS_LOCKED(meta_value__)) { std::stringstream stream; stream << "Releasing unlocked meta (" << meta_value__ << "; ID: " << static_cast<void*>(&NODE_ATOMIC) << "; Thread: " << std::this_thread::get_id() << ") at " << __FILE__ << ":" << __LINE__; std::cout << stream.str()<<std::endl;} \
    STORE_META(NODE_ATOMIC, (meta_value__&(~PEAR_NODE_LOCAL_VERSION_BIT_MASK))+std::max((PEAR_UNSIGNED_NODE_VERSION_TYPE)(PEAR_GET_NODE_VERSION_INLINE(((meta_value__&UNLOCK_BIT_MASK)+PEAR_MIN_NODE_UNRESERVED_VERSION))&UNLOCK_BIT_MASK),(PEAR_UNSIGNED_NODE_VERSION_TYPE)PEAR_MIN_NODE_UNRESERVED_VERSION))                                                                                                                      \
}

#define PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(BRANCH_ATOMIC) { \
    std::atomic_thread_fence(std::memory_order_release);        \
    auto const meta_value__ = BRANCH_ATOMIC.load(std::memory_order_relaxed); \
    if(LOCKING_DEBUG && !IS_LOCKED(meta_value__)) { std::stringstream stream; stream << "Releasing unlocked meta (" << meta_value__ << "; ID: " << static_cast<void*>(&BRANCH_ATOMIC) << "; Thread: " << std::this_thread::get_id() << ") at " << __FILE__ << ":" << __LINE__; std::cout << stream.str()<<std::endl;} \
    STORE_META(BRANCH_ATOMIC, (meta_value__&(~PEAR_BRANCH_LOCAL_VERSION_BIT_MASK))+std::max((PEAR_BRANCH_VERSION_TYPE)(PEAR_GET_BRANCH_VERSION_INLINE((meta_value__+1))&UNLOCK_BIT_MASK),(PEAR_BRANCH_VERSION_TYPE)PEAR_MIN_BRANCH_UNRESERVED_VERSION))                                                                                                                      \
}

#define PEAR_RELEASE_NODE_LOCK_WITH_VERSION(NODE_ATOMIC, NEW_VERSION) { \
    std::atomic_thread_fence(std::memory_order_release);                \
    auto const meta_value__ = NODE_ATOMIC.load(std::memory_order_relaxed); \
    if(LOCKING_DEBUG && !IS_LOCKED(meta_value__)) { std::stringstream stream; stream << "Releasing unlocked meta (" << meta_value__ << "; ID: " << static_cast<void*>(&NODE_ATOMIC) << "; Thread: " << std::this_thread::get_id() << ") at " << __FILE__ << ":" << __LINE__; std::cerr << stream.str()<<std::endl;} \
    STORE_META(NODE_ATOMIC, (meta_value__&(~PEAR_NODE_LOCAL_VERSION_BIT_MASK))+std::max((PEAR_NODE_VERSION_TYPE)(NEW_VERSION&UNLOCK_BIT_MASK),(PEAR_NODE_VERSION_TYPE)PEAR_MIN_NODE_UNRESERVED_VERSION))                                                                                                                      \
}


/** STRAT Basic **/

#define PEAR_USE_MAX_PMEM  (strategy&((size_t)1<<51))


/** END STRAT Basic **/


// Bit #1 Use MAX_DRAM_LEVELS --> Persist full levels
#define PEAR_STRAT_MAX_DRAM_LEVELS (1)
// Bit #2 Use background thread for persisting
#define PEAR_STRAT_BACKGROUND_PERSISTING (1<<2)
// Bit #3 Use MAX_DRAM_NODES
#define PEAR_STRAT_MAX_DRAM_NODES (1<<3)
// Bit #4 Persist partial levels through sibling linked list
#define PEAR_STRAT_PARTIAL_SIBLING_LIST_PERSISTENCE (1<<4)
// Bit #5 Use MAX_PMEM_LEVEL
#define PEAR_STRAT_MAX_PMEM_LEVEL (1<<5)
// Bit #6 Use partial insert path locking
#define PEAR_STRAT_COMPLETE_PATH_LOCKING (1<<6)
// Bit #7 Disable SIMD Load
#define PEAR_STRAT_ENABLE_SIMD (1<<7)
// Bit #8 Use old way of updating mbrs (lock and update in the same step)
#define PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE (1<<8)
// Bit #9 Don't use prefetching
#define PEAR_STRAT_ENABLE_PREFETCHING (1<<9)
// Bit #10 Query with DFS strategy (naive concurrency control)
#define PEAR_STRAT_BFS_QUERY (1<<10)
// Bit #11 Disable parent pointer during recovery
#define PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY (1<<11)
// Bit #12 Collect concurrency control stats
#define PEAR_STRAT_CONCURRENCY_STATS (1<<12)
// Bit #13 R*-tree insert leaf selection
#define PEAR_STRAT_RSTAR_LEAF_SELECTION (1<<13)


// Bit #25
#define PEAR_STRAT_RECOVERY_STATS (1<<25)
// Bit #26 Collect query stats
#define PEAR_STRAT_QUERY_STATS (1<<26)
// Bit #27 Partial persister stats
#define PEAR_STRAT_PARTIAL_PERSISTER_STATS (1<<27)

#define PEAR_USE_COMPLETE_PATH_LOCKING (strategy&PEAR_STRAT_COMPLETE_PATH_LOCKING)

#define PEAR_COLLECT_RECOVERY_STATS (strategy&PEAR_STRAT_RECOVERY_STATS)

#define PEAR_COLLECT_QUERY_STATS (strategy&PEAR_STRAT_QUERY_STATS)

#define PEAR_COLLECT_PARTIAL_PERSISTER_STATS (strategy&PEAR_STRAT_PARTIAL_PERSISTER_STATS)

#define PEAR_ENABLE_SIMD (strategy&PEAR_STRAT_ENABLE_SIMD)

#define PEAR_ENABLE_PREFETCHING (strategy&PEAR_STRAT_ENABLE_PREFETCHING)

#define PEAR_DISABLE_PARENT_PTR_RECOVERY (strategy&PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY)

#define PEAR_USE_OLD_MBR_UPDATE_ROUTINE (strategy&PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE)

#define PEAR_USE_BFS_QUERY (strategy&PEAR_STRAT_BFS_QUERY)

#define PEAR_COLLECT_CONCURRENCY_STATS (strategy&PEAR_STRAT_CONCURRENCY_STATS)

#define PEAR_USE_RSTAR_LEAF_SELECTION (strategy&PEAR_STRAT_RSTAR_LEAF_SELECTION)

#define PEAR_CHOOSE_SUBTREE_K 8

#define PEAR_TREE_TEMPLATE_DEF template<int max_node_size, int min_node_size, typename Pointtype, long strategy, int max_dram_levels, int max_pmem_level, long max_dram_nodes, int persisting_batch_size>

#define PEAR_TREE_TEMPLATE_ARGUMENTS max_node_size, min_node_size, Pointtype, strategy, max_dram_levels, max_pmem_level, max_dram_nodes, persisting_batch_size

/** STRAT 1 **/

#define PEAR_USE_BACKGROUND_PERSISTING (strategy&PEAR_STRAT_BACKGROUND_PERSISTING)

#define PEAR_USE_MAX_DRAM_LEVELS (strategy&PEAR_STRAT_MAX_DRAM_LEVELS)

#define PEAR_STRAT_LEVEL_BOUND(DRAM_LEVELS) PEAR_STRAT_MAX_DRAM_LEVELS+PEAR_STRAT_BACKGROUND_PERSISTING,DRAM_LEVELS,-1,-1l, -1

#define PEAR_VALIDATE_STRAT_LEVEL_BOUND { \
   static_assert(!PEAR_USE_MAX_DRAM_LEVELS || max_dram_levels > 0, "dram_budget too small;"); \
   static_assert(!PEAR_USE_MAX_DRAM_LEVELS || max_pmem_level == -1, "Cannot set max_pmem_level and dram_budget at the same time"); \
   static_assert(!PEAR_USE_MAX_DRAM_LEVELS || max_dram_nodes == -1, "Cannot set max_dram_nodes and dram_budget at the same time");       \
   static_assert(!PEAR_USE_MAX_DRAM_LEVELS || strategy == PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_MAX_DRAM_LEVELS+PEAR_STRAT_BACKGROUND_PERSISTING, "Unsupported strategy features for strat 1"); \
   static_assert(!PEAR_USE_MAX_DRAM_LEVELS || max_dram_levels >= 2, "Must allow at least the root and level beneath in DRAM"); \
}

#define PEAR_TIME_BACKGROUND_PERSISTER false

#ifdef NDEBUG
#if PEAR_TIME_BACKGROUND_PERSISTER
#pragma message("WARNING: PEAR_TIME_BACKGROUND_PERSISTER enabled")
#endif
#endif

#define PEAR_TIME_RECOVERY false

#ifdef NDEBUG
#if PEAR_TIME_RECOVERY
#pragma message("WARNING: PEAR_TIME_RECOVERY enabled")
#endif
#endif

#define PEAR_DEBUG_OUTPUT_PERSISTER false

#ifdef NDEBUG
#if PEAR_DEBUG_OUTPUT_PERSISTER
#pragma message("WARNING: PEAR_DEBUG_OUTPUT_PERSISTER enabled")
#endif
#endif

#define PEAR_DEBUG_OUTPUT_RECOVERY false

#ifdef NDEBUG
#if PEAR_DEBUG_OUTPUT_RECOVERY
#pragma message("WARNING: PEAR_DEBUG_OUTPUT_RECOVERY enabled")
#endif
#endif
/** END STRAT 1 **/



/** STRAT 2 **/

#define PEAR_USE_PARTIAL_SIBLING_PERSITENCE (strategy&PEAR_STRAT_PARTIAL_SIBLING_LIST_PERSISTENCE)

#define PEAR_STRAT_NODE_BOUND(MAX_DRAM_NODES) PEAR_STRAT_MAX_DRAM_NODES+PEAR_STRAT_PARTIAL_SIBLING_LIST_PERSISTENCE, -1, -1, MAX_DRAM_NODES, 1

#define PEAR_STRAT_NODE_BOUND_BATCHING(MAX_DRAM_NODES, BATCH_SIZE) PEAR_STRAT_MAX_DRAM_NODES+PEAR_STRAT_PARTIAL_SIBLING_LIST_PERSISTENCE, -1, -1, MAX_DRAM_NODES, BATCH_SIZE

/** END STRAT 2 **/


/** STRAT BASIC **/

#define PEAR_USE_MAX_PMEM_LEVEL (strategy&PEAR_STRAT_MAX_PMEM_LEVEL)

#define PEAR_STRAT_BASIC_VERSION(MAX_PMEM_LEVEL) PEAR_STRAT_MAX_PMEM_LEVEL, -1, MAX_PMEM_LEVEL, -1L, -1

#define PEAR_VALIDATE_STRAT_BASIC_VERSION { \
   static_assert(!PEAR_USE_MAX_PMEM_LEVEL || max_dram_levels == -1, "Cannot set dram_budget and max_pmem_level at the same time"); \
   static_assert(!PEAR_USE_MAX_PMEM_LEVEL || max_dram_nodes == -1, "Cannot set max_dram_nodes and max_pmem_level at the same time");       \
   static_assert(!PEAR_USE_MAX_PMEM_LEVEL || (strategy&~(PEAR_STRAT_RSTAR_LEAF_SELECTION+PEAR_STRAT_CONCURRENCY_STATS+PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY+PEAR_STRAT_ENABLE_PREFETCHING+PEAR_STRAT_BFS_QUERY+PEAR_STRAT_COMPLETE_PATH_LOCKING+PEAR_STRAT_QUERY_STATS+PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_ENABLE_SIMD+PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE)) == PEAR_STRAT_MAX_PMEM_LEVEL, "Unsupported strategy features for strat basic version"); \
   static_assert(!PEAR_USE_MAX_PMEM_LEVEL || (strategy&(PEAR_STRAT_COMPLETE_PATH_LOCKING+PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE)) != (PEAR_STRAT_COMPLETE_PATH_LOCKING+PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE), "Complete path locking and old update routine can not be used at the same time"); \
}

/** END STRAT BASIC **/

#define PEAR_IS_P_NODE_POINTER(BRANCH_META_VALUE) (BRANCH_META_VALUE & (((META_DATA_TYPE)1) << (META_DATA_BIT_SIZE-1)))

#define PEAR_SET_A_NODE_POINTER_FLAG_INLINE(BRANCH_META_VALUE) (BRANCH_META_VALUE | (((META_DATA_TYPE)1) << (META_DATA_BIT_SIZE-1)))

#define PEAR_CHECK_BITMAP(MAP_ULONG_VALUE,INDEX)  (MAP_ULONG_VALUE & (((META_DATA_TYPE)1) << INDEX))

#define PEAR_SET_POSITION_INLINE(MAP_ULONG_VALUE,INDEX) (MAP_ULONG_VALUE | (((META_DATA_TYPE)1) << INDEX))

#define PEAR_UNSET_POSITION_INLINE(MAP_ULONG_VALUE,INDEX) (MAP_ULONG_VALUE & ~(((META_DATA_TYPE)1) << INDEX))

#define PEAR_BYTES_BEFORE_BRANCHES 32