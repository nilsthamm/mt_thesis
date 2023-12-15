#pragma

#include <macros/macros.hpp>

#include "node.hpp"
#include "background_persister.hpp"
#include "p_node.hpp"
#include "parent_node.hpp"
#include "v_node.hpp"
#include "partial_persister.hpp"
#include "custom_stats.hpp"
#include "search_queue_entry.hpp"
#include "optional_args.hpp"

PEAR_TREE_TEMPLATE_DEF
class PearTree : public ParentNode<PEAR_TREE_TEMPLATE_ARGUMENTS> {
public:

    using node_type = Node<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using p_node_type = PNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using v_node_type = VNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using parent_node_type = ParentNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using background_persister_type = PearBackgroundPersister<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using partial_persister_type = PearPartialPersisterSingleton<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    static constexpr int t_max_node_size = max_node_size;

    static constexpr int max_tree_depth = (max_node_size >= 46) ? 6 : 8;

    node_type* _root;
    PEAR_LEVEL_TYPE _max_level;
    uint32_t const _global_version;
    std::atomic<META_DATA_TYPE> _meta_data;
    std::vector<node_type*> _level_start_nodes;
    PEAR_LEVEL_TYPE const _start_level;
    recovery_stats _recovered_stats;
    query_stats _point_query_stats = query_stats();

    PearTree(uint32_t const global_version, uint32_t const start_level = 0) : _global_version(global_version), _level_start_nodes({}), _start_level(start_level) {
        if(PEAR_USE_MAX_DRAM_LEVELS) {
            PEAR_VALIDATE_STRAT_LEVEL_BOUND
        }
        if(PEAR_USE_MAX_PMEM_LEVEL) {
            PEAR_VALIDATE_STRAT_BASIC_VERSION
        }
        if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE) {
            partial_persister_type::get_instance().reset();
            partial_persister_type::get_instance().initialize_global_version(global_version);
        }
        if(start_level == 0 && (!PEAR_USE_MAX_PMEM_LEVEL || max_pmem_level>=0)) {
            _root = (p_node_type*) PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(p_node_type),alignof(p_node_type)>();
            PearTreeAllocatorSingleton::getInstance().deregister_pointer(_root);
            new(_root) p_node_type(start_level);
            if(start_level == 0) {
                PearTreeAllocatorSingleton::getInstance().set_current_max_pmem_level(0);
            }
        } else {
            _root = (node_type*)PearTreeAllocatorSingleton::getInstance().aligned_allocate_dram_node<sizeof(v_node_type)>();
            new(_root) v_node_type(start_level);
        }
        STORE_META(_meta_data, 0);
        _root->set_parent_node(this);
        _max_level = start_level;
        _level_start_nodes.push_back(_root);
    }

    ~PearTree() {
        if(_root->is_pmem_node()) {
            return;
        }
        if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE) {
            if(PEAR_DEBUG_OUTPUT_PERSISTER) {
                std::cout << "Persisted nodes: " << partial_persister_type::get_instance()._counter << std::endl;
            }
            partial_persister_type::get_instance().reset();
        }
        _root->free_tree_dram();
    }

    static inline int getmax_dram_levels() {
        return max_dram_levels;
    }

    void insert_at_level(Rectangle<Pointtype> const &key, node_type* const value, uint32_t const at_level) {
        while(!try_insert(key,reinterpret_cast<size_t>(value), at_level));
    }

    void insert(Rectangle<Pointtype> const &key, size_t const value, optional_args* const args_ptr) {
#ifdef USE_PMEM_INSTRUCTIONS
        if(PEAR_USE_COMPLETE_PATH_LOCKING) {
            while(!try_insert(key, value, args_ptr));
        } else if(PEAR_USE_OLD_MBR_UPDATE_ROUTINE) {
            while(!try_insert_old_mbr_update(key, value, args_ptr));
        } else {
            while(!try_insert_partial_path_locking(key, value, args_ptr));
        }
#else
#pragma message ("always using PearTree::try_insert")
        while(!try_insert(key, value));
#endif
    }

    bool try_insert_old_mbr_update(Rectangle<Pointtype> const &key, size_t const value, optional_args* const args_ptr) {
        if(_max_level == 0) {
            ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_meta_data, if(!_root->is_leaf()) { if(PEAR_COLLECT_CONCURRENCY_STATS) {args_ptr->aborts_node_changed++; };return false; }; if(PEAR_COLLECT_CONCURRENCY_STATS) {args_ptr->node_wait_counter++; };)
            if(_root->get_level() != 0) {
                RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)
                return false;
            }
            std::atomic<long unsigned int>* temp = &(_meta_data);
            _root->insert_leaf(key, value, &(temp), _global_version, args_ptr);
            RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)
            return true;
        } else {
            META_DATA_TYPE const initial_tree_meta = LOAD_META_INLINE(_meta_data);
            if(IS_LOCKED(initial_tree_meta)) {
                return false;
            }
            return _root->insert_recursive(key, value, _global_version, args_ptr);
        }
    }

    bool try_insert_partial_path_locking(Rectangle<Pointtype> const &key, size_t const value, optional_args* const args_ptr, int32_t const at_level= 0) {


        if(_max_level == at_level) {
            ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_meta_data, if(!_root->is_leaf()) { if(PEAR_COLLECT_CONCURRENCY_STATS) {args_ptr->aborts_node_changed++; };return false; }; if(PEAR_COLLECT_CONCURRENCY_STATS) {args_ptr->node_wait_counter++; };)
            if(_root->get_level() != at_level) {
                RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)
                return false;
            }
            std::atomic<long unsigned int>* temp = &(_meta_data);
            _root->insert_leaf(key, value, &(temp), _global_version, args_ptr);
            RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)
            return true;
        } else {
            // Determine branches WITHOUT locking them
            int array_size = _max_level;
            META_DATA_TYPE const initial_tree_meta = LOAD_META_INLINE(_meta_data);
            if(IS_LOCKED(initial_tree_meta)) {
                return false;
            }
            // TODO static size needs to be addressed in some way
            typename node_type::path_entry_type path_array[max_tree_depth-1] = {};// = new path_array_type[array_size];
            while(!_root->find_insert_path(key, path_array, _global_version, args_ptr, at_level, (PEAR_USE_MAX_DRAM_LEVELS) ? _max_level-max_dram_levels+(int)(_root->get_branch_count()/(float)(max_node_size*0.75)) : INT32_MAX)) {
                if(_max_level != array_size) {
                    return false;
                }
            }

            // Start at level 0 (array[0])
            // while true:
            // Acquire branch lock
            // Check if tree or node (from which level?) meta has changed (if true release all locks and return false)
            // Check if branch needs update
            // If branch needs modification continue
            // If branch needs no modification release this branch lock and update all other mbrs, starting from the top
            // Don't release branch lock at level 1
            // Insert value
            // Break at _level == _max_level

            // TODO adapt to add_at_level
            for(int i = 0; i < array_size; i++) {
                _mm_prefetch(path_array[i].branch, _MM_HINT_T0);
                _mm_prefetch(&(path_array[i].node->_version_meta_data), _MM_HINT_T0);
            }
            for(int i = 0; i < array_size; i++) {
                _mm_prefetch(path_array[i].branch, _MM_HINT_T0);
                _mm_prefetch(&(path_array[i].node->_version_meta_data), _MM_HINT_T0);
                bool node_changed = initial_tree_meta != LOAD_META_INLINE(_meta_data);
                bool acquired_lock = false;
                // If a background persister is used nodes can become invalid if they are the last DRAM level. Since the pointers are invalid after the persist, we have to catch this case without accessing the pointers
                if(!node_changed && ((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && i < array_size - 1 && path_array[i].last_level_before_pmem && PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i + 1].node->_version_meta_data)) != path_array[i + 1].initial_version)) {
                    node_changed = true;
                } else if (!node_changed &&(PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i].node->_version_meta_data)) != path_array[i].initial_version)) {
                    node_changed = true;
                } else if (!node_changed) {
                    // TODO (recovery) add global lock
                    std::atomic<META_DATA_TYPE> &lock_ref = path_array[i].branch->atomic_version;
                    META_DATA_TYPE value_var___ = LOAD_META_RELAXED_INLINE(lock_ref);
                    value_var___ = value_var___&UNLOCK_BIT_MASK;
                    while(!lock_ref.compare_exchange_weak(value_var___, value_var___+1, std::memory_order_acq_rel)) {
                        if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && i < array_size - 1 && path_array[i].last_level_before_pmem && PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i + 1].node->_version_meta_data)) != path_array[i + 1].initial_version) {
                            node_changed = true;
                            if(PEAR_COLLECT_CONCURRENCY_STATS) {
                                args_ptr->aborts_node_changed++;
                            }
                            break;
                        } else if (PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i].node->_version_meta_data)) != path_array[i].initial_version) {
                            node_changed = true;
                            if(PEAR_COLLECT_CONCURRENCY_STATS) {
                                args_ptr->aborts_node_changed++;
                            }
                            break;
                        }
                        if(PEAR_COLLECT_CONCURRENCY_STATS) {
                            args_ptr->branch_wait_counter++;
                        }
                        value_var___ &= UNLOCK_BIT_MASK;
                    }
                    std::atomic_thread_fence(std::memory_order_acquire);
                    if (!node_changed) {
                        acquired_lock = true;
                        META_DATA_TYPE const check_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i].node->_version_meta_data));
                        if(check_version != path_array[i].initial_version) {
                            node_changed = true;
                        }
                    }
                }
                if(node_changed) {
                    if(!acquired_lock) {
                        i--;
                    }
                    // TODO adapt to add_at_level
                    for(; i >=0;i--) {
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i].branch->atomic_version)
                    }
                    return false;
                }
                // Check if Branch needs update
                __m128 const key_vec_min = _mm_set_ps(key.x_min,FLT_MAX,key.y_min,FLT_MAX);
                __m128 const candidate_vec = _mm_loadu_ps((float*)path_array[i].rectangle);
                bool key_fits_mbr = _mm_test_all_ones((__m128i)_mm_cmp_ps(candidate_vec, key_vec_min, _CMP_LE_OQ));
                if(key_fits_mbr) {
                    __m128 const key_vec_max = _mm_set_ps(-FLT_MAX,key.x_max,-FLT_MAX,key.y_max);
                    key_fits_mbr = _mm_test_all_ones((__m128i)_mm_cmp_ps(candidate_vec, key_vec_max, _CMP_GE_OQ));
                }
                if(key_fits_mbr) {
                    // TODO adapt to add_at_level
                    if(i>0) {
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i].branch->atomic_version)
                    }
                    i--;
                    // TODO adapt to add_at_level
                    for(; i > 0;i--) {
                        join(*(path_array[i].rectangle), key, path_array[i].rectangle);
                        if (at_level == 0 && ((PEAR_USE_MAX_PMEM && (i + 1 <= max_pmem_level))) || (at_level > 0 && (path_array[i].node->is_pmem_node()))) {
                            PearTreeAllocatorSingleton::fence_persist(path_array[i].rectangle, sizeof(typename node_type::rect_type));
                        }
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i].branch->atomic_version)
                    }
                    // TODO adapt to add_at_level
                    join(*(path_array[at_level].rectangle), key, path_array[at_level].rectangle);
                    if((at_level == 0 && (!PEAR_USE_MAX_PMEM_LEVEL || max_pmem_level > at_level)) || (PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level > at_level) || (at_level > 0 && path_array[at_level].branch->get_child_node()->is_pmem_node())) {
                        PearTreeAllocatorSingleton::fence_persist(path_array[at_level].rectangle, sizeof(typename node_type::rect_type));
                    }
                    std::atomic<META_DATA_TYPE> * branch_meta = &(path_array[at_level].branch->atomic_version);
                    std::atomic<long unsigned int>* const branch_to_unlock = path_array[at_level].branch->get_child_node()->insert_leaf(key, value, &branch_meta, _global_version, args_ptr);
                    PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((*(branch_to_unlock)))
                    return true;
                }
            }
            // TODO: What is the purpose of the following lines? (Not reached after successful insert since the function returns
            if(initial_tree_meta != LOAD_META_INLINE(_meta_data)) {
                for(int i = array_size-1; i >= 0; i--) {
                    PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i].branch->atomic_version)
                }
                return false;
            } else {
                for(int i = array_size-1; i > 0; i--) {
                    join(*(path_array[i].rectangle), key, path_array[i].rectangle);
                    if (at_level == 0 && ((PEAR_USE_MAX_PMEM && (i + 1 <= max_pmem_level))) || (at_level > 0 && (path_array[i].node->is_pmem_node()))) {
                        PearTreeAllocatorSingleton::fence_persist(path_array[i].rectangle, sizeof(typename node_type::rect_type));
                    }
                    PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i].branch->atomic_version)
                }
                // TODO adapt to add_at_level
                join(*(path_array[at_level].rectangle), key, path_array[at_level].rectangle);
                if((at_level == 0 && (!PEAR_USE_MAX_PMEM_LEVEL || max_pmem_level > at_level)) || (PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level > at_level) || (at_level > 0 && path_array[at_level].branch->get_child_node()->is_pmem_node())) {
                    PearTreeAllocatorSingleton::fence_persist(path_array[at_level].rectangle, sizeof(typename node_type::rect_type));
                }
                std::atomic<META_DATA_TYPE> * branch_meta = &(path_array[at_level].branch->atomic_version);
                std::atomic<long unsigned int>* const branch_to_unlock = path_array[at_level].branch->get_child_node()->insert_leaf(key, value, &branch_meta, _global_version, args_ptr);
                PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((*(branch_to_unlock)))
                return true;
            }
        }
        return false;
    }

    bool try_insert(Rectangle<Pointtype> const &key, size_t const value, optional_args* const args_ptr,  int32_t const at_level=0) {

        if(_max_level == at_level) {
            ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_meta_data, if(!_root->is_leaf()) { if(PEAR_COLLECT_CONCURRENCY_STATS) {args_ptr->aborts_node_changed++; };return false; }; if(PEAR_COLLECT_CONCURRENCY_STATS) {args_ptr->node_wait_counter++; };)
            if(_root->get_level() != at_level) {
                RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)
                return false;
            }
            std::atomic<long unsigned int>* temp = &(_meta_data);
            _root->insert_leaf(key, value, &(temp), _global_version, args_ptr);
            RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)
        } else {
            // Determine branches WITHOUT locking them
            int array_size = _max_level;
            META_DATA_TYPE const initial_tree_meta = LOAD_META_INLINE(_meta_data);
            if(IS_LOCKED(initial_tree_meta)) {
                return false;
            }
            // TODO static size needs to be addressed in some way
            typename node_type::path_entry_type path_array[max_tree_depth-1];// = new path_array_type[array_size];
            int path_lock_track_array[5] = {0};
            while(!_root->find_insert_path(key, path_array, _global_version, args_ptr, at_level, (PEAR_USE_MAX_DRAM_LEVELS) ? _max_level-max_dram_levels+(int)(_root->get_branch_count()/(float)(max_node_size*0.75)) : INT32_MAX)) {
                if(_max_level != array_size) {
                    return false;
                }
            }
            // Lock and update all branches, if a node was reorganized, start again at the node above
            // If no update happen it is save to update the node, since other future reorganizations would correct the branch rect if they cause the insert to choose another path
            for(int i = array_size-1; i >= at_level; i--) {
                _mm_prefetch(path_array[i].branch, _MM_HINT_T0);
                _mm_prefetch(&(path_array[i].node->_version_meta_data), _MM_HINT_T0);
            }
            for(int i = array_size-1; i >= at_level; i--) {
                _mm_prefetch(path_array[i].branch, _MM_HINT_T0);
                _mm_prefetch(&(path_array[i].node->_version_meta_data), _MM_HINT_T0);
                bool node_changed = false;
                // If a background persister is used nodes can become invalid if they are the last DRAM level. Since the pointers are invalid after the persist, we have to catch this case without accessing the pointers
                if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && i < array_size - 1 && path_array[i].last_level_before_pmem && PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i + 1].node->_version_meta_data)) != path_array[i + 1].initial_version) {
                    node_changed = true;
                } else if (PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i].node->_version_meta_data)) != path_array[i].initial_version) {
                    node_changed = true;
                } else {
                    // TODO (recovery) add global lock
                    ACQUIRE_META_ATOMIC_WAIT_COMMANDS(path_array[i].branch->atomic_version,\
                            if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && i < array_size - 1 && path_array[i].last_level_before_pmem && PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i + 1].node->_version_meta_data)) != path_array[i + 1].initial_version) { \
                                                node_changed = true; \
                                                if(PEAR_COLLECT_CONCURRENCY_STATS) { args_ptr->aborts_node_changed++;}; \
                                                break;\
                                            } else if (PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i].node->_version_meta_data)) != path_array[i].initial_version) { \
                                                        node_changed = true; \
                                                if(PEAR_COLLECT_CONCURRENCY_STATS) { args_ptr->aborts_node_changed++;}; \
                                                break; \
                                            }\
                                                if(PEAR_COLLECT_CONCURRENCY_STATS) { args_ptr->branch_wait_counter++;}; )
                    if (!node_changed) {
                        path_lock_track_array[i] = 1;
                        META_DATA_TYPE const check_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(path_array[i].node->_version_meta_data));
                        if(check_version != path_array[i].initial_version) {
                            node_changed = true;
                        }
                    }
                }
                if (node_changed) {
                    if (path_lock_track_array[i]) {
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i].branch->atomic_version)
                        path_lock_track_array[i] = 0;
                    }
                    if (i < array_size - 1 && path_lock_track_array[i + 1]) {
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i + 1].branch->atomic_version)
                        path_lock_track_array[i + 1] = 0;
                    }
                    if (initial_tree_meta != LOAD_META_INLINE(this->_meta_data)) {
                        return false;
                    }
                    while (!_root->find_insert_path(key, path_array, _global_version, args_ptr, at_level, (PEAR_USE_MAX_DRAM_LEVELS) ? _max_level-max_dram_levels+1 : INT32_MAX)) {
                        if (initial_tree_meta != LOAD_META_INLINE(this->_meta_data)) {
                            return false;
                        }
                    }
                    if (initial_tree_meta != LOAD_META_INLINE(this->_meta_data)) {
                        return false;
                    }
                    i = array_size;
                    continue;
                } else {
                    if(i<array_size-1 && path_lock_track_array[i+1]) {
                        join(*(path_array[i + 1].rectangle), key, path_array[i + 1].rectangle);
                        if (at_level == 0 && ((PEAR_USE_MAX_PMEM && (i + 2 <= max_pmem_level))) || (at_level > 0 && (path_array[i + 1].node->is_pmem_node()))) {
                            PearTreeAllocatorSingleton::fence_persist(path_array[i + 1].rectangle, sizeof(typename node_type::rect_type));
                        }
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(path_array[i + 1].branch->atomic_version)
                        path_lock_track_array[i+1] = 0;
                    }
                    if(i == at_level) {
                        // Check if branch points to ANode, if yes compare the current and initial version only proceed if they match
                        // If not check version of PNode if it matches the initial version, cache it and proceed
//                        if(path_array[at_level].node->get_local_version() == path_array[at_level].initial_version) {
                        join(*(path_array[at_level].rectangle), key, path_array[at_level].rectangle);
                        if((at_level == 0 && (!PEAR_USE_MAX_PMEM_LEVEL || max_pmem_level > at_level)) || (PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level > at_level) || (at_level > 0 && path_array[at_level].branch->get_child_node()->is_pmem_node())) {
                            PearTreeAllocatorSingleton::fence_persist(path_array[at_level].rectangle, sizeof(typename node_type::rect_type));
                        }                            std::atomic<META_DATA_TYPE> * branch_meta = &(path_array[at_level].branch->atomic_version);
                        std::atomic<long unsigned int>* const branch_to_unlock = path_array[at_level].branch->get_child_node()->insert_leaf(key, value, &branch_meta, _global_version, args_ptr);
                        PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((*(branch_to_unlock)))

                        path_lock_track_array[at_level] = 0;
                    }
                }
            }
        }
        return true;
    }

    void query_dfs(int const query_type, Rectangle<Pointtype> const &search_space, std::vector<size_t> &result, query_stats * const stats = nullptr) const {
        if(PEAR_COLLECT_QUERY_STATS) {
            while(!_root->query_dfs(query_type, search_space, result, ULONG_MAX, _global_version, stats)) {
                std::atomic_thread_fence(std::memory_order_acquire);
            }
            stats->results_count++;
        } else {
            while(!_root->query_dfs(query_type, search_space, result, ULONG_MAX, _global_version)) {
                std::atomic_thread_fence(std::memory_order_acquire);
            }
        }
    }

    void query(int const query_type, Rectangle<Pointtype> const &search_space, std::set<size_t> &result) {
        PEAR_QUERY_QUEUE_TYPE bfs_queue = PEAR_QUERY_QUEUE_TYPE();
        PEAR_QUERY_DRAM_QUEUE_TYPE* bfs_dram_queue;
        if(PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) {
            bfs_dram_queue = new PEAR_QUERY_DRAM_QUEUE_TYPE();
            bfs_dram_queue->emplace_back(_root->get_ptr_indirection(), _root->get_last_version_before_split(), false);
        } else {
            bfs_queue.emplace_back(_root, _root->get_last_version_before_split(), false);
        }
        int leaf_node_touch_count;
        int inner_node_touch_count;
        if(PEAR_COLLECT_QUERY_STATS) {
            leaf_node_touch_count = 0;
            inner_node_touch_count = 0;
        }
        while (!bfs_queue.empty() || ((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && !bfs_dram_queue->empty())) {
            while((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && !bfs_dram_queue->empty()) {
                node_type* const node = bfs_dram_queue->front().ptr->get_ptr();
                if(PEAR_COLLECT_QUERY_STATS && node->is_leaf()) {
                    leaf_node_touch_count++;
                } else if(PEAR_COLLECT_QUERY_STATS) {
                    inner_node_touch_count++;
                }
                node->query(query_type, search_space, bfs_queue, result, bfs_dram_queue->front().last_save_version, bfs_dram_queue->front().reinserted, _global_version, bfs_dram_queue);
                bfs_dram_queue->front().ptr->deregister_reference();
                bfs_dram_queue->pop_front();
            }
            if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && bfs_queue.empty()) {
                continue;
            }
            if(PEAR_COLLECT_QUERY_STATS && bfs_queue.front().ptr->is_leaf()) {
                leaf_node_touch_count++;
            } else if(PEAR_COLLECT_QUERY_STATS) {
                inner_node_touch_count++;
            }
            bfs_queue.front().ptr->query(0, search_space, bfs_queue, result, bfs_queue.front().last_save_version, bfs_queue.front().reinserted, _global_version, (PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) ? bfs_dram_queue : nullptr);
            bfs_queue.pop_front();
        }
        if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE)) {
            delete bfs_dram_queue;
        }
        if(PEAR_COLLECT_QUERY_STATS) {
            std::cout << "Inner nodes touched: " << inner_node_touch_count << std::endl;
            std::cout << "Leaf nodes touched: " << leaf_node_touch_count << std::endl;
        }
    }

    // Search space is inclusive min/max values
    void equals_query(Rectangle<Pointtype> const &search_space, std::set<size_t> &result) {
        if(PEAR_USE_BFS_QUERY) {
            this->query(1, search_space, result);
        } else {
            INVALID_BRANCH_TAKEN
        }
    }

    // Search space is inclusive min/max values
    void equals_query(Rectangle<Pointtype> const &search_space, std::vector<size_t> &result, query_stats * const stats = nullptr) const {
        this->query_dfs(1, search_space, result, stats);
    }


    // Search space is inclusive min/max values
    void contains_query(Rectangle<Pointtype> const &search_space, std::set<size_t> &result) {
        if(PEAR_USE_BFS_QUERY) {
            this->query(0, search_space, result);
        } else {
            INVALID_BRANCH_TAKEN
        }
    }

    // Search space is inclusive min/max values
    void contains_query(Rectangle<Pointtype> const &search_space, std::vector<size_t> &result) const {
        this->query_dfs(0, search_space, result);
    }

    void remove(Rectangle<Pointtype> const &key, long value) {
        _root->remove(key, value, _global_version);
    }

    bool split_child_blocking(parent_node_type * new_node_ptr, parent_node_type * overflow_node_ptr, typename node_type::rect_type const &new_mbr_overflown_node, typename node_type::rect_type const &new_mbr_new_node, std::atomic<META_DATA_TYPE> * const child_version_meta, PEAR_GLOBAL_VERSION_TYPE const global_version, optional_args* const args_ptr, std::atomic<META_DATA_TYPE>** const branch_lock_to_release=NULL, std::atomic<META_DATA_TYPE>** const branch_lock_new_branch=NULL, bool const lock_branch=false) override {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_meta_data, if (_max_level == _start_level) {break;} else {return false;})
        if(_max_level == 0) {
            STORE_META((*(child_version_meta)),PEAR_SET_NODE_VERSIONS_INLINE(PEAR_SPLIT_PARENT_STARTED_BIT_MASK, global_version))
        }

        PEAR_LEVEL_TYPE new_level = _root->get_level() + 1;
        if(new_level > max_tree_depth) {
            INVALID_BRANCH_TAKEN
        }
        node_type *new_root;
        if((PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level >= (int)new_level)) {
            new_root = (p_node_type*) PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(p_node_type),alignof(p_node_type)>();
            new(new_root) p_node_type(new_level, 1, global_version);
            PearTreeAllocatorSingleton::getInstance().switch_persist_level(new_level,new_root);
            PearTreeAllocatorSingleton::getInstance().deregister_pointer(new_root);
        } else {
            new_root = (node_type*)PearTreeAllocatorSingleton::getInstance().aligned_allocate_dram_node<sizeof(v_node_type)>();
            new(new_root) v_node_type(new_level, 1, _global_version);
        }
        new_root->set_parent_node(this);


        // add old _root to new _root
        new(&(new_root->get_branches_non_const()[0])) typename node_type::branch_type (PEAR_MIN_BRANCH_UNRESERVED_VERSION, _root, false, false);
        new(&(new_root->get_rects_non_const()[0])) typename node_type::rect_type(new_mbr_overflown_node.x_min, new_mbr_overflown_node.y_min, new_mbr_overflown_node.x_max, new_mbr_overflown_node.y_max);

        if(PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level >= (int)new_level) {
            PearTreeAllocatorSingleton::fence_persist(&(new_root->get_branches_non_const()[0]), sizeof(typename node_type::branch_type));
            PearTreeAllocatorSingleton::persist(&(new_root->get_rects_non_const()[0]), sizeof(typename node_type::rect_type));
        } else {
            std::atomic_thread_fence(std::memory_order_release);
        }
        _root->set_parent_node(new_root);

        // add new node to new _root
        auto casted_new_node_ptr = dynamic_cast<node_type *>(new_node_ptr);
        new(&(new_root->get_branches_non_const()[1])) typename node_type::branch_type(PEAR_MIN_BRANCH_UNRESERVED_VERSION, casted_new_node_ptr, new_level == _start_level+1, false);
        if(new_level == _start_level+1) {
            *branch_lock_new_branch = &(new_root->get_branches_non_const()[1].atomic_version);
        }
        new(&(new_root->get_rects_non_const()[1])) typename node_type::rect_type(new_mbr_new_node.x_min, new_mbr_new_node.y_min, new_mbr_new_node.x_max, new_mbr_new_node.y_max);
        if(PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level >= (int)new_level) {
            PearTreeAllocatorSingleton::fence_persist(&(new_root->get_branches_non_const()[1]), sizeof(typename node_type::branch_type));
            PearTreeAllocatorSingleton::persist(&(new_root->get_rects_non_const()[1]), sizeof(typename node_type::rect_type));
        } else {
            std::atomic_thread_fence(std::memory_order_release);
        }
        casted_new_node_ptr->set_parent_node(new_root);
        if(PEAR_USE_MAX_PMEM_LEVEL && max_pmem_level >= (int)new_level) {
            dynamic_cast<p_node_type *>(new_root)->_bitmap = PEAR_SET_POSITION_INLINE(PEAR_SET_POSITION_INLINE(0,0),1);
            PearTreeAllocatorSingleton::fence_persist(&(dynamic_cast<p_node_type *>(new_root)->_bitmap), sizeof(META_DATA_TYPE));
        } else {
            dynamic_cast<v_node_type *>(new_root)->_count = 2;
        }

        _root = new_root;
        _level_start_nodes.push_back(_root);
        _max_level = new_level;
        if(_max_level != _start_level+1) {RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_meta_data)}
        PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(new_root->get_version_lock())
        if(PEAR_USE_MAX_DRAM_LEVELS && PEAR_USE_BACKGROUND_PERSISTING && (int32_t)_max_level - (int32_t)max_dram_levels-1 > PearTreeAllocatorSingleton::getInstance().get_current_max_pmem_level()) {
            auto temp = new background_persister_type(_level_start_nodes[(int32_t)_max_level - (int32_t)max_dram_levels-1],_max_level - max_dram_levels-1, _global_version);
            temp->start();
        } else if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE) {
            partial_persister_type::get_instance().register_new_level(dynamic_cast<v_node_type *>(new_root));
        }
        return true;
    }

    bool merge_child(Pointtype const x_min, Pointtype const y_min, Pointtype const x_max, Pointtype const y_max, parent_node_type * const origin, int const underflown_branch_count, PEAR_GLOBAL_VERSION_TYPE const global_version) {
        return true;
    }

    bool is_pmem_node() const override {
        return false;
    }

    bool is_node() const override {
        return false;
    }

    bool update_mbr(Pointtype const x_min, Pointtype const y_min, Pointtype const x_max, Pointtype const y_max, parent_node_type * const origin, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        return true;
    }

    PEAR_LEVEL_TYPE get_level() const override {
        return _max_level+1;
    }

    PEAR_LEVEL_TYPE test_max_level() {
        return _max_level;
    }

    int test_num_branches(int level)  {
        PearTreeAllocatorSingleton::mfence();
        return this->_root->test_num_branches(level, _max_level);
    }

    static bool check_bitmap_for_index(META_DATA_TYPE const bitmap, int const index) {
        return PEAR_CHECK_BITMAP(bitmap, index);
    }

    int get_dram_node_count() {
        return _root->get_dram_node_count();
    }

    int get_pmem_node_count() {
        return _root->get_pmem_node_count();
    }

    int get_persisted_node_count() {
        if(PEAR_COLLECT_PARTIAL_PERSISTER_STATS) {
            if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE) {
                return partial_persister_type::get_instance().get_total_persisted_nodes();
            } else {
                return _root->get_pmem_node_count()-_root->test_num_branches(1, _max_level);
            }
        } else {
            return -1;
        }
    }

    Pointtype get_avg_mbr_area() const {
        return _root->total_mbr_area()/(Pointtype)_root->total_branch_count();
    }

    Pointtype get_avg_mbr_overlap() const {
        return _root->total_mbr_overlap()/(Pointtype)_root->total_branch_count();
    }

    size_t count_branch_updates() {
        return _root->count_branch_updates();
    }

    size_t count_node_updates() {
        return _root->count_node_updates();
    }

    void print_stats() {
        std::cout << "Printing root MBRs:" << std::endl;
        _root->print_mbrs();
        int num_inner_nodes = 0;
        float sum_areas = 0.0f;
        _root->gather_stats(num_inner_nodes, sum_areas);
        std::cout << std::endl << "RTree stats: " << std::endl
                  << "Number inner nodes: " << num_inner_nodes << std::endl
                  << "Avg MBR size inner nodes: " << sum_areas/(float)num_inner_nodes << std::endl
                ;
    }
};