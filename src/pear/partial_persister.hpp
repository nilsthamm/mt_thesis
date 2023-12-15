#pragma once

#include <thread>
#include <queue>

#include <macros/macros.hpp>

#include "node.hpp"
#include "v_node.hpp"
#include "p_node.hpp"

PEAR_TREE_TEMPLATE_DEF
class VNode;

PEAR_TREE_TEMPLATE_DEF
class PearPartialPersisterSingleton {
public:
    using node_type = Node<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using p_node_type = PNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using v_node_type = VNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;

    PearPartialPersisterSingleton(PearPartialPersisterSingleton const&)               = delete;
    void operator=(PearPartialPersisterSingleton const&)  = delete;

    static PearPartialPersisterSingleton& get_instance()
    {
        static PearPartialPersisterSingleton instance;

        return instance;
    }

    static constexpr int get_num_complete_dram_levels() {
        return 0;
    }


    inline void increase_nodes_to_persist() {
        int current_val_nodes_to_persist = _nodes_to_persist.load(std::memory_order_acquire);
        while(_nodes_to_persist.compare_exchange_weak(current_val_nodes_to_persist, current_val_nodes_to_persist+1, std::memory_order_acq_rel));
        if(current_val_nodes_to_persist >= _batch_size) {
            META_DATA_TYPE current_lock_val = (_concurrency_lock.load(std::memory_order_acquire))&UNLOCK_BIT_MASK;
            if(_concurrency_lock.compare_exchange_weak(current_lock_val, current_lock_val+1, std::memory_order_acq_rel)) {
                persist_next_node();
                current_lock_val++;
                if(!_concurrency_lock.compare_exchange_weak(current_lock_val, current_lock_val+1, std::memory_order_acq_rel)) {
                    INVALID_BRANCH_TAKEN
                }
                return;
            }
        }
        return;
    }

    void dram_node_created() {
        int current_val_not_persisted = _not_persisted_nodes.load(std::memory_order_acquire);
        if(current_val_not_persisted >= max_dram_nodes) {
            increase_nodes_to_persist();
        } else {
            while(_not_persisted_nodes.compare_exchange_weak(current_val_not_persisted, current_val_not_persisted+1, std::memory_order_acq_rel)) {
                if(current_val_not_persisted >= max_dram_nodes) {
                    increase_nodes_to_persist();
                    return;
                }
            }
        }
    }

    void initialize_global_version(uint32_t global_version) {
        _global_version = global_version;
    }

    void register_new_level(v_node_type* const first_node) {
        ACQUIRE_META_ATOMIC(_concurrency_lock)
        if(_next_v_node == NULL) {
            _next_v_node = first_node;
        } else {
            _level_start_nodes.push(first_node);
        }
        RELEASE_META_DATA_LOCK_WITH_INCREASE(_concurrency_lock)
    }

    void reset() {
        _nodes_to_persist = 0;
        _not_persisted_nodes = 0;
        _concurrency_lock = 0;
        _next_v_node = NULL;
        _last_persisted_node = NULL;
        _current_level_first_node = NULL;
        _level_start_nodes = std::queue<v_node_type*>();
        _global_version = -1;
        _counter = 0;
        _total_persisted_nodes = 0;
    }

    int get_total_persisted_nodes() {
        return _total_persisted_nodes;
    }


    int _counter = 0;
    int _total_persisted_nodes = 0;
private:
    std::atomic<int> _not_persisted_nodes;
    std::atomic<int> _nodes_to_persist;
    static constexpr int _batch_size = persisting_batch_size;
    std::atomic<META_DATA_TYPE> _concurrency_lock;
    v_node_type* _next_v_node;
    p_node_type* _last_persisted_node;
    p_node_type* _current_level_first_node;
    std::thread _persit_thread;
    std::queue<v_node_type*> _level_start_nodes;
    int32_t _global_version;
    std::atomic_flag _thread_running = false;

    PearPartialPersisterSingleton() {
        static_assert(!PEAR_USE_PARTIAL_SIBLING_PERSITENCE || max_dram_nodes >= max_node_size, "Must allow at least 2 DRAM levels");
    }

    void persist_task() {
        META_DATA_TYPE old_meta;
        while(_nodes_to_persist.load(std::memory_order_acquire)) {
            v_node_type * parent_node;
            if(!PEAR_USE_BACKGROUND_PERSISTING) {
                ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_next_v_node->_version_meta_data,
                                                  if (PEAR_IS_REORGANIZING(value_var___)) {
                                                      if (PEAR_USE_BACKGROUND_PERSISTING) {
                                                          RELEASE_META_DATA_LOCK_WITH_INCREASE(_concurrency_lock)
                                                      }
                                                      return;
                                                  })
                parent_node = dynamic_cast<v_node_type *>(_next_v_node->_parent_node);
                ACQUIRE_META_ATOMIC_WAIT_COMMANDS(parent_node->_version_meta_data,
                                                  if (PEAR_IS_REORGANIZING(value_var___)) {
                                                      PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(_next_v_node->_version_meta_data)
                                                      if (PEAR_USE_BACKGROUND_PERSISTING) {
                                                          RELEASE_META_DATA_LOCK_WITH_INCREASE(_concurrency_lock)
                                                      }
                                                      return;
                                                  }
                                                  parent_node = dynamic_cast<v_node_type *>(_next_v_node->_parent_node);)
                if(_last_persisted_node) {
                    while (_last_persisted_node->_sibling_pointer) {
                        _last_persisted_node = _last_persisted_node->_sibling_pointer;
                    }
                    ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_last_persisted_node->_version_meta_data,
                                                      if (PEAR_IS_REORGANIZING(value_var___)) {
                                                          PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(_next_v_node->_version_meta_data)
                        PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(parent_node->_version_meta_data)
                                                          if (PEAR_USE_BACKGROUND_PERSISTING) {
                                                              RELEASE_META_DATA_LOCK_WITH_INCREASE(_concurrency_lock)
                                                          }
                                                              return;
                                                      })
                }
                old_meta = LOAD_META_INLINE(_next_v_node->_version_meta_data);
                STORE_META(_next_v_node->_version_meta_data, 1)
            }
            p_node_type *const new_next_node = (p_node_type *) PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(p_node_type), alignof(p_node_type)>();
            if (_last_persisted_node) {
                if(PEAR_USE_BACKGROUND_PERSISTING) {
                    bool locked_last_p_node = false;
                    while (!locked_last_p_node) {
                        while (_last_persisted_node->_sibling_pointer) {
                            _last_persisted_node = _last_persisted_node->_sibling_pointer;
                        }
                        locked_last_p_node = true;
                        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_last_persisted_node->_version_meta_data,
                                                          if (PEAR_IS_REORGANIZING(
                                                                  value_var___)) { locked_last_p_node = false; })
                    }
                }
                // Set _sibling_pointer (_level already set in constructor)
                _last_persisted_node->set_sibling_pointer(new_next_node);
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(_last_persisted_node->_version_meta_data)
                PearTreeAllocatorSingleton::getInstance().deregister_pointer(new_next_node);
            } else {
                _current_level_first_node = new_next_node;
                PearTreeAllocatorSingleton::getInstance().start_partial_level(new_next_node);
            }
            if(PEAR_USE_BACKGROUND_PERSISTING) {
                ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_next_v_node->_version_meta_data,;)
                STORE_META(_next_v_node->_version_meta_data, 1)
            }
            PEAR_LEVEL_TYPE const node_level = _next_v_node->get_level();
            int const next_node_count = _next_v_node->_count;
            for (int i = 0; i < next_node_count; i++) {
                while (IS_LOCKED(LOAD_META_INLINE(_next_v_node->_branches[i].atomic_version))) {
                    PEAR_NODE_VERSION_TYPE const node_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(_next_v_node->_branches[i].get_child_node()->_version_meta_data));
                    if (node_level == 1 && PEAR_IS_REORGANIZING(node_version) &&
                        !PEAR_SPLIT_PARENT_STARTED(node_version)) {
                        break;
                    }
                }
            }

            // Constructor
            new(new_next_node) p_node_type(node_level, 1, _global_version);
            constexpr size_t branches_byte_size = max_node_size *sizeof(typename node_type::branch_type);
            constexpr size_t rect_byte_size = max_node_size * sizeof(typename node_type::rect_type);



            // Bitmap update
            new_next_node->_bitmap = META_DATA_MAX_VALUE >> (META_DATA_BIT_SIZE - next_node_count);

            // Copy _branches + set parent_ptr in children
            static_assert(64==CACHE_LINE_SIZE, "Assumed a cache line size of 64");
            constexpr size_t branches_per_cache_line =CACHE_LINE_SIZE / sizeof(typename node_type::branch_type);
            constexpr int branches_in_first_cache_line = (CACHE_LINE_SIZE - PEAR_BYTES_BEFORE_BRANCHES) / sizeof(typename node_type::branch_type);
            memcpy(&(new_next_node->_branches[0]), &(_next_v_node->_branches[0]), CACHE_LINE_SIZE-PEAR_BYTES_BEFORE_BRANCHES);
            for (int j = 0; j < branches_in_first_cache_line && j < next_node_count; j++) {
                _next_v_node->_branches[j].get_child_node()->set_parent_node(new_next_node);
            }

            for (int i = 1; i * CACHE_LINE_SIZE < (branches_byte_size +PEAR_BYTES_BEFORE_BRANCHES); i++) {
                _mm512_stream_si512(((__m512i *) new_next_node) + i,
                                    *(((__m512i *) _next_v_node) + i));
                for (int j = 0; j < branches_per_cache_line && ((i-1)*branches_per_cache_line + branches_in_first_cache_line + j) < next_node_count; j++) {
                    _next_v_node->_branches[(i-1)*branches_per_cache_line + branches_in_first_cache_line + j].get_child_node()->set_parent_node(new_next_node);
                }
            }
            constexpr size_t size_to_cover = (branches_byte_size + PEAR_BYTES_BEFORE_BRANCHES) %CACHE_LINE_SIZE;
            if (size_to_cover > 0) {
                constexpr size_t start_addr_offset = (branches_byte_size +sizeof(node_type))-size_to_cover;
                memcpy((((char *) new_next_node) + start_addr_offset),
                       (((char *) _next_v_node) + start_addr_offset), size_to_cover);
                PearTreeAllocatorSingleton::fence_persist(
                        (((char *) &(new_next_node->_branches[0])) + start_addr_offset), size_to_cover);
                constexpr int branches_to_cover = size_to_cover/sizeof(typename node_type::branch_type);
                for (int j = branches_to_cover; j > 0 && (max_node_size-j) < next_node_count; j--) {
                    _next_v_node->_branches[max_node_size-j].get_child_node()->set_parent_node(new_next_node);
                }
            }

            // Copy _rects
            for (int j = 0; (j) * CACHE_LINE_SIZE < ((long)rect_byte_size ); j++) {
                _mm512_stream_si512(((__m512i *) &(new_next_node->_rects[0])) + j,
                                    *(((__m512i *) &(_next_v_node->_rects[0])) + j));
            }

            // Set _parent_node and update branch in parent_node
            if(PEAR_USE_BACKGROUND_PERSISTING) {
                parent_node = dynamic_cast<v_node_type *>(_next_v_node->_parent_node);
                ACQUIRE_META_ATOMIC_WAIT_COMMANDS(parent_node->_version_meta_data,
                                                  parent_node = dynamic_cast<v_node_type *>(_next_v_node->_parent_node);)
            }
            new_next_node->set_parent_node(parent_node);
            parent_node->update_branch_pointer(_next_v_node, new_next_node);
            PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(parent_node->_version_meta_data)

            // Reset sibling pointer in case it was overritten by the rect copy
            new_next_node->_sibling_pointer = NULL;

            // select next node
            v_node_type *const old_node = _next_v_node;
            _next_v_node = _next_v_node->_sibling_pointer;
            memset(old_node,0,sizeof(v_node_type));
            _last_persisted_node = new_next_node;


            if(PEAR_COLLECT_PARTIAL_PERSISTER_STATS) {
                _total_persisted_nodes++;
            }
            STORE_META(new_next_node->_version_meta_data, old_meta)
            PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(new_next_node->_version_meta_data)
            if (_next_v_node == NULL) {
                PearTreeAllocatorSingleton::getInstance().switch_persist_level(new_next_node->get_level(),
                                                                               _current_level_first_node);
                _last_persisted_node = NULL;
                _current_level_first_node = NULL;
                _next_v_node = _level_start_nodes.front();
                _level_start_nodes.pop();
                if(PEAR_DEBUG_OUTPUT_PERSISTER) {
                    std::cout << "Persisted level: " << new_next_node->get_level() << " with " << _counter << " nodes" << std::endl;
                    _counter = 0;
                }
            }
            int current_val_nodes_to_persist = _nodes_to_persist.load(std::memory_order_acquire);
            while(_nodes_to_persist.compare_exchange_weak(current_val_nodes_to_persist, current_val_nodes_to_persist-1, std::memory_order_acq_rel));
            if(PEAR_DEBUG_OUTPUT_PERSISTER) {
                _counter++;
            }
        }
        _thread_running.clear(std::memory_order_relaxed);
    }

    void persist_next_node() {
        if(PEAR_USE_BACKGROUND_PERSISTING) {
            if(!_thread_running.test_and_set(std::memory_order_acquire)) {
                _persit_thread = std::thread(&PearPartialPersisterSingleton::persist_task, this);
                _persit_thread.detach();
            }
        } else {
            persist_task();
        }
    }
};