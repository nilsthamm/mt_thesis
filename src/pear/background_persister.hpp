#pragma once

#include <chrono>
#include <thread>

#include <macros/macros.hpp>

#include "node.hpp"
#include "v_node.hpp"
#include "p_node.hpp"


PEAR_TREE_TEMPLATE_DEF
class PearBackgroundPersister {
public:
    using node_type = Node<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using p_node_type = PNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using v_node_type = VNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using time_point_type = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;


    PearBackgroundPersister(node_type* const level_start_node, PEAR_LEVEL_TYPE const level_to_persist, PEAR_GLOBAL_VERSION_TYPE const global_version) : _level_start_node(level_start_node), _level_to_persist(level_to_persist), _global_version(global_version) {
        STORE_META(_thread_running, false)
        PearTreeAllocatorSingleton::sfence();
    }

    void persist_task() {
        if(PEAR_DEBUG_OUTPUT_PERSISTER) {
            std::cout << "Starting to persist level " << _level_to_persist << std::endl;
        }
        void* first_p_node;

        int counter = 0;
        time_point_type t1;
        if(PEAR_TIME_BACKGROUND_PERSISTER) {
            t1 = std::chrono::high_resolution_clock::now();
        }
        if(_level_start_node->get_type() == node_type::node_types::VNodeType) {
            v_node_type* next_node = dynamic_cast<v_node_type*>(_level_start_node);
            v_node_type* parent_node;
            p_node_type* new_next_node = (p_node_type*)PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(p_node_type) , alignof(p_node_type)>();
            PearTreeAllocatorSingleton::getInstance().deregister_pointer(new_next_node);
            first_p_node = new_next_node;
            while(true) {
                ACQUIRE_META_ATOMIC(next_node->_version_meta_data)
                META_DATA_TYPE const old_meta = LOAD_META_INLINE(next_node->_version_meta_data);
                STORE_META(next_node->_version_meta_data, 1)
                for (int i = 0; i < next_node->_count; i++) {
                    while (IS_LOCKED(LOAD_META_INLINE(next_node->_branches[i].atomic_version))) {
                        PEAR_NODE_VERSION_TYPE const node_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(next_node->_branches[i].get_child_node()->_version_meta_data));
                        if(_level_to_persist == 1 && PEAR_IS_REORGANIZING(node_version) && !PEAR_SPLIT_PARENT_STARTED(node_version)) {
                            break;
                        }
                    }
                }

                // Constructor
                new(new_next_node) p_node_type(_level_to_persist, 1, _global_version);
                constexpr size_t branches_byte_size = max_node_size *sizeof(typename node_type::branch_type);
                constexpr size_t rect_byte_size = max_node_size * sizeof(typename node_type::rect_type);

                // Set _bitmap
                int const next_node_count = next_node->_count;
                new_next_node->_bitmap = META_DATA_MAX_VALUE >> (META_DATA_BIT_SIZE - next_node_count);
                PearTreeAllocatorSingleton::fence_persist(((char *) &(new_next_node->_level)), sizeof(new_next_node->_level)+
                                                                                               sizeof(new_next_node->_bitmap ));

                // Copy _branches + set parent_ptr in children
                static_assert(64==CACHE_LINE_SIZE, "Assumed a cache line size of 64");
                constexpr size_t branches_per_cache_line =CACHE_LINE_SIZE / sizeof(typename node_type::branch_type);
                constexpr int branches_in_first_cache_line = (CACHE_LINE_SIZE - PEAR_BYTES_BEFORE_BRANCHES) / sizeof(typename node_type::branch_type);
                memcpy(&(new_next_node->_branches[0]), &(next_node->_branches[0]), CACHE_LINE_SIZE-PEAR_BYTES_BEFORE_BRANCHES);
                for (int j = 0; j < branches_in_first_cache_line && j < next_node_count; j++) {
                    next_node->_branches[j].get_child_node()->set_parent_node(new_next_node);
                }

                for (int i = 1; i * CACHE_LINE_SIZE < (branches_byte_size +PEAR_BYTES_BEFORE_BRANCHES); i++) {
                    _mm512_stream_si512(((__m512i *) new_next_node) + i,
                                        *(((__m512i *) next_node) + i));
                    for (int j = 0; j < branches_per_cache_line && ((i-1)*branches_per_cache_line + branches_in_first_cache_line + j) < next_node_count; j++) {
                        next_node->_branches[(i-1)*branches_per_cache_line + branches_in_first_cache_line + j].get_child_node()->set_parent_node(new_next_node);
                    }
                }
                constexpr size_t size_to_cover = (branches_byte_size + PEAR_BYTES_BEFORE_BRANCHES) %CACHE_LINE_SIZE;
                if (size_to_cover > 0) {
                    constexpr size_t start_addr_offset = (branches_byte_size +sizeof(node_type))-size_to_cover;
                    memcpy((((char *) new_next_node) + start_addr_offset),
                           (((char *) next_node) + start_addr_offset), size_to_cover);
                    PearTreeAllocatorSingleton::fence_persist(
                            (((char *) &(new_next_node->_branches[0])) + start_addr_offset), size_to_cover);
                    constexpr int branches_to_cover = size_to_cover/sizeof(typename node_type::branch_type);
                    for (int j = branches_to_cover; j > 0 && (max_node_size-j) < next_node_count; j--) {
                        next_node->_branches[max_node_size-j].get_child_node()->set_parent_node(new_next_node);
                    }
                }





                // Copy _rects
                for (int j = 0; (j) * CACHE_LINE_SIZE < ((long)rect_byte_size ); j++) {
                    _mm512_stream_si512(((__m512i *) &(new_next_node->_rects[0])) + j,
                                        *(((__m512i *) &(next_node->_rects[0])) + j));
                }

                // Set _parent_node and update branch in parent_node (_level already set in constructor)
                parent_node = dynamic_cast<v_node_type *>(next_node->_parent_node);
                ACQUIRE_META_ATOMIC_WAIT_COMMANDS(parent_node->_version_meta_data,
                                                  parent_node = dynamic_cast<v_node_type *>(next_node->_parent_node);)
                new_next_node->set_parent_node(parent_node);
                parent_node->update_branch_pointer(next_node, new_next_node);
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(parent_node->_version_meta_data)

                // Set _sibling_pointer
                v_node_type *const old_node = next_node;
                next_node = next_node->_sibling_pointer;
                if(next_node != NULL) {
                    new_next_node->_sibling_pointer = (p_node_type *) (p_node_type*)PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(p_node_type) , alignof(p_node_type)>();
                    PearTreeAllocatorSingleton::getInstance().deregister_pointer(new_next_node->_sibling_pointer);
                } else {
                    // Reset sibling pointer in case it was overritten by the rect copy
                    new_next_node->_sibling_pointer = NULL;
                }

                // Clean-up
//                delete old_node;
                STORE_META(new_next_node->_version_meta_data, old_meta)
//                old_node->_ptr_indirection->exchange_ptr(new_next_node);
//                old_node->_ptr_indirection = nullptr;
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(new_next_node->_version_meta_data)
                if (next_node == NULL) {
                    break;
                }
                new_next_node = new_next_node->_sibling_pointer;
                if(PEAR_DEBUG_OUTPUT_PERSISTER) {
                    counter++;
                }
            }
        } else if(_level_start_node->get_type() == node_type::node_types::ANodeType) {
            INVALID_BRANCH_TAKEN
        } else {
            throw std::runtime_error(std::string("[") + std::string(__FILE__) + std::string(":") + std::string(xstr_macro(__LINE__)) + std::string("] ") + std::string("Invalid branch taken in ") + std::string(xstr_macro(__FUNCTION__)) + std::string(" on an object of type ") + std::string(typeid(*this).name()));
        }

        if(PEAR_TIME_BACKGROUND_PERSISTER) {
            auto t2 = std::chrono::high_resolution_clock::now();
            auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1);
            PearTreeAllocatorSingleton::getInstance().switch_persist_level(_level_to_persist,first_p_node);
            std::cout << "Persisted level " << _level_to_persist << " nodes in " << ms_int.count() << "ms" << std::endl;
        }
        if(PEAR_DEBUG_OUTPUT_PERSISTER) {
            std::cout << "Persisted level " << _level_to_persist << " with " << counter << " nodes" << std::endl;
        }
    }

    void start() {
        _persit_thread = std::thread(&PearBackgroundPersister::persist_task, this);
        _persit_thread.detach();
    }

    std::thread const & get_const_thread_ref() {
        return _persit_thread;
    }

    std::thread & get_thread_ref() {
        return _persit_thread;
    }

    bool is_running() {
        return LOAD_META_INLINE(_thread_running);
    }

protected:

    node_type* _level_start_node;
    PEAR_LEVEL_TYPE _level_to_persist;
    std::thread _persit_thread;
    std::atomic_bool _thread_running;
    PEAR_GLOBAL_VERSION_TYPE _global_version;
};