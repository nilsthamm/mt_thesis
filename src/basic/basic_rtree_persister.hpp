#pragma once

#include <map>

#include "per_tree_allocator.hpp"
#include "macros/macros.hpp"

class BasicRTreePersisterSingleton {
public:
    BasicRTreePersisterSingleton(BasicRTreePersisterSingleton const&)               = delete;
    void operator=(BasicRTreePersisterSingleton const&)  = delete;

    ~BasicRTreePersisterSingleton() {
        // TODO handle clean shut-down
    }

    static BasicRTreePersisterSingleton& getInstance()
    {
        static BasicRTreePersisterSingleton instance;

        return instance;
    }

    void initialize_max_pmem_level_node_list(void* node) {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_atomic_lock, throw std::runtime_error("BasicRTreePersisterSingleton::initialize_max_pmem_level_node_list() shouldn't be called from multiple threads");)
        _current_list_pointer = PearTreeAllocatorSingleton::getInstance().get_linked_node_list_memory();
        new(_current_list_pointer) linked_node_list_element(node,NULL);
        PearTreeAllocatorSingleton::getInstance().persist_fence(_current_list_pointer, sizeof(linked_node_list_element));
        _max_pmem_level_nodes.emplace(node,_current_list_pointer);
        _current_list_pointer++;
        RELEASE_META_DATA_LOCK_WITH_INCREASE(_atomic_lock)
    }

    void register_max_pmem_level_node(void* left_sibling, void* new_node) {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_atomic_lock, ;)
        linked_node_list_element* sibling_element = _max_pmem_level_nodes.at(left_sibling);
        if(sibling_element->next_list_element != NULL) {
            new(_current_list_pointer) linked_node_list_element(new_node,sibling_element->next_list_element);
        } else {
            new(_current_list_pointer) linked_node_list_element(new_node,NULL);
        }
        sibling_element->next_list_element = _current_list_pointer;
        PearTreeAllocatorSingleton::getInstance().persist(sibling_element, sizeof(linked_node_list_element));
        PearTreeAllocatorSingleton::getInstance().persist_fence(_current_list_pointer, sizeof(linked_node_list_element));
        _max_pmem_level_nodes.emplace(new_node,_current_list_pointer);
        _current_list_pointer++;
        RELEASE_META_DATA_LOCK_WITH_INCREASE(_atomic_lock)
    }

private:
    std::map<void*,linked_node_list_element*> _max_pmem_level_nodes = std::map<void*,linked_node_list_element*>();
    linked_node_list_element* _current_list_pointer = NULL;
    std::atomic<META_DATA_TYPE> _atomic_lock;
    BasicRTreePersisterSingleton() {

    }
};