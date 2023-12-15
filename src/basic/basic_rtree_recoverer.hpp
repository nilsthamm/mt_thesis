#pragma once

#include "basic_rtree.hpp"
#include "basic_node.hpp"
#include "macros/macros.hpp"


template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
class BasicRTreeRecoverer {
    using tree_type = BasicRTree<max_node_size, min_node_size, max_pmem_level, Pointtype>;
    using node_type = BasicNode<max_node_size, min_node_size, max_pmem_level, Pointtype>;
    using rect_type = Rectangle<Pointtype>;
public:

    tree_type* recover_tree_object() {
        std::map<void*,std::vector<node_type*>> parent_ptr_to_new_node = std::map<void*,std::vector<node_type*>>();
        linked_node_list_element* current_element = PearTreeAllocatorSingleton::getInstance().get_linked_node_list_memory();
        do {
            node_type* const node = (node_type *)(current_element->element_ptr);
            // TODO std::map insert_or_assign
            if(!parent_ptr_to_new_node.contains(node->_parent_ptr)) {
                parent_ptr_to_new_node.insert(std::make_pair<void*,std::vector<node_type*>>(node->_parent_ptr, {}));
            }
            parent_ptr_to_new_node.at(node->_parent_ptr).push_back(node);
            current_element = current_element->next_list_element;
        } while(current_element != NULL);
        std::vector<std::pair<rect_type,node_type*>> new_parent_vector = std::vector<std::pair<rect_type,node_type*>>();
        for(auto iter = parent_ptr_to_new_node.begin(); iter != parent_ptr_to_new_node.end(); iter++) {
            Pointtype parent_min_x = std::numeric_limits<Pointtype>::max();
            Pointtype parent_min_y = std::numeric_limits<Pointtype>::max();
            Pointtype parent_max_x = -std::numeric_limits<Pointtype>::max();
            Pointtype parent_max_y = -std::numeric_limits<Pointtype>::max();
            int index_in_new_parent = 0;
            node_type* new_parent = new node_type(max_pmem_level+1,NULL,4);
            std::bitset<META_DATA_BIT_SIZE> new_node_node_bitset = std::bitset<META_DATA_BIT_SIZE>(BASIC_SET_GLOBAL_VERSION(4, PearTreeAllocatorSingleton::getInstance().get_global_version()));
            for(node_type* const node_ptr : iter->second) {
                Pointtype min_x = std::numeric_limits<Pointtype>::max();
                Pointtype min_y = std::numeric_limits<Pointtype>::max();
                Pointtype max_x = -std::numeric_limits<Pointtype>::max();
                Pointtype max_y = -std::numeric_limits<Pointtype>::max();
                LOAD_META_CONST(node_ptr->_meta_data, node_meta)
                for(int j = 0; j < max_node_size; j++) {
                    if(BASIC_CHECK_BIT_MAP(node_meta, j)) {
                        rect_type& branch_rect = node_ptr->_rects[j];
                        min_x = std::min(min_x, branch_rect.x_min);
                        min_y = std::min(min_y, branch_rect.y_min);
                        max_x = std::max(max_x, branch_rect.x_max);
                        max_y = std::max(max_y, branch_rect.y_max);
                    }
                }
                new(&(new_parent->_branches[index_in_new_parent])) typename node_type::branch_type(reinterpret_cast<long>(node_ptr));
                new(&(new_parent->_rects[index_in_new_parent])) typename node_type::rect_type(min_x, min_y, max_x, max_y);
                node_ptr->_parent_ptr = new_parent;
                PearTreeAllocatorSingleton::getInstance().persist(&(node_ptr->_parent_ptr), sizeof(typename node_type::parent_type*));
                parent_min_x = std::min(parent_min_x, min_x);
                parent_min_y = std::min(parent_min_y, min_y);
                parent_max_x = std::max(parent_max_x, max_x);
                parent_max_y = std::max(parent_max_y, max_y);
                new_node_node_bitset[index_in_new_parent + BASIC_META_DATA_RESERVED_BITS] = 1;
                index_in_new_parent++;
            }
            STORE_META_RELAXED(new_parent->_meta_data, new_node_node_bitset.to_ulong()+4);
            new_parent_vector.emplace_back(rect_type(parent_min_x,parent_min_y,parent_max_x,parent_max_y),new_parent);
        }
        tree_type* new_tree = new tree_type(PearTreeAllocatorSingleton::getInstance().get_global_version(), max_pmem_level + 2);
        for(auto const pair : new_parent_vector) {
            new_tree->insert_at_level(pair.first, pair.second, max_pmem_level+1);
        }
        PearTreeAllocatorSingleton::getInstance().sfence();
        return new_tree;
    }

    static tree_type* recover_tree_complete_level() {
        return NULL;
    }


};
