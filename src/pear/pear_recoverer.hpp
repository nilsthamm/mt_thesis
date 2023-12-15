#pragma once

#include <macros/macros.hpp>
#include <per_tree_allocator.hpp>
#include <x86intrin.h>
#include <map>

#include "pear_tree.hpp"
#include "custom_stats.hpp"

PEAR_TREE_TEMPLATE_DEF
class PearRecoverer {
public:
    using tree_type = PearTree<PEAR_TREE_TEMPLATE_ARGUMENTS>;

    static tree_type* recover_tree_complete_level() {
        recovery_stats stats;
        if(PEAR_COLLECT_RECOVERY_STATS) {
            stats = recovery_stats();
        }

        // TODO (recovery) indicate that the recovery has started
        std::map<void*,std::shared_ptr<std::vector<typename tree_type::p_node_type*>>> partial_level_parent_ptr_to_new_node;
        std::vector<typename tree_type::p_node_type*> partial_level_level_minus_one_nodes;
        if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE) {
            partial_level_parent_ptr_to_new_node = std::map<void*,std::shared_ptr<std::vector<typename tree_type::p_node_type*>>>();
            partial_level_level_minus_one_nodes = std::vector<typename tree_type::p_node_type*>();
        }

        persistence_info const & p_info = PearTreeAllocatorSingleton::getInstance().get_active_persistence_info();
        bool const partial_level_to_recover = p_info.partial_level_node_list_start != NULL;
        typename tree_type::p_node_type* next_node;
        tree_type* recovered_tree;
        typename tree_type::v_node_type* new_parent;
        uint32_t const max_complete_persisted_level = p_info.max_persisted_level;
        if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE && partial_level_to_recover) {
            next_node = (typename tree_type::p_node_type*) p_info.partial_level_node_list_start;
            recovered_tree = new tree_type(PearTreeAllocatorSingleton::getInstance().get_global_version(), max_complete_persisted_level + 3);
        } else if(PEAR_DISABLE_PARENT_PTR_RECOVERY) {
            next_node = (typename tree_type::p_node_type*) p_info.complete_level_node_list_start;
            recovered_tree = new tree_type(PearTreeAllocatorSingleton::getInstance().get_global_version(), max_complete_persisted_level + 1);
        } else {
            next_node = (typename tree_type::p_node_type*) p_info.complete_level_node_list_start;
            recovered_tree = new tree_type(PearTreeAllocatorSingleton::getInstance().get_global_version(), max_complete_persisted_level + 2);
        }
        size_t complete_level_count;
        size_t partial_level_count;
        if(PEAR_DEBUG_OUTPUT_RECOVERY || PEAR_COLLECT_RECOVERY_STATS) {
            stats.complete_persisted_level = max_complete_persisted_level;
            if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE && partial_level_to_recover) {
                stats.partial_persisted_level = max_complete_persisted_level+1;
            }
            complete_level_count = 0;
            partial_level_count = 0;
        }

        if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE && partial_level_to_recover) {
            if(PEAR_DISABLE_PARENT_PTR_RECOVERY) {
                INVALID_BRANCH_TAKEN_NO_TYPE
            }
            while(next_node) {
                auto &ptr = partial_level_parent_ptr_to_new_node[next_node->_parent_node];
                if(!ptr) {
                    ptr = std::make_shared<std::vector<typename tree_type::p_node_type*>>();
                }
                ptr->push_back(next_node);
                next_node = next_node->_sibling_pointer;
                if(PEAR_DEBUG_OUTPUT_RECOVERY || PEAR_COLLECT_RECOVERY_STATS) {
                    partial_level_count++;
                }
            }

            new_parent = new typename tree_type::v_node_type(max_complete_persisted_level + 2);

            for(auto iter = partial_level_parent_ptr_to_new_node.begin(); ;) {
                if(iter->second->size() < min_node_size) {
                    for(typename tree_type::p_node_type* const node_ptr : *(iter->second)) {
                        partial_level_level_minus_one_nodes.push_back(node_ptr);
                    }
                    iter++;
                    if(iter == partial_level_parent_ptr_to_new_node.end()) {
                        break;
                    }
                    continue;
                }
                Pointtype parent_min_x = std::numeric_limits<Pointtype>::max();
                Pointtype parent_min_y = std::numeric_limits<Pointtype>::max();
                Pointtype parent_max_x = -std::numeric_limits<Pointtype>::max();
                Pointtype parent_max_y = -std::numeric_limits<Pointtype>::max();
                int index_in_new_parent = 0;
                for(typename tree_type::p_node_type* const node_ptr : *(iter->second)) {
                    if(IS_LOCKED(LOAD_META_RELAXED_INLINE(node_ptr->_version_meta_data))) {
                        throw std::runtime_error("Node should not be locked");
                    }
                    Pointtype min_x = std::numeric_limits<Pointtype>::max();
                    Pointtype min_y = std::numeric_limits<Pointtype>::max();
                    Pointtype max_x = -std::numeric_limits<Pointtype>::max();
                    Pointtype max_y = -std::numeric_limits<Pointtype>::max();
                    for(int j = 0; j < max_node_size; j++) {
                        if(PEAR_CHECK_BITMAP(node_ptr->_bitmap, j)) {
                            typename tree_type::node_type::rect_type & branch_rect = node_ptr->_rects[j];
                            min_x = std::min(min_x, branch_rect.x_min);
                            min_y = std::min(min_y, branch_rect.y_min);
                            max_x = std::max(max_x, branch_rect.x_max);
                            max_y = std::max(max_y, branch_rect.y_max);
                        }
                    }
                    new(&(new_parent->_branches[index_in_new_parent])) typename tree_type::node_type::branch_type(reinterpret_cast<long>(node_ptr));
                    new(&(new_parent->_rects[index_in_new_parent])) typename tree_type::node_type::rect_type(min_x, min_y, max_x, max_y);
                    node_ptr->_parent_node = new_parent;
                    PearTreeAllocatorSingleton::getInstance().fence_persist(&(node_ptr->_parent_node), sizeof(typename tree_type::parent_node_type*));
                    parent_min_x = std::min(parent_min_x, min_x);
                    parent_min_y = std::min(parent_min_y, min_y);
                    parent_max_x = std::max(parent_max_x, max_x);
                    parent_max_y = std::max(parent_max_y, max_y);
                    index_in_new_parent++;
                }
                new_parent->_count = index_in_new_parent;
                recovered_tree->insert_at_level(typename tree_type::node_type::rect_type(parent_min_x,parent_min_y,parent_max_x,parent_max_y), new_parent, max_complete_persisted_level + 3);
                if(PEAR_COLLECT_RECOVERY_STATS) {
                    stats.recreated_v_nodes++;
                }
                iter++;
                if(iter != partial_level_parent_ptr_to_new_node.end()) {
                    new_parent->_sibling_pointer = new typename tree_type::v_node_type(max_complete_persisted_level + 2);
                    new_parent = new_parent->_sibling_pointer;
                } else {
                    break;
                }
            }

            for(typename tree_type::p_node_type* const node_ptr : partial_level_level_minus_one_nodes) {
                if(IS_LOCKED(LOAD_META_RELAXED_INLINE(node_ptr->_version_meta_data))) {
                    throw std::runtime_error("Node should not be locked");
                }
                Pointtype min_x = std::numeric_limits<Pointtype>::max();
                Pointtype min_y = std::numeric_limits<Pointtype>::max();
                Pointtype max_x = -std::numeric_limits<Pointtype>::max();
                Pointtype max_y = -std::numeric_limits<Pointtype>::max();
                for(int j = 0; j < max_node_size; j++) {
                    if(PEAR_CHECK_BITMAP(node_ptr->_bitmap, j)) {
                        typename tree_type::node_type::rect_type & branch_rect = node_ptr->_rects[j];
                        min_x = std::min(min_x, branch_rect.x_min);
                        min_y = std::min(min_y, branch_rect.y_min);
                        max_x = std::max(max_x, branch_rect.x_max);
                        max_y = std::max(max_y, branch_rect.y_max);
                    }
                }
                recovered_tree->insert_at_level(typename tree_type::node_type::rect_type(min_x,min_y,max_x,max_y), node_ptr, max_complete_persisted_level + 2);
                if(PEAR_COLLECT_RECOVERY_STATS) {
                    stats.reinserted_p_nodes++;
                }
            }
            next_node = (typename tree_type::p_node_type*) p_info.complete_level_node_list_start;
        }


        std::map<void*,std::shared_ptr<std::vector<typename tree_type::p_node_type*>>> complete_level_parent_ptr_to_new_node = std::map<void*,std::shared_ptr<std::vector<typename tree_type::p_node_type*>>>();
        std::vector<typename tree_type::p_node_type*> complete_level_level_minus_one_nodes = std::vector<typename tree_type::p_node_type*>();



        size_t start_parent_map_build;
        if(PEAR_TIME_RECOVERY) {
            start_parent_map_build = _rdtsc();
        }
        while(next_node) {
            if(PEAR_DEBUG_OUTPUT_RECOVERY || PEAR_COLLECT_RECOVERY_STATS) {
                complete_level_count++;
            }
            if(PEAR_DISABLE_PARENT_PTR_RECOVERY) {
                if(IS_LOCKED(LOAD_META_RELAXED_INLINE(next_node->_version_meta_data))) {
                    throw std::runtime_error("Node should not be locked");
                }
                Pointtype min_x = std::numeric_limits<Pointtype>::max();
                Pointtype min_y = std::numeric_limits<Pointtype>::max();
                Pointtype max_x = -std::numeric_limits<Pointtype>::max();
                Pointtype max_y = -std::numeric_limits<Pointtype>::max();
                for(int j = 0; j < max_node_size; j++) {
                    if(PEAR_CHECK_BITMAP(next_node->_bitmap, j)) {
                        typename tree_type::node_type::rect_type & branch_rect = next_node->_rects[j];
                        min_x = std::min(min_x, branch_rect.x_min);
                        min_y = std::min(min_y, branch_rect.y_min);
                        max_x = std::max(max_x, branch_rect.x_max);
                        max_y = std::max(max_y, branch_rect.y_max);
                    }
                }
                recovered_tree->insert_at_level(typename tree_type::node_type::rect_type(min_x,min_y,max_x,max_y), next_node, max_complete_persisted_level + 1);
                if(PEAR_COLLECT_RECOVERY_STATS) {
                    stats.reinserted_p_nodes++;
                }
            } else {
                auto &ptr = complete_level_parent_ptr_to_new_node[next_node->_parent_node];
                if(!ptr) {
                    ptr = std::make_shared<std::vector<typename tree_type::p_node_type*>>();
                }
                ptr->push_back(next_node);
            }
            next_node = next_node->_sibling_pointer;
        }
        if(PEAR_TIME_RECOVERY) {
            size_t const end_parent_map_build = _rdtsc();
            std::cout << "Parent map build: " << end_parent_map_build - start_parent_map_build << std::endl;
        }

        if(PEAR_DISABLE_PARENT_PTR_RECOVERY) {
            if(PEAR_COLLECT_RECOVERY_STATS) {
                stats.touched_p_nodes = complete_level_count;
                recovered_tree->_recovered_stats = stats;
            }
            return recovered_tree;
        }

        new_parent = new typename tree_type::v_node_type(max_complete_persisted_level + 1);

        size_t start_parent_node_build;
        if(PEAR_TIME_RECOVERY) {
            start_parent_node_build = _rdtsc();
        }
        for(auto iter = complete_level_parent_ptr_to_new_node.begin(); ;) {
            if(PEAR_USE_PARTIAL_SIBLING_PERSITENCE && PearTreeAllocatorSingleton::getInstance().is_pmem_pointer(iter->first)) {
                iter++;
                if(iter == complete_level_parent_ptr_to_new_node.end()) {
                    break;
                }
                continue;
            }
            if(iter->second->size() < min_node_size) {
                for(typename tree_type::p_node_type* const node_ptr : *(iter->second)) {
                    complete_level_level_minus_one_nodes.push_back(node_ptr);
                }
                continue;
            }
            Pointtype parent_min_x = std::numeric_limits<Pointtype>::max();
            Pointtype parent_min_y = std::numeric_limits<Pointtype>::max();
            Pointtype parent_max_x = -std::numeric_limits<Pointtype>::max();
            Pointtype parent_max_y = -std::numeric_limits<Pointtype>::max();
            int index_in_new_parent = 0;
            for(typename tree_type::p_node_type* const node_ptr : *(iter->second)) {
                if(IS_LOCKED(LOAD_META_RELAXED_INLINE(node_ptr->_version_meta_data))) {
                    throw std::runtime_error("Node should not be locked");
                }
                Pointtype min_x = std::numeric_limits<Pointtype>::max();
                Pointtype min_y = std::numeric_limits<Pointtype>::max();
                Pointtype max_x = -std::numeric_limits<Pointtype>::max();
                Pointtype max_y = -std::numeric_limits<Pointtype>::max();
                for(int j = 0; j < max_node_size; j++) {
                    if(PEAR_CHECK_BITMAP(node_ptr->_bitmap, j)) {
                        typename tree_type::node_type::rect_type & branch_rect = node_ptr->_rects[j];
                        min_x = std::min(min_x, branch_rect.x_min);
                        min_y = std::min(min_y, branch_rect.y_min);
                        max_x = std::max(max_x, branch_rect.x_max);
                        max_y = std::max(max_y, branch_rect.y_max);
                    }
                }
                new(&(new_parent->_branches[index_in_new_parent])) typename tree_type::node_type::branch_type(reinterpret_cast<long>(node_ptr));
                new(&(new_parent->_rects[index_in_new_parent])) typename tree_type::node_type::rect_type(min_x, min_y, max_x, max_y);
                node_ptr->_parent_node = new_parent;
                PearTreeAllocatorSingleton::getInstance().fence_persist(&(node_ptr->_parent_node), sizeof(typename tree_type::parent_node_type*));
                parent_min_x = std::min(parent_min_x, min_x);
                parent_min_y = std::min(parent_min_y, min_y);
                parent_max_x = std::max(parent_max_x, max_x);
                parent_max_y = std::max(parent_max_y, max_y);
                index_in_new_parent++;
            }
            new_parent->_count = index_in_new_parent;
            recovered_tree->insert_at_level(typename tree_type::node_type::rect_type(parent_min_x,parent_min_y,parent_max_x,parent_max_y), new_parent, max_complete_persisted_level + 2);
            if(PEAR_COLLECT_RECOVERY_STATS) {
                stats.recreated_v_nodes++;
            }
            iter++;
            if(iter != complete_level_parent_ptr_to_new_node.end()) {
                new_parent->_sibling_pointer = new typename tree_type::v_node_type(max_complete_persisted_level + 1);
                new_parent = new_parent->_sibling_pointer;
            } else {
                break;
            }
        }
        if(PEAR_TIME_RECOVERY) {
            auto const end_parent_node_build = _rdtsc();
            std::cout << "Parent node build: " << end_parent_node_build - start_parent_node_build << std::endl;
        }

        for(typename tree_type::p_node_type* const node_ptr : complete_level_level_minus_one_nodes) {
            if(IS_LOCKED(LOAD_META_RELAXED_INLINE(node_ptr->_version_meta_data))) {
                throw std::runtime_error("Node should not be locked");
            }
            Pointtype min_x = std::numeric_limits<Pointtype>::max();
            Pointtype min_y = std::numeric_limits<Pointtype>::max();
            Pointtype max_x = -std::numeric_limits<Pointtype>::max();
            Pointtype max_y = -std::numeric_limits<Pointtype>::max();
            for(int j = 0; j < max_node_size; j++) {
                if(PEAR_CHECK_BITMAP(node_ptr->_bitmap, j)) {
                    typename tree_type::node_type::rect_type & branch_rect = node_ptr->_rects[j];
                    min_x = std::min(min_x, branch_rect.x_min);
                    min_y = std::min(min_y, branch_rect.y_min);
                    max_x = std::max(max_x, branch_rect.x_max);
                    max_y = std::max(max_y, branch_rect.y_max);
                }
            }
            recovered_tree->insert_at_level(typename tree_type::node_type::rect_type(min_x,min_y,max_x,max_y), new_parent, max_complete_persisted_level + 1);
            if(PEAR_COLLECT_RECOVERY_STATS) {
                stats.reinserted_p_nodes++;
            }
        }
        if(PEAR_DEBUG_OUTPUT_RECOVERY) {
            std::cout << "Recovered nodes: " << complete_level_count << " with " << complete_level_parent_ptr_to_new_node.size() << " parent nodes"
                      << std::endl;
        }
        if(PEAR_COLLECT_RECOVERY_STATS) {
            stats.touched_p_nodes = partial_level_count+complete_level_count;
            stats.partial_level_p_nodes = partial_level_count;
            recovered_tree->_recovered_stats = stats;
        }
        // TODO (recovery) indicate that the recovery process is finished
        return recovered_tree;
    }
};