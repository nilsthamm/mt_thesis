#pragma once

#include <iostream>
#include <queue>
#include "basic_node.hpp"
#include "../basic/basic_rtree.hpp"

template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
bool testNodeMBRFit(std::pair<BasicNode<max_node_size,min_node_size, max_pmem_level, Pointtype> const * const, int> const &pair, bool data_branch= false) {
    using node_type = BasicNode<max_node_size,min_node_size, max_pmem_level, Pointtype>;
    bool fit_status = true;
    using rect_type = Rectangle<Pointtype>;
    rect_type const &rect = pair.first->_rects[pair.second];
    Branch<node_type> const &branch = pair.first->_branches[pair.second];
    if(data_branch) {
        throw std::logic_error("Data branches can't be tested");
    } else {
        bool x_min_hit = false;
        bool x_max_hit = false;
        bool y_min_hit = false;
        bool y_max_hit = false;
        for(int i = 0; i< max_node_size;i++) {
            if(BASIC_CHECK_BIT_MAP(branch.get_child_node()->_meta_data, i)) {
                if(branch.get_child_node()->_level > 1 && branch.get_child_node()->_level != branch.get_child_node()->_branches[i].get_child_node()->_level + 1) {
                    std::cerr << "Level misfit between levels " << branch.get_child_node()->_level << "->" << branch.get_child_node()->_branches[i].get_child_node()->_level << std::endl;
                    fit_status = false;
                }
                rect_type const &child_rect = (branch.get_child_node()->_rects[i]);
                if (rect.x_min > child_rect.x_min) {
                    std::cout << "x_min of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too large for ";
                    to_string_cout(branch.get_child_node()->_branches[i], child_rect, branch.get_child_node()->_level == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                if (rect.x_max < child_rect.x_max) {
                    std::cout << "x_max of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too low for ";
                    to_string_cout(branch.get_child_node()->_branches[i], child_rect, branch.get_child_node()->_level == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                if (rect.y_min > child_rect.y_min) {
                    std::cout << "y_min of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too large for ";
                    to_string_cout(branch.get_child_node()->_branches[i], child_rect, branch.get_child_node()->_level == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                if (rect.y_max < child_rect.y_max) {
                    std::cout << "y_max of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too low for ";
                    to_string_cout(branch.get_child_node()->_branches[i], child_rect, branch.get_child_node()->_level == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                x_min_hit = x_min_hit || rect.x_min == child_rect.x_min;
                x_max_hit = x_max_hit || rect.x_max == child_rect.x_max;
                y_min_hit = y_min_hit || rect.y_min == child_rect.y_min;
                y_max_hit = y_max_hit || rect.y_max == child_rect.y_max;
            }
        }
        if(!x_min_hit) {
            std::cout << "x_min too low for ";
            to_string_cout(branch, rect);
            std::cout << std::endl;
            fit_status = false;
        }
        if(!x_max_hit) {
            std::cout << "x_max too large for ";
            to_string_cout(branch, rect);
            std::cout << std::endl;
            fit_status = false;
        }
        if(!y_min_hit) {
            std::cout << "y_min too low for ";
            to_string_cout(branch, rect);
            std::cout << std::endl;
            fit_status = false;
        }
        if(!y_max_hit) {
            std::cout << "y_max too large for ";
            to_string_cout(branch, rect);
            std::cout << std::endl;
            fit_status = false;
        }
    }
    return fit_status;
}

template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
bool test_tree_MBR_fit(BasicRTree<max_node_size, min_node_size,max_pmem_level, Pointtype> const &tree) {
    using node_type = BasicNode<max_node_size,min_node_size, max_pmem_level, Pointtype>;
    bool tree_fit = true;
    std::cout << std::endl << std::endl << std::endl;
    std::cout << "----- START MBR FIT TEST -----" << std::endl;
    // TODO(RTree interface)
    std::queue<std::pair<node_type const * const, int>> bfs_queue = std::queue<std::pair<node_type const * const, int>>();
    bool level_status = true;
    for(int i =0; i< max_node_size;i++) {
        if(BASIC_CHECK_BIT_MAP(tree._root->_meta_data, i)) {
            bfs_queue.emplace(tree._root, i);
        }
    }
    int curr_level = tree._max_level + 1;
    int branches_level_1 = -1;
    // TODO(RTree interface)
    while (!bfs_queue.empty()) {
        std::pair<node_type const * const, int> pair = bfs_queue.front();
        Branch<node_type> const &branch = pair.first->_branches[pair.second];
        if(curr_level > 1 || branches_level_1 > 0) {
            if (curr_level > branch.get_child_node()->_level + 1) {
                if (level_status && curr_level < tree._max_level + 1) {
                    std::cout << "Branches from level " << curr_level << "->" << curr_level-1 << " ok" << std::endl;
                } else if(curr_level < tree._max_level + 1) {
                    std::cout << "MBR misfit in level " << curr_level << "->" << curr_level-1 << std::endl;
                    tree_fit = false;
                }
                curr_level = branch.get_child_node()->_level + 1;
                level_status = true;
                if(curr_level == 1) {
                    branches_level_1 = bfs_queue.size();
                }
            }
            level_status = testNodeMBRFit(pair) && level_status;
            for(int i = 0; i< max_node_size;i++) {
                if(BASIC_CHECK_BIT_MAP(branch.get_child_node()->_meta_data, i)) {
                    bfs_queue.emplace(branch.get_child_node(),i);
                }
            }
        }
        if(curr_level == 1) {
            branches_level_1--;
        }
        bfs_queue.pop();
    }
    if (level_status) {
        std::cout << "Branches from level 1->0 ok" << std::endl;
    } else {
        std::cout << "MBR misfit in level 1->0" << std::endl;
        tree_fit = false;
    }
    std::cout << "----- MBR FIT TEST DONE -----" << std::endl;
    return tree_fit;
}

