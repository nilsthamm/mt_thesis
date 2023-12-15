#pragma once

#include <macros/macros.hpp>
#include <iostream>
#include <queue>

#include "rectangle.hpp"
#include "branch.hpp"

template<int max_node_size, typename Tree, typename Node, typename Pointtype>
bool test_universal_NodeMBRFit(std::pair<Node const * const, int> const &pair, bool data_branch= false) {
    bool fit_status = true;
    using rect_type = Rectangle<Pointtype>;
    rect_type const &rect = pair.first->get_rects()[pair.second];
    Branch<Node> const &branch = pair.first->get_branches()[pair.second];
    if(data_branch) {
        throw std::logic_error("Data branches can't be tested");
    } else {
        bool x_min_hit = false;
        bool x_max_hit = false;
        bool y_min_hit = false;
        bool y_max_hit = false;
        int branch_count = branch.get_child_node()->get_branch_count();
        int count = 0;
        for(int i = 0; i< max_node_size && count < branch_count;i++) {
            if(!branch.get_child_node()->is_pmem_node() || Tree::check_bitmap_for_index(branch.get_child_node()->get_bitmap(), i)) {
                if(branch.get_child_node()->get_level() > 1 && branch.get_child_node()->get_level() != branch.get_child_node()->get_branches()[i].get_child_node()->get_level() + 1) {
                    std::cerr << "Level misfit between levels " << branch.get_child_node()->get_level() << "->" << branch.get_child_node()->get_branches()[i].get_child_node()->get_level() << std::endl;
                    fit_status = false;
                }
                rect_type const &child_rect = (branch.get_child_node()->get_rects()[i]);
                if (rect.x_min > child_rect.x_min) {
                    std::cout << "x_min of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too large for ";
                    to_string_cout(branch.get_child_node()->get_branches()[i], child_rect, branch.get_child_node()->get_level() == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                if (rect.x_max < child_rect.x_max) {
                    std::cout << "x_max of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too low for ";
                    to_string_cout(branch.get_child_node()->get_branches()[i], child_rect, branch.get_child_node()->get_level() == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                if (rect.y_min > child_rect.y_min) {
                    std::cout << "y_min of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too large for ";
                    to_string_cout(branch.get_child_node()->get_branches()[i], child_rect, branch.get_child_node()->get_level() == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                if (rect.y_max < child_rect.y_max) {
                    std::cout << "y_max of: ";
                    to_string_cout(branch, rect);
                    std::cout << " too low for ";
                    to_string_cout(branch.get_child_node()->get_branches()[i], child_rect, branch.get_child_node()->get_level() == 0);
                    std::cout << " at: " << i << std::endl;
                    fit_status = false;
                }
                x_min_hit = x_min_hit || rect.x_min == child_rect.x_min;
                x_max_hit = x_max_hit || rect.x_max == child_rect.x_max;
                y_min_hit = y_min_hit || rect.y_min == child_rect.y_min;
                y_max_hit = y_max_hit || rect.y_max == child_rect.y_max;
                count++;
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

template<int max_node_size, typename Tree, typename Node, typename Pointtype>
bool test_universal_tree_MBR_fit(Tree const &tree, bool const success_output=true) {
    bool tree_fit = true;
    if(success_output) {
        std::cout << std::endl << std::endl << std::endl;
        std::cout << "----- START MBR FIT TEST -----" << std::endl;
    }
    // TODO(RTree interface)
    std::queue<std::pair<Node const * const, int>> bfs_queue = std::queue<std::pair<Node const * const, int>>();
    bool level_status = true;
    int branch_count_root = tree._root->get_branch_count();
    int count_root = 0;
    for(int i = 0; i< max_node_size && count_root < branch_count_root ;i++) {
        if(!tree._root->is_pmem_node() || Tree::check_bitmap_for_index(tree._root->get_bitmap(), i)) {
            bfs_queue.emplace(tree._root, i);
            count_root++;
        }
    }
    int curr_level = tree._max_level + 1;
    int branches_level_1 = -1;
    // TODO(RTree interface)
    while (!bfs_queue.empty()) {
        std::pair<Node const * const, int> pair = bfs_queue.front();
        Branch<Node> const &branch = pair.first->get_branches()[pair.second];
        if(curr_level > 1 || branches_level_1 > 0) {
            if (curr_level > branch.get_child_node()->get_level() + 1) {
                if (success_output && level_status && curr_level < tree._max_level + 1) {
                    std::cout << "Branches from level " << curr_level << "->" << curr_level-1 << " ok" << std::endl;
                } else if(!level_status && curr_level < tree._max_level + 1) {
                    std::cout << "MBR misfit in level " << curr_level << "->" << curr_level-1 << std::endl;
                    tree_fit = false;
                }
                curr_level = branch.get_child_node()->get_level() + 1;
                level_status = true;
                if(curr_level == 1) {
                    branches_level_1 = bfs_queue.size();
                }
            }
            level_status = test_universal_NodeMBRFit<max_node_size, Tree, Node, Pointtype>(pair) && level_status;
            int count = 0;
            int branch_count = branch.get_child_node()->get_branch_count();
            for(int i = 0; i< max_node_size && count < branch_count ;i++) {
                if(!branch.get_child_node()->is_pmem_node() || Tree::check_bitmap_for_index(branch.get_child_node()->get_bitmap(), i)) {
                    bfs_queue.emplace(branch.get_child_node(),i);
                    count++;
                }
            }
        }
        if(curr_level == 1) {
            branches_level_1--;
        }
        bfs_queue.pop();
    }
    if (success_output && level_status) {
        std::cout << "Branches from level 1->0 ok" << std::endl;
    } else if(!level_status) {
        std::cout << "MBR misfit in level 1->0" << std::endl;
        tree_fit = false;
    }
    if(success_output) {
        std::cout << "----- MBR FIT TEST DONE -----" << std::endl;
    }
    return tree_fit;
}

