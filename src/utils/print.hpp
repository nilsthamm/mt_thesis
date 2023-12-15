#pragma once

#include "rtree.hpp"
#include <memory>
#include <queue>
#include <iostream>
#include "basic_node.hpp"

template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
void print_node(BasicNode<max_node_size, min_node_size,max_pmem_level, Pointtype>* const node_ptr, bool inner_node= true) {
    std::cout << "<<< ID:  " << (long)node_ptr << " ";
    for(int i = 0; i < max_node_size-1; i++) {
        if(BASIC_CHECK_BIT_MAP(node_ptr->_meta_data, i)) {
            Rectangle mbr = ((*node_ptr)._branches[i].m_rect);
            std::cout << "| Low Point: (" << mbr.x_min << ", " << mbr.y_min << ") High Point: (" << mbr.x_max << ", "
                      << mbr.y_max << ") ";
            if (inner_node) {
                std::cout << "To: " << (long) ((*node_ptr)._branches[i].m_child);
            } else {
                std::cout << "Value: " << ((*node_ptr)._branches[i].m_data);
            }
            std::cout << " |   ";
        }
    }
    if(BASIC_CHECK_BIT_MAP(node_ptr->_meta_data, max_node_size - 1)) {
        Rectangle mbr = ((*node_ptr)._branches[max_node_size - 1].m_rect);
        std::cout << "| Low Point: (" << mbr.x_min << ", " << mbr.y_min << ") High Point: (" << mbr.x_max << ", "
                  << mbr.y_max << ") ";
        if (inner_node) {
            std::cout << "To: " << (long) ((*node_ptr)._branches[max_node_size - 1].m_child);
        } else {
            std::cout << "Value: " << ((*node_ptr)._branches[max_node_size - 1].m_data);
        }
        std::cout << " |   ";
    }
    std::cout << ">>>    ";
}

template<class RT, int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
void print_tree(RT const &tree) {
    // TODO(RTree interface)
    std::queue<BasicNode<max_node_size, min_node_size,max_pmem_level, Pointtype>*> bfs_queue = std::queue<BasicNode<max_node_size, min_node_size,max_pmem_level, Pointtype>*>();
    // TODO(RTree interface)
    bfs_queue.push(tree._root);
    int curr_level = tree._max_level + 1;
//    std::string empty_nodes = "";
    while (!bfs_queue.empty()) {
        BasicNode<max_node_size, min_node_size,max_pmem_level, Pointtype>* node = bfs_queue.front();
        if(curr_level > node->_level) {
//            std::cout << empty_nodes;
//            empty_nodes = "";
            std::cout << std::endl;
            curr_level = node->_level;
            std::cout << "Level " << curr_level << ": ";
        }
        bool inner_node = node->_level > 0;
        print_node(node, inner_node);
        std::stringstream stream;
//        stream << empty_nodes;
//        for(int i = 0; i < (max_node_size-node->size);i++) {
//            stream << "<<< >>>    ";
//        }
//        empty_nodes = stream.str();
        stream.clear();
        if(inner_node) {
            for(int i = 0; i < max_node_size; i++) {
                if(BASIC_CHECK_BIT_MAP(node->_meta_data, i)) {
                    bfs_queue.push(node->_branches[i].m_child);
                }
            }
        }
        bfs_queue.pop();
    }
//    std::cout << empty_nodes;
    std::cout << std::endl;
    std::cout.flush();
}