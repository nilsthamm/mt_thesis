#pragma once

#include "rtree.hpp"
#include <memory>
#include <queue>
#include <iostream>
#include <fstream>
#include <map>

#include "../basic/basic_rtree.hpp"

struct node_info {
    void* id;
    Rectangle mbr;
    int level;
    bool leaf;
    long data;

    node_info(void* id, Rectangle mbr, int level, bool leaf, long data) : id(id),mbr(mbr),level(level),leaf(leaf),data(data) {}
};

template<class RTree>
void exportTree(RTree const &tree, std::string const &prefix) {
    std::map<void*, Rectangle*> node_to_mbr = std::map<void*, Rectangle*>();
    std::vector<node_info> nodes = std::vector<node_info>();
    std::ofstream network_file;
    network_file.open("exp_" + prefix + "rtree_network.csv");
    // BEGIN TODO(RTree interface)
    std::queue<BasicNode*> bfs_queue = std::queue<BasicNode*>();
    Rectangle* root_mbr = (Rectangle*)malloc(sizeof(Rectangle));
    tree._root->get_enclosing_rect(root_mbr);
    node_to_mbr.emplace(tree._root, root_mbr);
    bfs_queue.push(tree._root);
    // END TODO(RTree interface)
    std::string empty_nodes = "";
    while (!bfs_queue.empty()) {
        BasicNode* node = bfs_queue.front();
        bool inner_node = node->_level > 0;
        nodes.emplace_back(node, *(node_to_mbr.find(node)->second), node->_level, false, -1);

        if(inner_node) {
            for(int i =0; i< node->size;i++) {
                BasicNode* child = node->_branches[i].m_child;
                network_file << (long)node << ";" <<  (long)child << std::endl;
                node_to_mbr.emplace(child, node->_branches[i].m_rect);
                bfs_queue.push(child);
            }
        } else {
            for(int i =0; i< node->size;i++) {
                Branch value_branch = node->_branches[i];
                network_file << (long)node << " parent " <<  (long)(node+i+1) << std::endl;
                nodes.emplace_back((node+i+1),*(value_branch.m_rect),-1,true,value_branch.m_data);
            }
        }
        bfs_queue.pop();
    }
    std::ofstream node_file;
    node_file.open("exp_" + prefix + "rtree_nodes.csv");
    node_file << "shared_name,name,x_min,y_min,x_max,y_max,level,leaf,data" << std::endl;
    for(node_info node: nodes) {
        node_file << (long)node.id << ",";
        node_file << "px" << node.mbr.x_min << "py" << node.mbr.y_min << "qx" << node.mbr.x_max << "qy" << node.mbr.y_max << ",";
        node_file << node.mbr.x_min << ",";
        node_file << node.mbr.y_min << ",";
        node_file << node.mbr.x_max << ",";
        node_file << node.mbr.y_max << ",";
        node_file << node.level << ",";
        node_file << node.leaf << ",";
        if(node.leaf) {
            node_file << node.data;
        }
        node_file << std::endl;
    }
    network_file.flush();
    node_file.flush();
}