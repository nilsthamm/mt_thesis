#pragma once

#include <memory>
#include <rectangle.hpp>

/*
 * Interface to allow the node to call functions on their logical parent no matter if it is the tree object or another node
 */

class FPRParentNode {
public:
    virtual bool split_child(FPRParentNode * new_node_ptr, FPRParentNode * overflow_node_ptr, Rectangle<float> const &new_mbr_overflown_node, Rectangle<float> const &new_mbr_new_node, std::vector<FPRParentNode*> &parent_node_stack) =0;
};