#pragma once

#include <memory>
#include <rectangle.hpp>
#include <branch.hpp>
#include <atomic>
#include <macros/macros.hpp>
#include <bitset>

#define META_DATA_BIT_SIZE 64

/*
 * Interface to allow the node to call functions on their logical parent no matter if it is the tree object or another node
 */

template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
class BasicParentNode {
public:
    using parent_type = BasicParentNode<max_node_size, min_node_size, max_pmem_level, Pointtype>;
    using branch_type = Branch<parent_type>;
    using rect_type = Rectangle<Pointtype>;
    virtual int split_child_blocking(parent_type * new_node_ptr, parent_type * overflow_node_ptr, rect_type const &new_mbr_overflown_node, rect_type const &new_mbr_new_node, u_char const global_version) =0;
    virtual void add_branch_find_subtree(branch_type const * const branch_to_add, rect_type const & rect_to_add, parent_type const * origin, META_DATA_TYPE const meta_data_long, u_char const global_version) = 0;
    virtual void delete_branch(parent_type* const node_to_delete, META_DATA_TYPE const meta_data_long, u_char const global_version) = 0;
    virtual void release_branch_lock(int const position) = 0;
    virtual bool update_mbr(parent_type * const child, u_char const global_version) = 0;
    virtual bool is_node() = 0;
    virtual std::atomic<size_t> &get_meta_data() =0;
};