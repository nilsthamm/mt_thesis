#pragma once

#include "optional_args.hpp"

PEAR_TREE_TEMPLATE_DEF
class ParentNode {
public:
    using parent_node_type = ParentNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using rect_type = Rectangle<Pointtype>;

    virtual bool split_child_blocking(parent_node_type * new_node_ptr, parent_node_type * overflow_node_ptr, rect_type const &new_mbr_overflown_node, rect_type const &new_mbr_new_node, std::atomic<META_DATA_TYPE> * const child_version_meta, PEAR_GLOBAL_VERSION_TYPE const global_version, optional_args* const args_ptr, std::atomic<META_DATA_TYPE>** const branch_lock_to_release=NULL, std::atomic<META_DATA_TYPE>** const branch_lock_new_branch=NULL, bool const lock_branch=false) =0;
    virtual bool is_pmem_node() const = 0;
    virtual PEAR_LEVEL_TYPE get_level() const = 0;
    virtual bool update_mbr(Pointtype const x_min, Pointtype const y_min, Pointtype const x_max, Pointtype const y_max, parent_node_type * const origin, PEAR_GLOBAL_VERSION_TYPE const global_version) = 0;
    virtual bool merge_child(Pointtype const x_min, Pointtype const y_min, Pointtype const x_max, Pointtype const y_max, parent_node_type * const origin, int const underflown_branch_count, PEAR_GLOBAL_VERSION_TYPE const global_version) = 0;
    virtual bool is_node() const = 0;
};
