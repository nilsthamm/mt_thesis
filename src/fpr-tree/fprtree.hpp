#pragma once

#include <queue>


#include "rectangle.hpp"
#include "per_tree_allocator.hpp"
#include "fpr_parent_node.hpp"
#include "fpr_node.hpp"
#include "custom_stats.hpp"

template<int max_node_size, int min_node_size, int max_pmem_level>
class FPRTree : virtual public FPRParentNode {
public:
    using basic_node_type = FPRNode<max_node_size, min_node_size, max_pmem_level>;
    using rect_type = Rectangle<float>;
    using branch_type = FPRBranch<FPRNode<max_node_size, min_node_size, max_pmem_level>, long>;

    basic_node_type *root;
    int max_level;
    std::mutex _tree_mutex;

    FPRTree(int temp=0) {
        root = basic_node_type::allocate_node();
        new(root) basic_node_type(true);
        max_level = 0;
    }

    void contains_query(rect_type const &search_space,
                        std::vector<size_t> &result) {
        while(!root->contains_query(search_space, result)) {
            std::atomic_thread_fence(std::memory_order_acquire);
        }
    }

    void equals_query(rect_type const &search_space,
                      std::vector<size_t> &result, query_stats* const = nullptr) {
        while(!root->equals_query(search_space, result)) {
            std::atomic_thread_fence(std::memory_order_acquire);
        }
    }

    void insert(rect_type const &key, long value) {
        while(!try_insert(key, value)) {
            std::atomic_thread_fence(std::memory_order_acquire);
        }
    }

    bool try_insert(const rect_type &key, long value) {
        std::vector<FPRParentNode *> _parent_node_stack = std::vector<FPRParentNode *>();
        _parent_node_stack.emplace_back(this);
        if(max_level == 0) {
            _tree_mutex.lock();
            u_char root_version = root->get_version();
                    std::atomic_thread_fence(std::memory_order_acquire);
            bool result = root->insert(key, value, _parent_node_stack, root_version);
            std::atomic_thread_fence(std::memory_order_release);
            _tree_mutex.unlock();
            return result;
        }
        u_char root_version = root->get_version();
        return root->insert(key, value, _parent_node_stack, root_version, true);
    }

    bool split_child(FPRParentNode * new_node_ptr, FPRParentNode* overflow_node_ptr,
                     rect_type const &new_mbr_overflown_node,
                     rect_type const &new_mbr_new_node,
                     std::vector<FPRParentNode *> &parent_node_stack) override {
        if(max_level != 0) {
            _tree_mutex.lock();
            std::atomic_thread_fence(std::memory_order_acquire);
        }
        basic_node_type *new_root = basic_node_type::allocate_node();
        new(new_root) basic_node_type(false);
        max_level += 1;

        // add old root to new root
        new_root->_children[0].m_rect = new_mbr_overflown_node;
        new_root->_children[0].m_child = root;

        // add new node to new root
        branch_type *new_node_subtree = &(new_root->_children[1]);
        new_node_subtree->m_rect = new_mbr_new_node;
        new_node_subtree->m_child = reinterpret_cast<basic_node_type *>(new_node_ptr);

        std::bitset<META_DATA_BIT_SIZE> new_root_bitset = std::bitset<META_DATA_BIT_SIZE>();
        new_root_bitset[0] = 1;
        new_root_bitset[1] = 1;
        new_root_bitset[META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS] = 1;
        new_root->_meta_data = new_root_bitset.to_ulong();
        std::atomic_thread_fence(std::memory_order_release);

        PearTreeAllocatorSingleton::persist_fence(new_root, sizeof(basic_node_type));
        root = new_root;
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(&root, sizeof(&root));
        if(max_level != 1) {
            std::atomic_thread_fence(std::memory_order_release);
            _tree_mutex.unlock();
        }
        return true;
    }

    int get_max_level() {
        return max_level;
    }

    bool reinsert() {
        return false;
    }

    int test_max_level() {
        return max_level;
    }

    int get_dram_node_count() {
        return 0;
    }

    int get_pmem_node_count() {
        return 0;
    }

    int test_num_branches(int level) {
        return this->root->test_num_branches(level);
    }
};