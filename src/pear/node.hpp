#pragma once

#include <bitset>
#include <deque>
#include <set>

#include <path_entry.hpp>
#include "branch.hpp"
#include "per_tree_allocator.hpp"
#include "parent_node.hpp"
#include "pointer_indirection.hpp"
#include "search_queue_entry.hpp"
#include "custom_stats.hpp"

PEAR_TREE_TEMPLATE_DEF
class Node : public ParentNode<PEAR_TREE_TEMPLATE_ARGUMENTS> {
public:
    enum class node_types {VNodeType, ANodeType, PNodeType};

    using node_type = Node<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using parent_node_type = ParentNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using rect_type = typename parent_node_type::rect_type;
    using rect_float_type = Rectangle<float>;
    using branch_type = Branch<node_type>;
    using path_entry_type = path_entry<node_type , branch_type, rect_type>;
    using pointer_indirection_type = pointer_indirection<node_type>;

    std::atomic<META_DATA_TYPE> _version_meta_data;

    virtual bool query(int const query_type, rect_type const &search_space, PEAR_QUERY_QUEUE_TYPE &bfs_queue, std::set<size_t> &result, META_DATA_TYPE last_save_version, bool const reinserted, u_char const global_version, PEAR_QUERY_DRAM_QUEUE_TYPE* dram_queue= nullptr, uint32_t const dram_save_level=INT32_MAX) = 0;
    virtual bool query_dfs(int query_type, rect_type const &search_space, std::vector<size_t> &result, META_DATA_TYPE const version_before_split, u_char const global_version, query_stats * const stats = nullptr) const = 0;
    virtual int remove(rect_type const &key, size_t const value, PEAR_GLOBAL_VERSION_TYPE const global_version) = 0;
    virtual int remove_leaf_blocking(rect_type const &key, size_t const value, PEAR_GLOBAL_VERSION_TYPE const global_version) = 0;
    virtual std::atomic<META_DATA_TYPE>* insert_leaf(typename node_type::rect_type const &key, size_t const value, std::atomic<META_DATA_TYPE>** const branch_lock_to_release, PEAR_GLOBAL_VERSION_TYPE const global_version, optional_args* const args_ptr) = 0;
    virtual bool find_insert_path(rect_type const &key, path_entry_type* const path_array, uint32_t const global_version, optional_args* const args_ptr, uint32_t const at_level=0, uint32_t const dram_save_level=INT32_MAX) = 0;
    virtual int find_subtree(rect_type const &key) const = 0;
    virtual void add_subtree(node_type * const node_ptr, typename node_type::rect_type const &key) = 0;
    virtual void add_data(size_t const value, typename node_type::rect_type const &key) = 0;
    virtual void move_all_branches(node_type * const dest, PEAR_GLOBAL_VERSION_TYPE const global_version) = 0;
    virtual bool partially_move_branches(node_type * const dest, int const amount, typename node_type::rect_type * const rect_to_update, typename node_type::rect_type * const mbr, PEAR_GLOBAL_VERSION_TYPE const global_version) = 0;
    virtual std::shared_ptr<typename node_type::pointer_indirection_type> get_ptr_indirection() const = 0;
    virtual node_types get_type() const = 0;
    virtual META_DATA_TYPE get_version() const { return LOAD_META_INLINE(_version_meta_data); }
    virtual META_DATA_TYPE get_last_version_before_split() const =0;
     std::atomic<META_DATA_TYPE> &get_version_lock() { return _version_meta_data; }
    virtual parent_node_type* get_parent_node() const = 0;
    virtual node_type* get_sibling() const = 0;
    virtual void set_parent_node(parent_node_type* const parent_ptr) = 0;
    virtual META_DATA_TYPE get_bitmap() const = 0;
    virtual std::array<branch_type, max_node_size> const &get_branches() const = 0;
    virtual std::array<branch_type, max_node_size> &get_branches_non_const() = 0;
    virtual std::array<rect_type , max_node_size> const &get_rects() const = 0;
    virtual std::array<rect_type , max_node_size> &get_rects_non_const() = 0;
    virtual int get_branch_count() const = 0;
    virtual void free_tree_dram() = 0;
    virtual void set_sibling_pointer(node_type* const new_sibling) = 0;
    virtual std::bitset<max_node_size> get_bitmap_debug() const = 0;

    bool is_node() const override {
        return true;
    }

    bool insert_recursive(typename node_type::rect_type const &key, size_t const value, uint32_t const global_version, optional_args* const args_ptr) {
        if(!PEAR_USE_OLD_MBR_UPDATE_ROUTINE) {
            throw  std::runtime_error("Insert function called with wrong strategy");
        }
        bool inserted = false;
        while(!inserted) {
            int subtree = -1;
            branch_type *branch = nullptr;
            while (subtree < 0) {
                // TODO (delete): Get the real version and return if locked
                PEAR_NODE_VERSION_TYPE const initial_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
                if(PEAR_IS_REORGANIZING(initial_version)) {
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->aborts_node_changed++;
                    }
                    return false;
                } else if(IS_LOCKED(initial_version)) {
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->aborts_node_changed++;
                    }
                    subtree = -1;
                    continue;
                }
                subtree = this->find_subtree(key);
                branch = &(this->get_branches_non_const()[subtree]);
                PEAR_NODE_VERSION_TYPE const check_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
                if(PEAR_IS_REORGANIZING(check_version)) {
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->aborts_node_changed++;
                    }
                    return false;
                } else if(check_version != initial_version) {
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->aborts_node_changed++;
                    }
                    subtree = -1;
                } else {
                    ACQUIRE_META_ATOMIC_WAIT_COMMANDS(branch->atomic_version, \
                    PEAR_NODE_VERSION_TYPE const check_version_locking = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data)); \
                        if(PEAR_IS_REORGANIZING(check_version_locking)) { \
                            if(PEAR_COLLECT_CONCURRENCY_STATS) { \
                                args_ptr->aborts_node_changed++; \
                            }\
                                return false; \
                        } else if(check_version_locking != initial_version) { \
                            if(PEAR_COLLECT_CONCURRENCY_STATS) { \
                                args_ptr->aborts_node_changed++; \
                            } \
                            subtree = -1; \
                            break; \
                        })
                    if(subtree != -1) {
                        PEAR_NODE_VERSION_TYPE const check_version_locking = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
                        if(PEAR_IS_REORGANIZING(check_version_locking)) {
                            PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((branch->atomic_version))
                            if(PEAR_COLLECT_CONCURRENCY_STATS) {
                                args_ptr->aborts_node_changed++;
                            }
                            return false;
                        } else if(check_version_locking != initial_version) {
                            PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((branch->atomic_version))
                            if(PEAR_COLLECT_CONCURRENCY_STATS) {
                                args_ptr->aborts_node_changed++;
                            }
                            subtree = -1;
                        }
                    }
                }
            }
            _mm_prefetch((char const*)&(branch->get_child_node()->_version_meta_data),_MM_HINT_T0);
            rect_type &rect_non_const = this->get_rects_non_const()[subtree];
            rect_type const &rect_const = this->get_rects()[subtree];
            join(rect_const, key, &(rect_non_const));
            if(this->is_pmem_node()) {
                PearTreeAllocatorSingleton::fence_persist(&(rect_const), sizeof(rect_type));
            }
            if(this->get_level() == 1) {
                std::atomic<META_DATA_TYPE> * branch_meta = &(branch->atomic_version);
                std::atomic<long unsigned int>* const branch_to_unlock = branch->get_child_node()->insert_leaf(key, value, &branch_meta, global_version, args_ptr);
                PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((*(branch_to_unlock)))
                return true;
            } else {
                PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(branch->atomic_version)
                inserted = branch->get_child_node()->insert_recursive(key, value, global_version, args_ptr);
            }
        }
        return true;
    }

    bool is_leaf() const {
        return this->get_level() == 0;
    };

    int calculate_split(typename node_type::rect_type const * const rects, typename node_type::rect_type const &new_key,
                        std::vector<std::pair<const typename node_type::rect_type *, unsigned int>> *&min_margin_axis_rects,
                        typename node_type::rect_type * &new_mbr_overflown_node,
                        typename node_type::rect_type * &new_mbr_new_node, bool &new_key_in_this_node) {

        // Sort rects along each axis
        std::vector<std::pair<typename node_type::rect_type const *, unsigned int>> *sorted_rects_x = new std::vector<std::pair<typename node_type::rect_type const *, unsigned int>>();
        std::vector<std::pair<typename node_type::rect_type const *, unsigned int>> *sorted_rects_y = new std::vector<std::pair<typename node_type::rect_type const *, unsigned int>>();
        PearTreeAllocatorSingleton::mfence();
        for (int i = 0; i < max_node_size; i++) {
            sorted_rects_x->push_back(std::make_pair(&(rects[i]), i));
            sorted_rects_y->push_back(std::make_pair(&(rects[i]), i));
        }
        sorted_rects_x->push_back(std::make_pair(&new_key, max_node_size));
        sorted_rects_y->push_back(std::make_pair(&new_key, max_node_size));
        std::sort(sorted_rects_x->begin(), sorted_rects_x->end(), [](std::pair<typename node_type::rect_type const *, unsigned int> const rect_a,
                                                                   std::pair<typename node_type::rect_type const *, unsigned int> const rect_b) -> bool {
            return rect_a.first->x_min < rect_b.first->x_min ||
                   (rect_a.first->x_min == rect_b.first->x_min && rect_a.first->x_max < rect_b.first->x_max);
        });
        std::sort(sorted_rects_y->begin(), sorted_rects_y->end(), [](std::pair<typename node_type::rect_type const *, unsigned int> const rect_a,
                                                                   std::pair<typename node_type::rect_type const *, unsigned int> const rect_b) -> bool {
            return rect_a.first->y_min < rect_b.first->y_min ||
                   (rect_a.first->y_min == rect_b.first->y_min && rect_a.first->y_max < rect_b.first->y_max);
        });

        // To only iterate once in each direction over the sorted rects, the goodness values for each group (of each distribution) are build incrementally.
        // The first element in this order for the first group, is the bounding box the the min_node_size first elements on each axis
        // Vice versa this is the set of rectangles which is never part of the second group
        // For the second group we start on each axis with the largest rectangle and add the second, third, etc. largest step-by-step
        // The resulting two arrays (per axis and goodness value) can be used to calculate the goodness value for each distribution

        // Calculate the goodness values for the first group of each distribution and axis

        int const NUMBER_DISTRIBUTIONS = max_node_size - 2 * min_node_size + 2;

        Pointtype bb_x_min_x = sorted_rects_x->at(0).first->x_min;
        Pointtype bb_x_min_y = sorted_rects_x->at(0).first->y_min;
        Pointtype bb_x_max_x = sorted_rects_x->at(0).first->x_max;
        Pointtype bb_x_max_y = sorted_rects_x->at(0).first->y_max;

        Pointtype bb_y_min_x = sorted_rects_y->at(0).first->x_min;
        Pointtype bb_y_min_y = sorted_rects_y->at(0).first->y_min;
        Pointtype bb_y_max_x = sorted_rects_y->at(0).first->x_max;
        Pointtype bb_y_max_y = sorted_rects_y->at(0).first->y_max;

        for (int i = 1; i < min_node_size; i++) {
            bb_x_min_x = std::min(sorted_rects_x->at(i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x->at(i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x->at(i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x->at(i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y->at(i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y->at(i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y->at(i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y->at(i).first->y_max, bb_y_max_y);
        }

        Pointtype sum_margins_x = 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
        Pointtype sum_margins_y = 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

        std::vector<typename node_type::rect_type> first_gr_bbs_x = std::vector<typename node_type::rect_type>();
        std::vector<typename node_type::rect_type> first_gr_bbs_y = std::vector<typename node_type::rect_type>();

        first_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
        first_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);

        for (int i = 0; i < NUMBER_DISTRIBUTIONS - 1; i++) {
            bb_x_min_x = std::min(sorted_rects_x->at(i + min_node_size).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x->at(i + min_node_size).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x->at(i + min_node_size).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x->at(i + min_node_size).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y->at(i + min_node_size).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y->at(i + min_node_size).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y->at(i + min_node_size).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y->at(i + min_node_size).first->y_max, bb_y_max_y);

            sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
            sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

            first_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
            first_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);
        }

        // Calculate goodness values for second group
        std::vector<typename node_type::rect_type> second_gr_bbs_x = std::vector<typename node_type::rect_type>();
        std::vector<typename node_type::rect_type> second_gr_bbs_y = std::vector<typename node_type::rect_type>();

        bb_x_min_x = sorted_rects_x->at(max_node_size).first->x_min;
        bb_x_min_y = sorted_rects_x->at(max_node_size).first->y_min;
        bb_x_max_x = sorted_rects_x->at(max_node_size).first->x_max;
        bb_x_max_y = sorted_rects_x->at(max_node_size).first->y_max;

        bb_y_min_x = sorted_rects_y->at(max_node_size).first->x_min;
        bb_y_min_y = sorted_rects_y->at(max_node_size).first->y_min;
        bb_y_max_x = sorted_rects_y->at(max_node_size).first->x_max;
        bb_y_max_y = sorted_rects_y->at(max_node_size).first->y_max;

        for (int i = max_node_size; i > max_node_size-min_node_size; i--) {
            bb_x_min_x = std::min(sorted_rects_x->at(i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x->at(i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x->at(i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x->at(i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y->at(i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y->at(i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y->at(i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y->at(i).first->y_max, bb_y_max_y);
        }

        sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
        sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

        second_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
        second_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);

        for (int i = max_node_size-min_node_size; i >= min_node_size; i--) {
            bb_x_min_x = std::min(sorted_rects_x->at(i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x->at(i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x->at(i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x->at(i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y->at(i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y->at(i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y->at(i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y->at(i).first->y_max, bb_y_max_y);

            sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
            sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

            second_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
            second_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);
        }
        std::vector<typename node_type::rect_type> *first_group_bbs;
        std::vector<typename node_type::rect_type> *second_group_bbs;

        // Setting split axis

        if (sum_margins_x < sum_margins_y) {
            min_margin_axis_rects = sorted_rects_x;
            delete sorted_rects_y;
            first_group_bbs = &first_gr_bbs_x;
            second_group_bbs = &second_gr_bbs_x;
        } else {
            min_margin_axis_rects = sorted_rects_y;
            delete sorted_rects_x;
            first_group_bbs = &first_gr_bbs_y;
            second_group_bbs = &second_gr_bbs_y;
        }

        // Choose split index algorithm: Choosing the distribution (split at index in sorted array) with the minimal overlap (using area_2 as tie breaker)
        int split_index_candidate= min_node_size;
        Pointtype min_overlap = overlap_area(first_group_bbs->at(0), second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1));
        Pointtype min_area =
                area_inline(first_group_bbs->at(0)) + area_inline(second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1));

        for (int i = 1; i < NUMBER_DISTRIBUTIONS; i++) {
            Pointtype current_overlap = overlap_area(first_group_bbs->at(i),
                                                     second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - i));
            Pointtype current_area = area_inline(first_group_bbs->at(i)) +
                                     area_inline(second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - i));
            if (current_overlap < min_overlap || (current_overlap == min_overlap && current_area < min_area)) {
                split_index_candidate = min_node_size + i;
                min_overlap = current_overlap;
                min_area = current_area;
            }
        }
        rect_type const &temp_overflown_node_mbr = first_group_bbs->at(split_index_candidate - min_node_size);
        new_mbr_overflown_node = new Rectangle(temp_overflown_node_mbr.x_min, temp_overflown_node_mbr.y_min, temp_overflown_node_mbr.x_max, temp_overflown_node_mbr.y_max);
        rect_type const &temp_new_node_mbr = second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - (split_index_candidate - min_node_size));
        new_mbr_new_node= new Rectangle(temp_new_node_mbr.x_min, temp_new_node_mbr.y_min, temp_new_node_mbr.x_max, temp_new_node_mbr.y_max);

        new_key_in_this_node= false;
        for (int i = 0; i < split_index_candidate; i++) {
            if (min_margin_axis_rects->at(i).second == max_node_size) {
                new_key_in_this_node = true;
                break;
            }
        }
        return split_index_candidate;
    }

    int test_num_branches(int const level, int const max_level) const {
        int branch_count = this->get_branch_count();
        if (this->get_level() == level) {
            if(max_level != this->get_level() && branch_count < min_node_size) {
                std::cout << this->to_string() << " has too few branches (" << branch_count << ")" << std::endl;
            } else if(branch_count > max_node_size) {
                std::cout << this->to_string() << " has too many branches (" << branch_count << ")" << std::endl;
            }
            return branch_count;
        } else {
            int num_branches = 0;
            int size = 0;
            for (int i = 0; i < max_node_size && size < branch_count; i++) {
                if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                    size++;
                    num_branches += this->get_branches()[i].get_child_node()->test_num_branches(level, max_level);
                }
            }
            if(max_level != this->get_level() && size<min_node_size) {
                std::cout << this->to_string() << " has too few branches (" << size << ")" << std::endl;
            } else if(size > max_node_size) {
                std::cout << this->to_string() << " has too many branches (" << size << ")" << std::endl;
            }
            return num_branches;
        }
    }

    int get_dram_node_count() const {
        int branch_count = this->get_branch_count();
        if (this->is_leaf()) {
            return (this->is_pmem_node()) ? 0 : 1;
        } else {
            int num_dram_nodes = 0;
            int size = 0;
            for (int i = 0; i < max_node_size && size < branch_count; i++) {
                if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                    size++;
                    num_dram_nodes += this->get_branches()[i].get_child_node()->get_dram_node_count();
                }
            }
            if(!this->is_pmem_node()) {
                num_dram_nodes++;
            }
            return num_dram_nodes;
        }
    }

    int get_pmem_node_count() const {
        int branch_count = this->get_branch_count();
        if (this->is_leaf()) {
            return (this->is_pmem_node()) ? 1 : 0;
        } else {
            int num_pmem_nodes = 0;
            int size = 0;
            for (int i = 0; i < max_node_size && size < branch_count; i++) {
                if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                    size++;
                    num_pmem_nodes += this->get_branches()[i].get_child_node()->get_pmem_node_count();
                }
            }
            if(this->is_pmem_node()) {
                num_pmem_nodes++;
            }
            return num_pmem_nodes;
        }
    }

    std::string to_string() const {
        std::stringstream stream;
        stream << "Node {ID: " << (void*)this << "; Level: " << this->get_level() << "}";
        return stream.str();
    }

    void check_for_values(std::set<long> &values, Rectangle<float>const * const rects = NULL, int const rects_size=0) {
        int branch_count = this->get_branch_count();
        if(this->get_level() != 0) {
            for (int i = 0; i < max_node_size && (this->is_pmem_node() || i < branch_count); i++) {
                if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                    this->get_branches()[i].get_child_node()->check_for_values(values, rects, rects_size);
                }
            }
        } else {
            for (int i = 0; i < max_node_size && (this->is_pmem_node() || i < branch_count); i++) {
                if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                    if(values.contains(this->get_branches()[i].get_data())) {
                        if(rects && this->get_branches()[i].get_data() < rects_size && !(rects[this->get_branches()[i].get_data()] == this->get_rects()[i])) {
                            std::bitset<64> branch_meta = this->get_branches()[i].atomic_version.load();
                            std::bitset<max_node_size> node_meta = this->get_bitmap();
                            unsigned long branch_meta_without_bits = branch_meta.to_ulong()&(UINT64_MAX>>2);
                            rect_type rect = this->get_rects()[i];
                            std::cout << "Found value in leaf (" << this->to_string() << "), but the rect doesn't match: " << this->get_branches()[i].get_data() << std::endl;
                        }
                        values.erase(this->get_branches()[i].get_data());
                    } else {
                        std::bitset<64> branch_meta = this->get_branches()[i].atomic_version.load();
                        unsigned long branch_meta_without_bits = branch_meta.to_ulong()&(UINT64_MAX>>2);
                        rect_type rect = this->get_rects()[i];
                    }
                }
            }
        }
    }

    void print_mbrs() {
        int branch_count = this->get_branch_count();
        for (int i = 0; i < max_node_size && (this->is_pmem_node() || i < branch_count); i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                auto const &rect = this->get_rects()[i];
                std::cout << "Min (" << rect.x_min << ", " << rect.y_min << ") Max (" << rect.x_max << ", " << rect.y_max << ") with area: " << std::abs(rect.x_max-rect.x_min)*std::abs(rect.y_max-rect.y_min) << std::endl;
            }
        }
    }

    void gather_stats(int &inner_node_count, float &sum_areas) {
        int branch_count = this->get_branch_count();
        inner_node_count++;
        for (int i = 0; i < max_node_size && (this->is_pmem_node() || i < branch_count); i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                auto const &rect = this->get_rects()[i];
                sum_areas += std::abs(rect.x_max-rect.x_min)*std::abs(rect.y_max-rect.y_min);
                if(!this->is_leaf()) {
                    this->get_branches()[i].get_child_node()->gather_stats(inner_node_count, sum_areas);
                }
            }
        }
    }

    size_t count_branch_updates() const {
        int branch_count = this->get_branch_count();
        size_t branch_updates = 0l;
        int size = 0;
        for (int i = 0; i < max_node_size && size < branch_count; i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                branch_updates += (((this->get_branches()[i].get_version())&PEAR_BRANCH_LOCAL_VERSION_BIT_MASK)/PEAR_MIN_BRANCH_UNRESERVED_VERSION)-1l;
                if (this->get_level() > 0) {
                    branch_updates += this->get_branches()[i].get_child_node()->count_branch_updates();
                }
                size++;
            }
        }
        return branch_updates;
    }

    size_t count_node_updates() const {
        int branch_count = this->get_branch_count();
        size_t node_updates = 0l;
        int size = 0;
        node_updates += (((this->get_version())&PEAR_NODE_LOCAL_VERSION_BIT_MASK)/PEAR_MIN_NODE_UNRESERVED_VERSION)-1l;
        for (int i = 0; i < max_node_size && size < branch_count; i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                if (this->get_level() > 0) {
                    node_updates += this->get_branches()[i].get_child_node()->count_node_updates();
                }
                size++;
            }
        }
        return node_updates;
    }

    int total_branch_count(std::vector<int> const &level_list = {}) const {
        int branch_count = this->get_branch_count();
        int child_branch_count = 0;
        int size = 0;
        for (int i = 0; i < max_node_size && size < branch_count; i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                if (this->get_level() > 0) {
                    child_branch_count += this->get_branches()[i].get_child_node()->total_branch_count();
                }
                size++;
            }
        }
        if(level_list.size() == 0 || std::find(level_list.begin(), level_list.end(), this->get_level()) != level_list.end()) {
            child_branch_count += branch_count;
        }
        return child_branch_count;
    }

    Pointtype total_mbr_area(std::vector<int> const &level_list = {}) const {
        int branch_count = this->get_branch_count();
        Pointtype area = 0;
        int size = 0;
        for (int i = 0; i < max_node_size && size < branch_count; i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                if(level_list.size() == 0 || std::find(level_list.begin(), level_list.end(), this->get_level()) != level_list.end()) {
                    area += area_inline(this->get_rects()[i]);
                }
                if (this->get_level() > 0) {
                    area += this->get_branches()[i].get_child_node()->total_mbr_area(level_list);
                }
                size++;
            }
        }
        return area;
    }

    Pointtype total_mbr_overlap(std::vector<int> const &level_list = {}) const {
        int branch_count = this->get_branch_count();
        Pointtype overlap = 0;
        int size = 0;
        for (int i = 0; i < max_node_size && size < branch_count; i++) {
            if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), i)) {
                if(level_list.size() == 0 || std::find(level_list.begin(), level_list.end(), this->get_level()) != level_list.end()) {
                    for (int j = 0; j < max_node_size && size < branch_count; j++) {
                        if (i == j) continue;
                        if (!this->is_pmem_node() || PEAR_CHECK_BITMAP(this->get_bitmap(), j)) {
                            overlap += overlap_area(this->get_rects()[i], this->get_rects()[j]);
                        }
                    }
                }
                if (this->get_level() > 0) {
                    overlap += this->get_branches()[i].get_child_node()->total_mbr_overlap(level_list);
                }
                size++;
            }
        }
        return overlap;
    }
};