#pragma once

#include <array>
#include <cstring>
#include <mutex>
#include <per_tree_allocator.hpp>
#include <sstream>
#include <bitset>
#include <queue>
#include <cfloat>
#include "fpr_branch.hpp"
#include "fpr_parent_node.hpp"
#include "rectangle.hpp"


#define META_DATA_BIT_SIZE 64


// Defines how many bits of the meta_data bitset of the node are used for other purposes than the bitmap indicating the branch validity
// 8 bits as a version number
#define META_DATA_RESERVED_BITS 8

template<int max_node_size, int min_node_size, int max_pmem_level>
class FPRNode : virtual public FPRParentNode {
public:

    using branch_type = FPRBranch<FPRNode<max_node_size, min_node_size, max_pmem_level>, long>;
    using rect_type = Rectangle<float>;
    using node_type = FPRNode<max_node_size, min_node_size, max_pmem_level>;

    std::array<branch_type, max_node_size> _children;
    std::atomic<META_DATA_TYPE> _meta_data;
    std::mutex _node_mutex;
    std::atomic_flag _is_locked = false;
    bool const _is_leaf;


    static FPRNode<max_node_size, min_node_size, max_pmem_level> *allocate_node() {
        return ((FPRNode<max_node_size, min_node_size, max_pmem_level> *) PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(node_type), PMEM_CACHE_LINE_SIZE >());
    }

    FPRNode(bool const is_leaf, bool const locked=false) : _children({}), _node_mutex(), _meta_data(0), _is_leaf(is_leaf) {
        if(locked) {
            _node_mutex.lock();
            _is_locked.test_and_set(std::memory_order_release);
        }
    }

    FPRNode(FPRNode const &) = delete;

    bool split_child(FPRParentNode* new_node_ptr, FPRParentNode * overflow_node_ptr,
                     rect_type const &new_mbr_overflown_node,
                     rect_type const &new_mbr_new_node,
                     std::vector<FPRParentNode *> &parent_node_stack) override {
        this->_node_mutex.lock();
        std::atomic_thread_fence(std::memory_order_acquire);
        auto const meta_data = LOAD_META_INLINE(_meta_data);
        if (get_next_free() >= max_node_size) {
            // FPR split step 5 (has to be done before the split, otherwise the split will be calculated on the wrong MBR)
            int overflown_branch_id = -1;
            rect_type old_rect;
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data ,i)) {
                    if (_children[i].m_child == reinterpret_cast<FPRNode *>(overflow_node_ptr)) {
                        old_rect = _children[i].m_rect;
                        _children[i].m_rect = new_mbr_overflown_node;
                        std::atomic_thread_fence(std::memory_order_release);
                        overflown_branch_id = i;
                        break;
                    }
                }
            }
            if(overflown_branch_id == -1) {
                std::atomic_thread_fence(std::memory_order_release);
                this->_node_mutex.unlock();
                _is_locked.clear(std::memory_order_release);
                return false;
            }
            // FPR split step 3.1-3.6/4
            auto const result = this->r_star_split(new_mbr_new_node, parent_node_stack);
            if(!result.first) {
                _children[overflown_branch_id].m_rect = old_rect;
                std::atomic_thread_fence(std::memory_order_release);
                this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
                return false;
            }
            if (result.second) {
                // FPR split step 3.7 (adding the tree which was not there before)
                (result.second)->add_subtree(reinterpret_cast<FPRNode *>(new_node_ptr),
                                             &new_mbr_new_node);
                std::atomic_thread_fence(std::memory_order_release);
                (result.second)->_node_mutex.unlock();
                (result.second)->_is_locked.clear(std::memory_order_release);
            } else {
                // FPR split step 3.7 (adding the tree which was not there before)
                this->add_subtree(reinterpret_cast<FPRNode *>(new_node_ptr),
                                  &new_mbr_new_node);
            }
        } else {
            int overflown_branch_id = -1;
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data ,i)) {
                    if (_children[i].m_child == reinterpret_cast<FPRNode *>(overflow_node_ptr)) {
                        overflown_branch_id = i;
                        break;
                    }
                }
            }
            if(overflown_branch_id == -1) {
                std::atomic_thread_fence(std::memory_order_release);
                this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
                return false;
            }
            // FPR split step 3+4
            this->add_subtree(reinterpret_cast<FPRNode *>(new_node_ptr), &new_mbr_new_node);
            // FPR split step 5
            _children[overflown_branch_id].m_rect = new_mbr_overflown_node;
            std::atomic_thread_fence(std::memory_order_release);
        }
        std::atomic_thread_fence(std::memory_order_release);
        this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
        return true;
    }

    bool insert(rect_type const &key, long const value, std::vector<FPRParentNode *> &parent_node_stack, u_char const expected_version, bool const is_root = false) {
        if(is_root) {
            if (!this->_node_mutex.try_lock()) {
                return false;
            }
            _is_locked.test_and_set(std::memory_order_release);
        } else {
            this->_node_mutex.lock();
            _is_locked.test_and_set(std::memory_order_release);
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        if(this->get_version() != expected_version) {
            this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
            return false;
        }
        if (this->_is_leaf) {
            if (get_next_free() == max_node_size) {
                auto const split_result = this->r_star_split(key, parent_node_stack);
                if(!split_result.first) {
                    std::atomic_thread_fence(std::memory_order_release);
                    this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
                    return false;
                }
                if (split_result.second) {
                    // FPR split step 7
                    (split_result.second)->add_data(&key, value);
                    std::atomic_thread_fence(std::memory_order_release);
                    (split_result.second)->_node_mutex.unlock();
                    (split_result.second)->_is_locked.clear(std::memory_order_release);
                } else {
                    // FPR split step 7
                    this->add_data(&key, value);
                }
            } else {
                this->add_data(&key, value);
            }
            std::atomic_thread_fence(std::memory_order_release);
            this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
            return true;
        } else {
            branch_type *subtree = r_star_find_subtree(key);
            join(subtree->m_rect, key, &(subtree->m_rect));
            PearTreeAllocatorSingleton::persist_fence(&(subtree->m_rect), sizeof(rect_type));
            parent_node_stack.emplace_back(this);
            std::atomic_thread_fence(std::memory_order_release);
            node_type *child = subtree->m_child;
            u_char childs_version = child->get_version();
            this->_node_mutex.unlock();_is_locked.clear(std::memory_order_release);
            return child->insert(key, value, parent_node_stack, childs_version);
        }
    }

    // do the search BFS style to benefit from data locality
    bool contains_query(rect_type const &search_space,
               std::vector<size_t> &result) {
        int const initial_result_size = result.size();
        META_DATA_TYPE initial_meta = LOAD_META_INLINE(_meta_data);
        std::atomic_thread_fence(std::memory_order_acquire);
        if(_is_locked.test_and_set(std::memory_order_acq_rel)) {
            while(_is_locked.test_and_set(std::memory_order_acq_rel));
            _is_locked.clear(std::memory_order_release);
            return false;
        }
        _is_locked.clear(std::memory_order_release);
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(initial_meta ,i)) {
                branch_type &branch = _children[i];
                if (_is_leaf) {
                    if (covers(search_space, (branch.m_rect))) {
                        std::atomic_thread_fence(std::memory_order_acquire);
                        if (initial_meta != LOAD_META_INLINE(_meta_data)) {
                            result.erase(result.begin() + initial_result_size, result.end());
                            return false;
                        }
                        result.push_back(branch.m_data);
                    }
                } else {
                    if (overlap_area((branch.m_rect), search_space) > 0.0f) {
                        std::atomic_thread_fence(std::memory_order_acquire);
                        if (initial_meta != LOAD_META_INLINE(_meta_data)) {
                            result.erase(result.begin() + initial_result_size, result.end());
                            return false;
                        }
                        if(!branch.m_child->contains_query(search_space, result)) {
                            result.erase(result.begin() + initial_result_size, result.end());
                            std::atomic_thread_fence(std::memory_order_acquire);
                            if (initial_meta != LOAD_META_INLINE(_meta_data)) {
                                return false;
                            }
                            i = -1;
                        }
                    }
                }
            }
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        if (initial_meta != LOAD_META_INLINE(_meta_data)) {
            result.erase(result.begin() + initial_result_size, result.end());
            return false;
        }
        return true;
    }

    bool equals_query(rect_type const &search_space,
                      std::vector<size_t> &result) {
        int const initial_result_size = result.size();
        META_DATA_TYPE initial_meta = LOAD_META_INLINE(_meta_data);
        std::atomic_thread_fence(std::memory_order_acquire);
        if(_is_locked.test_and_set(std::memory_order_acq_rel)) {
            while(_is_locked.test_and_set(std::memory_order_acq_rel));
            _is_locked.clear(std::memory_order_release);
            return false;
        }
        _is_locked.clear(std::memory_order_release);
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(initial_meta ,i)) {
                branch_type &branch = _children[i];
                if (_is_leaf) {
                    if (search_space == branch.m_rect) {
                        std::atomic_thread_fence(std::memory_order_acquire);
                        if (initial_meta != LOAD_META_INLINE(_meta_data)) {
                            result.erase(result.begin() + initial_result_size, result.end());
                            return false;
                        }
                        result.push_back(branch.m_data);
                    }
                } else {
                    if (covers(branch.m_rect, search_space)) {
                        std::atomic_thread_fence(std::memory_order_acquire);
                        if (initial_meta != LOAD_META_INLINE(_meta_data)) {
                            result.erase(result.begin() + initial_result_size, result.end());
                            return false;
                        }
                        if(!branch.m_child->equals_query(search_space, result)) {
                            result.erase(result.begin() + initial_result_size, result.end());
                            std::atomic_thread_fence(std::memory_order_acquire);
                            if (initial_meta != LOAD_META_INLINE(_meta_data)) {
                                return false;
                            }
                            i = -1;
                        }
                    }
                }
            }
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        if (initial_meta != LOAD_META_INLINE(_meta_data)) {
            result.erase(result.begin() + initial_result_size, result.end());
            return false;
        }
        return true;
    }

    branch_type *find_subtree(std::shared_ptr<rect_type> const key) {
        int min_area = INT_MAX;
        branch_type *child_candidate;
        //TODO concurrency problem?
        unsigned long meta_data_long = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (meta_data_long & (1 << i)) {
                int next_area = area_2(&(_children[i].m_rect), key.get()) - area_inline(_children[i].m_rect);
                if (next_area < min_area) {
                    min_area = next_area;
                    child_candidate = &(_children[i]);
                }
            }
        }
        return child_candidate;
    }

    branch_type *r_star_find_subtree(rect_type const &key) {
        branch_type *child_candidate;
        child_candidate = r_star_find_subtree_inner(key);
        return child_candidate;
    }

    branch_type *r_star_find_subtree_inner(rect_type const &key) {
        branch_type *child_candidate;
        float min_area = FLT_MAX;
        float min_candidate_area = FLT_MAX;
        auto const initial_meta = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(initial_meta ,i)) {
                float current_area = area_inline(_children[i].m_rect);
                float candidate_area = area_2(_children[i].m_rect, key) - current_area;
                if (candidate_area < min_candidate_area ||
                    (candidate_area == min_candidate_area && current_area < min_area)) {
                    min_candidate_area = candidate_area;
                    min_area = current_area;
                    child_candidate = &(_children[i]);
                }
            }
        }
        return child_candidate;
    }

    branch_type *r_star_find_subtree_leaf(rect_type const &key) {
        branch_type *child_candidate;
        float min_area_enlargement = FLT_MAX;
        float min_overlap_enlargement = FLT_MAX;
        // TODO see optimization in paper (end of page 4): Sort the children by area_2 enlargement (radix sort?) and only calculate the overlap enlargemnent for first X

        //TODO concurrency problem?
        unsigned long meta_data_long = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (meta_data_long & ((unsigned long) 1 << i)) {
                int area_enlargement = area_2(_children[i].m_rect, key) - area_inline(_children[i].m_rect);
                int candidate_overlap_enlargement;
                for (int j = 0; i < max_node_size; i++) {
                    if (i == j)
                        continue;
                    candidate_overlap_enlargement += (overlap_area(_children[i].m_rect, _children[j].m_rect) -
                                                      overlap_area(_children[i].m_rect, key, _children[j].m_rect));
                }
                if (candidate_overlap_enlargement < min_overlap_enlargement ||
                    (candidate_overlap_enlargement == min_area_enlargement &&
                     area_enlargement < min_area_enlargement)) {
                    min_overlap_enlargement = candidate_overlap_enlargement;
                    min_area_enlargement = area_enlargement;
                    child_candidate = &(_children[i]);
                }
            }
        }
        return child_candidate;
    }

    std::pair<bool, FPRNode *> r_star_split(rect_type const &new_key,
                            std::vector<FPRParentNode *> &parent_node_stack) {
        unsigned long old_version = reset_version();
        // ChooseSplitAxis
        // Sort rects along each axis
        std::vector<std::pair<rect_type const *, int>> sorted_rects_x = std::vector<std::pair<rect_type const *, int>>();
        std::vector<std::pair<rect_type const *, int>> sorted_rects_y = std::vector<std::pair<rect_type const *, int>>();
        for (int i = 0; i < max_node_size; i++) {
            sorted_rects_x.push_back(std::make_pair(&(_children[i].m_rect), i));
            sorted_rects_y.push_back(std::make_pair(&(_children[i].m_rect), i));
        }
        sorted_rects_x.push_back(std::make_pair(&new_key, max_node_size));
        sorted_rects_y.push_back(std::make_pair(&new_key, max_node_size));
        std::sort(sorted_rects_x.begin(), sorted_rects_x.end(), [](std::pair<rect_type const *, int> const rect_a,
                                                                   std::pair<rect_type const *, int> const rect_b) -> bool {
            return rect_a.first->x_min < rect_b.first->x_min ||
                   (rect_a.first->x_min == rect_b.first->x_min && rect_a.first->x_max < rect_b.first->x_max);
        });
        std::sort(sorted_rects_y.begin(), sorted_rects_y.end(), [](std::pair<rect_type const *, int> const rect_a,
                                                                   std::pair<rect_type const *, int> const rect_b) -> bool {
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

        float bb_x_min_x = sorted_rects_x.at(0).first->x_min;
        float bb_x_min_y = sorted_rects_x.at(0).first->y_min;
        float bb_x_max_x = sorted_rects_x.at(0).first->x_max;
        float bb_x_max_y = sorted_rects_x.at(0).first->y_max;

        float bb_y_min_x = sorted_rects_y.at(0).first->x_min;
        float bb_y_min_y = sorted_rects_y.at(0).first->y_min;
        float bb_y_max_x = sorted_rects_y.at(0).first->x_max;
        float bb_y_max_y = sorted_rects_y.at(0).first->y_max;

        for (int i = 1; i < min_node_size; i++) {
            bb_x_min_x = std::min(sorted_rects_x.at(i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x.at(i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x.at(i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x.at(i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y.at(i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y.at(i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y.at(i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y.at(i).first->y_max, bb_y_max_y);
        }

        float sum_margins_x = 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
        float sum_margins_y = 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

        std::vector<rect_type> first_gr_bbs_x = std::vector<rect_type>();
        std::vector<rect_type> first_gr_bbs_y = std::vector<rect_type>();

        first_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
        first_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);

        for (int i = 0; i < NUMBER_DISTRIBUTIONS - 1; i++) {
            bb_x_min_x = std::min(sorted_rects_x.at(i + min_node_size).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x.at(i + min_node_size).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x.at(i + min_node_size).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x.at(i + min_node_size).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y.at(i + min_node_size).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y.at(i + min_node_size).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y.at(i + min_node_size).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y.at(i + min_node_size).first->y_max, bb_y_max_y);

            sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
            sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

            first_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
            first_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);
        }

        // Calculate goodness values for second group
        std::vector<rect_type> second_gr_bbs_x = std::vector<rect_type>();
        std::vector<rect_type> second_gr_bbs_y = std::vector<rect_type>();

        bb_x_min_x = sorted_rects_x.at(max_node_size).first->x_min;
        bb_x_min_y = sorted_rects_x.at(max_node_size).first->y_min;
        bb_x_max_x = sorted_rects_x.at(max_node_size).first->x_max;
        bb_x_max_y = sorted_rects_x.at(max_node_size).first->y_max;

        bb_y_min_x = sorted_rects_y.at(max_node_size).first->x_min;
        bb_y_min_y = sorted_rects_y.at(max_node_size).first->y_min;
        bb_y_max_x = sorted_rects_y.at(max_node_size).first->x_max;
        bb_y_max_y = sorted_rects_y.at(max_node_size).first->y_max;

        for (int i = max_node_size; i > max_node_size-min_node_size; i--) {
            bb_x_min_x = std::min(sorted_rects_x.at(i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x.at(i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x.at(i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x.at(i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y.at(i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y.at(i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y.at(i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y.at(i).first->y_max, bb_y_max_y);
        }

        sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
        sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

        second_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
        second_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);

        for (int i = max_node_size-min_node_size; i >= min_node_size; i--) {
            bb_x_min_x = std::min(sorted_rects_x.at(i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x.at(i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x.at(i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x.at(i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y.at(i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y.at(i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y.at(i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y.at(i).first->y_max, bb_y_max_y);

            sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
            sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

            second_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
            second_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);
        }

        std::vector<std::pair<rect_type const *, int>> *min_margin_axis_rects;
        std::vector<rect_type> *first_group_bbs;
        std::vector<rect_type> *second_group_bbs;

        // Setting split axis

        if (sum_margins_x < sum_margins_y) {
            min_margin_axis_rects = &sorted_rects_x;
            first_group_bbs = &first_gr_bbs_x;
            second_group_bbs = &second_gr_bbs_x;
        } else {
            min_margin_axis_rects = &sorted_rects_y;
            first_group_bbs = &first_gr_bbs_y;
            second_group_bbs = &second_gr_bbs_y;
        }

        // Choose split index algorithm: Choosing the distribution (split at index in sorted array) with the minimal overlap (using area_2 as tie breaker)

        int split_index_candidate = min_node_size;
        float min_overlap = overlap_area(first_group_bbs->at(0), second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1));
        float min_area =
                area_inline(first_group_bbs->at(0)) + area_inline(second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1));

        for (int i = 1; i < NUMBER_DISTRIBUTIONS; i++) {
            int current_overlap = overlap_area(first_group_bbs->at(i),
                                               second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - i));
            int current_area = area_inline(first_group_bbs->at(i)) +
                               area_inline(second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - i));
            if (current_overlap < min_overlap || (current_overlap == min_overlap && current_area < min_area)) {
                split_index_candidate = min_node_size + i;
                min_overlap = current_overlap;
                min_area = current_area;
            }
        }


        rect_type* const new_mbr_overflown_node =
                &(first_group_bbs->at(split_index_candidate - min_node_size));
        rect_type* const new_mbr_new_node = &(
                second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - (split_index_candidate - min_node_size)));

        // TODO update comment: The second group will be copied to the new node. Since the groups are not in the same order as they are in the children array, some elements of the first group might need to be copied. After the split all elements from the first group should be continues in the children array of the current ndoe
        FPRNode * const new_node = allocate_node();
        new(new_node) FPRNode<max_node_size, min_node_size, max_pmem_level>(_is_leaf, true);
        // TODO: sort and summarize memory ranges the minimal amount of memory copies is needed
        // TODO: Determine which is the smaller group of the distribution (use something like start_index and end_index in the loop and set these before accordingly). Maybe even do a cost calculation based on the next array. Meaning factor in the inner copies which need to be made
        bool new_key_in_first_group = false;
        for (int i = 0; i < split_index_candidate; i++) {
            if (min_margin_axis_rects->at(i).second == max_node_size) {
                new_key_in_first_group = true;
                break;
            }
        }
        // Whenever an entry with a children index smaller than the split index is copied. Pop one index from the stack and copy it to the index which just got freed.
        int copy_index = 0;
        std::bitset<META_DATA_BIT_SIZE> current_node_bitset = std::bitset<META_DATA_BIT_SIZE>(
                (ULLONG_MAX >> META_DATA_RESERVED_BITS) & LOAD_META_INLINE(_meta_data));
        std::bitset<META_DATA_BIT_SIZE> new_node_node_bitset = std::bitset<META_DATA_BIT_SIZE>(
                ((unsigned long) 2) << (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS));
        // FPR split step 1
        for (int i = split_index_candidate; i < max_node_size + 1; i++) {
            if (new_key_in_first_group || min_margin_axis_rects->at(i).second < max_node_size) {
                int src_index = min_margin_axis_rects->at(i).second;
                new_node->_children[copy_index] = _children[src_index];
                current_node_bitset[src_index] = 0;
                new_node_node_bitset[copy_index] = 1;
                copy_index++;
            }
        }
        STORE_META(new_node->_meta_data, new_node_node_bitset.to_ulong())
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(&(new_node->_meta_data), META_DATA_BIT_SIZE / 8);
        // FPR split step 2
        unsigned long old_bitset = LOAD_META_INLINE(_meta_data);
        if(old_version == (ULONG_MAX >> (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS))) {
            current_node_bitset = std::bitset<META_DATA_BIT_SIZE>(current_node_bitset.to_ullong() + (((unsigned long) 2) << (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS)));
        } else {
            current_node_bitset = std::bitset<META_DATA_BIT_SIZE>(current_node_bitset.to_ullong() + old_version + (((unsigned long) 2) << (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS)));
        }

        // FPR split step 3-5
        // The reason why we pass branch_to_split through: Let the parent node change both MBRs (for the already existing branch and the new one), to keep architecturally clean.
        FPRParentNode *parent_node = parent_node_stack.back();
        parent_node_stack.pop_back();
        bool const parent_result = parent_node->split_child(new_node, this,
                                 *new_mbr_overflown_node, *new_mbr_new_node, parent_node_stack);
        if(!parent_result) {
            STORE_META(_meta_data, old_bitset)
            std::atomic_thread_fence(std::memory_order_release);
            PearTreeAllocatorSingleton::persist_fence(&(_meta_data), META_DATA_BIT_SIZE / 8);
            return std::pair<bool, FPRNode *>(false, nullptr);
        }

        // FPR split step 6
        STORE_META(_meta_data, current_node_bitset.to_ulong())
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(&(_meta_data), META_DATA_BIT_SIZE / 8);
        if (!new_key_in_first_group) {
            return std::pair<bool, FPRNode *>(true, new_node);
        } else {
            std::atomic_thread_fence(std::memory_order_release);
            new_node->_node_mutex.unlock();
            new_node->_is_locked.clear(std::memory_order_release);
            return std::pair<bool, FPRNode *>(true, nullptr);
        }
    }

    void get_enclosing_rect(rect_type **const rect) const {
        float x_min = INT_MAX;
        float x_max = INT_MIN;
        float y_min = INT_MAX;
        float y_max = INT_MIN;
        //TODO concurrency problem?
        unsigned long meta_data_long = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (meta_data_long & ((unsigned long) 1 << i)) {
                x_min = std::min(x_min, _children[i].m_rect.x_min);
                x_max = std::max(x_max, _children[i].m_rect.x_max);
                y_min = std::min(y_min, _children[i].m_rect.y_min);
                y_max = std::max(y_max, _children[i].m_rect.y_max);
            }
        }
        new(*rect) rect_type(x_min, y_min, x_max, y_max);
    }

    void add_subtree(node_type * const node_ptr,
                     rect_type const * const key) {
        int const position = get_next_free();
        branch_type *new_subtree = &(_children[position]);
        new_subtree->m_rect = rect_type(key->x_min, key->y_min, key->x_max, key->y_max);
        new_subtree->m_child = node_ptr;
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(new_subtree,sizeof(branch_type));
        validate_and_increase_version_persist(position);
    }

    void add_data(rect_type const * const key, long const value) {
        int const position = get_next_free();
        branch_type *subtree = &(_children[position]);
        subtree->m_rect = rect_type(key->x_min, key->y_min, key->x_max, key->y_max);
        subtree->m_data = value;
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(subtree,sizeof(branch_type));
        validate_and_increase_version_persist(position);
    }

    int get_next_free() const {
        //TODO concurrency problem?
        unsigned long meta_data_long = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (!(meta_data_long & ((unsigned long) 1 << i)))
                return i;
        }
        return max_node_size;
    }

    u_char get_version() const {
        std::atomic_thread_fence(std::memory_order_acquire);
        return static_cast<u_char>(LOAD_META_INLINE(_meta_data) >> (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS));
    }

    static u_char get_version(META_DATA_TYPE meta_data) {
        return static_cast<u_char>(meta_data >> (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS));
    }

    std::shared_ptr<rect_type> get_enclosing_rect() const {
        float x_min = INT_MAX;
        float x_max = INT_MIN;
        float y_min = INT_MAX;
        float y_max = INT_MIN;
        //TODO concurrency problem?
        unsigned long meta_data_long = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (meta_data_long & ((unsigned long) 1 << i)) {
                x_min = std::min(x_min, _children[i].m_rect.x_min);
                x_max = std::max(x_max, _children[i].m_rect.x_max);
                y_min = std::min(y_min, _children[i].m_rect.y_min);
                y_max = std::max(y_max, _children[i].m_rect.y_max);
            }
        }

        return std::make_shared<rect_type>(x_min, y_min, x_max, y_max);
    }

    unsigned long reset_version() {
        unsigned long old_version = LOAD_META_INLINE(_meta_data) & (ULONG_MAX << (META_DATA_BIT_SIZE-META_DATA_RESERVED_BITS));
        STORE_META(_meta_data, LOAD_META_INLINE(_meta_data) & (ULONG_MAX >> META_DATA_RESERVED_BITS))
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(&(_meta_data), META_DATA_BIT_SIZE / 8);
        return old_version;
    }

    void validate_and_increase_version_persist(int position) {
        auto const initial_meta = LOAD_META_INLINE(_meta_data);
        if((initial_meta&(ULONG_MAX<<(META_DATA_BIT_SIZE-META_DATA_RESERVED_BITS))) == (ULONG_MAX<<(META_DATA_BIT_SIZE-META_DATA_RESERVED_BITS))) {
            STORE_META(_meta_data, std::bitset<64>(initial_meta).set(position, true).to_ulong() +  (((unsigned long) 2) << (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS)))
        } else {
            STORE_META(_meta_data, (std::bitset<64>(initial_meta).set(position, true).to_ulong()&(ULONG_MAX>>META_DATA_RESERVED_BITS)) + (((unsigned long) ((LOAD_META_INLINE(_meta_data)>>(META_DATA_BIT_SIZE-META_DATA_RESERVED_BITS))+1l)) << (META_DATA_BIT_SIZE - META_DATA_RESERVED_BITS)));
        }
        std::atomic_thread_fence(std::memory_order_release);
        PearTreeAllocatorSingleton::persist_fence(&(_meta_data), META_DATA_BIT_SIZE / 8);
    }

    /**
     * Test helpers BEGIN
     */
    int test_num_branches(int const level) const {
        auto const initial_meta = LOAD_META_INLINE(_meta_data);
        if (this->_is_leaf) {
            int size = 0;
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(initial_meta ,i)) {
                    size++;
                }
            }
            return size;
        } else {
            int num_branches = 0;
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(initial_meta ,i)) {
                    num_branches += this->_children[i].m_child->test_num_branches(level);
                }
            }
            return num_branches;
        }
    }

    /**
     * Test helpers END
     */
#ifdef USE_REINSERT
    std::shared_ptr<BasicRTree> tree_object;
#endif
};
