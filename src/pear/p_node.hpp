#pragma once

#include <cfloat>

#include <per_tree_allocator.hpp>
#include <macros/macros.hpp>

#include "node.hpp"
#include "parent_node.hpp"

PEAR_TREE_TEMPLATE_DEF
#ifdef UNALIGNED_BUILD
class PNode : public Node<PEAR_TREE_TEMPLATE_ARGUMENTS> {
#else
#ifdef CPU_CACHE_ALIGNED_BUILD
class alignas(CACHE_LINE_SIZE) PNode : public Node<PEAR_TREE_TEMPLATE_ARGUMENTS> {
#else
class alignas(PMEM_CACHE_LINE_SIZE) PNode : public Node<PEAR_TREE_TEMPLATE_ARGUMENTS> {
#endif
#endif
public:

    using node_type = Node<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using p_node_type = PNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;
    using parent_node_type = ParentNode<PEAR_TREE_TEMPLATE_ARGUMENTS>;

    // Optimized for multiple of 8 branches. So that _rects is 256B aligned (PMEM cache line)
    PEAR_LEVEL_TYPE const _level;
    META_DATA_TYPE _bitmap;
    std::array<typename node_type::branch_type, max_node_size> _branches;
    std::array<typename node_type::rect_type, max_node_size> _rects;
    parent_node_type* _parent_node;
    p_node_type* _sibling_pointer;

    PNode(int const level) : _rects({}), _branches({}), _bitmap(0), _parent_node(NULL), _sibling_pointer(NULL), _level(level) {
        STORE_META(this->_version_meta_data, PEAR_SET_NODE_VERSION_INLINE(0, 4))
        static_assert(max_node_size <= (META_DATA_BIT_SIZE), "PNode max_node_size too big");
    }

    PNode(int const level, PEAR_NODE_VERSION_TYPE const initial_version, PEAR_GLOBAL_VERSION_TYPE const global_version) : _rects({}), _branches({}), _bitmap(0), _parent_node(NULL), _sibling_pointer(NULL), _level(level) {
        STORE_META(this->_version_meta_data, PEAR_SET_NODE_VERSIONS_INLINE(initial_version, global_version))
        static_assert(max_node_size <= (META_DATA_BIT_SIZE), "PNode max_node_size too big");
    }

    bool query_dfs(int const query_type, typename node_type::rect_type const &search_space, std::vector<size_t> &result, META_DATA_TYPE const version_before_split, u_char const global_version, query_stats * const stats = nullptr) const override {
        std::vector<node_type*> candidates = std::vector<node_type*>();
        if(PEAR_COLLECT_QUERY_STATS) {
            stats->touched_p_nodes++;
            if(_level == 0) {
                stats->touched_leafs++;
            } else {
                stats->touched_inner_nodes++;
            }
        }
        if(PEAR_ENABLE_PREFETCHING) {
            for(int i = 0; i < max_node_size; i+=4) {
                _mm_prefetch((char const*)&(this->_rects[i]), _MM_HINT_T1);
            }
        }
        META_DATA_TYPE const initial_meta_data = LOAD_META_INLINE(this->_version_meta_data);
        std::atomic_thread_fence(std::memory_order_acquire);
        META_DATA_TYPE bitmap_at_begin = _bitmap;
        int const initial_result_size = result.size();
        if(initial_meta_data > version_before_split || IS_LOCKED(initial_meta_data)) {
            return false;
        }

        if(query_type == 0) {
            contains_query_selection(search_space, result, bitmap_at_begin, candidates);
        } else if(query_type == 1) {
            equals_query_selection(search_space, result, bitmap_at_begin, candidates);
        } else {
            INVALID_BRANCH_TAKEN
        }

        bool rerrun_node = false;
        META_DATA_TYPE const current_meta = LOAD_META_INLINE(this->_version_meta_data);
        if(current_meta > version_before_split) {
            result.resize(initial_result_size);
            return false;
        } else if(initial_meta_data != current_meta
//            TODO: && node_not_deleted
        ) {
            rerrun_node = true;
        }

        for(auto const candidate: candidates) {
            if (rerrun_node) {
                break;
            }
            while(!candidate->query_dfs(query_type,search_space, result,ULONG_MAX,global_version, stats)) {
                META_DATA_TYPE const current_meta_intermediate = LOAD_META_INLINE(this->_version_meta_data);
                if(current_meta_intermediate > version_before_split) {
            result.resize(initial_result_size);
                    return false;
                } else if(initial_meta_data != current_meta_intermediate
//            TODO: && node_not_deleted
                        ) {
                    rerrun_node = true;
                    break;
                }
            }
        }

        META_DATA_TYPE const current_meta_final = LOAD_META_INLINE(this->_version_meta_data);
        if(current_meta_final > version_before_split) {
            result.resize(initial_result_size);
            return false;
        } else if(rerrun_node || initial_meta_data != current_meta_final
//            TODO: && node_not_deleted
        ) {
            result.resize(initial_result_size);
            return this->query_dfs(query_type,search_space,result,ULLONG_MAX,global_version, stats);
        }
        return true;
    }

    bool query(int const query_type, typename node_type::rect_type const &search_space,
               PEAR_QUERY_QUEUE_TYPE &bfs_queue,
               std::set<size_t> &result, META_DATA_TYPE last_save_version, bool const reinserted, u_char const global_version, PEAR_QUERY_DRAM_QUEUE_TYPE* dram_queue= nullptr, uint32_t const dram_save_level=INT32_MAX) override {
        if(PEAR_ENABLE_PREFETCHING) {
            for(int i = 0; i < max_node_size; i+=4) {
                _mm_prefetch((char const*)&(this->_rects[i]), _MM_HINT_T1);
            }
        }
        META_DATA_TYPE const initial_meta_data = LOAD_META_INLINE(this->_version_meta_data);
        std::atomic_thread_fence(std::memory_order_acquire);
        META_DATA_TYPE bitmap_at_begin = _bitmap;
        int const initial_queue_size = bfs_queue.size();
        int const initial_result_size = result.size();
        META_DATA_TYPE const initial_version = PEAR_GET_NODE_VERSION_INLINE(initial_meta_data);
        META_DATA_TYPE const version_before_split = initial_meta_data+(max_node_size-count_valid_branches(bitmap_at_begin))*PEAR_MIN_NODE_UNRESERVED_VERSION;
        if(initial_meta_data > last_save_version || PEAR_IS_REORGANIZING(initial_version)) {
            if(_parent_node->is_node()) {
                node_type * parent = dynamic_cast<node_type *>(_parent_node);
                if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && !parent->is_pmem_node()) {
                    dram_queue->emplace_back(parent->get_ptr_indirection(), parent->get_last_version_before_split(), true);
                } else {
                    bfs_queue.emplace_back(parent, parent->get_last_version_before_split(), true);
                }
            } else {
                bfs_queue.emplace_back(this, (initial_meta_data > last_save_version) ? version_before_split : last_save_version, true);
            }
            return false;
        }
        if(IS_LOCKED(initial_version)) {
            bfs_queue.emplace_back(this, last_save_version, false);
            return false;
        }

        if(query_type == 0) {
            contains_query_selection(search_space, bfs_queue, result, bitmap_at_begin);
        } else if(query_type == 1) {
            equals_query_selection(search_space, bfs_queue, result, bitmap_at_begin);
        } else {
            INVALID_BRANCH_TAKEN
        }

        META_DATA_TYPE const current_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
        if(current_version > PEAR_GET_NODE_VERSION_INLINE(last_save_version) || (reinserted && result.size() == initial_result_size && bfs_queue.size() == initial_queue_size)) {
            bfs_queue.erase(bfs_queue.begin()+initial_queue_size, bfs_queue.end());
            if(_parent_node->is_node()) {
                node_type * parent = dynamic_cast<node_type *>(_parent_node);
                if((PEAR_USE_BACKGROUND_PERSISTING || PEAR_USE_PARTIAL_SIBLING_PERSITENCE) && !parent->is_pmem_node()) {
                    dram_queue->emplace_back(parent->get_ptr_indirection(), parent->get_last_version_before_split(), true);
                } else {
                    bfs_queue.emplace_back(parent, parent->get_last_version_before_split(), true);
                }
            } else {
                bfs_queue.emplace_back(this, version_before_split, true);
            }
            return false;
        }
        if(initial_version != current_version) {
            bfs_queue.erase(bfs_queue.begin()+initial_queue_size, bfs_queue.end());
            bfs_queue.emplace_back(this, last_save_version, false);
            return false;
        }
        return true;
    }

    void contains_query_selection(typename node_type::rect_type const &search_space, PEAR_QUERY_QUEUE_TYPE &bfs_queue, std::set<size_t> &result, META_DATA_TYPE const bitmap_at_begin) const {
        // TODO: find better solution
        if(!PEAR_ENABLE_SIMD) {
            for (int i = 0; i < max_node_size; i ++) {
                if(PEAR_CHECK_BITMAP(bitmap_at_begin, i)) {
                    typename node_type::rect_type const &rect = _rects[i];
                    if (_level == 0) {
                        if (covers(search_space, (rect))) {
                            result.insert(_branches[i].get_data());
                        }
                    } else {
                        if (overlap_area((rect), search_space) > 0.0f) {
                            bfs_queue.emplace_back(_branches[i].get_child_node(), _branches[i].get_child_node()->get_last_version_before_split(), false);
                        }
                    }
                }
            }
        } else {
            __m512 const v_negation_mask = _mm512_set4_ps(-0.0f, -0.0f, 0.0f, 0.0f);
            __m512 const v_key = _mm512_xor_ps(
                    _mm512_set4_ps(search_space.y_max, search_space.x_max, search_space.y_min, search_space.x_min),
                    v_negation_mask);
            if(PEAR_ENABLE_PREFETCHING) {
                _mm_prefetch((char const *) &(this->_rects[0]), _MM_HINT_T0);
                _mm_prefetch((char const *) &(this->_rects[4]), _MM_HINT_T0);
            }
            __mmask16 const cmp_result_mask = 0b1111;
            for (int i = 0; i < max_node_size; i += 4) {
                if(PEAR_ENABLE_PREFETCHING) {
                    _mm_prefetch((char const *) &(this->_rects[i + 8]), _MM_HINT_T0);
                }
                /** Use SIMD AVX instruction to compute
                    overlap_area() > 0


                __m512 const v_rects = _mm512_xor_ps(_mm512_loadu_ps(&(this->_rects[i])),v_negation_mask);
                __m512 const v_merged = _mm512_xor_ps(_mm512_max_ps(v_rects,v_key),v_negation_mask);
                const int permute_mask = 0b01001110;
                __m512 const v_permuted = _mm512_permute_ps(v_merged, permute_mask);
                __m512 const v_zero = _mm512_setzero_ps();
                __m512 const v_result = _mm512_xor_ps(_mm512_sub_ps(v_permuted,v_merged),v_negation_mask);
                    */
#ifdef TEST_BUILD
                __m512 const v_merged = _mm512_xor_ps(_mm512_max_ps(_mm512_xor_ps(_mm512_loadu_ps(&(this->_rects[i])),v_negation_mask),v_key),v_negation_mask);
#else
                __m512 const v_merged = _mm512_xor_ps(
                        _mm512_max_ps(_mm512_xor_ps(_mm512_load_ps(&(this->_rects[i])), v_negation_mask), v_key),
                        v_negation_mask);
#endif
                const int permute_mask = 0b01001110;
                __m512 const v_permuted = _mm512_permute_ps(v_merged, permute_mask);
                __m512 const v_zero = _mm512_setzero_ps();

                __mmask16 const v_cmp_result = _mm512_cmp_ps_mask(
                        _mm512_xor_ps(_mm512_sub_ps(v_permuted, v_merged), v_negation_mask), v_zero, _CMP_GE_OQ);
                for (int j = 0; j < 4 && j + i < max_node_size; j++) {
                    if (PEAR_CHECK_BITMAP(bitmap_at_begin, (i + j)) &&
                        (v_cmp_result & (cmp_result_mask << (j * 4))) == (cmp_result_mask << (j * 4))) {
                        if (_level > 0) {
                            node_type * const child_node = this->_branches[i+j].get_child_node();
                            bfs_queue.emplace_back(child_node, child_node->get_last_version_before_split(), false);
                        } else {
                            result.insert(this->_branches[i + j].get_data());
                        }
                    }
                }
            }
        }
    }

    void contains_query_selection(typename node_type::rect_type const &search_space, std::vector<size_t> &result, META_DATA_TYPE const bitmap_at_begin, std::vector<node_type*> &candidates) const {
        // TODO: find better solution
        if(PEAR_ENABLE_SIMD) {
            for (int i = 0; i < max_node_size; i ++) {
                if(PEAR_CHECK_BITMAP(bitmap_at_begin, i)) {
                    typename node_type::rect_type const &rect = _rects[i];
                    if (_level == 0) {
                        if (covers(search_space, (rect))) {
                            result.push_back(_branches[i].get_data());
                        }
                    } else {
                        if (overlap_area((rect), search_space) > 0.0f) {
                            candidates.push_back(this->_branches[i].get_child_node());
                        }
                    }
                }
            }
        } else {
            __m512 const v_negation_mask = _mm512_set4_ps(-0.0f, -0.0f, 0.0f, 0.0f);
            __m512 const v_key = _mm512_xor_ps(
                    _mm512_set4_ps(search_space.y_max, search_space.x_max, search_space.y_min, search_space.x_min),
                    v_negation_mask);
            if(PEAR_ENABLE_PREFETCHING) {
                _mm_prefetch((char const *) &(this->_rects[0]), _MM_HINT_T0);
                _mm_prefetch((char const *) &(this->_rects[4]), _MM_HINT_T0);
            }
            __mmask16 const cmp_result_mask = 0b1111;
            for (int i = 0; i < max_node_size; i += 4) {
                if(PEAR_ENABLE_PREFETCHING) {
                    _mm_prefetch((char const *) &(this->_rects[i + 8]), _MM_HINT_T0);
                }
                /** Use SIMD AVX instruction to compute
                    overlap_area() > 0


                __m512 const v_rects = _mm512_xor_ps(_mm512_loadu_ps(&(this->_rects[i])),v_negation_mask);
                __m512 const v_merged = _mm512_xor_ps(_mm512_max_ps(v_rects,v_key),v_negation_mask);
                const int permute_mask = 0b01001110;
                __m512 const v_permuted = _mm512_permute_ps(v_merged, permute_mask);
                __m512 const v_zero = _mm512_setzero_ps();
                __m512 const v_result = _mm512_xor_ps(_mm512_sub_ps(v_permuted,v_merged),v_negation_mask);
                    */
#ifdef TEST_BUILD
                __m512 const v_merged = _mm512_xor_ps(_mm512_max_ps(_mm512_xor_ps(_mm512_loadu_ps(&(this->_rects[i])),v_negation_mask),v_key),v_negation_mask);
#else
                __m512 const v_merged = _mm512_xor_ps(
                        _mm512_max_ps(_mm512_xor_ps(_mm512_load_ps(&(this->_rects[i])), v_negation_mask), v_key),
                        v_negation_mask);
#endif
                const int permute_mask = 0b01001110;
                __m512 const v_permuted = _mm512_permute_ps(v_merged, permute_mask);
                __m512 const v_zero = _mm512_setzero_ps();

                __mmask16 const v_cmp_result = _mm512_cmp_ps_mask(
                        _mm512_xor_ps(_mm512_sub_ps(v_permuted, v_merged), v_negation_mask), v_zero, _CMP_GE_OQ);
                for (int j = 0; j < 4 && j + i < max_node_size; j++) {
                    if (PEAR_CHECK_BITMAP(bitmap_at_begin, (i + j)) && (v_cmp_result & (cmp_result_mask << (j * 4))) == (cmp_result_mask << (j * 4))) {
                        if (_level > 0) {
                            candidates.push_back(this->_branches[i+j].get_child_node());
                        } else {
                            result.push_back(this->_branches[i + j].get_data());
                        }
                    }
                }
            }
        }
    }

    void equals_query_selection(typename node_type::rect_type const &search_space, PEAR_QUERY_QUEUE_TYPE &bfs_queue, std::set<size_t> &result, META_DATA_TYPE const bitmap_at_begin) const {
        if(!PEAR_ENABLE_SIMD) {
            for (int i = 0; i < max_node_size; i ++) {
                if(PEAR_CHECK_BITMAP(bitmap_at_begin, i)) {
                    typename node_type::rect_type const &rect = _rects[i];
                    if (_level == 0) {
                        if (covers(search_space, (rect))) {
                            result.insert(_branches[i].get_data());
                        }
                    } else {
                        if (overlap_area((rect), search_space) > 0.0f) {
                            bfs_queue.emplace_back(_branches[i].get_child_node(), _branches[i].get_child_node()->get_last_version_before_split(), false);
                        }
                    }
                }
            }
        } else {
            INVALID_BRANCH_TAKEN
            __m512 const v_negation_mask = _mm512_set4_ps(-0.0f, -0.0f, 0.0f, 0.0f);
            __m512 const v_key = _mm512_xor_ps(
                    _mm512_set4_ps(search_space.y_max, search_space.x_max, search_space.y_min, search_space.x_min),
                    v_negation_mask);
            if (PEAR_ENABLE_PREFETCHING) {
                _mm_prefetch((char const *) &(this->_rects[0]), _MM_HINT_T0);
                _mm_prefetch((char const *) &(this->_rects[4]), _MM_HINT_T0);
            }
            __mmask16 const cmp_result_mask = 0b1111;
            for (int i = 0; i < max_node_size; i += 4) {
                if (PEAR_ENABLE_PREFETCHING) {
                    _mm_prefetch((char const *) &(this->_rects[i + 8]), _MM_HINT_T0);
                }
                /** Use SIMD AVX instruction to compute
                    *
                           rectA.x_min >= rectB.x_min
                        && rectA.y_min >= rectB.y_min
                        && rectA.x_max <= rectB.x_max
                        && rectA.y_max <= rectB.y_max
                    */
                //                __m512 const v_rects = _mm512_set_ps(_rects[i].x_min,_rects[i].y_min,_rects[i].x_max,_rects[i].y_max,_rects[i + 1].x_min,_rects[i + 1].y_min,_rects[i + 1].x_max,_rects[i + 1].y_max)
#ifdef TEST_BUILD
                __m512 const v_rects = _mm512_xor_ps(_mm512_loadu_ps(&(this->_rects[i])),v_negation_mask);
#else
                __m512 const v_rects = _mm512_xor_ps(_mm512_load_ps(&(this->_rects[i])), v_negation_mask);
#endif
                __mmask16 v_cmp_result;
                if (_level == 0) {
                    v_cmp_result = _mm512_cmp_ps_mask(v_key, v_rects, _CMP_EQ_OQ);
                } else {
                    v_cmp_result = _mm512_cmp_ps_mask(v_key, v_rects, _CMP_GE_OQ);
                }
                for (int j = 0; j < 4 && j + i < max_node_size; j++) {
                    if (PEAR_CHECK_BITMAP(bitmap_at_begin, (i + j)) &&
                        (v_cmp_result & (cmp_result_mask << (j * 4))) == (cmp_result_mask << (j * 4))) {
                        if (_level > 0) {
                            node_type *const child_node = this->_branches[i + j].get_child_node();
                            bfs_queue.emplace_back(child_node, child_node->get_last_version_before_split(), false);
                        } else {
                            result.insert(this->_branches[i + j].get_data());
                        }
                    }
                }
            }
        }
    }

    void equals_query_selection(typename node_type::rect_type const &search_space, std::vector<size_t> &result, META_DATA_TYPE const bitmap_at_begin, std::vector<node_type*> &candidates) const {
        // TODO: find better solution
        if(!PEAR_ENABLE_SIMD) {
            for (int i = 0; i < max_node_size; i ++) {
                if(PEAR_CHECK_BITMAP(bitmap_at_begin, i)) {
                    typename node_type::rect_type const &rect = _rects[i];
                    if (_level == 0) {
                        if (search_space == rect) {
                            result.push_back(_branches[i].get_data());
                        }
                    } else {
                        if (covers((rect), search_space)) {
                            candidates.push_back(this->_branches[i].get_child_node());
                        }
                    }
                }
            }
        } else {
            __m512 const v_negation_mask = _mm512_set4_ps(-0.0f, -0.0f, 0.0f, 0.0f);
            __m512 const v_key = _mm512_xor_ps(
                    _mm512_set4_ps(search_space.y_max, search_space.x_max, search_space.y_min, search_space.x_min),
                    v_negation_mask);
            if (PEAR_ENABLE_PREFETCHING) {
                _mm_prefetch((char const *) &(this->_rects[0]), _MM_HINT_T0);
                _mm_prefetch((char const *) &(this->_rects[4]), _MM_HINT_T0);
            }
            __mmask16 const cmp_result_mask = 0b1111;
            for (int i = 0; i < max_node_size; i += 4) {
                if (PEAR_ENABLE_PREFETCHING) {
                    _mm_prefetch((char const *) &(this->_rects[i + 8]), _MM_HINT_T0);
                }
                /** Use SIMD AVX instruction to compute
                    *
                           rectA.x_min >= rectB.x_min
                        && rectA.y_min >= rectB.y_min
                        && rectA.x_max <= rectB.x_max
                        && rectA.y_max <= rectB.y_max
                    */
                //                __m512 const v_rects = _mm512_set_ps(_rects[i].x_min,_rects[i].y_min,_rects[i].x_max,_rects[i].y_max,_rects[i + 1].x_min,_rects[i + 1].y_min,_rects[i + 1].x_max,_rects[i + 1].y_max)
#ifdef TEST_BUILD
                __m512 const v_rects = _mm512_xor_ps(_mm512_loadu_ps(&(this->_rects[i])),v_negation_mask);
#else
                __m512 const v_rects = _mm512_xor_ps(_mm512_load_ps(&(this->_rects[i])), v_negation_mask);
#endif
                __mmask16 v_cmp_result;
                if (_level == 0) {
                    v_cmp_result = _mm512_cmp_ps_mask(v_key, v_rects, _CMP_EQ_OQ);
                } else {
                    v_cmp_result = _mm512_cmp_ps_mask(v_key, v_rects, _CMP_GE_OQ);
                }
                for (int j = 0; j < 4 && j + i < max_node_size; j++) {
                    if (PEAR_CHECK_BITMAP(bitmap_at_begin, (i + j)) &&
                        (v_cmp_result & (cmp_result_mask << (j * 4))) == (cmp_result_mask << (j * 4))) {
                        if (_level > 0) {
                            candidates.push_back(this->_branches[i+j].get_child_node());
                        } else {
                            result.push_back(this->_branches[i + j].get_data());
                        }
                    }
                }
            }
        }
    }

    // return codes (0 = value not found; 1 = value deleted; 2 = branch is reorganizing)
    int remove(typename node_type::rect_type const &key, size_t const value, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        PEAR_NODE_VERSION_TYPE initial_version;
        bool subtree_reorg;
        do {
            subtree_reorg = false;
            initial_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
            if(PEAR_IS_REORGANIZING(initial_version)) {
                return 2;
            } else if (IS_LOCKED(initial_version)) {
//                repair_node(this, meta_data_long, global_version);
                continue;
            }
            size_t initial_bitmap = this->_bitmap;
            for (int i = 0; i < max_node_size; i++) {
                if(!PEAR_CHECK_BITMAP(initial_bitmap,i)) {
                    continue;
                }
                if (contains(_rects[i], key)) {
                    int remove_code;
                    if(_level > 1) {
                        remove_code = _branches[i].get_child_node()->remove(key, value, global_version);
                    } else {
                        remove_code = _branches[i].get_child_node()->remove_leaf_blocking(key, value, global_version);
                    }
                    if(remove_code == 1) {
                        // TODO (delete): Update parent branch MBR but only if the branch was not merged
                        return 1;
                    } else if(remove_code == 2) {
                        subtree_reorg = true;
                        break;
                    }
                }
            }
        } while (subtree_reorg || initial_version != PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data)));
        return 0;
    }

    int remove_leaf_blocking(typename node_type::rect_type const &key, size_t const value, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_meta_data, if(PEAR_IS_REORGANIZING(value_var___)) return 2;)
        bool value_deleted = false;
        for (int i = 0; i < max_node_size; i++) {
            if (PEAR_CHECK_BITMAP(this->_bitmap, i) && _rects[i] == key && _branches[i].get_data() == value) {
                unset_and_persist(i);
                value_deleted = true;
                break;
            }
        }
        if(!value_deleted) {
            PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
            return false;
        }
        Pointtype min_x = std::numeric_limits<Pointtype>::max();
        Pointtype min_y = std::numeric_limits<Pointtype>::max();
        Pointtype max_x = -std::numeric_limits<Pointtype>::max();
        Pointtype max_y = -std::numeric_limits<Pointtype>::max();
        for(int j = 0; j < max_node_size; j++) {
            if(PEAR_CHECK_BITMAP(this->_bitmap, j)) {
                typename node_type::rect_type const & branch_rect = _rects[j];
                min_x = std::min(min_x, branch_rect.x_min);
                min_y = std::min(min_y, branch_rect.y_min);
                max_x = std::max(max_x, branch_rect.x_max);
                max_y = std::max(max_y, branch_rect.y_max);
            }
        }
        int const branch_count = count_valid_branches();
        if(branch_count < min_node_size) {
//            reset_version(meta_data_long);
            // Call merge_child on parent (like split_child)
            PEAR_NODE_VERSION_TYPE old_version = reset_version(global_version);
            while(!_parent_node->merge_child(min_x, min_y, max_x, max_y, this, branch_count, global_version));
            PEAR_RELEASE_NODE_LOCK_WITH_VERSION(this->_version_meta_data, old_version)
            return true;
        } else {
            while(!_parent_node->update_mbr(min_x, min_y, max_x, max_y, this, global_version));
        }
        PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
        return 1;
    }

    //
    bool update_mbr(Pointtype const x_min, Pointtype const y_min, Pointtype const x_max, Pointtype const y_max, parent_node_type * const origin, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        if(IS_LOCKED(LOAD_META_INLINE(this->_version_meta_data))) {
            return false;
        }
        META_DATA_TYPE const initial_bitmap = this->_bitmap;
        bool rect_updated = false;
        for(int i = 0; i < max_node_size; i++) {
            if(PEAR_CHECK_BITMAP(initial_bitmap, i) && this->_branches[i].get_child_node() == origin) {
                ACQUIRE_META_ATOMIC(this->_branches[i].atomic_version)
                if(PEAR_IS_REORGANIZING(LOAD_META_INLINE(this->_version_meta_data))) {
                    PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(this->_branches[i].atomic_version)
                    return false;
                }
                typename node_type::rect_type const & branch_rect = this->_rects[i];
                if(branch_rect.x_min != x_min || branch_rect.y_min != y_min || branch_rect.x_max != x_max || branch_rect.y_max != y_max) {
                    new(&(this->_rects[i])) typename node_type::rect_type(x_min, y_min, x_max, y_max);
                    rect_updated = true;
                }
                PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE(this->_branches[i].atomic_version)
                break;
            }
        }
        if(rect_updated) {
            ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_meta_data, if(PEAR_IS_REORGANIZING(value_var___)) return true;)
            for (int i = 0; i < max_node_size; i++) {
                while (PEAR_CHECK_BITMAP(this->_bitmap, i) && IS_LOCKED(LOAD_META_INLINE(this->_branches[i].atomic_version))) {
                    PEAR_NODE_VERSION_TYPE const node_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_branches[i].get_child_node()->_version_meta_data));
                    if(_level == 1 && PEAR_IS_REORGANIZING(node_version) && !PEAR_SPLIT_PARENT_STARTED(node_version)) {
                        break;
                    }
                }
            }
            std::atomic_thread_fence(std::memory_order_acquire);
            Pointtype min_x = std::numeric_limits<Pointtype>::max();
            Pointtype min_y = std::numeric_limits<Pointtype>::max();
            Pointtype max_x = -std::numeric_limits<Pointtype>::max();
            Pointtype max_y = -std::numeric_limits<Pointtype>::max();
            for(int i = 0; i < max_node_size; i++) {
                if(PEAR_CHECK_BITMAP(this->_bitmap, i)) {
                    typename node_type::rect_type const & branch_rect = this->_rects[i];
                    min_x = std::min(min_x, branch_rect.x_min);
                    min_y = std::min(min_y, branch_rect.y_min);
                    max_x = std::max(max_x, branch_rect.x_max);
                    max_y = std::max(max_y, branch_rect.y_max);
                }
            }
            RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_version_meta_data)
            while(!_parent_node->update_mbr(min_x, min_y, max_x, max_y, this, global_version));
        }
        return true;
    }

    bool merge_child(Pointtype const x_min, Pointtype const y_min, Pointtype const x_max, Pointtype const y_max, parent_node_type * const origin, int const underflown_branch_count, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_meta_data, if(PEAR_IS_REORGANIZING(value_var___)) return false;)
        // If parent acquires lock successfully select subtree with least area enlargement if both mbrs are joined (!!!recalculate MBR of removed MBR)
        int underflown_id = -1;
        for (int i = 0; i < max_node_size; i++) {
            while (PEAR_CHECK_BITMAP(this->_bitmap, i) && IS_LOCKED(LOAD_META_INLINE(this->_branches[i].atomic_version))) {
                PEAR_NODE_VERSION_TYPE const node_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_branches[i].get_child_node()->_version_meta_data));
                if(_level == 1 && PEAR_IS_REORGANIZING(node_version) && !PEAR_SPLIT_PARENT_STARTED(node_version)) {
                    break;
                }
            }
            if(PEAR_CHECK_BITMAP(this->_bitmap, i) && this->_branches[i].get_child_node() == origin) {
                new(&(this->_rects[i])) typename node_type::rect_type(x_min, y_min, x_max, y_max);
                underflown_id = i;
            }
        }

        int const subtree_candidate = r_star_find_subtree_inner(this->_rects[underflown_id], underflown_id);
        int const candidate_branch_count = this->_branches[subtree_candidate].get_child_node()->get_branch_count();
        if(candidate_branch_count + underflown_branch_count > max_node_size) {
            // Use split algorithm to rearrange both nodes if they overflow
            // Simple approach since it is not part of the performance measurements:
            // Copy missing rectangles from larger node to underflown node (e.g. call copy_nodes(amount, target_node) on larger node)
            if(!this->_branches[subtree_candidate].get_child_node()->partially_move_branches(this->_branches[underflown_id].get_child_node(), min_node_size-underflown_branch_count, &(this->_rects[underflown_id]) ,&(this->_rects[subtree_candidate]), global_version)) {
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
                return false;
            }
        } else {
            // copy all branches from underflown node to new node
            // (e.g.) call copy_nodes(underflown_branch_count, larger_node) on underflown node
            ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_branches[subtree_candidate].get_child_node()->_version_meta_data, if(PEAR_IS_REORGANIZING(value_var___)) {
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
                return false;
            })
            this->_branches[underflown_id].get_child_node()->move_all_branches(this->_branches[subtree_candidate].get_child_node(), global_version);
            join(this->_rects[subtree_candidate], this->_rects[underflown_id], &(this->_rects[subtree_candidate]));
            this->unset_and_persist(underflown_id);
            PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_branches[subtree_candidate].get_child_node()->_version_meta_data)
        }
        Pointtype min_x = std::numeric_limits<Pointtype>::max();
        Pointtype min_y = std::numeric_limits<Pointtype>::max();
        Pointtype max_x = -std::numeric_limits<Pointtype>::max();
        Pointtype max_y = -std::numeric_limits<Pointtype>::max();
        for(int i = 0; i < max_node_size; i++) {
            if(PEAR_CHECK_BITMAP(this->_bitmap, i)) {
                typename node_type::rect_type const & branch_rect = this->_rects[i];
                min_x = std::min(min_x, branch_rect.x_min);
                min_y = std::min(min_y, branch_rect.y_min);
                max_x = std::max(max_x, branch_rect.x_max);
                max_y = std::max(max_y, branch_rect.y_max);
            }
        }
        RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_version_meta_data)
        while(!_parent_node->update_mbr(min_x, min_y, max_x, max_y, this, global_version));
        return true;
    }

    void move_all_branches(node_type * const dest, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        if(_level == 0 ) {
            for(int i = 0; i < max_node_size; i++) {
                if(PEAR_CHECK_BITMAP(this->_bitmap, i)) {
                    dest->add_data(reinterpret_cast<size_t>(_branches[i].m_child), _rects[i]);
                    unset_and_persist(i);
                }
            }
        } else {
            for(int i = 0; i < max_node_size; i++) {
                if(PEAR_CHECK_BITMAP(this->_bitmap, i)) {
                    dest->add_subtree(_branches[i].get_child_node(), _rects[i]);
                    unset_and_persist(i);
                }
            }
        }
    }

    bool partially_move_branches(node_type * const dest, int const amount, typename node_type::rect_type * const rect_to_update, typename node_type::rect_type * const mbr, PEAR_GLOBAL_VERSION_TYPE const global_version) override {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_meta_data, if(PEAR_IS_REORGANIZING(value_var___)) return false;)
        int branches_copied = 0;
        for(int i = 0; i < max_node_size && branches_copied < amount; i++) {
            if(PEAR_CHECK_BITMAP(this->_bitmap, i)) {
                dest->add_subtree(_branches[i].get_child_node(), _rects[i]);
                while (PEAR_CHECK_BITMAP(this->_bitmap, i) && IS_LOCKED(LOAD_META_INLINE(this->_branches[i].atomic_version))) {
                    PEAR_NODE_VERSION_TYPE const node_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_branches[i].get_child_node()->_version_meta_data));
                    if(_level == 1 && PEAR_IS_REORGANIZING(node_version) && !PEAR_SPLIT_PARENT_STARTED(node_version)) {
                        break;
                    }
                }
                join(*rect_to_update, this->_rects[i], rect_to_update);
                unset_and_persist(i);
            }
        }
        // Set MBR
        Pointtype min_x = std::numeric_limits<Pointtype>::max();
        Pointtype min_y = std::numeric_limits<Pointtype>::max();
        Pointtype max_x = -std::numeric_limits<Pointtype>::max();
        Pointtype max_y = -std::numeric_limits<Pointtype>::max();
        for(int i = 0; i < max_node_size; i++) {
            if(PEAR_CHECK_BITMAP(this->_bitmap, i)) {
                typename node_type::rect_type const & branch_rect = this->_rects[i];
                min_x = std::min(min_x, branch_rect.x_min);
                min_y = std::min(min_y, branch_rect.y_min);
                max_x = std::max(max_x, branch_rect.x_max);
                max_y = std::max(max_y, branch_rect.y_max);
            }
        }
        new(mbr) typename node_type::rect_type(min_x, min_y, max_x, max_y);
        PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
        return true;
    }


    bool find_insert_path(typename node_type::rect_type const &key, typename node_type::path_entry_type* const path_array, uint32_t const global_version, optional_args* const args_ptr, uint32_t const at_level=0, uint32_t const dram_save_level=INT32_MAX) override {
        if(PEAR_USE_MAX_PMEM && max_pmem_level == 0) {
            INVALID_FUNCTION_CALL
        }
        bool found_path = false;
        while(!found_path) {
            int subtree = -1;
            uint32_t initial_version;
            while (subtree == -1) {
                initial_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
                if(PEAR_IS_REORGANIZING(initial_version)) {
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->aborts_node_changed++;
                    }
                    return false;
                } else if(IS_LOCKED(initial_version)) {
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->aborts_node_locked++;
                    }
                    continue;
                }
                subtree = find_subtree(key);
            }

            META_DATA_TYPE const check_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
            if(PEAR_IS_REORGANIZING(check_version)) {
                if(PEAR_COLLECT_CONCURRENCY_STATS) {
                    args_ptr->aborts_node_changed++;
                }
                return false;
            } else if(initial_version != PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data))) {
                if(PEAR_COLLECT_CONCURRENCY_STATS) {
                    args_ptr->aborts_node_locked++;
                }
                continue;
            }

            path_array[_level - 1] = typename node_type::path_entry_type(this, initial_version, &(this->_branches[subtree]), &(this->_rects[subtree]));
            if (_level == at_level+1) {
                return true;
            } else {
                found_path = this->_branches[subtree].get_child_node()->find_insert_path(key, path_array, global_version, args_ptr,at_level);
            }
        }
        return true;
    }

    void r_star_find_subtree_inner_sequential(typename node_type::rect_type const &key, __m128 &v_min, int &child_candidate, int const start, int const skip = -1) const {
        for(int i = start; i < max_node_size; i++) {
            if (PEAR_CHECK_BITMAP(_bitmap, i)) {
                if(skip == i) {
                    continue;
                }
                float const current_area = area_inline(this->_rects[i]);
                __m128 const v_candidate = {current_area, area_2(this->_rects[i], key) - current_area, std::numeric_limits<float>::max(), std::numeric_limits<float>::max()};
                __m128 const a = _mm_cmp_ps(v_min,v_candidate,_CMP_GT_OQ);
                __m128 const b = _mm_cmp_ps(v_min,v_candidate,_CMP_EQ_OQ);
                if (a[1] || (b[1] && a[0])) {
                    v_min = v_candidate;
                    child_candidate = i;
                    if(PEAR_ENABLE_PREFETCHING) {
                        _mm_prefetch((char const*)&(this->_branches[i].atomic_version),_MM_HINT_T1);
                    }
                }
            }
        }
    }

    void r_star_find_subtree_leaf_sequential(typename node_type::rect_type const &key, __m128 &v_min, int &child_candidate, int const start, int const skip = -1) const {

        std::priority_queue<std::pair<Pointtype, int>> max_heap;
        int count = 0;
        for(int i = start; i < max_node_size; i++) {
            if (PEAR_CHECK_BITMAP(_bitmap, i)) {
                if(skip == i) {
                    continue;
                }
                Pointtype const area_enlargement = area_2(this->_rects[i], key) - area_inline(this->_rects[i]);
                if(count < PEAR_CHOOSE_SUBTREE_K) {
                    max_heap.emplace(area_enlargement, i);
                    count++;
                } else if(area_enlargement < max_heap.top().first) {
                    max_heap.pop();
                    max_heap.emplace(area_enlargement, i);
                }
            }
        }
        for(int i = 0; i < PEAR_CHOOSE_SUBTREE_K; i++) {
            int const candidate_index  = max_heap.top().second;
            max_heap.pop();
            typename node_type::rect_type const &rect = this->_rects[candidate_index];
            Pointtype current_overlap = 0;
            Pointtype new_overlap = 0;
            for(int j = start; j < max_node_size; j++) {
                if(j == candidate_index) skip;
                if (PEAR_CHECK_BITMAP(_bitmap, j)) {
                    current_overlap += overlap_area(rect,this->_rects[j]);
                    new_overlap += overlap_area(rect,key,this->_rects[j]);
                }
            }
            __m128 const v_candidate = {area_inline(rect),new_overlap-current_overlap, std::numeric_limits<float>::max(), std::numeric_limits<float>::max()};
            __m128 const a = _mm_cmp_ps(v_min,v_candidate,_CMP_GT_OQ);
            __m128 const b = _mm_cmp_ps(v_min,v_candidate,_CMP_EQ_OQ);
            if (a[1] || (b[1] && a[0])) {
                v_min = v_candidate;
                child_candidate = candidate_index;
                if(PEAR_ENABLE_PREFETCHING) {
                    _mm_prefetch((char const*)&(this->_branches[i].atomic_version),_MM_HINT_T1);
                }
            }
        }

    }

    int find_subtree(typename node_type::rect_type const &key) const override {
        if(PEAR_USE_RSTAR_LEAF_SELECTION) {
            return r_star_find_subtree(key);
        } else {
            return r_star_find_subtree_inner(key);
        }
    }

    int r_star_find_subtree(typename node_type::rect_type const &key) const {
        if(this->_level == 1) {
            return r_star_find_subtree_leaf(key);
        } else {
            return r_star_find_subtree_inner(key);
        }
    }

    int r_star_find_subtree_leaf(typename node_type::rect_type const &key, int const skip = -1) const {
        if(PEAR_ENABLE_PREFETCHING) {
            for(int i = 0; i < max_node_size; i+=4) {
                _mm_prefetch((char const*)&(this->_rects[i]), _MM_HINT_T1);
            }
        }
        int child_candidate = -1;
        __m128 v_min = _mm_set1_ps(FLT_MAX);
        if(!PEAR_ENABLE_SIMD) {
            r_star_find_subtree_leaf_sequential(key,v_min,child_candidate,0, skip);
            return child_candidate;
        } else {
            UNIMPLEMENTED_FUNCTION
        }

    }

    int r_star_find_subtree_inner(typename node_type::rect_type const &key, int const skip = -1) const {
        if(PEAR_ENABLE_PREFETCHING) {
            for(int i = 0; i < max_node_size; i+=4) {
                _mm_prefetch((char const*)&(this->_rects[i]), _MM_HINT_T1);
            }
        }
        int child_candidate = -1;
        __m128 v_min = _mm_set1_ps(FLT_MAX);
        if(!PEAR_ENABLE_SIMD) {
            r_star_find_subtree_inner_sequential(key,v_min,child_candidate,0, skip);
            return child_candidate;
        }
#ifdef USE_PMEM_INSTRUCTIONS
        __m256 const v_key_x_max = _mm256_set1_ps(key.x_max);
        __m256 const v_key_x_min = _mm256_set1_ps(key.x_min);
        __m256 const v_key_y_max = _mm256_set1_ps(key.y_max);
        __m256 const v_key_y_min = _mm256_set1_ps(key.y_min);
        __m256i const minus1 = _mm256_set1_epi32(-1);
        __m256 const mask = _mm256_castsi256_ps(_mm256_srli_epi32(minus1, 1));
        if(PEAR_ENABLE_PREFETCHING) {
            _mm_prefetch((char const*)&(this->_rects[0]), _MM_HINT_T0);
            _mm_prefetch((char const*)&(this->_rects[4]), _MM_HINT_T0);
        }
        for (int i = 0; i <= max_node_size-6; i+=8) {
            if(PEAR_ENABLE_PREFETCHING) {
                _mm_prefetch((char const*)&(this->_rects[i + 8]), _MM_HINT_T0);
                _mm_prefetch((char const*)&(this->_rects[i + 12]), _MM_HINT_T0);
            }
            __m256 const v_x_max = {this->_rects[i].x_max, this->_rects[i + 1].x_max, this->_rects[i + 2].x_max, this->_rects[i + 3].x_max, this->_rects[i + 4].x_max, this->_rects[i + 5].x_max, this->_rects[i + 6].x_max, this->_rects[i + 7].x_max};
            __m256 const v_x_min = {this->_rects[i].x_min, this->_rects[i + 1].x_min, this->_rects[i + 2].x_min, this->_rects[i + 3].x_min, this->_rects[i + 4].x_min, this->_rects[i + 5].x_min, this->_rects[i + 6].x_min, this->_rects[i + 7].x_min};
            __m256 const v_y_max = {this->_rects[i].y_max, this->_rects[i + 1].y_max, this->_rects[i + 2].y_max, this->_rects[i + 3].y_max, this->_rects[i + 4].y_max, this->_rects[i + 5].y_max, this->_rects[i + 6].y_max, this->_rects[i + 7].y_max};
            __m256 const v_y_min = {this->_rects[i].y_min, this->_rects[i + 1].y_min, this->_rects[i + 2].y_min, this->_rects[i + 3].y_min, this->_rects[i + 4].y_min, this->_rects[i + 5].y_min, this->_rects[i + 6].y_min, this->_rects[i + 7].y_min};
            __m256 const v_abs_areas = _mm256_and_ps(mask,_mm256_mul_ps(_mm256_sub_ps(v_x_max,v_x_min),_mm256_sub_ps(v_y_max,v_y_min)));
            __m256 const v_abs_areas_2_subtracted = _mm256_sub_ps(
                    _mm256_and_ps(
                            mask,
                            _mm256_mul_ps(
                                    _mm256_sub_ps(
                                            _mm256_max_ps(v_key_x_max, v_x_max),
                                            _mm256_min_ps(v_key_x_min, v_x_min)
                                    ),
                                    _mm256_sub_ps(
                                            _mm256_max_ps(v_key_y_max, v_y_max),
                                            _mm256_min_ps(v_key_y_min, v_y_min))
                            )
                    ),
                    v_abs_areas
            );

            for(int j = 0; j < 8 && i+j < max_node_size; j++) {
                if (PEAR_CHECK_BITMAP(_bitmap, (i+j))) {
//                repair_branch(_children[i], this, global_version);
                    if(skip == i+j) {
                        continue;
                    }
                    __m128 const v_candidate = {v_abs_areas[j],v_abs_areas_2_subtracted[j],std::numeric_limits<float>::max(),std::numeric_limits<float>::max()};
                    __m128 const a = _mm_cmp_ps(v_min,v_candidate,_CMP_GT_OQ);
                    __m128 const b = _mm_cmp_ps(v_min,v_candidate,_CMP_EQ_OQ);
                    if (a[1] || (b[1] && a[0])) {
                        v_min = v_candidate;
                        child_candidate = i+j;
                        if(PEAR_ENABLE_PREFETCHING) {
                            _mm_prefetch((char const*)&(this->_branches[i].atomic_version),_MM_HINT_T1);
                        }
                    }
                }
            }
        }
        if(max_node_size%8 > 0 && max_node_size%8 < 6) {
            r_star_find_subtree_inner_sequential(key,v_min,child_candidate,max_node_size-(max_node_size%8), skip);
        }
#else
        r_star_find_subtree_inner_sequential(key,v_min,child_candidate,0, skip);
#endif
        return child_candidate;
    }

    std::atomic<META_DATA_TYPE>* insert_leaf(typename node_type::rect_type const &key, size_t const value, std::atomic<META_DATA_TYPE>** const branch_lock_overflown_branch, PEAR_GLOBAL_VERSION_TYPE const global_version, optional_args* const args_ptr) override {
        int const position = get_next_free_position();
        if (position == max_node_size) {
            std::atomic<META_DATA_TYPE> * new_branch_lock = NULL;
            if (p_node_type *const new_node_ptr = this->r_star_split(key, global_version, branch_lock_overflown_branch, &(new_branch_lock), args_ptr)) {
                // FPR split step 7
                int const new_position = new_node_ptr->get_next_free_position();
                new_node_ptr->add_data_with_position(key, value, new_position);
                if(_level != 0) {
                    reinterpret_cast<node_type *>(value)->set_parent_node(new_node_ptr);
                }
                PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((*(new_branch_lock)))
            } else {
                // FPR split step 7
                PEAR_RELEASE_BRANCH_LOCK_WITH_INCREASE((*(new_branch_lock)))
                int const new_position = get_next_free_position();
                this->add_data_with_position(key, value, new_position);
                if(_level != 0) {
                    reinterpret_cast<node_type *>(value)->set_parent_node(this);
                }
            }
            return *branch_lock_overflown_branch;
        }
        this->add_data_with_position(key, value, position);
        if(_level != 0) {
            reinterpret_cast<node_type *>(value)->set_parent_node(this);
        }
        return *branch_lock_overflown_branch;
    }

    void add_data(size_t const value, typename node_type::rect_type const &key) override {
        add_data_with_position(key, value, get_next_free_position());
    }

    void add_data_with_position(typename node_type::rect_type const  & key, size_t const value, int const position) {
        new(&(this->_branches[position])) typename node_type::branch_type(value);
        new(&(this->_rects[position])) typename node_type::rect_type (key.x_min, key.y_min, key.x_max, key.y_max);

        PearTreeAllocatorSingleton::fence_persist(&(this->_branches[position]), sizeof(typename node_type::branch_type));
        PearTreeAllocatorSingleton::persist(&(this->_rects[position]), sizeof(typename node_type::rect_type));
        set_and_persist(position);
    }

    void add_subtree(node_type * const node_ptr, typename node_type::rect_type const &key) override {
        add_subtree_with_position(node_ptr, key, get_next_free_position());
    }

    void add_subtree_with_position(node_type * const node_ptr, typename node_type::rect_type const &key, int const position, bool const locked = false) {
        new(&(this->_branches[position])) typename node_type::branch_type(PEAR_MIN_BRANCH_UNRESERVED_VERSION, node_ptr, locked, false);
        new(&(this->_rects[position])) typename node_type::rect_type(key.x_min, key.y_min, key.x_max, key.y_max);

        node_ptr->set_parent_node(this);
        PearTreeAllocatorSingleton::fence_persist(&(this->_branches[position]), sizeof(typename node_type::branch_type));
        PearTreeAllocatorSingleton::persist(&(this->_rects[position]), sizeof(typename node_type::rect_type));
        set_and_persist(position);
    }

    void unset_and_persist(int const position) {
        this->_bitmap = PEAR_UNSET_POSITION_INLINE(this->_bitmap, position);
        PearTreeAllocatorSingleton::fence_persist(&(this->_bitmap), sizeof(this->_bitmap));
    }

    void set_and_persist(int const position) {
        _bitmap = PEAR_SET_POSITION_INLINE(_bitmap, position);
        PearTreeAllocatorSingleton::fence_persist(&(this->_bitmap), sizeof(this->_bitmap));
    }

    inline void set_branch_lock_ptr(std::atomic<META_DATA_TYPE>** const branch_lock_ptr, int const position) {
        *branch_lock_ptr = &(this->_branches[position].atomic_version);
    }

    void set_sibling_pointer(p_node_type* const new_sibling) {
        if(_sibling_pointer) {
            new_sibling->set_sibling_pointer(_sibling_pointer);
        }
        _sibling_pointer = new_sibling;
        PearTreeAllocatorSingleton::fence_persist(&_sibling_pointer, sizeof(p_node_type*));
    }

    void set_sibling_pointer(node_type* const new_sibling) override {
        p_node_type* const casted_new_sibling = dynamic_cast<p_node_type*>(new_sibling);
        if(_sibling_pointer) {
            casted_new_sibling->set_sibling_pointer(_sibling_pointer);
        }
        _sibling_pointer = casted_new_sibling;
        PearTreeAllocatorSingleton::fence_persist(&_sibling_pointer, sizeof(p_node_type*));
    }

    std::shared_ptr<typename node_type::pointer_indirection_type> get_ptr_indirection() const override {
        return nullptr;
    }

    typename node_type::node_types get_type() const override {
        return node_type::node_types::PNodeType;
    }

    bool is_pmem_node() const override {
        return true;
    }


    parent_node_type* get_parent_node() const override {
        return (parent_node_type*)_parent_node;
    }

    node_type* get_sibling() const override {
        return _sibling_pointer;
    }

    PEAR_LEVEL_TYPE get_level() const override {
        return _level;
    }

    void set_parent_node(parent_node_type* const parent_ptr) override {
        _parent_node = parent_ptr;
        PearTreeAllocatorSingleton::fence_persist(&(_parent_node), sizeof(parent_node_type*));
    }


    META_DATA_TYPE get_bitmap() const override {
        return _bitmap;
    }

    std::bitset<max_node_size> get_bitmap_debug() const override {
        return _bitmap;
    }

    std::array<typename node_type::branch_type, max_node_size> const &get_branches() const override {
        return _branches;
    }

    std::array<typename node_type::branch_type, max_node_size> &get_branches_non_const() override {
        return _branches;
    }

    std::array<typename node_type::rect_type, max_node_size> const &get_rects() const override {
        return _rects;
    }

    std::array<typename node_type::rect_type, max_node_size> &get_rects_non_const() override {
        return _rects;
    }

    META_DATA_TYPE get_last_version_before_split() const override {
        std::atomic_thread_fence(std::memory_order_acquire);
        if(_level > 0) {
            return (LOAD_META_INLINE(this->_version_meta_data)&~(PEAR_NODE_VERSION_RESERVED_BITS_MASK))+(max_node_size-count_valid_branches())*PEAR_MIN_NODE_UNRESERVED_VERSION;
        } else {
            return (LOAD_META_INLINE(this->_version_meta_data)&~(PEAR_NODE_VERSION_RESERVED_BITS_MASK))+PEAR_MIN_NODE_UNRESERVED_VERSION;
        }
    }

    int get_branch_count() const override {
        return count_valid_branches();
    }

    int get_next_free_position() {
        for (int i = 0; i < max_node_size; i++) {
            if (!(PEAR_CHECK_BITMAP(_bitmap, i)))
                return i;
        }
        return max_node_size;
    }

    void free_tree_dram() override {
        return;
    }

    static int count_valid_branches(META_DATA_TYPE const bitmap) {
        // source http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetNaive (Count bits set (rank) from the most-significant bit upto a given position)
        unsigned int const pos = 64; // Bit position to count bits upto.
        uint64_t r;       // Resulting rank of bit at pos goes here.

        // Shift out bits after given position.
        r = bitmap >> (sizeof(_bitmap) * CHAR_BIT - pos);
        // Count set bits in parallel.
        // r = (r & 0x5555...) + ((r >> 1) & 0x5555...);
        r = r - ((r >> 1) & ~0UL/3);
        // r = (r & 0x3333...) + ((r >> 2) & 0x3333...);
        r = (r & ~0UL/5) + ((r >> 2) & ~0UL/5);
        // r = (r & 0x0f0f...) + ((r >> 4) & 0x0f0f...);
        r = (r + (r >> 4)) & ~0UL/17;
        // r = r % 255;
        return (r * (~0UL/255)) >> ((sizeof(_bitmap) - 1) * CHAR_BIT);
    }

    int count_valid_branches() const {
        // source http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetNaive (Count bits set (rank) from the most-significant bit upto a given position)
        unsigned int const pos = 64; // Bit position to count bits upto.
        uint64_t r;       // Resulting rank of bit at pos goes here.

        // Shift out bits after given position.
        r = _bitmap >> (sizeof(_bitmap) * CHAR_BIT - pos);
        // Count set bits in parallel.
        // r = (r & 0x5555...) + ((r >> 1) & 0x5555...);
        r = r - ((r >> 1) & ~0UL/3);
        // r = (r & 0x3333...) + ((r >> 2) & 0x3333...);
        r = (r & ~0UL/5) + ((r >> 2) & ~0UL/5);
        // r = (r & 0x0f0f...) + ((r >> 4) & 0x0f0f...);
        r = (r + (r >> 4)) & ~0UL/17;
        // r = r % 255;
        return (r * (~0UL/255)) >> ((sizeof(_bitmap) - 1) * CHAR_BIT);
    }

    bool split_child_blocking(parent_node_type * new_node_ptr, parent_node_type * overflow_node_ptr, typename node_type::rect_type const &new_mbr_overflown_node, typename node_type::rect_type const &new_mbr_new_node, std::atomic<META_DATA_TYPE> * const child_version_meta, PEAR_GLOBAL_VERSION_TYPE const global_version, optional_args* const args_ptr, std::atomic<META_DATA_TYPE>** const branch_lock_overflown_branch=NULL, std::atomic<META_DATA_TYPE>** const branch_lock_new_branch=NULL, bool const lock_branch=false) override {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_meta_data, return false;)
        if(_level == 1) {
            STORE_META((*(child_version_meta)),PEAR_SET_NODE_VERSIONS_INLINE(PEAR_SPLIT_PARENT_STARTED_BIT_MASK, global_version))
        }
        int overflow_position = -1;
        int const position = get_next_free_position();
        if (position == max_node_size) {
            // FPR split step 5 (has to be done before the split, otherwise the split will be calculated on the wrong MBR)
            for (int i = 0; i < max_node_size; i++) {
                if (this->_branches[i].get_child_node() == dynamic_cast<node_type *>(overflow_node_ptr)) {
                    // TODO (recovery): set lock for this branch
                    overflow_position = i;
                    PearTreeAllocatorSingleton::mfence();
                    new(&(this->_rects[i])) typename node_type::rect_type(new_mbr_overflown_node.x_min, new_mbr_overflown_node.y_min, new_mbr_overflown_node.x_max, new_mbr_overflown_node.y_max);
                    PearTreeAllocatorSingleton::getInstance().persist(&(this->_rects[i]), sizeof(typename node_type::rect_type) );
                    break;
                }
            }
            // FPR split step 3.1-3.6/4
            auto casted_new_node_ptr = dynamic_cast<node_type *>(new_node_ptr);
            if (p_node_type *const new_sibling_ptr = this->r_star_split(new_mbr_new_node, global_version, branch_lock_overflown_branch, overflow_position, lock_branch, args_ptr)) {
                // FPR split step 3.7 (adding the tree which was not there before)
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
                int const new_position = new_sibling_ptr->get_next_free_position();
                new_sibling_ptr->add_subtree_with_position(casted_new_node_ptr, new_mbr_new_node,new_position, lock_branch);
                if(lock_branch) {
                    new_sibling_ptr->set_branch_lock_ptr(branch_lock_new_branch, new_position);
                }
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(new_sibling_ptr->_version_meta_data)
            } else {
                // FPR split step 3.7 (adding the tree which was not there before)
                int const new_position = get_next_free_position();
                this->add_subtree_with_position(casted_new_node_ptr, new_mbr_new_node,new_position, lock_branch);
                if(lock_branch) {
                    this->set_branch_lock_ptr(branch_lock_new_branch, new_position);
                }
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
            }
            PearTreeAllocatorSingleton::getInstance().deregister_pointer(new_node_ptr);
            return true;
        } else if(position > max_node_size) {
            INVALID_BRANCH_TAKEN
        } else {
            // FPR split step 3+4
            this->add_subtree_with_position(dynamic_cast<node_type *>(new_node_ptr), new_mbr_new_node, position, lock_branch);
            PearTreeAllocatorSingleton::getInstance().deregister_pointer(new_node_ptr);
            if(lock_branch) {
                this->set_branch_lock_ptr(branch_lock_new_branch, position);
            }
            // FPR split step 5
            for (int i = 0; i < max_node_size; i++) {
                if (PEAR_CHECK_BITMAP(_bitmap, i)) {
                    if (this->_branches[i].get_child_node() == dynamic_cast<node_type *>(overflow_node_ptr)) {
                        overflow_position = i;
                        PearTreeAllocatorSingleton::mfence();
                        new(&(this->_rects[i])) typename node_type::rect_type(new_mbr_overflown_node.x_min, new_mbr_overflown_node.y_min, new_mbr_overflown_node.x_max, new_mbr_overflown_node.y_max);
                        PearTreeAllocatorSingleton::sfence();
                        break;
                    }
                }
            }
        }
        if(lock_branch) {
            *branch_lock_overflown_branch = &(this->_branches[overflow_position].atomic_version);
        }
        PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(this->_version_meta_data)
        return true;
    }

    PEAR_NODE_VERSION_TYPE reset_version(PEAR_GLOBAL_VERSION_TYPE const global_version) {
        PEAR_NODE_VERSION_TYPE old_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_version_meta_data));
        STORE_META(this->_version_meta_data, PEAR_SET_NODE_VERSIONS_INLINE(1,global_version));
        PearTreeAllocatorSingleton::persist((void*)&(this->_version_meta_data), sizeof(this->_version_meta_data));
        return old_version;
    }

    inline p_node_type *r_star_split(typename node_type::rect_type const &new_key, PEAR_GLOBAL_VERSION_TYPE const global_version, std::atomic<META_DATA_TYPE>** const branch_lock_overflown_branch, std::atomic<META_DATA_TYPE>** const branch_lock_new_branch, optional_args* const args_ptr) {
        return r_star_split_full_params(new_key,global_version,args_ptr,branch_lock_overflown_branch, branch_lock_new_branch, -1,true);
    }

    inline p_node_type *r_star_split(typename node_type::rect_type const &new_key, PEAR_GLOBAL_VERSION_TYPE const global_version, std::atomic<META_DATA_TYPE>** const branch_lock_overflown_branch, int const overflow_branch_position, bool const set_overflown_branch_lock, optional_args* const args_ptr) {
        if(set_overflown_branch_lock) {
            return r_star_split_full_params(new_key,global_version,args_ptr, branch_lock_overflown_branch, NULL, overflow_branch_position, false, true);
        } else {
            return r_star_split_full_params(new_key,global_version,args_ptr, NULL, NULL, overflow_branch_position);
        }
    }

    p_node_type *r_star_split_full_params(typename node_type::rect_type const &new_key, PEAR_GLOBAL_VERSION_TYPE const global_version, optional_args* const args_ptr, std::atomic<META_DATA_TYPE>** const branch_lock_overflown_branch=NULL, std::atomic<META_DATA_TYPE>** const branch_lock_new_branch=NULL, int const overflow_branch_position=-1, bool const called_from_add_data=false, bool const set_overflown_branch_lock=false) {
        // FPR split step 2 (reset already here to indicate to other workers a split is happening)
        PEAR_NODE_VERSION_TYPE const old_version = reset_version(global_version);
        // ChooseSplitAxis
        std::vector<std::pair<const typename node_type::rect_type *, unsigned int>> *min_margin_axis_rects;
        typename node_type::node_type::rect_type * new_mbr_overflown_node;
        typename node_type::node_type::rect_type * new_mbr_new_node;
        bool new_key_in_this_node;
        std::array<bool, max_node_size> branch_locked = {false};
        if(!called_from_add_data) {
            for (int i = 0; i < max_node_size; i++) {
                while (i != overflow_branch_position && IS_LOCKED(LOAD_META_INLINE(this->_branches[i].atomic_version))) {
                    PEAR_NODE_VERSION_TYPE const node_version = PEAR_GET_NODE_VERSION_INLINE(LOAD_META_INLINE(this->_branches[i].get_child_node()->_version_meta_data));
                    if(_level == 1 && PEAR_IS_REORGANIZING(node_version) && !PEAR_SPLIT_PARENT_STARTED(node_version)) {
                        branch_locked[i] = true;
                        break;
                    }
                    if(PEAR_COLLECT_CONCURRENCY_STATS) {
                        args_ptr->node_wait_counter++;
                    }
                }
            }
            branch_locked[overflow_branch_position] = true;
            PearTreeAllocatorSingleton::mfence();
        }

        int split_index_candidate = this->calculate_split(&(this->_rects[0]), new_key, min_margin_axis_rects,
                        new_mbr_overflown_node,
                        new_mbr_new_node, new_key_in_this_node);

        // Whenever an entry with a _children index smaller than the split index is copied. Pop one index from the stack and copy it to the index which just got freed.
        p_node_type *new_node = (p_node_type*)PearTreeAllocatorSingleton::getInstance().aligned_allocate_pmem_node<sizeof(p_node_type) , alignof(p_node_type)>();
        new(new_node) p_node_type(_level, (!called_from_add_data) ? 1 : PEAR_MIN_NODE_UNRESERVED_VERSION, global_version);
        int copy_index = 0;
        std::bitset<META_DATA_BIT_SIZE> current_node_bitset = _bitmap;
        std::bitset<META_DATA_BIT_SIZE> new_node_node_bitset = 0;
        std::atomic<META_DATA_TYPE>* overflown_branch_meta = NULL;

        // FPR split step 1
        for (int i = split_index_candidate; i < max_node_size + 1; i++) {
            if (new_key_in_this_node || min_margin_axis_rects->at(i).second < max_node_size) {
                int const src_index = min_margin_axis_rects->at(i).second;
                if(set_overflown_branch_lock) {
                    new(&(new_node->_branches[copy_index])) typename node_type::branch_type(this->_branches[src_index].atomic_version ,this->_branches[src_index].m_child, branch_locked[src_index], false);
                    this->_branches[src_index].get_child_node()->set_parent_node(new_node);
                    if(src_index == overflow_branch_position) {
                        overflown_branch_meta = &(new_node->_branches[copy_index].atomic_version);
                    }
                } else if(_level > 0) {
                    new(&(new_node->_branches[copy_index])) typename node_type::branch_type(this->_branches[src_index].atomic_version, this->_branches[src_index].m_child, false, false);
                    this->_branches[src_index].get_child_node()->set_parent_node(new_node);
                } else {
                    new(&(new_node->_branches[copy_index])) typename node_type::branch_type(this->_branches[src_index].atomic_version, this->_branches[src_index].m_child);
                }
                new(&(new_node->_rects[copy_index])) typename node_type::rect_type(this->_rects[src_index].x_min, this->_rects[src_index].y_min, this->_rects[src_index].x_max, this->_rects[src_index].y_max);
                current_node_bitset[src_index] = 0;
                new_node_node_bitset[copy_index] = 1;
                copy_index++;
            }
        }
        if(set_overflown_branch_lock && overflown_branch_meta == NULL) {
            *branch_lock_overflown_branch = &(this->_branches[overflow_branch_position].atomic_version);
        } else if(set_overflown_branch_lock) {
            *branch_lock_overflown_branch = overflown_branch_meta;
        }
        delete min_margin_axis_rects;
        new_node->_bitmap = new_node_node_bitset.to_ulong();
        PearTreeAllocatorSingleton::fence_persist(new_node, sizeof(p_node_type));
        // FPR split step 2

        // FPR split step 3-5
        // The reason why we pass branch_to_split through: Let the parent node change both MBRs (for the already existing branch and the new one), to keep architecturally clean.

        while(!_parent_node->split_child_blocking(new_node, this, *new_mbr_overflown_node, *new_mbr_new_node, (called_from_add_data) ? &(this->_version_meta_data) : NULL, global_version, args_ptr, branch_lock_overflown_branch, branch_lock_new_branch, called_from_add_data)) {
            if(PEAR_COLLECT_CONCURRENCY_STATS) {
                args_ptr->node_wait_counter++;
            }
            PearTreeAllocatorSingleton::mfence();
        }
        delete new_mbr_overflown_node;
        delete new_mbr_new_node;

        // FPR split step 6
        _bitmap = current_node_bitset.to_ulong();
        PearTreeAllocatorSingleton::fence_persist(&(this->_bitmap), sizeof(this->_bitmap));
        STORE_META(this->_version_meta_data, PEAR_SET_NODE_VERSIONS_INLINE(old_version, global_version))
        PearTreeAllocatorSingleton::fence_persist(&(this->_version_meta_data), sizeof(this->_version_meta_data));
        this->set_sibling_pointer(new_node);

        if (!new_key_in_this_node) {
            return new_node;
        } else {
            if(!called_from_add_data) {
                PEAR_RELEASE_NODE_LOCK_WITH_INCREASE(new_node->_version_meta_data)
            }
            return nullptr;
        }
    }
};
