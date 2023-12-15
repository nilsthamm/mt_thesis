#pragma once

#include <array>
#include <cstring>
#include <per_tree_allocator.hpp>
#include <sstream>
#include <bitset>
#include <queue>
#include <set>
#include <cmath>
#include <immintrin.h>
#include <xmmintrin.h>
#include <cfloat>

#include <path_entry.hpp>
#include "macros/macros.hpp"
#include "branch.hpp"
#include "rectangle.hpp"
#include "reinsertable.hpp"
#include "basic_parent_node.hpp"
#include "basic_rtree_persister.hpp"


template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
class BasicNode : public BasicParentNode<max_node_size, min_node_size, max_pmem_level, Pointtype> {
public:
    using node_type = BasicNode<max_node_size, min_node_size, max_pmem_level, Pointtype>;
    using node_float_type = BasicNode<max_node_size, min_node_size, max_pmem_level, float>;
    using branch_type = Branch<node_type>;
    using branch_float_type = branch_type;
    using parent_type = BasicParentNode<max_node_size, min_node_size, max_pmem_level, Pointtype>;
    using parent_float_type = BasicParentNode<max_node_size, min_node_size, max_pmem_level, float>;
    using branch_parent_type =  Branch<parent_type>;
    using branch_void_type =  Branch<void>;
    using rect_type = Rectangle<Pointtype>;
    using rect_float_type = Rectangle<float>;
    using path_entry_type = path_entry<node_type, branch_type, rect_type>;


    std::array<rect_type, max_node_size> _rects;
    std::array<branch_type, max_node_size> _branches;
    std::atomic<size_t> _meta_data;
    parent_type* _parent_ptr;
    int const _level;

    unsigned char get_level() const {
        return _level;
    }

    bool is_pmem_node() const {
        return _level <= max_pmem_level;
    }

    std::array<branch_type, max_node_size> const &get_branches() const {
        return _branches;
    }

    std::array<rect_type , max_node_size> const &get_rects() const {
        return _rects;
    }

    int const get_branch_count() const {
        int size = 0;
        META_DATA_TYPE meta_data_long = LOAD_META_INLINE(_meta_data);
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                size++;
            }
        }
        return size;
    }

    uint32_t get_local_version() {
        return BASIC_GET_VERSION(LOAD_META_INLINE(_meta_data));
    }

    META_DATA_TYPE get_bitmap() {
        return LOAD_META_INLINE(_meta_data);
    }

    std::atomic<size_t> &get_meta_data() override {
        return _meta_data;
    }

    static constexpr size_t BRANCHES_SIZE = branch_type::BRANCH_SIZE * max_node_size;
    static constexpr size_t RECTS_SIZE = rect_type::RECTANGLE_SIZE * max_node_size;
    static constexpr size_t META_DATA_SIZE = sizeof(std::atomic<size_t>);
    static constexpr size_t LEVEL_SIZE = sizeof(int);
    static constexpr size_t PARENT_CLASS_SIZE = sizeof(parent_type);
    static constexpr size_t PARENT_POINTER_SIZE = sizeof(parent_type *);
    static constexpr size_t ALIGNMENT = alignof(node_type);
    static constexpr size_t NODE_SIZE =
            ALLIGN_TO_SIZE(BRANCHES_SIZE, ALIGNMENT) +
            ALLIGN_TO_SIZE(RECTS_SIZE, ALIGNMENT) +
            ALLIGN_TO_SIZE(META_DATA_SIZE, ALIGNMENT) +
            ALLIGN_TO_SIZE(LEVEL_SIZE, ALIGNMENT) +
            ALLIGN_TO_SIZE(PARENT_CLASS_SIZE, ALIGNMENT) +
            ALLIGN_TO_SIZE(PARENT_POINTER_SIZE, ALIGNMENT);

    static node_type *allocate_node(int const level) {
        if (level > max_pmem_level) {
            return ((node_type *) PearTreeAllocatorSingleton::getInstance().allocate_dram(
                    sizeof(BasicNode)));
        } else {
            return ((node_type *) PearTreeAllocatorSingleton::getInstance().allocate_pmem_node(
                    sizeof(BasicNode)));
        }
    }

    BasicNode(int const level, parent_type* const parent_ptr, META_DATA_TYPE const initial_version) : _rects({}), _branches({}), _parent_ptr(parent_ptr), _level(level) {
        static_assert(BRANCHES_SIZE == sizeof(_branches));
        static_assert(RECTS_SIZE == sizeof(_rects));
        static_assert(META_DATA_SIZE == sizeof(_meta_data));
        static_assert(LEVEL_SIZE == sizeof(this->_level));
        static_assert(PARENT_POINTER_SIZE == sizeof(_parent_ptr));
        static_assert(NODE_SIZE == sizeof(*this));
        static_assert(64 >= BASIC_META_DATA_RESERVED_BITS + max_node_size);
        static_assert(min_node_size <= max_node_size/2);
        STORE_META(this->_meta_data,initial_version)
    }

    BasicNode(BasicNode const &) = delete;
    BasicNode() = delete;


    void release_branch_lock(int const position) override {
        if(position<max_node_size) {
            PearTreeAllocatorSingleton::sfence();
            BASIC_RELEASE_META_DATA_LOCK(_branches[position].atomic_version)
        }
    }

    int split_child_blocking(parent_type * new_node_ptr, parent_type * overflow_node_ptr,
                     rect_type const &new_mbr_overflown_node,
                     rect_type const &new_mbr_new_node, u_char const global_version) override {
        META_DATA_TYPE meta_data_long;
        BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(this->_meta_data, meta_data_long, return -1;, global_version)
        int overflow_position = -1;
        if (get_next_free(meta_data_long) >= max_node_size) {
            // FPR split step 5 (has to be done before the split, otherwise the split will be calculated on the wrong MBR)
            for (int i = 0; i < max_node_size; i++) {
                if (_branches[i].get_child_node() == dynamic_cast<BasicNode *>(overflow_node_ptr)) {
                    // TODO (recovery): set lock for this branch
                    overflow_position = i;
                    BASIC_ACQUIRE_META_ATOMIC(_branches[i].atomic_version, global_version)
                    PearTreeAllocatorSingleton::mfence();
                    new(&(_rects[i])) rect_type(new_mbr_overflown_node.x_min, new_mbr_overflown_node.y_min, new_mbr_overflown_node.x_max, new_mbr_overflown_node.y_max);
                    PearTreeAllocatorSingleton::sfence();
                    break;
                }
            }
            // FPR split step 3.1-3.6/4
            auto casted_new_node_ptr = dynamic_cast<node_type *>(new_node_ptr);
            if (BasicNode *const new_sibling_ptr = this->r_star_split(new_mbr_new_node,meta_data_long, global_version, overflow_position)) {
                // FPR split step 3.7 (adding the tree which was not there before)
                BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)
                new_sibling_ptr->add_subtree(casted_new_node_ptr, new_mbr_new_node);
                PearTreeAllocatorSingleton::mfence();
                BASIC_RELEASE_META_DATA_LOCK(new_sibling_ptr->_meta_data)
                // TODO (recovery): Release lock on updated MBR
                return max_node_size;
            } else {
                // FPR split step 3.7 (adding the tree which was not there before)
                this->add_subtree(casted_new_node_ptr, new_mbr_new_node);\
                // TODO (recovery): Release lock on updated MBR
            }
            overflow_position = max_node_size;
        } else {
            // FPR split step 3+4
            this->add_subtree(dynamic_cast<node_type *>(new_node_ptr), new_mbr_new_node);
            // FPR split step 5
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    if (_branches[i].get_child_node() == dynamic_cast<BasicNode *>(overflow_node_ptr)) {
                        overflow_position = i;
                        BASIC_ACQUIRE_META_ATOMIC(_branches[i].atomic_version, global_version)
                        PearTreeAllocatorSingleton::mfence();
                        new(&(_rects[i])) rect_type(new_mbr_overflown_node.x_min, new_mbr_overflown_node.y_min, new_mbr_overflown_node.x_max, new_mbr_overflown_node.y_max);
                        PearTreeAllocatorSingleton::sfence();
                        break;
                    }
                }
            }
        }
        PearTreeAllocatorSingleton::sfence();
        BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)
        return overflow_position;
    }

    void add_branch_find_subtree(branch_parent_type const * const branch_to_add, rect_type const & rect_to_add, parent_type const * origin, META_DATA_TYPE const meta_data_long, u_char const global_version) override {
        int subtree = -1;
        META_DATA_TYPE subtree_meta_data_long;
        while(subtree == -1) {
            subtree = r_star_find_subtree_inner(rect_to_add, meta_data_long, origin);
            BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(_branches[subtree].get_child_node()->_meta_data, subtree_meta_data_long, if(BASIC_GET_VERSION(subtree_meta_data_long) == 0) { subtree = -1; break;}, global_version)
        }
        BASIC_ACQUIRE_META_ATOMIC(_branches[subtree].atomic_version, global_version)
        PearTreeAllocatorSingleton::mfence();
        join(_rects[subtree], rect_to_add, &(_rects[subtree]));
        PearTreeAllocatorSingleton::sfence();
        BASIC_RELEASE_META_DATA_LOCK(_branches[subtree].atomic_version)
        if (_level <= max_pmem_level)
            PearTreeAllocatorSingleton::persist(&(_rects[subtree]), sizeof(rect_type));
        _branches[subtree].get_child_node()->move_subtree(branch_to_add, rect_to_add, subtree_meta_data_long);
        PearTreeAllocatorSingleton::sfence();
        BASIC_RELEASE_META_DATA_LOCK(_branches[subtree].get_child_node()->_meta_data)
    }

    void delete_branch(parent_type* const node_to_delete, META_DATA_TYPE const meta_data_long, u_char const global_version) override {
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i) && _branches[i].get_data() == reinterpret_cast<long>(node_to_delete)) {
                unset_and_persist(i,meta_data_long);
                break;
            }
        }
        if(count_valid_branches() < min_node_size) {
            META_DATA_TYPE parent_meta_data_long;
            BASIC_ACQUIRE_META_ATOMIC_VALUE(_parent_ptr->get_meta_data(), parent_meta_data_long, global_version)
            bool is_parent_a_node = _parent_ptr->is_node();
            if(!is_parent_a_node) {
                BASIC_RELEASE_META_DATA_LOCK(_parent_ptr->get_meta_data())
                return;
            }
            META_DATA_TYPE updated_meta_data_long;
            LOAD_META(this->_meta_data, updated_meta_data_long)
            reset_version(updated_meta_data_long);
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(updated_meta_data_long, i)) {
                    _parent_ptr->add_branch_find_subtree(
                            reinterpret_cast<branch_parent_type const *>(&(_branches[i])), _rects[i], this, parent_meta_data_long, global_version);
                    unset_and_persist(i,updated_meta_data_long);
                }
            }
            LOAD_META(_parent_ptr->get_meta_data(), parent_meta_data_long)
            _parent_ptr->delete_branch(this,parent_meta_data_long, global_version);
            BASIC_RELEASE_META_DATA_LOCK(_parent_ptr->get_meta_data())
        } else {
            while(!_parent_ptr->update_mbr(this, global_version));
        }
    }

    bool is_node() override {
        return true;
    }

    bool update_mbr(parent_type * const child, u_char const global_version) override {
        int subtree = -1;
        while (subtree == -1) {
            LOAD_META_CONST(this->_meta_data, meta_data_long);
            // TODO (delete): Get the real version and return if locked
            u_char const initial_version = BASIC_GET_VERSION_UNLOCKED(meta_data_long);
            if(initial_version == 0) {
                return false;
            } else if(IS_LOCKED(meta_data_long)) {
                repair_node(this, meta_data_long, global_version);
                subtree = -1;
                continue;
            }
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i) && _branches[i].get_data() == reinterpret_cast<long>(child)) {
                    subtree = i;
                    break;
                }
            }

            LOAD_META_CONST(this->_meta_data, check_meta)
            if(BASIC_GET_VERSION_UNLOCKED(check_meta) == 0) {
                return false;
            } else if(BASIC_GET_VERSION(check_meta) != initial_version) {
                subtree = -1;
            } else {
                BASIC_ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_branches[subtree].atomic_version, \
                    LOAD_META_CONST(this->_meta_data, check_meta) \
                        if(BASIC_GET_VERSION_UNLOCKED(check_meta) == 0) { \
                                return false; \
                        } else if(BASIC_GET_VERSION(check_meta) != initial_version) { \
                            subtree = -1; \
                            break; \
                        } \
                    , global_version)
            }
        }
        dynamic_cast<node_type*>(child)->get_enclosing_rect(&(_rects[subtree]));
        PearTreeAllocatorSingleton::mfence();
        BASIC_RELEASE_META_DATA_LOCK(_branches[subtree].atomic_version)
        LOAD_META_CONST(this->_meta_data, meta_data_long);
        if (0 == BASIC_GET_VERSION_UNLOCKED(meta_data_long)) {
            return false;
        }
        while(!_parent_ptr->update_mbr(this, global_version));
        return true;
    }

    bool insert_leaf(rect_type const &key, size_t const value, u_char const global_version, bool const update_parent_ptr=false) {
        META_DATA_TYPE meta_data_long;
        BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(this->_meta_data, meta_data_long, repair_node(this, meta_data_long, global_version);if(BASIC_GET_VERSION_UNLOCKED(meta_data_long) == 0) return false;, global_version)
        if (get_next_free(meta_data_long) == max_node_size) {
            if (BasicNode *const new_node_ptr = this->r_star_split(key, meta_data_long, global_version)) {
                // FPR split step 7
                META_DATA_TYPE sibling_meta_data_long;
                LOAD_META(new_node_ptr->_meta_data,sibling_meta_data_long)
                new_node_ptr->add_data(key, value, sibling_meta_data_long);
                if(update_parent_ptr) {
                    reinterpret_cast<node_type *>(value)->_parent_ptr = new_node_ptr;
                }
                PearTreeAllocatorSingleton::sfence();
                BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)
            } else {
                META_DATA_TYPE new_meta_data_long;
                LOAD_META(this->_meta_data, new_meta_data_long)
                // FPR split step 7
                this->add_data(key, value, new_meta_data_long);
                if(update_parent_ptr) {
                    reinterpret_cast<node_type *>(value)->_parent_ptr = this;
                }
            }
            return true;
        }
        this->add_data(key, value, meta_data_long);
        if(update_parent_ptr) {
            reinterpret_cast<node_type *>(value)->_parent_ptr = this;
        }
        return true;
    }

    bool find_insert_path(rect_type const &key, path_entry_type* const path_array, u_char const global_version, int const at_level=0) {
        bool inserted = false;
        while(!inserted) {
            int subtree = -1;
            u_char initial_version;
            while (subtree == -1) {
                LOAD_META_CONST(this->_meta_data, meta_data_long);
                // TODO (delete): Get the real version and return if locked
                initial_version = BASIC_GET_VERSION_UNLOCKED(meta_data_long);
                if (initial_version == 0) {
                    return false;
                } else if (IS_LOCKED(meta_data_long)) {
                    repair_node(this, meta_data_long, global_version);
                    subtree = -1;
                    continue;
                }
                subtree = r_star_find_subtree_inner(key, meta_data_long);
            }
            _mm_prefetch((char const *) &(_branches[subtree].get_child_node()->_meta_data), _MM_HINT_T0);

            path_array[_level - 1] = path_entry_type(this, initial_version, &(_branches[subtree]), &(_rects[subtree]));
            if (_level == at_level + 1) {
                return true;
            } else {
                inserted = _branches[subtree].get_child_node()->find_insert_path(key, path_array, global_version, at_level);
            }
        }
        return true;
    }

    bool insert(rect_type const &key, size_t const value, u_char const global_version, int const at_level=0) {
        bool inserted = false;
        while(!inserted) {
            int subtree = -1;
            u_char initial_version;
            while (subtree == -1) {
                LOAD_META_CONST(this->_meta_data, meta_data_long);
                // TODO (delete): Get the real version and return if locked
                initial_version = BASIC_GET_VERSION_UNLOCKED(meta_data_long);
                if(initial_version == 0) {
                    return false;
                } else if(IS_LOCKED(meta_data_long)) {
                    repair_node(this, meta_data_long, global_version);
                    subtree = -1;
                    continue;
                }
                subtree = r_star_find_subtree_inner(key, meta_data_long);
                LOAD_META_CONST(this->_meta_data, check_meta)
                if(BASIC_GET_VERSION_UNLOCKED(check_meta) == 0) {
                    return false;
                } else if(BASIC_GET_VERSION(check_meta) != initial_version) {
                    subtree = -1;
                }
                else {
                    BASIC_ACQUIRE_META_ATOMIC_WAIT_COMMANDS(_branches[subtree].atomic_version, \
                    LOAD_META_CONST(this->_meta_data, check_meta) \
                        if(BASIC_GET_VERSION_UNLOCKED(check_meta) == 0) { \
                                return false; \
                        } else if(BASIC_GET_VERSION(check_meta) != initial_version) { \
                            subtree = -1; \
                            break; \
                        } \
                    , global_version)
                }
            }
            _mm_prefetch((char const*)&(_branches[subtree].get_child_node()->_meta_data), _MM_HINT_T0);
            PearTreeAllocatorSingleton::mfence();
            join(_rects[subtree], key, &(_rects[subtree]));
            // TODO (delete)
            if (_level <= max_pmem_level) {
                PearTreeAllocatorSingleton::persist(&(_rects[subtree]), sizeof(rect_type));
            }
            PearTreeAllocatorSingleton::sfence();
            BASIC_RELEASE_META_DATA_LOCK(_branches[subtree].atomic_version)
            if (_level == at_level+1) {
                inserted = _branches[subtree].get_child_node()->insert_leaf(key, value, global_version, at_level != 0);
            } else {
                inserted = _branches[subtree].get_child_node()->insert(key, value, global_version, at_level);
            }
        }
        return true;
    }

    // do the search BFS style to benefit from data locality
    bool query(rect_type const &search_space,
               std::queue<node_type *> &bfs_queue,
               std::vector<branch_void_type const *> &result, u_char const global_version) const {
        for(int i = 0; i < max_node_size; i+=4) {
            _mm_prefetch((char const*)&(_rects[i]), _MM_HINT_T2);
        }
        LOAD_META_CONST(this->_meta_data, meta_data_long);
        u_char const initial_version = BASIC_GET_VERSION(meta_data_long);
        if(IS_LOCKED(initial_version )) {
            // TODO (recovery) start recovery if wrongly locked
            return false;
        }
        __m512 const v_negation_mask = _mm512_set4_ps(-0.0f,-0.0f,0.0f,0.0f);
        __m512 const v_key = _mm512_xor_ps(_mm512_set4_ps(search_space.y_max,search_space.x_max,search_space.y_min,search_space.x_min), v_negation_mask);
        _mm_prefetch((char const*)&(_rects[0]), _MM_HINT_T0);
        __mmask16 const cmp_result_mask = 0b1111;
        for (int i = 0; i < max_node_size-3; i += 4) {
            _mm_prefetch((char const*)&(_rects[i + 8]), _MM_HINT_T0);
            /** Use SIMD AVX instruction to compute
                *
                       rectA.x_min >= rectB.x_min
                    && rectA.y_min >= rectB.y_min
                    && rectA.x_max <= rectB.x_max
                    && rectA.y_max <= rectB.y_max
                */
//                __m512 const v_rects = _mm512_set_ps(_rects[i].x_min,_rects[i].y_min,_rects[i].x_max,_rects[i].y_max,_rects[i + 1].x_min,_rects[i + 1].y_min,_rects[i + 1].x_max,_rects[i + 1].y_max)
            __m512 const v_rects = _mm512_xor_ps(_mm512_loadu_ps(&(_rects[i])),v_negation_mask);
            __mmask16 v_cmp_result;
            if(_level == 0) {
                v_cmp_result = _mm512_cmp_ps_mask(v_key,v_rects, _CMP_EQ_OQ);
            } else {
                v_cmp_result = _mm512_cmp_ps_mask(v_key,v_rects, _CMP_GE_OQ);
            }
            for(int j = 0; j < 4; j++) {
                if((v_cmp_result&(cmp_result_mask<<(j*4)))==(cmp_result_mask<<(j*4)) && BASIC_CHECK_BIT_MAP(meta_data_long, i + j)) {
                    if(_level > 0) {
                        bfs_queue.emplace(_branches[i+j].get_child_node());
                    } else {
                        result.insert(reinterpret_cast<branch_void_type const *>(&(_branches[i+j])));
                    }
                }
            }
        }
        if(max_node_size%4 != 0) {
            typedef float v4ps __attribute__ ((vector_size (16)));
            v4ps const key_vector = {search_space.x_min, search_space.y_min, -search_space.x_max, -search_space.y_max};
            for(int i = max_node_size-(max_node_size%4); i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    branch_type const *branch = &(_branches[i]);
                    v4ps const vector_2 = {_rects[i].x_min, _rects[i].y_min, -_rects[i].x_max,
                                           -_rects[i].y_max};
                    /** Use SIMD AVX instruction to compute
                     *
                            rectA.x_min >= rectB.x_min
                         && rectA.y_min >= rectB.y_min
                         && -rectA.x_max >= -rectB.x_max
                         && -rectA.y_max >= -rectB.y_max
                     */
                    if(_level > 0 && _mm_test_all_ones((__m128i) _mm_cmp_ps(key_vector, vector_2, _CMP_GE_OQ))) {
                        bfs_queue.emplace(_branches[i].get_child_node());
                    } else if(_level == 0 && _mm_test_all_ones((__m128i) _mm_cmp_ps(key_vector, vector_2, _CMP_EQ_OQ))) {
                        result.insert(reinterpret_cast<branch_void_type const *>(&(_branches[i])));
                    }
                }
            }
        }
        return true;
    }

    int remove_leaf_blocking(rect_type const &key, size_t const value, u_char const global_version) {
        META_DATA_TYPE meta_data_long;
        BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(this->_meta_data, meta_data_long, repair_node(this, meta_data_long, global_version); if(BASIC_GET_VERSION(meta_data_long) == 0) return 2;, global_version)
        int return_value = 0;
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i) && _rects[i] == key && _branches[i].get_data() == value) {
                unset_and_persist(i,meta_data_long);
                return_value = 1;
                break;
            }
        }
        if(count_valid_branches() < min_node_size) {
            reset_version(meta_data_long);
            META_DATA_TYPE parent_meta_data_long;
            BASIC_ACQUIRE_META_ATOMIC_VALUE(_parent_ptr->get_meta_data(), parent_meta_data_long, global_version)
            bool is_parent_node = _parent_ptr->is_node();
            if(!is_parent_node) {
                // TODO (merge): If the _root has only one branch the size of the tree should be reduced
                BASIC_RELEASE_META_DATA_LOCK(_parent_ptr->get_meta_data())
                return return_value;
            }
            LOAD_META(this->_meta_data,meta_data_long)
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    _parent_ptr->add_branch_find_subtree(
                            reinterpret_cast<branch_parent_type const *>(&(_branches[i])), _rects[i], this, parent_meta_data_long, global_version);
                    unset_and_persist(i,meta_data_long);
                }
            }
            LOAD_META(_parent_ptr->get_meta_data(),parent_meta_data_long)
            _parent_ptr->delete_branch(this, parent_meta_data_long, global_version);
            BASIC_RELEASE_META_DATA_LOCK(_parent_ptr->get_meta_data())
        } else {
            while(!_parent_ptr->update_mbr(this, global_version));
        }
        BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)
        return return_value;
    }

    // return codes (0 = value not found; 1 = value deleted; 2 = branch is reorganizing)
    int remove(rect_type const &key, size_t const value, u_char const global_version) {
        // Find leaf through DFS, build path incrementally
        // Lock every node top-down
        META_DATA_TYPE meta_data_long;
        bool subtree_reorg;
        int initial_version;
        do {
            LOAD_META(this->_meta_data,meta_data_long)
            subtree_reorg = false;
            initial_version = BASIC_GET_VERSION_UNLOCKED(meta_data_long);
            if(initial_version == 0) {
                return 2;
            } else if (IS_LOCKED(meta_data_long)) {
                repair_node(this, meta_data_long, global_version);
                continue;
            }
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
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
            }
        } while (subtree_reorg || initial_version != BASIC_LOAD_VERSION(this->_meta_data));
        return 0;
    }

    int r_star_find_subtree(rect_type const &key, META_DATA_TYPE const meta_data_long, parent_type const * const skip = NULL) {
        if (this->_level == 1) {
            return r_star_find_subtree_leaf(key, meta_data_long, skip);
        } else {
            return r_star_find_subtree_inner(key, meta_data_long, skip);

        }
    }



    int r_star_find_subtree_inner(rect_float_type const &key, META_DATA_TYPE const meta_data_long, parent_float_type const * const skip = NULL) {
        for(int i = 0; i < max_node_size; i+=4) {
            _mm_prefetch((char const*)&(_rects[i]), _MM_HINT_T1);
        }
        int child_candidate = -1;
        __m128 v_min = _mm_set1_ps(FLT_MAX);
        int i = 0;
        __m256 const v_key_x_max = _mm256_set1_ps(key.x_max);
        __m256 const v_key_x_min = _mm256_set1_ps(key.x_min);
        __m256 const v_key_y_max = _mm256_set1_ps(key.y_max);
        __m256 const v_key_y_min = _mm256_set1_ps(key.y_min);
        __m256i const minus1 = _mm256_set1_epi32(-1);
        __m256 const mask = _mm256_castsi256_ps(_mm256_srli_epi32(minus1, 1));
        _mm_prefetch((char const*)&(_rects[0]), _MM_HINT_T0);
        _mm_prefetch((char const*)&(_rects[4]), _MM_HINT_T0);
        for (; i < max_node_size-7;) {
            _mm_prefetch((char const*)&(_rects[i + 8]), _MM_HINT_T0);
            _mm_prefetch((char const*)&(_rects[i + 12]), _MM_HINT_T0);
            __m256 const v_x_max = {_rects[i].x_max, _rects[i + 1].x_max, _rects[i + 2].x_max, _rects[i + 3].x_max, _rects[i + 4].x_max, _rects[i + 5].x_max, _rects[i + 6].x_max, _rects[i + 7].x_max};
            __m256 const v_x_min = {_rects[i].x_min, _rects[i + 1].x_min, _rects[i + 2].x_min, _rects[i + 3].x_min, _rects[i + 4].x_min, _rects[i + 5].x_min, _rects[i + 6].x_min, _rects[i + 7].x_min};
            __m256 const v_y_max = {_rects[i].y_max, _rects[i + 1].y_max, _rects[i + 2].y_max, _rects[i + 3].y_max, _rects[i + 4].y_max, _rects[i + 5].y_max, _rects[i + 6].y_max, _rects[i + 7].y_max};
            __m256 const v_y_min = {_rects[i].y_min, _rects[i + 1].y_min, _rects[i + 2].y_min, _rects[i + 3].y_min, _rects[i + 4].y_min, _rects[i + 5].y_min, _rects[i + 6].y_min, _rects[i + 7].y_min};
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

            for(int j = 0; j < 8; j++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[i].get_data())) {
                        continue;
                    }
                    __m128 const v_candidate = {v_abs_areas[j],v_abs_areas_2_subtracted[j],std::numeric_limits<float>::max(),std::numeric_limits<float>::max()};
                    __m128 const a = _mm_cmp_ps(v_min,v_candidate,_CMP_GT_OQ);
                    __m128 const b = _mm_cmp_ps(v_min,v_candidate,_CMP_EQ_OQ);
                    if (a[1] || (b[1] && a[0])) {
                        v_min = v_candidate;
                        child_candidate = i;
                        _mm_prefetch((char const*)&(_branches[i].atomic_version),_MM_HINT_T1);
                    }
                }
                i++;
            }
        }
        while(i<max_node_size) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[i].get_data())) {
                    continue;
                }
                float const current_area = area_inline(_rects[i]);
                __m128 const v_candidate = {current_area, area_2(_rects[i], key) - current_area, std::numeric_limits<float>::max(), std::numeric_limits<float>::max()};
                __m128 const a = _mm_cmp_ps(v_min,v_candidate,_CMP_GT_OQ);
                __m128 const b = _mm_cmp_ps(v_min,v_candidate,_CMP_EQ_OQ);
                if (a[1] || (b[1] && a[0])) {
                    v_min = v_candidate;
                    child_candidate = i;
                    _mm_prefetch((char const*)&(_branches[i].atomic_version),_MM_HINT_T1);
                }
            }
            i++;
        }
        return child_candidate;
    }

    int r_star_find_subtree_leaf_unoptimized(rect_type const &key, META_DATA_TYPE const meta_data_long, parent_type const * const skip = NULL) {
        int child_candidate = -1;
        Pointtype min_area_enlargement = std::numeric_limits<Pointtype>::max();
        Pointtype min_overlap_enlargement = std::numeric_limits<Pointtype>::max();
        // TODO see optimization in paper (end of page 4): Sort the _children by area_2 enlargement (radix sort?) and only calculate the overlap enlargemnent for first X

        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[i].get_data())) {
                    continue;
                }
                Pointtype const area_enlargement = area_2(_rects[i], key) - area_inline(_rects[i]);
                Pointtype candidate_overlap_enlargement;
                for (int j = 0; i < max_node_size; i++) {
                    if (i == j || !BASIC_CHECK_BIT_MAP(meta_data_long, i))
                        continue;
                    candidate_overlap_enlargement += (overlap_area(_rects[i], _rects[j]) -
                                                      overlap_area(_rects[i], key, _rects[j]));
                }
                if (candidate_overlap_enlargement < min_overlap_enlargement ||
                    (candidate_overlap_enlargement == min_area_enlargement &&
                     area_enlargement < min_area_enlargement)) {
                    min_overlap_enlargement = candidate_overlap_enlargement;
                    min_area_enlargement = area_enlargement;
                    child_candidate = i;
                    _mm_prefetch((char const*)&(_branches[i].atomic_version),_MM_HINT_T1);
                }
            }
        }
        return child_candidate;
    }

    int r_star_find_subtree_leaf(rect_float_type const &key, META_DATA_TYPE const meta_data_long, parent_float_type const * const skip = NULL) {
        for(int i = 0; i < max_node_size; i+=4) {
            _mm_prefetch((char const*)&(_rects[i]), _MM_HINT_T2);
        }
        int child_candidate = -1;
        std::vector<std::pair<float,int>> area_enlargements;
        area_enlargements.resize(max_node_size);
        __m512 const v_negation_mask_max_values = _mm512_set4_ps(-0.0f, -0.0f, 0.0f, 0.0f);
        __m512 const v_key = _mm512_set4_ps(key.y_max, key.x_max, key.y_min, key.x_min);
        __m512 const v_key_masked = _mm512_xor_ps(v_key, v_negation_mask_max_values);
        _mm_prefetch((char const*)&(_rects[0]), _MM_HINT_T0);
        __mmask16 const cmp_result_mask = 0b1111;
        __m512i const permute_mask_group_by_coordinates = _mm512_set_epi32(15u, 11u, 7u, 3u, 14u, 10u, 6u, 2u, 13u, 9u, 5u, 1u, 12u, 8u, 4u, 0u);
        int const permute_mask_shift_two = 0b01001110;
        int const permute_mask_shift_one_right = 0b10010011;
        __m128i const minus1 = _mm_set1_epi32(-1);
        __m128 const mask = _mm_castsi128_ps(_mm_srli_epi32(minus1, 1));
        __m512 const v_0 = _mm512_set1_ps(0);
        for (int i = 0; i < max_node_size-3; i += 4) {
            _mm_prefetch((char const *) &(_rects[i + 8]), _MM_HINT_T0);
            __m512 const v_rects = _mm512_loadu_ps(&(_rects[i]));
            __m512 const v_joined_rects = _mm512_xor_ps(
                    _mm512_min_ps(
                            _mm512_xor_ps(
                                    v_rects
                                    , v_negation_mask_max_values
                            )
                            , v_key_masked
                    )
                    , v_negation_mask_max_values
            );
            // {x_min_1,x_min_2,...,y_min_1,y_min_2,...,x_max_1,x_max_2,...}
            __m512 const v_permuted_joinded_rects = _mm512_permutexvar_ps(permute_mask_group_by_coordinates, v_joined_rects);
            __m512 const v_permuted_rects = _mm512_permutexvar_ps(permute_mask_group_by_coordinates, v_rects);
            __m128 const v_joined_x_min = _mm_load_ps((const float*)&v_permuted_joinded_rects);
            __m128 const v_joined_y_min = _mm_load_ps(((const float*)(&v_permuted_joinded_rects))+4);
            __m128 const v_joined_x_max = _mm_load_ps(((const float*)(&v_permuted_joinded_rects))+8);
            __m128 const v_joined_y_max = _mm_load_ps(((const float*)(&v_permuted_joinded_rects))+12);
            __m128 const v_x_min = _mm_load_ps((const float*)&v_permuted_rects);
            __m128 const v_y_min = _mm_load_ps(((const float*)(&v_permuted_rects))+4);
            __m128 const v_x_max = _mm_load_ps(((const float*)(&v_permuted_rects))+8);
            __m128 const v_y_max = _mm_load_ps(((const float*)(&v_permuted_rects))+12);
            __m128 const v_areas_joined = _mm_and_ps(
                    mask,
                    _mm_mul_ps(
                            _mm_sub_ps(
                                    v_joined_x_max,v_joined_x_min
                            ),
                            _mm_sub_ps(
                                    v_joined_y_max,v_joined_y_min
                            )
                    )
            );
            __m128 const v_areas = _mm_and_ps(
                    mask,
                    _mm_mul_ps(
                            _mm_sub_ps(
                                    v_x_max,v_x_min
                            ),
                            _mm_sub_ps(
                                    v_y_max,v_y_min
                            )
                    )
            );
            __m128 const v_area_enlargements = _mm_sub_ps(v_areas_joined, v_areas);
            for(int j = 0; j < 4; j++) {
                if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[i + j].get_data())) {
                    continue;
                }
                if(BASIC_CHECK_BIT_MAP(meta_data_long, i + j)) {
                    area_enlargements[i + j] = std::make_pair(v_area_enlargements[j], i + j);
                } else {
                    area_enlargements[i+j] = std::make_pair(MAXFLOAT,max_node_size);
                }
            }
            // TODO only save the enlargement, not the the joined area
        }
        if(max_node_size%4 != 0) {
            typedef float v4ps __attribute__ ((vector_size (16)));
            v4ps const key_vector = {key.x_min, key.y_min, -key.x_max, -key.y_max};
            for(int i = max_node_size-(max_node_size%4); i < max_node_size; i++) {
                if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[i].get_data())) {
                    continue;
                }
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    area_enlargements[i] = std::make_pair(area_2(_rects[i], key)-area_inline(_rects[i]), i);
                } else {
                    area_enlargements[i] = std::make_pair(MAXFLOAT,max_node_size);
                }
            }
        }
        std::sort(area_enlargements.rbegin(),area_enlargements.rend(),[](std::pair<float,int> const &a, std::pair<float,int> const &b) -> bool {return a.first > b.first;});
        std::vector<std::pair<float,int>> overlap_enlargements;
        overlap_enlargements.resize(std::min(max_node_size,4));
        for(int i = 0; i < overlap_enlargements.size(); i++) {
            int const candidate_id = area_enlargements[i].second;
            if(candidate_id >= max_node_size) {
                for(; i < overlap_enlargements.size(); i++) {
                    overlap_enlargements[i] = std::make_pair(MAXFLOAT,candidate_id);
                }
                break;
            } else if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[candidate_id].get_data())) {
                continue;
            }
            overlap_enlargements[i] = std::make_pair(0.0f,candidate_id);
            Rectangle<float> const &candidate_rect = _rects[candidate_id];
            __m512 const v_candidate_rects = _mm512_set4_ps(candidate_rect.y_max,candidate_rect.x_max,candidate_rect.y_min,candidate_rect.x_min);
            __m512 const v_candidate_rects_masked = _mm512_xor_ps(v_candidate_rects, v_negation_mask_max_values);
            __m512 const v_candidate_joined_key = _mm512_xor_ps(
                    _mm512_min_ps(
                            _mm512_xor_ps(
                                    v_candidate_rects
                                    , v_negation_mask_max_values
                            )
                            , v_key_masked
                    )
                    , v_negation_mask_max_values
            );
            __m512 const v_candidate_joined_key_masked = _mm512_xor_ps(v_candidate_joined_key, v_negation_mask_max_values);
            for(int j = 0; j < max_node_size-3; j+=4) {
                __m512 const v_overlapping_rects = _mm512_loadu_ps(&(_rects[j]));
                __m512 const v_overlapping_rects_masked = _mm512_xor_ps(v_overlapping_rects, v_negation_mask_max_values);
                __m512 const v_overlapping_inner_joined_rects = _mm512_xor_ps(
                        _mm512_max_ps(v_overlapping_rects_masked, v_candidate_rects_masked)
                        , v_negation_mask_max_values
                );
                __m512 const v_overlapping_inner_joined_rects_joined_keys = _mm512_xor_ps(
                        _mm512_max_ps(v_overlapping_rects_masked, v_candidate_joined_key_masked)
                        , v_negation_mask_max_values
                );
                __m512 const v_overlap_axes_overlapping_joined_rects = _mm512_max_ps(v_0, _mm512_sub_ps(v_overlapping_inner_joined_rects,_mm512_permute_ps(v_overlapping_inner_joined_rects, permute_mask_shift_two)));
                __m512 const v_overlap_axes_overlapping_joined_rects_joined_keys = _mm512_max_ps(v_0,_mm512_sub_ps(v_overlapping_inner_joined_rects_joined_keys,_mm512_permute_ps(v_overlapping_inner_joined_rects_joined_keys,permute_mask_shift_two)));
                __m512 const v_overlap_area_overlapping_joined_rects = _mm512_mul_ps(v_overlap_axes_overlapping_joined_rects,_mm512_permute_ps(v_overlap_axes_overlapping_joined_rects, permute_mask_shift_one_right));
                __m512 const v_overlap_area_overlapping_joined_rects_joined_keys = _mm512_mul_ps(v_overlap_axes_overlapping_joined_rects_joined_keys,_mm512_permute_ps(v_overlap_axes_overlapping_joined_rects_joined_keys, permute_mask_shift_one_right));
                __m512 const v_overlap_enlargement = _mm512_sub_ps(v_overlap_area_overlapping_joined_rects_joined_keys,v_overlap_area_overlapping_joined_rects);
                for(int x = 0; x < 4; x++) {
                    if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[j+x].get_data())) {
                        continue;
                    }
                    if(j + x != candidate_id && BASIC_CHECK_BIT_MAP(meta_data_long, j + x)) {
                        overlap_enlargements[i].first += v_overlap_enlargement[3+(x*4)];
                    }
                }
            }
            if(max_node_size%4 != 0) {
                for(int j = max_node_size-(max_node_size%4); j < max_node_size; j++) {
                    if(skip != NULL && skip == reinterpret_cast<parent_type const * const>(_branches[ j].get_data())) {
                        continue;
                    }
                    if(j != candidate_id && BASIC_CHECK_BIT_MAP(meta_data_long, j)) {
                        overlap_enlargements[i].first += (overlap_area(candidate_rect, key, _rects[j]) - overlap_area(candidate_rect, _rects[j]));
                    }
                }
            }
        }
        child_candidate = std::min_element(std::begin(overlap_enlargements),std::end(overlap_enlargements),[](std::pair<float,int> const a, std::pair<float,int> const b)->bool {return a.first < b.first;})->second;
        _mm_prefetch((char const*)&(_branches[child_candidate].atomic_version),_MM_HINT_T1);
        return child_candidate;
    }

    BasicNode *r_star_split(rect_type const &new_key, META_DATA_TYPE const meta_data_long, u_char const global_version, int const overflow_branch_position=-1) {
        // FPR split step 2 (reset already here to indicate to other workers a split is happening)
        u_char old_version = reset_version(meta_data_long);
        // now that the node is marked as splitting the lock on the overflown and therefore soon to change MBR can be released
        if(overflow_branch_position>=0) {
            BASIC_RELEASE_META_DATA_LOCK(_branches[overflow_branch_position].atomic_version)
        }
        // ChooseSplitAxis

        // Sort rects along each axis
        // TODO does it make sense to create in-memory copies (from the rects) here?
        std::vector<std::pair<rect_type const *, unsigned int>> sorted_rects_x = std::vector<std::pair<rect_type const *, unsigned int>>();
        std::vector<std::pair<rect_type const *, unsigned int>> sorted_rects_y = std::vector<std::pair<rect_type const *, unsigned int>>();
        for (int i = 0; i < max_node_size; i++) {
            while(IS_LOCKED(_branches[i].atomic_version.load(std::memory_order_acquire)));
        }
        PearTreeAllocatorSingleton::mfence();
        for (int i = 0; i < max_node_size; i++) {
            sorted_rects_x.push_back(std::make_pair(&(_rects[i]), i));
            sorted_rects_y.push_back(std::make_pair(&(_rects[i]), i));
        }
        sorted_rects_x.push_back(std::make_pair(&new_key, max_node_size));
        sorted_rects_y.push_back(std::make_pair(&new_key, max_node_size));
        std::sort(sorted_rects_x.begin(), sorted_rects_x.end(), [](std::pair<rect_type const *, unsigned int> const rect_a,
                                                                   std::pair<rect_type const *, unsigned int> const rect_b) -> bool {
            return rect_a.first->x_min < rect_b.first->x_min ||
                   (rect_a.first->x_min == rect_b.first->x_min && rect_a.first->x_max < rect_b.first->x_max);
        });
        std::sort(sorted_rects_y.begin(), sorted_rects_y.end(), [](std::pair<rect_type const *, unsigned int> const rect_a,
                                                                   std::pair<rect_type const *, unsigned int> const rect_b) -> bool {
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

        Pointtype bb_x_min_x = sorted_rects_x.at(0).first->x_min;
        Pointtype bb_x_min_y = sorted_rects_x.at(0).first->y_min;
        Pointtype bb_x_max_x = sorted_rects_x.at(0).first->x_max;
        Pointtype bb_x_max_y = sorted_rects_x.at(0).first->y_max;

        Pointtype bb_y_min_x = sorted_rects_y.at(0).first->x_min;
        Pointtype bb_y_min_y = sorted_rects_y.at(0).first->y_min;
        Pointtype bb_y_max_x = sorted_rects_y.at(0).first->x_max;
        Pointtype bb_y_max_y = sorted_rects_y.at(0).first->y_max;

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

        Pointtype sum_margins_x = 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
        Pointtype sum_margins_y = 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

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

        for (int i = 1; i < min_node_size; i++) {
            bb_x_min_x = std::min(sorted_rects_x.at(max_node_size - i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x.at(max_node_size - i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x.at(max_node_size - i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x.at(max_node_size - i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y.at(max_node_size - i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y.at(max_node_size - i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y.at(max_node_size - i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y.at(max_node_size - i).first->y_max, bb_y_max_y);
        }

        sum_margins_x = 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
        sum_margins_y = 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

        second_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
        second_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);

        for (int i = 0; i < NUMBER_DISTRIBUTIONS - 1; i++) {
            bb_x_min_x = std::min(sorted_rects_x.at(max_node_size - min_node_size - i).first->x_min, bb_x_min_x);
            bb_x_min_y = std::min(sorted_rects_x.at(max_node_size - min_node_size - i).first->y_min, bb_x_min_y);
            bb_x_max_x = std::max(sorted_rects_x.at(max_node_size - min_node_size - i).first->x_max, bb_x_max_x);
            bb_x_max_y = std::max(sorted_rects_x.at(max_node_size - min_node_size - i).first->y_max, bb_x_max_y);

            bb_y_min_x = std::min(sorted_rects_y.at(max_node_size - min_node_size - i).first->x_min, bb_y_min_x);
            bb_y_min_y = std::min(sorted_rects_y.at(max_node_size - min_node_size - i).first->y_min, bb_y_min_y);
            bb_y_max_x = std::max(sorted_rects_y.at(max_node_size - min_node_size - i).first->x_max, bb_y_max_x);
            bb_y_max_y = std::max(sorted_rects_y.at(max_node_size - min_node_size - i).first->y_max, bb_y_max_y);

            sum_margins_x += 2 * ((bb_x_max_x - bb_x_min_x) + (bb_x_max_y - bb_x_min_y));
            sum_margins_y += 2 * ((bb_y_max_x - bb_y_min_x) + (bb_y_max_y - bb_y_min_y));

            second_gr_bbs_x.emplace_back(bb_x_min_x, bb_x_min_y, bb_x_max_x, bb_x_max_y);
            second_gr_bbs_y.emplace_back(bb_y_min_x, bb_y_min_y, bb_y_max_x, bb_y_max_y);
        }

        std::vector<std::pair<rect_type const *, unsigned int>> *min_margin_axis_rects;
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

        Pointtype split_index_candidate = min_node_size;
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


        rect_type* const new_mbr_overflown_node =
                &(first_group_bbs->at(split_index_candidate - min_node_size));
        rect_type* const new_mbr_new_node = &(
                second_group_bbs->at(NUMBER_DISTRIBUTIONS - 1 - (split_index_candidate - min_node_size)));

        // TODO update comment: The second group will be copied to the new node. Since the groups are not in the same order as they are in the _children array, some elements of the first group might need to be copied. After the split all elements from the first group should be continues in the _children array of the current ndoe
        BasicNode *new_node = allocate_node(_level);
        new(new_node) node_type(_level, _parent_ptr, BASIC_SET_GLOBAL_VERSION(3, global_version));
        // TODO: sort and summarize memory ranges the minimal amount of memory copies is needed
        // TODO: Determine which is the smaller group of the distribution (use something like start_index and end_index in the loop and set these before accordingly). Maybe even do a cost calculation based on the next array. Meaning factor in the inner copies which need to be made
        bool new_key_in_this_node = false;
        for (int i = 0; i < split_index_candidate; i++) {
            if (min_margin_axis_rects->at(i).second == max_node_size) {
                new_key_in_this_node = true;
                break;
            }
        }
        // Create a stack of indices in _children, which are greater than the split index, meaning they need to be moved
        std::vector<int> indices_to_move = std::vector<int>();

        // Whenever an entry with a _children index smaller than the split index is copied. Pop one index from the stack and copy it to the index which just got freed.
        int copy_index = 0;
        std::bitset<META_DATA_BIT_SIZE> current_node_bitset = std::bitset<META_DATA_BIT_SIZE>(
                (~BASIC_META_DATA_MASK_VERSION) & meta_data_long);
        std::bitset<META_DATA_BIT_SIZE> new_node_node_bitset = std::bitset<META_DATA_BIT_SIZE>(BASIC_SET_GLOBAL_VERSION(1, global_version));
        // FPR split step 1
        std::bitset<64> const moved = 4+((size_t)1<<63);
        std::bitset<64> const deleted = 4+((size_t)1<<62);
        for (int i = split_index_candidate; i < max_node_size + 1; i++) {
            if (new_key_in_this_node || min_margin_axis_rects->at(i).second < max_node_size) {
                int const src_index = min_margin_axis_rects->at(i).second;
                if(_level > 0) {
                    _branches[src_index].get_child_node()->_parent_ptr = new_node;
                }
                new(&(new_node->_branches[copy_index])) branch_type(moved.to_ulong(), _branches[src_index].m_child);
                new(&(new_node->_rects[copy_index])) rect_type(_rects[src_index].x_min, _rects[src_index].y_min, _rects[src_index].x_max, _rects[src_index].y_max);
                STORE_META_RELAXED(_branches[src_index].atomic_version, deleted.to_ulong())
                current_node_bitset[src_index + BASIC_META_DATA_RESERVED_BITS] = 0;
                new_node_node_bitset[copy_index + BASIC_META_DATA_RESERVED_BITS] = 1;
                copy_index++;
            }
        }
        PearTreeAllocatorSingleton::sfence();
        STORE_META_RELAXED(new_node->_meta_data, new_node_node_bitset.to_ulong())
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::persist(&(new_node->_meta_data), sizeof(new_node->_meta_data));
        }
        // FPR split step 2
        current_node_bitset = current_node_bitset.to_ulong() + old_version;
        PearTreeAllocatorSingleton::mfence();

        // FPR split step 3-5
        // The reason why we pass branch_to_split through: Let the parent node change both MBRs (for the already existing branch and the new one), to keep architecturally clean.
        int parent_overflow_branch_position = _parent_ptr->split_child_blocking(new_node, this, *new_mbr_overflown_node, *new_mbr_new_node, global_version);
        while(parent_overflow_branch_position == -1) {
            PearTreeAllocatorSingleton::mfence();
            parent_overflow_branch_position = _parent_ptr->split_child_blocking(new_node, this, *new_mbr_overflown_node, *new_mbr_new_node, global_version);
        }

        // FPR split step 6
        STORE_META_RELAXED(this->_meta_data, current_node_bitset.to_ulong())
        _parent_ptr->release_branch_lock(parent_overflow_branch_position);
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::fence_persist(&(this->_meta_data), sizeof(this->_meta_data));
        }
        if(_level == max_pmem_level) {
            BasicRTreePersisterSingleton::getInstance().register_max_pmem_level_node(this, new_node);
        }

        if (!new_key_in_this_node) {
            return new_node;
        } else {
            BASIC_RELEASE_META_DATA_LOCK(new_node->_meta_data)
            return nullptr;
        }
    }

    void get_enclosing_rect(rect_type * rect) const {
        Pointtype x_min = std::numeric_limits<Pointtype>::max();
        Pointtype x_max = -std::numeric_limits<Pointtype>::max();
        Pointtype y_min = std::numeric_limits<Pointtype>::max();
        Pointtype y_max = -std::numeric_limits<Pointtype>::max();
        META_DATA_TYPE meta_data_long;
        LOAD_META(this->_meta_data, meta_data_long)
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                x_min = std::min(x_min, _rects[i].x_min);
                x_max = std::max(x_max, _rects[i].x_max);
                y_min = std::min(y_min, _rects[i].y_min);
                y_max = std::max(y_max, _rects[i].y_max);
            }
        }
        new(rect) rect_type(x_min, y_min, x_max, y_max);
    }

    void move_subtree(branch_parent_type const * const branch_to_add, rect_type const &rect, META_DATA_TYPE const meta_data_long) {
        int const position = get_next_free(meta_data_long);
        new(&(_branches[position])) branch_type(reinterpret_cast<node_type*>(branch_to_add->get_child_node()));
        new(&(_rects[position])) rect_type(rect.x_min, rect.y_min, rect.x_max, rect.y_max);
        if(_level > 0) {
            _branches[position].get_child_node()->_parent_ptr = this;
        }
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::fence_persist(&(_branches[position]), sizeof(branch_type));
            PearTreeAllocatorSingleton::persist(&(_rects[position]), sizeof(rect_type));
        }
        set_and_persist(position,meta_data_long);
    }

    void add_subtree(node_type * const node_ptr, rect_type const &key) {
        LOAD_META_CONST(this->_meta_data, meta_data_long)
        int const position = get_next_free(meta_data_long);
        LOAD_META_CONST_RELAXED(_branches[position].atomic_version, old_version)
        new(&(_branches[position])) branch_type(4+(((size_t)1<<61)|(old_version&(((size_t)0b1111)<<60))),node_ptr);
        new(&(_rects[position])) rect_type(key.x_min, key.y_min, key.x_max, key.y_max);

        PearTreeAllocatorSingleton::sfence();
        node_ptr->_parent_ptr = this;
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::persist(&(_branches[position]), sizeof(branch_type));
            PearTreeAllocatorSingleton::persist(&(_rects[position]), sizeof(rect_type));
            PearTreeAllocatorSingleton::persist((node_ptr)->_parent_ptr, sizeof(parent_type*));
        }
        set_and_persist(position,meta_data_long);
    }

    void add_data(rect_type const  & key, size_t const value, META_DATA_TYPE const meta_data_long) {
            LOAD_META_CONST(this->_meta_data, meta_reloaded)
        int const position = get_next_free(meta_reloaded);
        new(&(_branches[position])) branch_type(value & UNLOCK_BIT_MASK, reinterpret_cast<node_type *>(value));
        new(&(_rects[position])) rect_type (key.x_min, key.y_min, key.x_max, key.y_max);
        if (_level <= max_pmem_level) {
          PearTreeAllocatorSingleton::fence_persist(&(_branches[position]), sizeof(branch_type));
          PearTreeAllocatorSingleton::persist(&(_rects[position]), sizeof(rect_type));
        }
        set_and_persist(position,meta_reloaded,true);
    }

    int get_next_free(META_DATA_TYPE const meta_data_long) {
        for (int i = 0; i < max_node_size; i++) {
            if (!(BASIC_CHECK_BIT_MAP(meta_data_long, i)))
                return i;
        }
        return max_node_size;
    }

    int count_valid_branches() const {
        META_DATA_TYPE meta_data_long;
        LOAD_META(this->_meta_data, meta_data_long)
        // source http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetNaive (Count bits set (rank) from the most-significant bit upto a given position)
        META_DATA_TYPE masked_meta_data = (meta_data_long) & BASIC_META_DATA_MASK_BIT_SET; // Compute the rank (bits set) in v from the MSB to pos.
        unsigned int const pos = 64; // Bit position to count bits upto.
        uint64_t r;       // Resulting rank of bit at pos goes here.

        // Shift out bits after given position.
        r = masked_meta_data >> (sizeof(masked_meta_data) * CHAR_BIT - pos);
        // Count set bits in parallel.
        // r = (r & 0x5555...) + ((r >> 1) & 0x5555...);
        r = r - ((r >> 1) & ~0UL/3);
        // r = (r & 0x3333...) + ((r >> 2) & 0x3333...);
        r = (r & ~0UL/5) + ((r >> 2) & ~0UL/5);
        // r = (r & 0x0f0f...) + ((r >> 4) & 0x0f0f...);
        r = (r + (r >> 4)) & ~0UL/17;
        // r = r % 255;
        return (r * (~0UL/255)) >> ((sizeof(masked_meta_data) - 1) * CHAR_BIT);
    }

    std::shared_ptr<rect_type> get_enclosing_rect() const {
        Pointtype x_min = std::numeric_limits<Pointtype>::max();
        Pointtype x_max = -std::numeric_limits<Pointtype>::max();
        Pointtype y_min = std::numeric_limits<Pointtype>::max();
        Pointtype y_max = -std::numeric_limits<Pointtype>::max();
        META_DATA_TYPE meta_data_long = this->_meta_data;
        for (int i = 0; i < max_node_size; i++) {
            if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                x_min = std::min(x_min, _branches[i].m_rect.x_min);
                x_max = std::max(x_max, _branches[i].m_rect.x_max);
                y_min = std::min(y_min, _branches[i].m_rect.y_min);
                y_max = std::max(y_max, _branches[i].m_rect.y_max);
            }
        }

        return std::make_shared<rect_type>(x_min, y_min, x_max, y_max);
    }

    u_char reset_version(META_DATA_TYPE const meta_data_long) {
        u_char old_version = BASIC_GET_VERSION(meta_data_long);
        META_DATA_TYPE new_meta;
        BASIC_SET_VERSION(1, new_meta);
        STORE_META(this->_meta_data, (this->_meta_data&(~BASIC_META_DATA_MASK_VERSION)) + new_meta)
        PearTreeAllocatorSingleton::mfence();
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::persist(&(this->_meta_data), sizeof(this->_meta_data));
        }
        return old_version;
    }

    void unset_and_persist(int const position, META_DATA_TYPE const meta_data_long) {
        std::bitset<META_DATA_BIT_SIZE> bit_mask = UINT32_MAX;
        bit_mask[position + BASIC_META_DATA_RESERVED_BITS] = 0;
        STORE_META(this->_meta_data, meta_data_long&bit_mask.to_ulong())
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::fence_persist(&(this->_meta_data), sizeof(this->_meta_data));
        }
    }

    void set_and_persist(int const position, META_DATA_TYPE const meta_data_long, bool const release_lock=false) {
        std::bitset<META_DATA_BIT_SIZE> bit_mask = 0;
        bit_mask[position + BASIC_META_DATA_RESERVED_BITS] = 1;
        if(release_lock) {
            META_DATA_TYPE const temp = meta_data_long|bit_mask.to_ulong();
            PearTreeAllocatorSingleton::sfence();
            BASIC_RELEASE_META_DATA_LOCK_WITH_VALUE(this->_meta_data, temp);
        } else {
            STORE_META_RELAXED(this->_meta_data,meta_data_long|bit_mask.to_ulong());
        }
        if (_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::fence_persist(&(this->_meta_data), sizeof(this->_meta_data));
        }
    }

    void repair_node(node_type* node, META_DATA_TYPE meta_data_long, u_char const global_version) {
        if(IS_LOCKED(meta_data_long) && BASIC_GET_GLOBAL_VERSION(meta_data_long) != global_version) {
            std::stringstream stream;
            stream << "Malformed node detected at " << __FILE__ << ":" << __LINE__ << std::endl;
            throw std::runtime_error(stream.str());
        }
        // TODO check global version, if it is not equal throw exception
    }

    void repair_branch(branch_type* branch, node_type* parent_node, u_char const global_version) {
        META_DATA_TYPE meta_data_long;
        LOAD_META(branch->atomic_version, meta_data_long)
        if(IS_LOCKED(meta_data_long) && BASIC_GET_GLOBAL_VERSION(meta_data_long) != global_version) {
            std::stringstream stream;
            stream << "Malformed branch detected at " << __FILE__ << ":" << __LINE__ << std::endl;
            throw std::runtime_error(stream.str());
        }
    }

    void free_tree_dram() {
        if(max_pmem_level >= _level-1) {
            return;
        } else if(_level > 0) {
            for(int i = 0; i < max_node_size; i++) {
                META_DATA_TYPE meta_data;
                LOAD_META(this->_meta_data, meta_data)
                if(BASIC_CHECK_BIT_MAP(meta_data, i)) {
                    _branches[i].get_child_node()->free_tree_dram();
                    delete _branches[i].get_child_node();
                }
            }
        }
    }


#ifdef USE_REINSERT
    bool overflow_treatment(int level_to_insert) {
        if(level != tree_object->get_max_level() && level_to_insert != level) {
            tree_object->reinsert();
        } else {

        }
    }
#endif


#ifdef USE_REINSERT
    bool reeinsert() {}
#endif

    /**
     * Test helpers BEGIN
     */
    void check_for_values(std::set<long> &values, Rectangle<float> const * const rects) {
        META_DATA_TYPE meta_data_long = this->_meta_data.load(std::memory_order_acquire);
        if(this->_level != 0) {
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    this->_branches[i].get_child_node()->check_for_values(values, rects);
                }
            }
        } else {
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    if(values.contains(this->_branches[i].get_data())) {
                        if(this->_branches[i].get_data() < 10 * 1000 * 1000 && !(rects[this->_branches[i].get_data()] == this->_rects[i])) {
                            std::bitset<64> branch_meta = this->_branches[i].atomic_version.load();
                            std::bitset<64> node_meta = _meta_data.load();
                            unsigned long branch_meta_without_bits = branch_meta.to_ulong()&(UINT64_MAX>>2);
                            rect_type rect = this->_rects[i];
                            std::cout << "Found value in leaf (" << this->to_string() << "), but the rect doesn't match: " << this->_branches[i].get_data() << std::endl;
                        }
                        values.erase(this->_branches[i].get_data());
                    } else {
                        std::bitset<64> branch_meta = this->_branches[i].atomic_version.load();
                        std::bitset<64> node_meta = _meta_data.load();
                        unsigned long branch_meta_without_bits = branch_meta.to_ulong()&(UINT64_MAX>>2);
                        rect_type rect = this->_rects[i];
                        std::cout << "Found value in leaf (" << this->to_string() << "), which is not expected: " << this->_branches[i].get_data() << std::endl;
                    }
                }
            }
        }
    }

    int test_num_branches(int const level) const {
        META_DATA_TYPE meta_data_long = this->_meta_data.load(std::memory_order_acquire);
        if (this->_level == level) {
            int size = 0;
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    size++;
                }
            }
            if(size<min_node_size) {
               std::cout << this->to_string() << " has too few branches (" << size << ")" << std::endl;
            }
            return size;
        } else {
            int num_branches = 0;
            int size = 0;
            for (int i = 0; i < max_node_size; i++) {
                if (BASIC_CHECK_BIT_MAP(meta_data_long, i)) {
                    size++;
                    num_branches += this->_branches[i].get_child_node()->test_num_branches(level);
                }
            }
            if(size<min_node_size) {
               std::cout << this->to_string() << " has too few branches (" << size << ")" << std::endl;
            }
            return num_branches;
        }
    }

    std::string to_string() const {
        std::stringstream stream;
        stream << "Node {ID: " << (void*)this << "; Level: " << _level << "}";
        return stream.str();
    }

    /**
     * Test helpers END
     */
#ifdef USE_REINSERT
    std::shared_ptr<BasicRTree> tree_object;
#endif
};
