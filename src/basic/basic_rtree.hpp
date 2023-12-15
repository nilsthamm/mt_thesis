#pragma once

#include <queue>

#include <libpmem2.h>

#include "rectangle.hpp"
#include "per_tree_allocator.hpp"
#include <path_entry.hpp>
#include "reinsertable.hpp"
#include "basic_parent_node.hpp"
#include "basic_node.hpp"
#include "basic_rtree_persister.hpp"
#include "macros/macros.hpp"

template<int max_node_size, int min_node_size, int max_pmem_level, typename Pointtype>
class BasicRTree : virtual public Reinsertable, virtual public BasicParentNode<max_node_size, min_node_size, max_pmem_level, Pointtype> {
public:
    using node_type = BasicNode<max_node_size, min_node_size, max_pmem_level, Pointtype>;
    static int const t_max_node_size = max_node_size;

    std::atomic<size_t> _meta_data;
    node_type *_root;
    int _max_level;
    u_char const _global_version;

    static int get_pmem_counter() {
        return PearTreeAllocatorSingleton::getInstance().pmem_counter;
    }

    static int get_dram_counter() {
        return PearTreeAllocatorSingleton::getInstance().dram_counter;
    }

    std::atomic<size_t> &get_meta_data() override {
        return _meta_data;
    }

    BasicRTree(u_char const g_version=1, int const start_level = 0) : _global_version(g_version) {
        _root = node_type::allocate_node(start_level);
        new(_root) node_type(start_level, this, BASIC_SET_GLOBAL_VERSION(4, _global_version));
        _max_level = start_level;
        this->_meta_data = BASIC_SET_GLOBAL_VERSION(4, _global_version);
        if(_max_level == max_pmem_level) {
            BasicRTreePersisterSingleton::getInstance().initialize_max_pmem_level_node_list(_root);
        }
    }

    ~BasicRTree() {
        if(max_pmem_level >= _root->_level - 1) {
            return;
        }
        _root->free_tree_dram();
        delete this->_root;
    }

    // Search space is inclusive min/max values
    void equals_query(Rectangle<Pointtype> const &search_space,
                      std::vector<typename node_type::branch_void_type const*> &result) {
        std::queue<node_type *> bfs_queue = std::queue<node_type *>();
        bfs_queue.emplace(_root);
        while (!bfs_queue.empty()) {
            _mm_prefetch(&(bfs_queue.front()->_meta_data), _MM_HINT_T0);
            if(!bfs_queue.front()->query(search_space, bfs_queue, result, _global_version)) {
                bfs_queue.emplace(bfs_queue.front());
            }
            bfs_queue.pop();
        }
    }

    void insert_at_level(Rectangle<Pointtype> const &key, node_type* const node, int const level) {
        typename node_type::path_entry_type* path_array = (typename node_type::path_entry_type*)malloc(sizeof(typename node_type::path_entry_type) * _max_level);
        if(_root->_level - 1 == level) {
            _root->insert_leaf(key, reinterpret_cast<long>(node), _global_version, true);
        } else {
            _root->insert(key, reinterpret_cast<long>(node), _global_version, level);
        }
        delete path_array;
    }

    void insert(Rectangle<Pointtype> const &key, size_t const value) {
        while(!try_insert(key,value));
    }

    bool try_insert(Rectangle<Pointtype> const &key, size_t const value) {
        typedef path_entry<node_type, typename node_type::branch_type, typename node_type::rect_type> path_entry_type;

        if(_root->_level > 0) {
            // Determine branches WITHOUT locking them
            int array_size = _max_level;
            path_entry_type path_array[array_size];// = new path_array_type[array_size];
            int path_lock_track_array[array_size] = {0};
            while(!_root->find_insert_path(key, path_array, _global_version)) {
                if(_root->_level != array_size) {
                    return false;
                }
            }
            // Lock and update all branches, if a node was reorganized, start again at the node above
            // If no update happen it is save to update the node, since other future reorganizations would correct the branch rect if they cause the insert to choose another path
            // TODO (optimization): Determine if prefetching the node meta data is needed here
            for(int i = array_size-1; i >= 0; i--) {
                bool node_reorganized = false;
                META_DATA_TYPE const check_meta__ = (path_array[i].node->_meta_data).load(std::memory_order_acquire);
                if(IS_LOCKED(check_meta__) || BASIC_GET_VERSION(check_meta__) != path_array[i].initial_version) {
                    node_reorganized = true;
                } else {
                    BASIC_ACQUIRE_META_ATOMIC_WAIT_COMMANDS(path_array[i].branch->atomic_version, \
                        LOAD_META_CONST(path_array[i].node->_meta_data, check_meta) \
                            if(IS_LOCKED(check_meta) || BASIC_GET_VERSION(check_meta) != path_array[i].initial_version) { \
                                node_reorganized = true; break;\
                            } \
                        , _global_version)
                    META_DATA_TYPE const check_meta__2 = (path_array[i].node->_meta_data).load(std::memory_order_acquire);
                    if(!node_reorganized) {
                        path_lock_track_array[i] = 1;
                    }
                    if(IS_LOCKED(check_meta__2)) {
                        node_reorganized = true;
                    }
                }
                if (node_reorganized) {
                    if(path_lock_track_array[i]) {
                        BASIC_RELEASE_META_DATA_LOCK(path_array[i].branch->atomic_version)
                        path_lock_track_array[i] = 0;
                    }
                    if(i==array_size-1) {
                        if(_max_level != array_size) {
                            return false;
                        }
                        while(!_root->find_insert_path(key, path_array, _global_version)) {
                            if(_root->_level != array_size) {
                                return false;
                            }
                        }
                        i = array_size;
                        continue;
                    }
                    if(path_lock_track_array[i+1]) {
                        BASIC_RELEASE_META_DATA_LOCK(path_array[i + 1].branch->atomic_version)
                        path_lock_track_array[i+1] = 0;
                    }
                    while(!path_array[i+1].node->find_insert_path(key, path_array, _global_version));
                    i = i+2;
                    continue;
                } else {
                    if(i<array_size-1 && path_lock_track_array[i+1]) {
                        PearTreeAllocatorSingleton::mfence();
                        join(*(path_array[i + 1].rectangle), key, path_array[i + 1].rectangle);
                        PearTreeAllocatorSingleton::sfence();
                        if (i + 2 <= max_pmem_level) {
                            PearTreeAllocatorSingleton::persist(path_array[i + 1].rectangle,
                                                                sizeof(typename node_type::rect_type));
                        }
                        BASIC_RELEASE_META_DATA_LOCK(path_array[i + 1].branch->atomic_version)
                        path_lock_track_array[i+1] = 0;
                    }
                    if(i == 0) {
                        META_DATA_TYPE const check_meta = (path_array[0].branch->get_child_node()->_meta_data).load(std::memory_order_acquire);
                        if(!IS_LOCKED(check_meta)) {
                            PearTreeAllocatorSingleton::mfence();
                            join(*(path_array[0].rectangle), key, path_array[0].rectangle);
                            PearTreeAllocatorSingleton::sfence();
                            if (1 <= max_pmem_level) {
                                //                PearTreeAllocatorSingleton::nt_persist_64(&(subtree->m_rect));
                                PearTreeAllocatorSingleton::persist(path_array[0].rectangle, sizeof(typename node_type::rect_type));
                            }
                            BASIC_RELEASE_META_DATA_LOCK(path_array[0].branch->atomic_version)
                            path_lock_track_array[0] = 0;
                            if(!path_array[0].branch->get_child_node()->insert_leaf(key, value, _global_version)) {
                                i = 1;
                            }
                        } else {
                            BASIC_RELEASE_META_DATA_LOCK(path_array[0].branch->atomic_version)
                            path_lock_track_array[0] = 0;
                            while(!path_array[0].node->find_insert_path(key, path_array, _global_version));
                            i = 1;
                        }
                    }
                }
            }
        } else {
            BASIC_ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_meta_data, if(_root->_level > 0) return false;, _global_version)
            while(!_root->insert_leaf(key, value, _global_version)) {
                if(_root->_level > 0) {
                    BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)
                    return false;
                }
            }
            BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)
        }
        return true;
    }

    void remove(Rectangle<Pointtype> const &key, long value) {
        _root->remove(key, value, _global_version);
    }

    int split_child_blocking(typename node_type::parent_type * new_node_ptr, typename node_type::parent_type * overflow_node_ptr,
                             Rectangle<Pointtype> const &new_mbr_overflown_node,
                             Rectangle<Pointtype> const &new_mbr_new_node, u_char const global_version) override {

        META_DATA_TYPE meta_data_long;
        BASIC_ACQUIRE_META_ATOMIC_VALUE_WAIT_COMMANDS(this->_meta_data, meta_data_long, if (_max_level == 0) {break;} else {return -1;}, _global_version)

        int new_level = _root->_level + 1;
        node_type *new_root = node_type::allocate_node(new_level);
        new(new_root) node_type(new_level, this, BASIC_SET_GLOBAL_VERSION(1, global_version));
        _max_level = new_level;

        // add old _root to new _root
        new(&(new_root->_branches[0])) typename node_type::branch_type (BASIC_SET_GLOBAL_VERSION(1, global_version) + ((size_t)1 << 60), reinterpret_cast<node_type *>(_root));
        new(&(new_root->_rects[0])) typename node_type::rect_type(new_mbr_overflown_node.x_min, new_mbr_overflown_node.y_min, new_mbr_overflown_node.x_max, new_mbr_overflown_node.y_max);
        _root->_parent_ptr = new_root;

        // add new node to new _root
        new(&(new_root->_branches[1])) typename node_type::branch_type(BASIC_SET_GLOBAL_VERSION(4, global_version) + ((size_t)1 << 60), reinterpret_cast<node_type *>(new_node_ptr));
        new(&(new_root->_rects[1])) typename node_type::rect_type(new_mbr_new_node.x_min, new_mbr_new_node.y_min, new_mbr_new_node.x_max, new_mbr_new_node.y_max);

        (dynamic_cast<node_type *>(new_node_ptr))->_parent_ptr = new_root;

        std::bitset<META_DATA_BIT_SIZE> new_root_bitset = 0;
        new_root_bitset[0 + BASIC_META_DATA_RESERVED_BITS] = 1;
        new_root_bitset[1 + BASIC_META_DATA_RESERVED_BITS] = 1;
        META_DATA_TYPE new_root_meta;
        PearTreeAllocatorSingleton::sfence();
        LOAD_META(new_root->_meta_data,new_root_meta)
        STORE_META_RELAXED(new_root->_meta_data,new_root_meta+new_root_bitset.to_ulong() )
        if (new_root->_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::persist_fence(new_root,
                                                      sizeof(node_type));
        }
        if (_root->_level <= max_pmem_level) {
            PearTreeAllocatorSingleton::persist_fence(&_root, sizeof(&_root));
        }
        _root = new_root;
        if(_max_level == max_pmem_level) {
            BasicRTreePersisterSingleton::getInstance().initialize_max_pmem_level_node_list(_root);
        }
        _max_level = new_level;
        PearTreeAllocatorSingleton::sfence();
        BASIC_RELEASE_META_DATA_LOCK(new_root->_meta_data)
        if(_max_level != 1) {BASIC_RELEASE_META_DATA_LOCK(this->_meta_data)}
        return 0;
    }

    void release_branch_lock(int const position) override {
        std::stringstream stream;
        stream << "[Unreachable branch] BasicRTree<>::release_branch_lock (_max_level: " << _max_level << ")";
        throw std::runtime_error(stream.str());
    }

    void add_branch_find_subtree(typename node_type::branch_parent_type const * const branch_to_add, typename node_type ::rect_type  const &rect_to_add, typename node_type::parent_type const * origin, META_DATA_TYPE const meta_data_long, u_char const global_version) override {
        // TODO (merge): If the _root has only one branch the size of the tree should be reduced
        std::stringstream stream;
        stream << "[Unreachable branch] BasicRTree<>::add_branch_find_subtree (_max_level: " << _max_level << ")";
        throw std::runtime_error(stream.str());
          }

    void delete_branch(typename node_type::parent_type * const node_to_delete, META_DATA_TYPE const meta_data_long, u_char const global_version) override {
        std::stringstream stream;
        stream << "[Unreachable branch] BasicRTree<>::delete_branch";
        throw std::runtime_error(stream.str());
    }
    bool update_mbr(typename node_type::parent_type * const child, u_char const global_version) override {
        return true;
    }

    bool is_node() override {
        return false;
    }

    int get_max_level()  {
        return _max_level;
    }

    bool reinsert() override {
        return false;
    }

    int test_max_level() {
        return _max_level;
    }

    int test_num_branches(int level)  {
        return this->_root->test_num_branches(level);
    }

    static bool check_bitmap_for_index(META_DATA_TYPE const bitmap, int const index) {
        return BASIC_CHECK_BIT_MAP(bitmap, index);
    }

    int get_dram_node_count() {
        return -1;
    }

    int get_pmem_node_count() {
        return -1;
    }
};