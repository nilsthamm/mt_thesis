#pragma once

//#include <memkind.h>
#include <errno.h>
#include <emmintrin.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <xmmintrin.h>
#include <climits>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sstream>
#include <queue>
#include <map>
#include <unistd.h>
#include <libpmem2.h>

#include "macros/macros.hpp"

#define DEFAULT_META_FILE_PATH "/mnt/pmem1/nils.thamm/meta"
#define DEFAULT_DEV_DAX_FILE_PATH "/dev/dax1.0"

#define NODE_POOL_SIZE (size_t)(1024l*1024l*1024l*20l)

// Create A RTreePersister
// use allocator to get start pointer for linked_node_list
// use allocator to persist elements (persist_fence)
// have a recovery flag in the alloactor
// after initializing the allocator, call the persister for a possible recovery/maybe even get the RTree object

struct persistence_info {
    void* complete_level_node_list_start;
    void* partial_level_node_list_start;
    uint32_t max_persisted_level;
};

struct pool_meta_data {
    uint64_t flag;
    void* node_pool_start;
    size_t* not_persisted_pointers;
    uint64_t assigned_bytes;
    uint64_t global_version;
    persistence_info info_1;
    persistence_info info_2;
    int32_t valid_info_number;
    char* get_node_pool_start() const {
        return static_cast<char*>(node_pool_start);
    }
};

class PearTreeAllocatorSingleton {
public:
    PearTreeAllocatorSingleton(PearTreeAllocatorSingleton const&)               = delete;
    void operator=(PearTreeAllocatorSingleton const&)  = delete;

    ~PearTreeAllocatorSingleton() {
        this->shut_down();
    }

    static PearTreeAllocatorSingleton& getInstance()
    {
        static PearTreeAllocatorSingleton instance;

        return instance;
    }
    void* allocate_dram(int bytes) {
        return malloc(bytes);
    }

    template<int bytes>
    void* aligned_allocate_dram_node() {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_lock, ;)
        if (_allocated_dram_bytes < _assigned_dram_bytes + ALLIGN_TO_SIZE(bytes, CACHE_LINE_SIZE)) {
            _assigned_dram_bytes = _allocated_dram_bytes;
            _allocated_dram_bytes += NODE_POOL_SIZE/5;
            _current_vnode_pointer = (char*)aligned_alloc(CACHE_LINE_SIZE,ALLIGN_TO_SIZE(NODE_POOL_SIZE/5, CACHE_LINE_SIZE));
            _vnode_pointer_pools.push_back(_current_vnode_pointer);
        }
        void *return_pointer = _current_vnode_pointer;
        _current_vnode_pointer = _current_vnode_pointer + ALLIGN_TO_SIZE(bytes, CACHE_LINE_SIZE);
        _assigned_pmem_bytes += ALLIGN_TO_SIZE(bytes, CACHE_LINE_SIZE);
        RELEASE_META_DATA_LOCK_WITH_INCREASE(_version_lock)
        return return_pointer;
    }

    template<int bytes, int alignement>
    void* aligned_allocate_pmem_node() {
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_lock, ;)
        if (NODE_POOL_SIZE < _assigned_pmem_bytes + ALLIGN_TO_SIZE(bytes, alignement)) {

            std::stringstream err_msg;
            err_msg << "Out of memory while trying to allocate " << (float) bytes << "B (aligned: " << ALLIGN_TO_SIZE(bytes,alignement) << "B) and having "
                    << (float) _assigned_pmem_bytes / 1024.0 / 1024.0 << "MB assigned (max: "
                    << (float) NODE_POOL_SIZE / 1024.0 / 1024.0 << "MB)" << std::endl;
            throw std::runtime_error(err_msg.str());

        }
        void *return_pointer = _current_pnode_pointer;
        _current_pnode_pointer = _current_pnode_pointer + ALLIGN_TO_SIZE(bytes, alignement);
        _assigned_pmem_bytes += ALLIGN_TO_SIZE(bytes, alignement);
        #ifndef FPR_TREE
        _meta_struct->assigned_bytes = _assigned_pmem_bytes;
        int index = _free_positions.front();
        _free_positions.pop();
        _not_persisted_pointers[index] = reinterpret_cast<size_t>(return_pointer);
        _ptr_to_index.emplace(return_pointer,index);
        persist_fence(_meta_struct, sizeof(void*));
        persist_fence(&(_not_persisted_pointers[index]), sizeof(pool_meta_data));
        #endif
        RELEASE_META_DATA_LOCK_WITH_INCREASE(_version_lock)
        return return_pointer;
    }

    void deregister_pointer(void* const addrr) {
        if(!is_pmem_pointer(addrr)) {
            return;
        }
        ACQUIRE_META_ATOMIC_WAIT_COMMANDS(this->_version_lock, ;)
        auto index_it = _ptr_to_index.find(addrr);
        if(index_it == _ptr_to_index.end()) {
            RELEASE_META_DATA_LOCK_WITH_INCREASE(_version_lock)
            return;
        }
        _not_persisted_pointers[index_it->second] = 0;
        _free_positions.push(index_it->second);
        _ptr_to_index.erase(index_it);
        RELEASE_META_DATA_LOCK_WITH_INCREASE(_version_lock)
    }

    uint64_t get_global_version() {
        return _meta_struct->global_version;
    }


    // src: https://github.com/pmem/pmdk/blob/7dc2e301032e090a903a3bc2f2e7aef29c6a5d87/src/libpmem2/x86_64/flush.h
    static inline void persist(void const * const addr, size_t len) {
#ifdef USE_PMEM_INSTRUCTIONS
        uintptr_t uptr;

        /*
         * Loop through cache-line-size (typically 64B) aligned chunks
         * covering the given range.
         */
        for (uptr = (uintptr_t)addr & ~(((uintptr_t)64) - 1);
             uptr < (uintptr_t)addr + len;
             uptr += ((uintptr_t)64)) {
            _mm_clwb((void*)uptr);
        }
#else
#pragma message ("persist not used")
#endif
    }

    static inline void sfence() {
#ifdef USE_PMEM_INSTRUCTIONS
        _mm_sfence();
#else
#pragma message ("sfence not used")
#endif
    }

    static inline void mfence() {
#ifdef USE_PMEM_INSTRUCTIONS
        _mm_mfence();
#else
#pragma message ("mfence not used")
#endif
    }

    static inline void persist_fence(void const * const addr, size_t len) {
        persist(addr,len);
        mfence();
    }

    static inline void fence_persist(void const * const addr, size_t len) {
        sfence();
        persist(addr,len);
    }

    void reset(){
        for(void* ptr: _vnode_pointer_pools) {
            free(ptr);
        }
        _vnode_pointer_pools.clear();
        _current_vnode_pointer = (char*)aligned_alloc(CACHE_LINE_SIZE, ALLIGN_TO_SIZE(NODE_POOL_SIZE, CACHE_LINE_SIZE));
        _vnode_pointer_pools.push_back(_current_vnode_pointer);
        _assigned_dram_bytes = 0;
        _allocated_dram_bytes = NODE_POOL_SIZE;
        _current_pnode_pointer = _meta_struct->get_node_pool_start();
        memset(_current_pnode_pointer, 0, _assigned_pmem_bytes);
        _not_persisted_pointers = _meta_struct->not_persisted_pointers;
        _free_positions = std::queue<int>();
        for(int i = 0; i < 200; i++) {
            _free_positions.push(i);
        }
        _ptr_to_index = std::map<void*, int>();
        _assigned_pmem_bytes = 0;

        _current_pnode_pointer = _meta_struct->get_node_pool_start();
        _meta_struct->assigned_bytes = 0;
        _meta_struct->flag = 0;
        _meta_struct->info_1.complete_level_node_list_start = _current_pnode_pointer;
        _meta_struct->info_1.max_persisted_level = 0;
        _meta_struct->info_1.partial_level_node_list_start = NULL;
        _meta_struct->valid_info_number = 1;
        _meta_struct->info_2.complete_level_node_list_start = _current_pnode_pointer;
        _meta_struct->info_2.max_persisted_level = 0;
        _meta_struct->info_2.partial_level_node_list_start = NULL;
        persist_fence(_meta_struct,sizeof(pool_meta_data));
    }

    void shut_down() {
        pmem2_map_delete(&_meta_pmem2_map);
        pmem2_source_delete(&_meta_pmem2_src);
        pmem2_config_delete(&_cfg);

        close(_meta_fd);
    }

    bool is_pmem_pointer(void* ptr) const {
        return ptr > _meta_struct->node_pool_start && ptr < (char*)(_meta_struct->node_pool_start)+NODE_POOL_SIZE;
    }


    size_t get_actually_assigned_pmem() {
        return _assigned_pmem_bytes;
    }

    char* get_meta_file_path() {
        if(meta_file_path == NULL) {
            #pragma GCC diagnostic push
            #pragma GCC diagnostic ignored "-Wwrite-strings"
            meta_file_path = DEFAULT_META_FILE_PATH;
            #pragma GCC diagnostic pop
        }
        return meta_file_path;
    }

    char* get_dev_dax_file_path() {
        if(meta_file_path == NULL) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
            meta_file_path = DEFAULT_DEV_DAX_FILE_PATH;
#pragma GCC diagnostic pop
        }
        return meta_file_path;
    }

    void switch_persist_level(PEAR_LEVEL_TYPE const new_persisted_level, void* start_node_list) {
        if(_meta_struct->valid_info_number == 1) {
            _meta_struct->info_2.max_persisted_level = new_persisted_level;
            _meta_struct->info_2.complete_level_node_list_start = start_node_list;
            _meta_struct->info_2.partial_level_node_list_start = NULL;
            fence_persist(&(_meta_struct->info_2), sizeof(persistence_info));
            _meta_struct->valid_info_number = 2;
        } else {
            _meta_struct->info_1.max_persisted_level = new_persisted_level;
            _meta_struct->info_1.complete_level_node_list_start = start_node_list;
            _meta_struct->info_1.partial_level_node_list_start = NULL;
            fence_persist(&(_meta_struct->info_1), sizeof(persistence_info));
            _meta_struct->valid_info_number = 1;
        }
        fence_persist(&(_meta_struct->valid_info_number), sizeof(int32_t));
        mfence();
    }

    void start_partial_level(void* start_node_list) {
        if(_meta_struct->valid_info_number == 1) {
            _meta_struct->info_1.partial_level_node_list_start = start_node_list;
            fence_persist(&(_meta_struct->info_1.partial_level_node_list_start), sizeof(persistence_info));
        } else {
            _meta_struct->info_2.partial_level_node_list_start = start_node_list;
            fence_persist(&(_meta_struct->info_2.partial_level_node_list_start), sizeof(persistence_info));
        }
        mfence();
    }

    int32_t get_current_max_pmem_level() const {
        if(_meta_struct->valid_info_number == 1) {
            return _meta_struct->info_1.max_persisted_level;
        } else {
            return _meta_struct->info_2.max_persisted_level;
        }
    }

    void set_current_max_pmem_level(int32_t const max_level) {
        ACQUIRE_META_ATOMIC(this->_version_lock)
        if(_meta_struct->valid_info_number == 1) {
            _meta_struct->info_1.max_persisted_level = max_level;
            fence_persist(&(_meta_struct->info_1.max_persisted_level), sizeof(int32_t));
        } else {
            _meta_struct->info_2.max_persisted_level = max_level;
            fence_persist(&(_meta_struct->info_2.max_persisted_level), sizeof(int32_t));
        }
        RELEASE_META_DATA_LOCK_WITH_INCREASE(this->_version_lock)
    }

    persistence_info const & get_active_persistence_info() {
        if (_meta_struct->valid_info_number == 1) {
            return _meta_struct->info_1;
        } else {
            return _meta_struct->info_2;
        }
    }

    void persist_meta_data_for_recovery() {
        _meta_struct->flag = 42;
        persist_fence(_meta_struct,1);
    }

    void initialize() {
        STORE_META(this->_version_lock, 0)

        _current_vnode_pointer = (char*)aligned_alloc(CACHE_LINE_SIZE, ALLIGN_TO_SIZE(NODE_POOL_SIZE, CACHE_LINE_SIZE));
        _vnode_pointer_pools.push_back(_current_vnode_pointer);


        if ((_meta_fd = open(get_meta_file_path(), O_RDWR)) < 0) {
            perror("open");
            exit(1);
        }
#ifdef USE_PMEM_INSTRUCTIONS
        if ((_dev_dax_fd = open(get_dev_dax_file_path(), O_RDWR)) < 0) {
            perror("open");
            exit(1);
        }
#else
#pragma message ("_dev_dax_fd not initialized")
#endif




        if (pmem2_config_new(&_cfg)) {
            pmem2_perror("pmem2_config_new");
            exit(1);
        }

        if (pmem2_config_set_required_store_granularity(_cfg,
                                                        PMEM2_GRANULARITY_PAGE)) {
            pmem2_perror("pmem2_config_set_required_store_granularity");
            exit(1);
        }


        _meta_struct =  (pool_meta_data*)initialize_mapping(_cfg, _meta_fd, &_meta_pmem2_src, &_meta_pmem2_map);
        _free_positions = std::queue<int>();
        for(int i = 0; i < 200; i++) {
            _free_positions.push(i);
        }
        _ptr_to_index = std::map<void*, int>();
        if(_meta_struct->flag == 42) {
            _current_pnode_pointer = initialize_mmap(NODE_POOL_SIZE, _dev_dax_fd, _meta_struct->node_pool_start) + _meta_struct->assigned_bytes;
            _not_persisted_pointers = (size_t*)initialize_mmap(100*sizeof(size_t), _dev_dax_fd);
            _meta_struct->not_persisted_pointers = _not_persisted_pointers;
            _assigned_pmem_bytes = _meta_struct->assigned_bytes;
#ifdef USE_PMEM_INSTRUCTIONS
            close(_dev_dax_fd);
#endif
            recovered_pools = true;
            _meta_struct->global_version++;
            persist_fence(_meta_struct, sizeof(pool_meta_data));
        } else {
            _current_pnode_pointer = initialize_mmap(NODE_POOL_SIZE, _dev_dax_fd);
#ifdef UNALIGNED_BUILD
            _current_pnode_pointer += 24;
#else
#ifdef CPU_CACHE_ALIGNED_BUILD
#endif
            _current_pnode_pointer += 128;
#endif
            _not_persisted_pointers = (size_t*)initialize_mmap(100*sizeof(size_t), _dev_dax_fd);
            _meta_struct->not_persisted_pointers = _not_persisted_pointers;
#ifdef USE_PMEM_INSTRUCTIONS
            close(_dev_dax_fd);
#endif
            _meta_struct->flag = 0;
            _meta_struct->node_pool_start = _current_pnode_pointer;
            _meta_struct->assigned_bytes = 0;
            _meta_struct->global_version = 1;
            _meta_struct->info_1.complete_level_node_list_start = _current_pnode_pointer;
            _meta_struct->info_1.max_persisted_level = 0;
            _meta_struct->valid_info_number = 1;
            _meta_struct->info_2.complete_level_node_list_start = _current_pnode_pointer;
            _meta_struct->info_2.max_persisted_level = 0;
            persist_fence(_meta_struct, sizeof(pool_meta_data));
        }
    }

    int pmem_counter = 0;
    int dram_counter =0;
    char* meta_file_path = NULL;
    bool recovered_pools = false;

private:
    char* _current_pnode_pointer;
    char* _current_vnode_pointer;
    std::vector<char*> _vnode_pointer_pools = std::vector<char*>();
    uint64_t _assigned_pmem_bytes = 0;
    uint64_t _assigned_dram_bytes = 0;
    uint64_t _allocated_dram_bytes = NODE_POOL_SIZE;
    int _dev_dax_fd;
    struct pmem2_config *_cfg;
    pool_meta_data* _meta_struct = NULL;
    int _meta_fd;
    struct pmem2_map *_meta_pmem2_map;
    struct pmem2_source *_meta_pmem2_src;
    std::atomic<META_DATA_TYPE> _version_lock;
    std::atomic<META_DATA_TYPE> _allways_locked_lock;
    std::queue<int> _free_positions;
    std::map<void*,int> _ptr_to_index;
    size_t* _not_persisted_pointers;

    PearTreeAllocatorSingleton() {
        STORE_META(_allways_locked_lock,1)
        initialize();
    }

    char* initialize_mmap(size_t const size, int fd, void* start_addr = NULL, size_t const offset=0) {
#ifdef USE_PMEM_INSTRUCTIONS
        void* temp = mmap(start_addr,size,PROT_READ|PROT_WRITE,MAP_PRIVATE | MAP_ANON,fd,offset);
        if((long)temp == -1) {
            std::cerr << "Error with mmap: " << strerror(errno) <<std::endl;
            exit(1);
        }
        return (char*)temp;
#else
#pragma message ("mmap not used. start_addr and offset parameters will be ignored")
        return (char*)calloc(size, 1);
#endif
    }

    char* initialize_mapping(struct pmem2_config *cfg, int fd, struct pmem2_source **src, struct pmem2_map **map) {

        if (pmem2_source_from_fd(src, fd)) {
            pmem2_perror("pmem2_source_from_fd");
            exit(1);
        }

        if (pmem2_map_new(map, cfg, *src)) {
            pmem2_perror("pmem2_map_new");
            exit(1);
        }

        return (char*)pmem2_map_get_address(*map);
    }

};