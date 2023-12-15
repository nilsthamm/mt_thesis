#pragma once

#include <benchmark/benchmark.h>
#include <iostream>
#include <memory>
#include <random>
#include <fstream>
#include <sstream>
#include <thread>

#include <rectangle.hpp>
#include <per_tree_allocator.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry.hpp>

#include "micro_benchmark.hpp"
#include "custom_stats.hpp"
#include "optional_args.hpp"
#include "test_universal_helpers.hpp"




#define QUERY_00001_BATCH_SIZE 100000
#define QUERY_0001_BATCH_SIZE 10000
#define QUERY_001_BATCH_SIZE 1000

//#ifndef PROJECT_ROOT
//#define PROJECT_ROOT "not a directory"
//#endif



template <typename RT>
class MicroBenchmarkFixture : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override;


    static void insert_task(RT* tree, Rectangle<float> const * const rects, int const amount, int const offset, long const value_offset, optional_args* const args_ptr) {
        for (int i = offset; i < amount+offset; i++) {
            tree->insert(rects[i], (unsigned long) (i-offset)+value_offset, args_ptr);
        }
        std::atomic_thread_fence(std::memory_order_release);
    }

    template<typename T>
    static RT* create_tree(Rectangle<T> const * const rects, int const size, bool const single_threaded, optional_args* const args_ptr) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
        int const num_initial_insert_threads = single_threaded ? 1 : std::max(std::min(size/10000000,OPTIMAL_NUMBER_INSERT_THREADS),1);
        std::vector<optional_args*> pointer_stats = std::vector<optional_args*>();
        for(int i = 0; i < num_initial_insert_threads; i++) {
            CPU_SET(cpus_socket_two[i], &cpuset);
            pointer_stats.emplace_back(new optional_args());
        }
        RT* new_tree = new RT(0);
        std::vector<std::thread> initial_insert_threads = std::vector<std::thread>();
        int const initial_inserts = 100000 + ((size-100000)-(((size-100000)/num_initial_insert_threads)*num_initial_insert_threads));
        std::thread initial_insert_thread = std::thread(insert_task, new_tree, rects,initial_inserts,0, 0l,args_ptr);
        int rc = pthread_setaffinity_np(initial_insert_thread.native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
        initial_insert_thread.join();
//        for (int i = 0; i < initial_inserts; i++) {
//            new_tree->insert(rects[i], (long) i);
//        }
        for(int i = 0; i < num_initial_insert_threads; i++) {
            initial_insert_threads.emplace_back(insert_task, new_tree, rects,(size-100000)/num_initial_insert_threads,(((size-100000)/num_initial_insert_threads)*i)+initial_inserts, (((size-100000)/num_initial_insert_threads)*i)+initial_inserts, pointer_stats[i]);
            int rc = pthread_setaffinity_np(initial_insert_threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
        for(int i = 0; i < initial_insert_threads.size(); i++) {
            initial_insert_threads[i].join();
        }
        if(args_ptr != nullptr) {
            for(int i = 0; i < num_initial_insert_threads; i++) {
                args_ptr->branch_wait_counter += pointer_stats[i]->branch_wait_counter;
                args_ptr->node_wait_counter += pointer_stats[i]->node_wait_counter;
                args_ptr->aborts_node_changed += pointer_stats[i]->aborts_node_changed;
                args_ptr->aborts_node_locked += pointer_stats[i]->aborts_node_locked;
                delete pointer_stats[i];
            }
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        return new_tree;
    }

    template<typename T>
    void prepare_tree(Rectangle<T> const * const rects, int const size, optional_args* const args_ptr= nullptr, bool const single_threaded=false) {
        if(_tree != NULL) {
            if(single_threaded) {
                return;
            }
            delete _tree;
            _tree = NULL;
        }
        std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
        std::thread temp([&temp] {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
            for(int i = 0; i < cpus_socket_two.size(); i++) {
                CPU_SET(cpus_socket_two[i], &cpuset);
            }
            int rc = pthread_setaffinity_np(temp.native_handle(),
                                                              sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
            PearTreeAllocatorSingleton::getInstance().reset();
        });

        temp.join();
        PearTreeAllocatorSingleton::getInstance().reset();
        _tree = create_tree(rects, size, single_threaded, args_ptr);
    }

    template<size_t max_node_size, size_t min_node_size = 24, size_t reinsert = 0, typename T = float>
    void prepare_boost_tree(Rectangle<T> const * const rects, int const size, std::shared_ptr<boost::geometry::index::rtree<micro_benchmark::boost_value, boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >> tree) const {
        for (int i = 0; i < size; i++) {
            tree->insert(std::make_pair(micro_benchmark::boost_rect(micro_benchmark::boost_point(rects[i].x_min,rects[i].y_min),micro_benchmark::boost_point(rects[i].x_max,rects[i].y_max)),(int64_t)i));
        }
    }

    std::atomic<int> preparation_flag;
    RT* _tree = NULL;
};

template<typename RT>
void MicroBenchmarkFixture<RT>::SetUp(benchmark::State &state) {
    preparation_flag.store(0, std::memory_order_seq_cst);
}

template<typename tree_type>
static void eval_insert_benchmark(micro_benchmark::rect_float_type const * const initial_rects, micro_benchmark::rect_float_type const * const added_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int added_size = state.range(1)/state.threads;
    int initial_size = state.range(0);
//    if(ipow(tree_type::t_max_node_size, tree_type::max_tree_depth)*0.7 < added_size+initial_size) {
//        std::cout << "WARNING: Trying to add " << added_size+initial_size << " to a tree with a max capacity of " << ipow(tree_type::t_max_node_size, tree_type::max_tree_depth) << std::endl;
//    }
    if(state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(initial_rects, initial_size, true);
    }
    for(auto _: state) {
        for (int i = 0; i < added_size; i++) {
            fixture._tree->insert(added_rects[i+(added_size*(state.thread_index))], (long) i+(added_size*(state.thread_index))+initial_size,nullptr);
        }
    }
    if(state.threads == 1 || state.thread_index == 0) {
        int count = fixture._tree->test_num_branches(0);
        test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(*(fixture._tree), false);
        if (count != initial_size + (added_size * (state.threads))) {
            std::cout << "Not the right number of leafes: " << count << std::endl;
            std::cout << "Expected: " << initial_size + (added_size * (state.threads)) << std::endl;
            std::set<long> values;
            for(int i = 0; i < (added_size * (state.threads))+initial_size; i++) {
                values.insert((long)i);
            };
            fixture._tree->_root->check_for_values(values, initial_rects, initial_size);
            std::stringstream missing_values;
            for(auto it = values.begin(); it != values.end(); it++) {
                missing_values << *it << ", ";
            }
            if(!values.empty()) {
                std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
            }
        }
        state.counters["DRAM_nodes"] = fixture._tree->get_dram_node_count();
        state.counters["PMEM_nodes"] = fixture._tree->get_pmem_node_count();
        if(fixture._tree->get_persisted_node_count() > 0) {
            state.counters["persisted_nodes"] = fixture._tree->get_persisted_node_count();
        } else {
            state.counters["persisted_nodes"] = -1;
        }
        delete fixture._tree;
        fixture._tree = NULL;
        fixture.preparation_flag.store(1, std::memory_order_seq_cst);
    }
    state.SetItemsProcessed((added_size*(state.threads)));
    while(fixture.preparation_flag.load(std::memory_order_acquire) == 0);
}


template<typename tree_type>
static void insert_task(tree_type * tree, micro_benchmark::rect_float_type const * const rects, int const amount, int const offset, long const value_offset, optional_args* const args_ptr) {
    for (int i = offset; i < amount+offset; i++) {
        tree->insert(rects[i], (unsigned long) (i-offset)+value_offset, args_ptr);
    }
}

template<typename tree_type>
static void eval_insert_benchmark_hyperthreading(micro_benchmark::rect_float_type const * const initial_rects, micro_benchmark::rect_float_type const * const added_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);
    int threads = state.range(2);
    int added_size = state.range(1)/threads;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
    std::vector<optional_args*> pointer_stats = std::vector<optional_args*>();
    for(int i = 0; i < threads; i++) {
        CPU_SET(cpus_socket_two[i], &cpuset);
        pointer_stats.emplace_back(new optional_args());
    }

    if(state.threads != 1) {
        INVALID_BRANCH_TAKEN_NO_TYPE
    }
    optional_args* sum_args = new optional_args();
    fixture.prepare_tree(initial_rects, initial_size, sum_args, threads==1);
    std::vector<std::thread> insert_threads = std::vector<std::thread>();
//    auto start = std::chrono::high_resolution_clock::now();
    state.counters["stalls_branch_locked_preparation"] = sum_args->branch_wait_counter;
    state.counters["stalls_node_locked_preparation"] = sum_args->node_wait_counter;
    state.counters["aborts_node_changed_preparation"] = sum_args->aborts_node_changed;
    state.counters["aborts_node_locked_preparation"] = sum_args->aborts_node_locked;
    state.counters["node_updates_preparation"] = fixture._tree->count_node_updates();
    state.counters["branch_updates_preparation"] = fixture._tree->count_branch_updates();
    for(auto _: state) {
        for(int i = 0; i < threads; i++) {
            insert_threads.emplace_back(insert_task<tree_type>, fixture._tree, added_rects,added_size,added_size*i, added_size*i+initial_size, pointer_stats[i]);
            int rc = pthread_setaffinity_np(insert_threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
        for(int i = 0; i < insert_threads.size(); i++) {
            insert_threads[i].join();
        }
    }
//    auto const total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() -start).count();
//    std::cout << "ITERATION: " << total_duration/1000000.0 << "ms" << std::endl;
    int count = fixture._tree->test_num_branches(0);
    test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(*(fixture._tree), false);
    if (count != initial_size + (added_size * (threads))) {
        std::cout << "Not the right number of leafes: " << count << std::endl;
        std::cout << "Expected: " << initial_size + (added_size * (threads)) << std::endl;
        std::set<long> values;
        for(int i = 0; i < (added_size * (threads))+initial_size; i++) {
            values.insert((long)i);
        };
        fixture._tree->_root->check_for_values(values, initial_rects, initial_size);
        std::stringstream missing_values;
        for(auto it = values.begin(); it != values.end(); it++) {
            missing_values << *it << ", ";
        }
        if(!values.empty()) {
            std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
        }
    }
    state.counters["DRAM_nodes"] = fixture._tree->get_dram_node_count();
    state.counters["PMEM_nodes"] = fixture._tree->get_pmem_node_count();
    if(fixture._tree->get_persisted_node_count() > 0) {
        state.counters["persisted_nodes"] = fixture._tree->get_persisted_node_count();
    } else {
        state.counters["persisted_nodes"] = -1;
    }
    optional_args measured_sum_args = optional_args();
    for(int i = 0; i < threads; i++) {
        measured_sum_args.branch_wait_counter += pointer_stats[i]->branch_wait_counter;
        measured_sum_args.node_wait_counter += pointer_stats[i]->node_wait_counter;
        measured_sum_args.aborts_node_changed += pointer_stats[i]->aborts_node_changed;
        measured_sum_args.aborts_node_locked += pointer_stats[i]->aborts_node_locked;
        delete pointer_stats[i];
    }
    state.counters["stalls_branch_locked"] = measured_sum_args.branch_wait_counter;
    state.counters["stalls_node_locked"] = measured_sum_args.node_wait_counter;
    state.counters["aborts_node_changed"] = measured_sum_args.aborts_node_changed;
    state.counters["aborts_node_locked"] = measured_sum_args.aborts_node_locked;
    state.counters["node_updates"] = fixture._tree->count_node_updates()-state.counters["node_updates_preparation"];
    state.counters["branch_updates"] = fixture._tree->count_branch_updates()-state.counters["branch_updates_preparation"];
    delete fixture._tree;
    fixture._tree = NULL;
    fixture.preparation_flag.store(1, std::memory_order_seq_cst);
    state.SetItemsProcessed((added_size*(threads)));
    while(fixture.preparation_flag.load(std::memory_order_acquire) == 0);
    delete sum_args;
}

template<typename tree_type>
static void eval_range_query_benchmark(micro_benchmark::rect_float_type const * const &rects, SEARCH_VECTOR_TYPE search_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);

    if(state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(rects, initial_size);
        int count = fixture._tree->test_num_branches(0);
        if (count != initial_size) {
            std::stringstream stream;
            stream << "Not the right number of leafes: " << count << std::endl;
            stream << "Expected: " << initial_size << std::endl;
            std::cout << stream.str();
            std::set<long> values;
            for(int i = 0; i < initial_size; i++) {
                values.insert((long)i);
            };
            fixture._tree->_root->check_for_values(values, rects, initial_size);
            std::stringstream missing_values;
            for(auto it = values.begin(); it != values.end(); it++) {
                missing_values << *it << ", ";
            }
            if(!values.empty()) {
                std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
            }
        }
    }
    size_t results = 0;
    auto result = std::vector<size_t>();
    micro_benchmark::rect_float_type const &search_rect = search_rects->at(state.thread_index);
    long const batch_size = state.range(1);
    for(auto _: state) {
        for(int i = 0; i < batch_size; i++) {
            result.clear();
            fixture._tree->contains_query(search_rect, result);
            results += result.size();
        }
    }
    if(state.threads == 1 || state.thread_index == 0) {
        state.SetItemsProcessed(results);
    }
}


template<typename tree_type>
static void range_query_task(tree_type * tree, int const batch_size, micro_benchmark::rect_float_type *const search_rect) {
    auto result = std::vector<size_t>();
    for(int i = 0; i < batch_size; i++) {
        result.clear();
        tree->contains_query(*search_rect, result);
        if(result.size() < 1) {
            std::cerr << "Warning: too few results for: " <<std::endl;
        }
    }
}

template<typename tree_type>
static void eval_range_query_benchmark_thread_binding(micro_benchmark::rect_float_type const * const &rects, SEARCH_VECTOR_TYPE search_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);
    int threads = state.range(2);

    if(state.threads != 1) {
        INVALID_BRANCH_TAKEN_NO_TYPE
    }
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
    for(int i = 0; i < threads; i++) {
        CPU_SET(cpus_socket_two[i], &cpuset);
    }
    fixture.prepare_tree(rects, initial_size, nullptr,true);
    int count = fixture._tree->test_num_branches(0);
    if (count != initial_size) {
        std::stringstream stream;
        stream << "Not the right number of leafes: " << count << std::endl;
        stream << "Expected: " << initial_size << std::endl;
        std::cout << stream.str();
        std::set<long> values;
        for(int i = 0; i < initial_size; i++) {
            values.insert((long)i);
        };
        fixture._tree->_root->check_for_values(values, rects, initial_size);
        std::stringstream missing_values;
        for(auto it = values.begin(); it != values.end(); it++) {
            missing_values << *it << ", ";
        }
        if(!values.empty()) {
            std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
        }
    }

    std::vector<std::thread> query_threads = std::vector<std::thread>();
    long const batch_size = state.range(1);
    for(auto _: state) {
        for(int i = 0; i < threads; i++) {
            query_threads.emplace_back(range_query_task<tree_type>, fixture._tree, batch_size,&(search_rects->at(i)));
            int rc = pthread_setaffinity_np(query_threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
        for(int i = 0; i < query_threads.size(); i++) {
            query_threads[i].join();
        }
    }
    state.SetItemsProcessed(batch_size*threads);
}

template<typename tree_type>
static void eval_point_query_benchmark(micro_benchmark::rect_float_type const * const &rects, int search_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);

    if(state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(rects, initial_size, nullptr, true);
#ifndef FPR_TREE_TYPE
        int count = fixture._tree->test_num_branches(0);
        if (count != initial_size) {
            std::stringstream stream;
            stream << "Not the right number of leafes: " << count << std::endl;
            stream << "Expected: " << initial_size << std::endl;
            std::cout << stream.str();
            std::set<long> values;
            for(int i = 0; i < initial_size; i++) {
                values.insert((long)i);
            };
            fixture._tree->_root->check_for_values(values, rects, initial_size);
            std::stringstream missing_values;
            for(auto it = values.begin(); it != values.end(); it++) {
                missing_values << *it << ", ";
            }
            if(!values.empty()) {
                std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
            }
        }
#endif
    }
    size_t results = 0;
    auto result = std::vector<size_t>();
    int const offset = state.thread_index*search_rects;
    query_stats stats = query_stats();
    query_stats* const stats_ptr = &(stats);
    for(auto _: state) {
        for(int i = 0; i < search_rects; i++) {
            result.clear();
            fixture._tree->equals_query(rects[i+offset], result, stats_ptr);
            results += result.size();
        }
    }
    state.counters["DRAM_nodes"] = fixture._tree->get_dram_node_count();
    state.counters["PMEM_nodes"] = fixture._tree->get_pmem_node_count();
    state.counters["touched_DRAM_nodes"] = stats.touched_v_nodes;
    state.counters["touched_PMEM_nodes"] = stats.touched_p_nodes;
    state.counters["touched_inner_nodes"] = stats.touched_inner_nodes;
    state.counters["touched_leaf_nodes"] = stats.touched_leafs;
    state.counters["num_results"] = stats.results_count;
    state.counters["avg_area"] = fixture._tree->get_avg_mbr_area();
    state.counters["avg_overlap"] = fixture._tree->get_avg_mbr_overlap();
    state.SetItemsProcessed(state.threads*search_rects*state.iterations());
}


template<typename tree_type>
static void point_query_task(tree_type * tree, micro_benchmark::rect_float_type const * const rects, int const amount, int const offset, query_stats* const stats_ptr) {
    auto result = std::vector<size_t>();
    for(int i = 0; i < amount; i++) {
        result.clear();
        tree->equals_query(rects[i+offset], result, stats_ptr);
        if(result.size() < 1) {
            std::cerr << "Warning: too few results for: " <<std::endl;
        }
    }
}

template<typename tree_type>
static void eval_point_query_benchmark_thread_binding(micro_benchmark::rect_float_type const * const &rects, int search_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);
    int threads = state.range(2);
    if(state.threads != 1) {
        INVALID_BRANCH_TAKEN_NO_TYPE
    }
    fixture.prepare_tree(rects, initial_size, nullptr, true);
#ifndef FPR_TREE_TYPE
    int count = fixture._tree->test_num_branches(0);
    if (count != initial_size) {
        std::stringstream stream;
        stream << "Not the right number of leafes: " << count << std::endl;
        stream << "Expected: " << initial_size << std::endl;
        std::cout << stream.str();
        std::set<long> values;
        for(int i = 0; i < initial_size; i++) {
            values.insert((long)i);
        };
        fixture._tree->_root->check_for_values(values, rects, initial_size);
        std::stringstream missing_values;
        for(auto it = values.begin(); it != values.end(); it++) {
            missing_values << *it << ", ";
        }
        if(!values.empty()) {
            std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
        }
    }
#endif
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
    std::vector<query_stats*> pointer_stats = std::vector<query_stats*>();
    for(int i = 0; i < threads; i++) {
        CPU_SET(cpus_socket_two[i], &cpuset);
        pointer_stats.emplace_back(new query_stats());
    }
    size_t results = 0;
    int const offset = state.thread_index*search_rects;
    std::vector<std::thread> query_threads = std::vector<std::thread>();
    for(auto _: state) {
        for(int i = 0; i < threads; i++) {
            query_threads.emplace_back(point_query_task<tree_type>, fixture._tree, rects,search_rects,search_rects*i, pointer_stats[i]);
            int rc = pthread_setaffinity_np(query_threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
        for(int i = 0; i < query_threads.size(); i++) {
            query_threads[i].join();
        }
    }
    state.counters["DRAM_nodes"] = fixture._tree->get_dram_node_count();
    state.counters["PMEM_nodes"] = fixture._tree->get_pmem_node_count();
    query_stats stats = query_stats();
    stats.touched_v_nodes = 0;
    stats.touched_p_nodes = 0;
    stats.touched_inner_nodes = 0;
    stats.touched_leafs = 0;
    stats.results_count = 0;
    for(int i = 0; i < threads; i++) {
        stats.touched_v_nodes += pointer_stats[i]->touched_v_nodes;
        stats.touched_p_nodes += pointer_stats[i]->touched_p_nodes;
        stats.touched_inner_nodes += pointer_stats[i]->touched_inner_nodes;
        stats.touched_leafs += pointer_stats[i]->touched_leafs;
        stats.results_count += pointer_stats[i]->results_count;
        delete pointer_stats[i];
    }
    state.counters["touched_DRAM_nodes"] = stats.touched_v_nodes;
    state.counters["touched_PMEM_nodes"] = stats.touched_p_nodes;
    state.counters["touched_inner_nodes"] = stats.touched_inner_nodes;
    state.counters["touched_leaf_nodes"] = stats.touched_leafs;
    state.counters["num_results"] = stats.results_count;
    state.SetItemsProcessed(threads*search_rects);
}

template<typename tree_type>
static void
eval_fpr_insert_benchmark(micro_benchmark::rect_float_type const *const rects,
                          micro_benchmark::rect_float_type const *const added_rects, benchmark::State &state,
                          MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);
    int added_size = state.range(1) / state.threads;
    if (state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(rects, initial_size);
    }
    for (auto _: state) {
        state.SetItemsProcessed((added_size * (state.threads)));
        for (int i = 0; i < added_size; i++) {
            fixture._tree->insert(added_rects[i + (added_size * (state.thread_index))],
                                  (long) i + (added_size * (state.thread_index)) + initial_size);
        }
    }
}

template<typename tree_type>
static void eval_fpr_range_query_benchmark(micro_benchmark::rect_float_type const * const &rects, SEARCH_VECTOR_TYPE search_rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);

    if(state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(rects, initial_size);
    }
    size_t results = 0;
    auto result = std::vector<size_t>();
    micro_benchmark::rect_float_type const &search_rect = search_rects->at(state.thread_index);
    long const batch_size = state.range(1);
    for(auto _: state) {
        for(int i = 0; i < batch_size; i++) {
            result.clear();
            fixture._tree->contains_query(search_rect, result);
            results += result.size();
        }
    }
    if(state.threads == 1 || state.thread_index == 0) {
        state.SetItemsProcessed(results);
    }
}