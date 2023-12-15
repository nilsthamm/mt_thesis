#pragma once

#include <thread>
#include <macros/macros.hpp>
#include <vector>

#include "../micro_benchmark.hpp"
#include "pear_tree.hpp"
#include "fprtree.hpp"
#include "mixed_workload_measurement.hpp"
#include "test_universal_helpers.hpp"

template<typename T>
struct TypeParseTraits;

#define REGISTER_PARSE_TYPE(X) template <> struct TypeParseTraits<X> \
    { static const char* name; } ; const char* TypeParseTraits<X>::name = #X;

#define PEAR_TREE_TYPE_PMEM PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1024)>
#define PEAR_TREE_TYPE_OPTIMAL_DRAM PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(PEAR_OPTIMAL_DRAM_COUNT)>
#define PEAR_TREE_TYPE_LEAF_PMEM PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>

REGISTER_PARSE_TYPE(PEAR_TREE_TYPE_PMEM)
REGISTER_PARSE_TYPE(PEAR_TREE_TYPE_OPTIMAL_DRAM)
REGISTER_PARSE_TYPE(PEAR_TREE_TYPE_LEAF_PMEM)
REGISTER_PARSE_TYPE(FPR_TREE_TYPE)

#define INITIAL_SIZE SZ_TAXI_XL_INITIAL_POINTS_AMONT
#define INSERT_SIZE SZ_TAXI_XL_ADDED_POINTS_AMONT

#define QUERY_TASK_SIGNATURE void(Pear*, micro_benchmark::rect_float_type  * const, int const, int const, int const, std::atomic_bool *, AvgWorkloadMeasurement*)


template <typename Pear>
static void insert_task(Pear* tree, Rectangle<float> const * const rects, int const amount, int const offset, long const value_offset) {
    for (int i = offset; i < amount+offset; i++) {
        tree->insert(rects[i], (unsigned long) (i-offset)+value_offset);
    }
    std::atomic_thread_fence(std::memory_order_release);
}

template <typename Pear>
static Pear* create_tree(micro_benchmark::rect_float_type const * const rects, int const size) {
    Pear* new_tree = new Pear(0);
    int const num_initial_insert_threads = std::max(std::min(size/10000000,OPTIMAL_NUMBER_INSERT_THREADS),1);
    std::vector<std::thread> initial_insert_threads = std::vector<std::thread>();
    int const initial_inserts = 100000 + ((size-100000)-(((size-100000)/num_initial_insert_threads)*num_initial_insert_threads));
    for (int i = 0; i < initial_inserts; i++) {
        new_tree->insert(rects[i], (long) i);
    }
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
//    for(int i : {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17}) {
    for(int i : {18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34}) {
        CPU_SET(i, &cpuset);
    }
    for(int i = 0; i < num_initial_insert_threads; i++) {
        initial_insert_threads.emplace_back(insert_task<Pear>, new_tree, rects,(size-100000)/num_initial_insert_threads,(((size-100000)/num_initial_insert_threads)*i)+initial_inserts, (((size-100000)/num_initial_insert_threads)*i)+initial_inserts);
        int rc = pthread_setaffinity_np(initial_insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i < initial_insert_threads.size(); i++) {
        initial_insert_threads[i].join();
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    return new_tree;
}

template <typename Pear>
void insert_task_measured(Pear*  tree, micro_benchmark::rect_float_type  * const rects, int const size, int const offset, AvgWorkloadMeasurement*  measurement) {
    measurement->start_event_measurement();
    for(int i = offset; i < offset+size; i++) {
        tree->insert(rects[i], (size_t)i);
    }
    measurement->stop_event_measurement();
    measurement->increase_counter(size);
}


template <typename Pear>
void point_query_task(Pear*  tree, micro_benchmark::rect_float_type  * const rects, int const size, int const offset, int const thread_id, std::atomic_bool *stop_thread, AvgWorkloadMeasurement*  measurement) {
    measurement->start_event_measurement();
    auto result = std::vector<size_t>();
    while(true) {
        for(int i = offset; i < offset+size; i++) {
            result.clear();
            tree->equals_query(rects[i], result);
            if(result.size()  < 1) {
                std::cout << "WARNING: Value at " << i << " not found!" << std::endl;
            }
            if(stop_thread->load(std::memory_order_acquire)) {
                measurement->stop_event_measurement();
                measurement->increase_counter(i-offset);
                return;
            }
        }
        measurement->increase_counter(size);
    }
}

template <typename Pear>
void range_query_task_0_00001(Pear*  tree, micro_benchmark::rect_float_type  * const rects, int const size, int const offset, int const thread_id, std::atomic_bool *stop_thread, AvgWorkloadMeasurement*  measurement) {
    micro_benchmark::rect_float_type const & search_rect = micro_benchmark::get_pear_query_rects_100m_0_00001()->at(thread_id);
    auto result = std::vector<size_t>();
    measurement->start_event_measurement();
    while(true) {
        for(int i = 0; i < 10000; i++) {
            result.clear();
            tree->contains_query(search_rect, result);
            if(result.size()  < 600) {
                std::cout << "WARNING: Too few values (" << result.size() << ") in thread " << thread_id << " for 0_00001 not found!" << std::endl;
            }
            if(stop_thread->load(std::memory_order_acquire)) {
                measurement->stop_event_measurement();
                measurement->increase_counter(i);
                return;
            }
        }
        measurement->increase_counter(10000);
    }
}

template <typename Pear>
void range_query_task_0_0001(Pear*  tree, micro_benchmark::rect_float_type  * const rects, int const size, int const offset, int const thread_id, std::atomic_bool *stop_thread, AvgWorkloadMeasurement*  measurement) {
    micro_benchmark::rect_float_type const & search_rect = micro_benchmark::get_pear_query_rects_100m_0_0001()->at(thread_id);
    auto result = std::vector<size_t>();
    measurement->start_event_measurement();
    while(true) {
        for(int i = 0; i < 10000; i++) {
            result.clear();
            tree->contains_query(search_rect, result);
            if(result.size()  < 8400) {
                std::cout << "WARNING: Too few values (" << result.size() << ") in thread " << thread_id << " for 0_0001 not found!" << std::endl;
            }
            if(stop_thread->load(std::memory_order_acquire)) {
                measurement->stop_event_measurement();
                measurement->increase_counter(i);
                return;
            }
        }
        measurement->increase_counter(10000);
    }
}

template <typename Pear>
void range_query_task_0_001(Pear*  tree, micro_benchmark::rect_float_type  * const rects, int const size, int const offset, int const thread_id, std::atomic_bool *stop_thread, AvgWorkloadMeasurement*  measurement) {
    micro_benchmark::rect_float_type const & search_rect = micro_benchmark::get_pear_query_rects_100m_0_001()->at(thread_id);
    auto result = std::vector<size_t>();
    measurement->start_event_measurement();
    while(true) {
        for(int i = 0; i < 10000; i++) {
            result.clear();
            tree->contains_query(search_rect, result);
            if(result.size()  < 90000) {
                std::cout << "WARNING: Too few values (" << result.size() << ") in thread " << thread_id << " for 0_001 not found!" << std::endl;
            }
            if(stop_thread->load(std::memory_order_acquire)) {
                measurement->stop_event_measurement();
                measurement->increase_counter(i);
                return;
            }
        }
        measurement->increase_counter(10000);
    }
}

template <typename Pear>
void run_queries_and_insert(int const query_thread_count, std::function<QUERY_TASK_SIGNATURE> query_function, std::vector<AvgWorkloadMeasurement*> &query_measurements, std::vector<AvgWorkloadMeasurement*> &insert_measurements) {
    PearTreeAllocatorSingleton::getInstance().reset();
    micro_benchmark::get_pear_query_rects_100m_0_00001();
    micro_benchmark::get_pear_query_rects_100m_0_0001();
    micro_benchmark::get_pear_query_rects_100m_0_001();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for(int i : {18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34}) {
        CPU_SET(i, &cpuset);
    }
    Pear* tree = create_tree<Pear>(SZ_TAXI_200M, INITIAL_SIZE);
    std::vector<std::thread> query_threads = std::vector<std::thread>();
    std::vector<std::atomic_bool*> shutdown_flags = std::vector<std::atomic_bool*>();
    for(int i = 0; i < query_thread_count; i++) {
        shutdown_flags.emplace_back(new std::atomic_bool());
        shutdown_flags[i]->store(false, std::memory_order_release);
        query_threads.emplace_back(query_function, tree, SZ_TAXI_200M, (int)((INITIAL_SIZE-100)/query_thread_count), (int)(i*((INITIAL_SIZE-100)/query_thread_count)), i, shutdown_flags[i], query_measurements[i]);
        int rc = pthread_setaffinity_np(query_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    std::vector<std::thread> insert_threads = std::vector<std::thread>();
    for(int i = 0; i < 18-query_thread_count; i++) {
        insert_threads.emplace_back(insert_task_measured<Pear>, tree, SZ_TAXI_200M, INSERT_SIZE/(18-query_thread_count), (int)(i*(INSERT_SIZE/(18-query_thread_count))+INITIAL_SIZE), insert_measurements[i]);
        int rc = pthread_setaffinity_np(insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i < 18-query_thread_count; i++) {
        insert_threads[i].join();
    }
    for(int i = 0; i < query_thread_count; i++) {
        shutdown_flags[i]->store(true, std::memory_order_release);
    }
#ifndef FPR_TREE
    int count = tree->test_num_branches(0);
    test_universal_tree_MBR_fit<Pear::t_max_node_size,Pear, typename Pear::node_type,float>(*(tree), false);
    if (count < INITIAL_SIZE+INSERT_SIZE-16) {
        std::cout << "Not the right number of leafes: " << count << std::endl;
        std::cout << "Expected: " << INITIAL_SIZE+INSERT_SIZE << std::endl;
        std::set<long> values;
        for(int i = 0; i < INITIAL_SIZE+INSERT_SIZE; i++) {
            values.insert((long)i);
        };
        tree->_root->check_for_values(values, SZ_TAXI_200M, INITIAL_SIZE+INSERT_SIZE);
        std::stringstream missing_values;
        for(auto it = values.begin(); it != values.end(); it++) {
            missing_values << *it << ", ";
        }
        if(!values.empty()) {
            std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
        }
    }
#endif
    for(int i = 0; i < query_thread_count; i++) {
        query_threads[i].join();
        delete shutdown_flags[i];
    }
    delete tree;
}

template <typename Pear>
void mixed_workload_benchmark(std::ostream & out_stream) {
//    std::cout << "Clock precision: " << std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now()-std::chrono::high_resolution_clock::now()).count() << std::endl;
    std::vector<std::pair<std::function<QUERY_TASK_SIGNATURE>,std::string>> query_types = {{point_query_task<Pear>, "_POINT"},{range_query_task_0_00001<Pear>, "_RANGE_0_00001"},{range_query_task_0_0001<Pear>, "_RANGE_0_0001"},{range_query_task_0_001<Pear>, "_RANGE_0_001"}};
//    std::vector<std::pair<std::function<QUERY_TASK_SIGNATURE>,std::string>> query_types = {{range_query_task_0_00001<Pear>, "_RANGE_0_00001"},{range_query_task_0_0001<Pear>, "_RANGE_0_0001"},{range_query_task_0_001<Pear>, "_RANGE_0_001"}};
    for(int query_thread_count = 16; query_thread_count >= 2; query_thread_count-=2) {
//    for(int query_thread_count = 8; query_thread_count >= 2; query_thread_count-=2) {
        for(auto const query_type : query_types) {
            std::vector<AvgWorkloadMeasurement> aggregated_query_measurements = std::vector<AvgWorkloadMeasurement>();
            std::vector<AvgWorkloadMeasurement> aggregated_insert_measurements = std::vector<AvgWorkloadMeasurement>();
            std::stringstream insert_name_stream;
            std::stringstream query_name_stream;
            insert_name_stream << "MIXED_WORKLOAD_" << TypeParseTraits<Pear>::name << "_" << query_thread_count << "_QTHREADS_INSERT" << query_type.second;
            query_name_stream << "MIXED_WORKLOAD_" << TypeParseTraits<Pear>::name << "_" << query_thread_count << "_QTHREADS_QUERY" << query_type.second;
//            for(int iteration = 0; iteration < 1; iteration++) {
            for(int iteration = 0; iteration < NUMBER_REPETITIONS_INSERT; iteration++) {
                std::stringstream query_iteration_name_stream;
                query_iteration_name_stream << query_name_stream.str() << "_ITERATION_" << iteration+1;
                std::stringstream insert_iteration_name_stream;
                insert_iteration_name_stream << insert_name_stream.str() << "_ITERATION_" << iteration+1;
                std::vector<AvgWorkloadMeasurement*> query_measurements = std::vector<AvgWorkloadMeasurement*>();
                for(int j = 0; j < query_thread_count; j++) {
                    query_measurements.emplace_back(new AvgWorkloadMeasurement("Query"));
                }
                std::vector<AvgWorkloadMeasurement*> insert_measurements = std::vector<AvgWorkloadMeasurement*>();
                for(int j = 0; j < 18-query_thread_count; j++) {
                    insert_measurements.emplace_back(new AvgWorkloadMeasurement("Insert"));
                }
                run_queries_and_insert<Pear>(query_thread_count, query_type.first, query_measurements, insert_measurements);
                aggregated_insert_measurements.emplace_back(insert_iteration_name_stream.str(),insert_measurements);
                aggregated_query_measurements.emplace_back(query_iteration_name_stream.str(),query_measurements);
                for(auto ptr : query_measurements) {
                    delete ptr;
                }
                for(auto ptr : insert_measurements) {
                    delete ptr;
                }
            }
            AvgWorkloadMeasurement::join_measurement_list(aggregated_insert_measurements,out_stream,insert_name_stream.str(), true);
            AvgWorkloadMeasurement::join_measurement_list(aggregated_query_measurements,out_stream,query_name_stream.str(), true);
        }
//        return;
    }
};