#pragma once

#include <thread>
#include <macros/macros.hpp>
#include <vector>

#include "../micro_benchmark.hpp"
#include "pear_tree.hpp"
#include "fprtree.hpp"
#include "../evaluation/mixed_workload_measurement.hpp"
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
void range_query_task(Pear * tree, int const batch_size, micro_benchmark::rect_float_type *const search_rect, AvgWorkloadMeasurement*  measurement) {
    auto result = std::vector<size_t>();
    measurement->start_event_measurement();
    for(int i = 0; i < batch_size; i++) {
        result.clear();
        tree->contains_query(*search_rect, result);
        if(result.size() < 1) {
            std::cerr << "Warning: too few results for: " <<std::endl;
        }
    }
    measurement->stop_event_measurement();
    measurement->increase_counter(batch_size);
}

template <typename Pear>
void run_insert(std::vector<AvgWorkloadMeasurement*> &insert_measurements, int threads=18) {
    PearTreeAllocatorSingleton::getInstance().reset();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
    for(int i = 0; i < threads; i++) {
        CPU_SET(cpus_socket_two[i], &cpuset);
    }
    Pear* tree = create_tree<Pear>(SZ_TAXI_200M, INITIAL_SIZE);
    std::vector<std::thread> insert_threads = std::vector<std::thread>();
    AvgWorkloadMeasurement temp = AvgWorkloadMeasurement("Run took: ");
    temp.start_event_measurement();
    for(int i = 0; i < threads; i++) {
        insert_threads.emplace_back(insert_task_measured<Pear>, tree, SZ_TAXI_200M, INSERT_SIZE/(threads), (int)(i*(INSERT_SIZE/(threads))+INITIAL_SIZE), insert_measurements[i]);
        int rc = pthread_setaffinity_np(insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i < threads; i++) {
        insert_threads[i].join();
    }
    temp.stop_event_measurement();
    temp.print_result(std::cout);
    int count = tree->test_num_branches(0);
    test_universal_tree_MBR_fit<Pear::t_max_node_size,Pear, typename Pear::node_type,float>(*(tree), true);
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
    delete tree;
}

template <typename Pear>
void run_query(std::vector<AvgWorkloadMeasurement*> &measurements, std::function<QUERY_TASK_SIGNATURE>query_function, int threads=18) {
    PearTreeAllocatorSingleton::getInstance().reset();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
    for(int i = 0; i < threads; i++) {
        CPU_SET(cpus_socket_two[i], &cpuset);
    }
    Pear* tree = create_tree<Pear>(SZ_TAXI_200M, INITIAL_SIZE);
    std::vector<std::thread> insert_threads = std::vector<std::thread>();
    AvgWorkloadMeasurement temp = AvgWorkloadMeasurement("Run took: ");
    temp.start_event_measurement();
    for(int i = 0; i < threads; i++) {
        insert_threads.emplace_back(insert_task_measured<Pear>, tree, SZ_TAXI_200M, INSERT_SIZE/(threads), (int)(i*(INSERT_SIZE/(threads))+INITIAL_SIZE), measurements[i]);
        int rc = pthread_setaffinity_np(insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i < threads; i++) {
        insert_threads[i].join();
    }
    temp.stop_event_measurement();
    temp.print_result(std::cout);
    int count = tree->test_num_branches(0);
    test_universal_tree_MBR_fit<Pear::t_max_node_size,Pear, typename Pear::node_type,float>(*(tree), true);
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
    delete tree;
}

template <typename Pear>
void insert_benchmark(std::ostream & out_stream, std::string name, int threads =OPTIMAL_NUMBER_INSERT_THREADS) {

            std::vector<AvgWorkloadMeasurement> aggregated_insert_measurements = std::vector<AvgWorkloadMeasurement>();
    std::stringstream insert_name_stream;
    insert_name_stream << name << "_" << threads << "_" << TypeParseTraits<Pear>::name;

            for(int iteration = 0; iteration < NUMBER_REPETITIONS_INSERT; iteration++) {
                std::stringstream insert_iteration_name_stream;
                insert_iteration_name_stream << insert_name_stream.str() << "_ITERATION_" << iteration+1;
                std::vector<AvgWorkloadMeasurement*> query_measurements = std::vector<AvgWorkloadMeasurement*>();
                std::vector<AvgWorkloadMeasurement*> insert_measurements = std::vector<AvgWorkloadMeasurement*>();
                for(int j = 0; j < OPTIMAL_NUMBER_INSERT_THREADS; j++) {
                    insert_measurements.emplace_back(new AvgWorkloadMeasurement("Insert"));
                }
                run_insert<Pear>(insert_measurements);
                aggregated_insert_measurements.emplace_back(insert_iteration_name_stream.str(),insert_measurements);
                for(auto ptr : insert_measurements) {
                    delete ptr;
                }
            }
            AvgWorkloadMeasurement::join_measurement_list(aggregated_insert_measurements,out_stream,insert_name_stream.str(), true);

};


template <typename Pear>
void query_fpr_benchmark(std::ostream & out_stream) {

    Pear* tree = create_tree<Pear>(SZ_TAXI_200M, INITIAL_SIZE);
    SEARCH_VECTOR_TYPE search_rects = micro_benchmark::get_pear_query_rects_180m_0_001();
    for(int threads : {1,2,4,8,16,18,24,32,26}) {
        std::vector<AvgWorkloadMeasurement> aggregated_query_measurements = std::vector<AvgWorkloadMeasurement>();
        std::stringstream query_name_stream;
        query_name_stream << "FPR" << "_" << threads << "_" << TypeParseTraits<Pear>::name;
        PearTreeAllocatorSingleton::getInstance().reset();
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        std::vector<int> cpus_socket_two = {CPUS_SOCKET_TWO};
        for (int i = 0; i < threads; i++) {
            CPU_SET(cpus_socket_two[i], &cpuset);
        }

        for (int iteration = 0; iteration < NUMBER_REPETITIONS_QUERY; iteration++) {
            std::stringstream query_iteration_name_stream;
            query_iteration_name_stream << query_name_stream.str() << "_ITERATION_" << iteration + 1;
            std::vector<AvgWorkloadMeasurement *> query_measurements = std::vector<AvgWorkloadMeasurement *>();
            for (int j = 0; j < OPTIMAL_NUMBER_INSERT_THREADS; j++) {
                query_measurements.emplace_back(new AvgWorkloadMeasurement("Insert"));
            }
            AvgWorkloadMeasurement temp = AvgWorkloadMeasurement("Run took: ");
            std::vector<std::thread> query_threads = std::vector<std::thread>();
            temp.start_event_measurement();
            for (int i = 0; i < threads; i++) {
                query_threads.emplace_back(range_query_task < Pear > , tree, 1000,&(search_rects->at(i)), query_measurements[i]);
                int rc = pthread_setaffinity_np(query_threads[i].native_handle(),
                                                sizeof(cpu_set_t), &cpuset);
                if (rc != 0) {
                    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
                }
            }
            for (int i = 0; i < threads; i++) {
                query_threads[i].join();
            }
            temp.stop_event_measurement();
            temp.print_result(std::cout);
            aggregated_query_measurements.emplace_back(query_iteration_name_stream.str(), query_measurements);
            for (auto ptr : query_measurements) {
                delete ptr;
            }
        }
        AvgWorkloadMeasurement::join_measurement_list(aggregated_query_measurements, out_stream,
                                                      query_name_stream.str(), true);
    }

};