#include <thread>
#include <macros/macros.hpp>

#include "micro_benchmark.hpp"
#include "pear_tree.hpp"

//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NO_1(5)>
//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_DISABLE_SIMD+PEAR_STRAT_NO_PREFETCHING+PEAR_STRAT_BASIC_VERSION(1020)>
//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NO_PREFETCHING+PEAR_STRAT_BASIC_VERSION(1020)>
//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_DISABLE_SIMD+PEAR_STRAT_BASIC_VERSION(1020)>
//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,10)>
//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_COMPLETE_PATH_LOCKING+PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(3200000,1)>
//#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(300000000,1)>
#define PEAR_TREE_TYPE PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING((int)(40.0*1024.0*1024.0/1280.0),1)>

//#define TREE_TYPE BASIC_TREE_TYPE
#define TREE_TYPE PEAR_TREE_TYPE


void insert_task(TREE_TYPE * tree, Rectangle<float>* const rects, int const amount, int const offset, long const value_offset) {
    for (int i = offset; i < amount+offset; i++) {
        tree->insert(rects[i], (unsigned long) (i-offset)+value_offset);
    }
}

int main_thread() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
//    for(int i : {0,1,2,5,6,9,10,14,15}) {
//    for(int i : {0,1,2,5,6,9,10,14,15,3,4,7,8,11,12,13,16,17}) {
    for(int i : {18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35}) {
//    for(int i : {10,14,15,11,12,13,16,17}) {
        CPU_SET(i, &cpuset);
    }
    int const num_initial_insert_threads = 18;
//    int const initial_inserts = 100*1000*1000;
    int const initial_inserts = 150*1000*1000;
    PearTreeAllocatorSingleton::getInstance().reset();
    std::vector<std::thread> initial_insert_threads = std::vector<std::thread>();
    Rectangle<float>* rects = micro_benchmark::get_sz_taxi_rects_200m();
    TREE_TYPE* tree = new TREE_TYPE(42);

    for(int i = 0; i < 180000; i++) {
        tree->insert(rects[i], (unsigned long) i);
    }

    for(int i = 0; i < num_initial_insert_threads; i++) {
        initial_insert_threads.emplace_back(insert_task, tree, rects,(initial_inserts-180000)/(num_initial_insert_threads),((initial_inserts-180000)/(num_initial_insert_threads))*i, (((initial_inserts-180000)/(num_initial_insert_threads))*i));
        int rc = pthread_setaffinity_np(initial_insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }

//    Rectangle<float>* rects_insert = micro_benchmark::get_sz_taxi_rects_200m();
    for(int i = 0; i < initial_insert_threads.size(); i++) {
        initial_insert_threads[i].join();
    }


    std::vector<std::thread> threads = std::vector<std::thread>();
//    std::cerr << "PMEM nodes: " << BasicRTree<49,24,MAX_PMEM_LEVEL,float>::get_pmem_counter() << "\n";
//    std::cerr << "DRAM nodes: " << BasicRTree<49,24,MAX_PMEM_LEVEL,float>::get_dram_counter() << "\n";
//    if(tree->test_num_branches(0) != 10000000) {
//        std::cout << "Error initial insert, actually inserted: " << tree->test_num_branches(0) << "\n";
//    } else {
//        std::cout << "Initial insert successful (number leaves)" << std::endl;
//    }
//
//    if(tree->_max_level < 4) {
//        std::cout << "Error initial insert, actual max_level: " << tree->_max_level << "\n";
//    } else {
//        std::cout << "Initial insert successful (max level: " << tree->_max_level << ")" << std::endl;
//    }

    Rectangle<float>* rects_insert = micro_benchmark::get_sz_taxi_rects_200m();
    int const num_threads = 18;
    int const total_inserts = 50000000;
    for(int i = 0; i < num_threads; i++) {
        threads.emplace_back(insert_task, tree, rects_insert,total_inserts/num_threads,(total_inserts/num_threads)*i+initial_inserts, initial_inserts+((total_inserts/num_threads)*i));
        int rc = pthread_setaffinity_np(threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
//    std::set<long> values;
//    for(int i = 0; i < total_inserts+initial_inserts; i++) {
//        values.insert((long)i);
//    };
//    std::set<long> values2;
//    for(int i = 0; i < total_inserts+initial_inserts; i++) {
//        values2.insert((long)i);
//    };
    for(int i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
//    int count_0 = tree->test_num_branches(0);
//    int count_1 = tree->test_num_branches(1);
//    int count_2 = tree->test_num_branches(2);
//    int count_3 = tree->test_num_branches(3);
//    int count_4 = tree->test_num_branches(4);
//    int count_5 = 0;
//    if(tree->_max_level >= 5) {
//        count_5 = tree->test_num_branches(5);
//    }
//
//    std::cout << "Branches on level 0/Nodes on level -1: " << count_0 << std::endl;
//    std::cout << "Branches on level 1/Nodes on level 0: " << count_1 << std::endl;
//    std::cout << "Branches on level 2/Nodes on level 1: " << count_2 << std::endl;
//    std::cout << "Branches on level 3/Nodes on level 2: " << count_3 << std::endl;
//    std::cout << "Branches on level 4/Nodes on level 3: " << count_4 << std::endl;
//    std::cout << "Branches on level 5/Nodes on level 4: " << count_5 << std::endl;
//    int num_dram_nodes = tree->get_dram_node_count();
//    int num_pmem_nodes = tree->get_pmem_node_count();
//    std::cout << num_dram_nodes+num_pmem_nodes << " nodes in total (DRAM: " << num_dram_nodes << "; PMEM: " << num_pmem_nodes << ")" << std::endl;
//    int count = tree->test_num_branches(0);
//    test_tree_MBR_fit(*(tree));
//    if (count != total_inserts+initial_inserts) {
//        std::cout << "Not the right number of leafes: " << count << std::endl;
//        std::cout << "Expected: " << total_inserts+initial_inserts << std::endl;
//    }
//    tree->_root->check_for_values(values, rects);
//    std::stringstream missing_values;
//    for(auto it = values.begin(); it != values.end(); it++) {
//        missing_values << *it << ", ";
//    }
//    if(!values.empty()) {
//    std::cout << "Missing values ("<< values.size() <<"): " << missing_values.str() << std::endl;
//    } else {
//        std::cout << "All values found";
//    }
//    tree->_root->check_for_values(values2, rects);
//    std::cerr << "PMEM nodes: " << BasicRTree<49,24,MAX_PMEM_LEVEL,float>::get_pmem_counter() << "\n";
//    std::cerr << "DRAM nodes: " << BasicRTree<49,24,MAX_PMEM_LEVEL,float>::get_dram_counter() << "\n";

//    if(tree->test_num_branches(0) != 10000000+total_inserts) {
//        std::cerr << "Error consecutive insert, actually inserted: " << tree->test_num_branches(0) << "\n";
//    } else {
//        std::cout << "Consecutive insert successful (number leaves)" << std::endl;
//    }

//    if(tree->_max_level < 4) {
//        std::cerr << "Error consecutive insert, actual max_level: " << tree->_max_level << "\n";
//    } else {
//        std::cout << "Consecutive insert successful (max level: " << tree->_max_level << ")" << std::endl;
//    }
    delete tree;
    return 0;
}

int main(int argc, char *argv[]) {

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
//    for(int i : {0,1,2,5,6,9,10,14,15}) {
//    for(int i : {0,1,2,5,6,9,10,14,15,3,4,7,8,11,12,13,16,17}) {
    for(int i : {18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35}) {
//    for(int i : {10,14,15,11,12,13,16,17}) {
        CPU_SET(i, &cpuset);
    }

    for(int i = 0; i < NUMBER_REPETITIONS_INSERT; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        auto main_t = std::thread(main_thread);
        int rc = pthread_setaffinity_np(main_t.native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
        main_t.join();
        auto const total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() -start).count();
        std::cout << "ITERATION " << i << ": " << total_duration/1000000.0 << "ms" << std::endl;
    }
    return 0;
}