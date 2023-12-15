#include <thread>
#include <macros/macros.hpp>


#include "micro_benchmark.hpp"
#include "pear_recoverer.hpp"
#include "test_helpers.hpp"

#define BASIC_TREE_TYPE BasicRTree<48,24,0,float>
#define PEAR_TREE_TYPE PearTree<46,16, float, PEAR_STRAT_LEVEL_BOUND(4)>

//#define TREE_TYPE BASIC_TREE_TYPE
#define TREE_TYPE PEAR_TREE_TYPE


void insert_task(TREE_TYPE * tree, Rectangle<float>* const rects, int const amount, int const offset, long const value_offset) {
    for (int i = offset; i < amount+offset; i++) {
        tree->insert(rects[i], (unsigned long) (i-offset)+value_offset);
    }
}
void recovery_task(int const old_max_level) {
    TREE_TYPE* recovered_tree;
//    PearTreeAllocatorSingleton::getInstance().initialize();
    recovered_tree = PearRecoverer<46,16, float, PEAR_STRAT_LEVEL_BOUND(4)>::recover_tree_complete_level();
    if(old_max_level != recovered_tree->_max_level) {
        std::cout << "Max level mismatch. New tree has a max_level of " << recovered_tree->_max_level << " but expected " << old_max_level << std::endl;
    }
}

int main(int argc, char *argv[]) {
    PearTreeAllocatorSingleton::getInstance().reset();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for(int i : {0,1,2,5,6,9,10,14,15}) {
        CPU_SET(i, &cpuset);
    }
    int const num_initial_insert_threads = 8;
    int const initial_inserts = 10*1000*1000;
    std::vector<std::thread> initial_insert_threads = std::vector<std::thread>();
    Rectangle<float>* rects = micro_benchmark::get_sz_taxi_rects_100m();
    TREE_TYPE* tree = new TREE_TYPE(42);

    for(int i = 0; i < num_initial_insert_threads; i++) {
        initial_insert_threads.emplace_back(insert_task, tree, rects,initial_inserts/num_initial_insert_threads,(initial_inserts/num_initial_insert_threads)*i, ((initial_inserts/num_initial_insert_threads)*i));
        int rc = pthread_setaffinity_np(initial_insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }

    Rectangle<float>* rects_insert = micro_benchmark::get_second_sz_taxi_rects_10m();
    for(int i = 0; i < initial_insert_threads.size(); i++) {
        initial_insert_threads[i].join();
    }

    std::cout << "Insert finished" << std::endl;
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


    int num_recovered_branches = tree->test_num_branches(1);
    std::cout << "Num recovered branches: " << num_recovered_branches << std::endl;
    PearTreeAllocatorSingleton::getInstance().reset_for_recovery();
    std::thread t1(recovery_task, tree->_max_level);
    int rc = pthread_setaffinity_np(t1.native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
//    std::set<long> values;
//    for(int i = 0; i < total_inserts+initial_inserts; i++) {
//        values.insert((long)i);
//    };
//    std::set<long> values2;
//    for(int i = 0; i < total_inserts+initial_inserts; i++) {
//        values2.insert((long)i);
//    };
    t1.join();
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

//    PearTreeAllocatorSingleton::getInstance().shut_down();
    return 0;
}