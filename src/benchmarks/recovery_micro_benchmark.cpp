#include <benchmark/benchmark.h>


#include <pear_recoverer.hpp>

#include "macros/macros.hpp"

#include "micro_benchmark_fixture.hpp"
#include "test_universal_helpers.hpp"
#include "micro_benchmark.hpp"


#define RECOVER_BENCHMARK_SZ_TAXI(NAME,TREE_TYPE, RECOVER_TYPE, MAX_PMEM_LEVEL, INITIAL_DATASET, INITIAL_SIZE)              \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, BM_PEAR_RECOVERY_SZ_TAXI_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    rtree_recover_benchmark<argument_type<void(TREE_TYPE)>::type, argument_type<void(RECOVER_TYPE)>::type, MAX_PMEM_LEVEL>(INITIAL_DATASET, state, *this);                        \
}                                                                                                                           \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, BM_PEAR_RECOVERY_SZ_TAXI_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Arg(INITIAL_SIZE);

template<typename RTree, class Recoverer, int max_pmem_level>
static void rtree_recover_benchmark(micro_benchmark::rect_float_type * const &rects, benchmark::State& state, MicroBenchmarkFixture<RTree> &fixture) {
    using branch_type = Branch<void>;
    int initial_size = state.range(0);
    fixture.prepare_tree(rects, initial_size);
    int count = fixture._tree->test_num_branches(0);
    int num_recovered_branches = fixture._tree->test_num_branches(max_pmem_level+1);
//    test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(*(fixture._tree));
    if (count != initial_size) {
        std::stringstream stream;
        stream << "Not the right number of leafes: " << count << std::endl;
        stream << "Expected: " << initial_size << std::endl;
        std::cout << stream.str();
//        throw std::runtime_error(stream.str());
    }
    int old_max_level = fixture._tree->_max_level;
    state.SetItemsProcessed(num_recovered_branches);
    std::cout << "Branches to recover: " << num_recovered_branches << std::endl;
    delete fixture._tree;
    fixture._tree = NULL;
    RTree* recovered_tree;
    for(auto _: state) {
        recovered_tree = Recoverer::recover_tree_complete_level();
        if(old_max_level != recovered_tree->_max_level) {
            std::cout << "Max level mismatch. New tree has a max_level of " << recovered_tree->_max_level << " but expected " << old_max_level << std::endl;
        }
    }
    int count_new = recovered_tree->test_num_branches(0);
    test_universal_tree_MBR_fit<RTree::t_max_node_size,RTree, typename RTree::node_type,float>(*recovered_tree);
    std::set<long> values;
    for(int i = 0; i < initial_size; i++) {
        values.insert((long)i);
    };
    recovered_tree->_root->check_for_values(values, rects, initial_size);
    std::stringstream missing_values;
    for(auto it = values.begin(); it != values.end(); it++) {
        missing_values << *it << ", ";
    }
    if(!values.empty()) {
        std::cout << "Missing values (" << values.size() << "): " << missing_values.str() << std::endl;
    }
    if (count_new != initial_size) {
        std::stringstream stream;
        stream << "Not the right number of leafes in new tree: " << count_new << std::endl;
        stream << "Expected: " << initial_size << std::endl;
        std::cout << stream.str();
    }
    delete recovered_tree;
}

RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_0_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_100M, 30 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_1_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_100M, 30 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_1_5_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(16000)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(16000)>), 1, SZ_TAXI_100M, 30 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_2_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_100M, 30 * 1000 * 1000)

//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_0_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_100M, 30 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_1_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_100M, 30 * 1000 * 1000)
//
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_0_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_100M, 30 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_M_1_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_100M, 30 * 1000 * 1000)

RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XL_2_5_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(1700)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(1700)>), 3, SZ_TAXI_100M, 100 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XL_2_5_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_NO_2(1700)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_NO_2(1700)>), 3, SZ_TAXI_100M, 100 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XL_2_5_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_NO_2(1700)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_NO_2(1700)>), 3, SZ_TAXI_100M, 100 * 1000 * 1000)

RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_0_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_100M, 100 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_1_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_100M, 100 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_2_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_100M, 100 * 1000 * 1000)

//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_0_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_100M, 100 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_1_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_100M, 100 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_2_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_100M, 100 * 1000 * 1000)
//
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_0_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_100M, 100 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_1_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_100M, 100 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XL_2_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_100M, 100 * 1000 * 1000)

RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XXL_2_5_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(3550)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(3550)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XXL_1_5_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(110000)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(110000)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XXL_2_5_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_NO_2(3550)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_NO_2(3550)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(PARTIAL_LEVEL_XXL_2_5_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_NO_2(3550)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_NO_2(3550)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)

RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_0_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_1_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_200M, 200 * 1000 * 1000)
RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_2_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_200M, 200 * 1000 * 1000)

//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_0_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_1_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_200M, 200 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_2_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_200M, 200 * 1000 * 1000)
//
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_0_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), 0, SZ_TAXI_200M, 200 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_1_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), 1, SZ_TAXI_200M, 200 * 1000 * 1000)
//RECOVER_BENCHMARK_SZ_TAXI(COMPLETE_LEVEL_XXL_2_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), 2, SZ_TAXI_200M, 200 * 1000 * 1000)

