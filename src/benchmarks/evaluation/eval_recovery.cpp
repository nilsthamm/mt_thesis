#include <benchmark/benchmark.h>


#include <pear_recoverer.hpp>

#include "macros/macros.hpp"

#include "../micro_benchmark_fixture.hpp"
#include "test_universal_helpers.hpp"
#include "custom_stats.hpp"


#define EVAL_RECOVERY(NAME,TREE_TYPE, RECOVER_TYPE, COLLECT_STATS)              \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EVAL_RECOVERY_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    rtree_recover_benchmark<argument_type<void(TREE_TYPE)>::type, argument_type<void(RECOVER_TYPE)>::type>(state, *this, COLLECT_STATS);                        \
}                                                                                                                           \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_RECOVERY_ ## NAME)->Unit(benchmark::kMillisecond)->Iterations(1)->Repetitions((COLLECT_STATS) ? 1 : NUMBER_REPETITIONS_INSERT)->Arg(100*1000*1000); \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_RECOVERY_ ## NAME)->Unit(benchmark::kMillisecond)->Iterations(1)->Repetitions((COLLECT_STATS) ? 1 : NUMBER_REPETITIONS_INSERT)->Arg(150*1000*1000); \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_RECOVERY_ ## NAME)->Unit(benchmark::kMillisecond)->Iterations(1)->Repetitions((COLLECT_STATS) ? 1 : NUMBER_REPETITIONS_INSERT)->Arg(200*1000*1000);

template<typename RTree, class Recoverer>
static void rtree_recover_benchmark(benchmark::State& state, MicroBenchmarkFixture<RTree> &fixture, bool const collect_stats) {
    using branch_type = Branch<void>;
    int initial_size = state.range(0);
    micro_benchmark::rect_float_type * const rects = SZ_TAXI_200M;
    int old_max_level;
    if(state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(rects, initial_size);
        int count = fixture._tree->test_num_branches(0);
        if (count != initial_size) {
            std::stringstream stream;
            stream << "Not the right number of leafes: " << count << std::endl;
            stream << "Expected: " << initial_size << std::endl;
            std::cout << stream.str();
        }
        old_max_level = fixture._tree->_max_level;
        delete fixture._tree;
        fixture._tree = NULL;
    }
    RTree *recovered_tree;
    for(auto _: state) {
        recovered_tree = Recoverer::recover_tree_complete_level();
        if(old_max_level != recovered_tree->_max_level) {
            std::cout << "Max level mismatch. New tree has a max_level of " << recovered_tree->_max_level << " but expected " << old_max_level << std::endl;
        }
    }
    if(state.threads == 1 || state.thread_index == 0) {
        int count = recovered_tree->test_num_branches(0);
        test_universal_tree_MBR_fit<RTree::t_max_node_size,RTree, typename RTree::node_type,float>(*(recovered_tree), false);
        if (count != initial_size) {
            std::cout << "Not the right number of leafes: " << count << std::endl;
            std::cout << "Expected: " << initial_size << std::endl;
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
        }
        int num_dram_nodes = recovered_tree->get_dram_node_count();
        int num_pmem_nodes = recovered_tree->get_pmem_node_count();
        if(collect_stats) {
            recovery_stats stats = recovered_tree->_recovered_stats;
            state.counters["touched_p_nodes"] = stats.touched_p_nodes;
            state.counters["recreated_v_nodes"] = stats.recreated_v_nodes;
            state.counters["reinserted_p_nodes"] = stats.reinserted_p_nodes;
            state.counters["partial_level_p_nodes"] = stats.partial_level_p_nodes;
            state.counters["complete_persisted_level"] = stats.complete_persisted_level;
            state.counters["partial_persisted_level"] = stats.partial_persisted_level;
            state.counters["persistent_nodes"] = num_pmem_nodes;
            state.counters["volatile_nodes"] = num_dram_nodes;
        } else {
            state.counters["touched_p_nodes"] = -1;
            state.counters["recreated_v_nodes"] = -1;
            state.counters["reinserted_p_nodes"] = -1;
            state.counters["partial_level_p_nodes"] = -1;
            state.counters["complete_persisted_level"] = -1;
            state.counters["partial_persisted_level"] = -1;
            state.counters["persistent_nodes"] = num_pmem_nodes;
            state.counters["volatile_nodes"] = num_dram_nodes;
        }
        delete recovered_tree;
    }
}

// only leaf in PMEM = 270MB DRAM
EVAL_RECOVERY(NODE_WISE_MAX_DRAM_NODES, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(320000)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(320000)>), false)
EVAL_RECOVERY(NODE_WISE_MAX_DRAM_NODES_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(320000)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(320000)>), true)


EVAL_RECOVERY(NODE_WISE_380MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(380.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(380.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_380MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(380.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(380.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_360MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(360.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(360.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_360MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(360.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(360.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_340MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(340.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(340.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_340MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(340.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(340.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_320MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(320.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(320.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_320MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(320.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(320.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_300MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(300.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(300.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_300MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(300.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(300.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_280MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(280.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(280.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_280MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(280.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(280.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_260MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(260.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(260.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_260MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(260.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(260.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_240MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(240.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(240.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_240MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(240.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(240.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_220MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(220.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(220.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_220MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(220.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(220.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_200MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(200.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(200.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_200MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(200.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(200.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_180MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(180.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(180.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_180MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(180.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(180.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_160MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(160.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(160.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_160MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(160.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(160.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_140MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(140.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(140.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_140MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(140.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(140.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_120MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(120.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(120.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_120MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(120.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(120.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_100MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(100.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(100.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_100MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(100.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(100.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_80MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(80.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(80.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_80MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(80.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(80.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_60MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(60.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(60.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_60MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(60.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(60.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_40MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(40.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(40.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_40MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(40.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(40.0*1024.0*1024.0/1280.0)))>), true)

EVAL_RECOVERY(NODE_WISE_20MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(20.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(((int)(20.0*1024.0*1024.0/1280.0)))>), false)
EVAL_RECOVERY(NODE_WISE_20MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(20.0*1024.0*1024.0/1280.0)))>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(((int)(20.0*1024.0*1024.0/1280.0)))>), true)

// 5 MB DRAM = only two levels in DRAM
EVAL_RECOVERY(NODE_WISE_1MB_DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(900)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_NODE_BOUND(900)>), false)
EVAL_RECOVERY(NODE_WISE_1MB_DRAM_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(900)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS + PEAR_STRAT_NODE_BOUND(900)>), true)

EVAL_RECOVERY(LEVEL_WISE_PMEM_0, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), false)
EVAL_RECOVERY(LEVEL_WISE_PMEM_0_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(0)>), true)
EVAL_RECOVERY(LEVEL_WISE_PMEM_1, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), false)
EVAL_RECOVERY(LEVEL_WISE_PMEM_1_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(1)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(1)>), true)
EVAL_RECOVERY(LEVEL_WISE_PMEM_2, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), false)
EVAL_RECOVERY(LEVEL_WISE_PMEM_2_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(2)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(2)>), true)

EVAL_RECOVERY(LEVEL_WISE_PMEM_0_NO_PARENT_PTR, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY+PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY+PEAR_STRAT_BASIC_VERSION(0)>), false)
EVAL_RECOVERY(LEVEL_WISE_PMEM_0_NO_PARENT_PTR_STATS, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY+PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(0)>), (PearRecoverer<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_DISABLE_PARENT_PTR_RECOVERY+PEAR_STRAT_RECOVERY_STATS+PEAR_STRAT_BASIC_VERSION(0)>), true)
