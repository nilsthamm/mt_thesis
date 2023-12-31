#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "../micro_benchmark_fixture.hpp"

#define PERSISTING_INSERT(NAME,TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, PERSISTING_INSERT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_insert_benchmark_hyperthreading(SZ_TAXI_100M, SZ_TAXI_200M + SZ_TAXI_XL_INITIAL_POINTS_AMONT, state, *this);  \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, PERSISTING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), OPTIMAL_NUMBER_INSERT_THREADS});



PERSISTING_INSERT(290_MB_DRAM_BATCHING_1, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,1)>))
PERSISTING_INSERT(290_MB_DRAM_BATCHING_10, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,10)>))
PERSISTING_INSERT(290_MB_DRAM_BATCHING_50, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,50)>))
PERSISTING_INSERT(290_MB_DRAM_BATCHING_100, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,100)>))
PERSISTING_INSERT(290_MB_DRAM_BATCHING_500, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,500)>))
PERSISTING_INSERT(290_MB_DRAM_BATCHING_1000, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(240100,1000)>))

PERSISTING_INSERT(290_MB_DRAM_BACKGROUND_BATCHING_1, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_BACKGROUND_PERSISTING + PEAR_STRAT_NODE_BOUND_BATCHING(240100,1)>))
PERSISTING_INSERT(290_MB_DRAM_BACKGROUND_BATCHING_10, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_BACKGROUND_PERSISTING + PEAR_STRAT_NODE_BOUND_BATCHING(240100,10)>))
PERSISTING_INSERT(290_MB_DRAM_BACKGROUND_BATCHING_50, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_BACKGROUND_PERSISTING + PEAR_STRAT_NODE_BOUND_BATCHING(240100,50)>))
PERSISTING_INSERT(290_MB_DRAM_BACKGROUND_BATCHING_100, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_BACKGROUND_PERSISTING + PEAR_STRAT_NODE_BOUND_BATCHING(240100,100)>))
PERSISTING_INSERT(290_MB_DRAM_BACKGROUND_BATCHING_500, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_BACKGROUND_PERSISTING + PEAR_STRAT_NODE_BOUND_BATCHING(240100,500)>))
PERSISTING_INSERT(290_MB_DRAM_BACKGROUND_BATCHING_1000, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_BACKGROUND_PERSISTING + PEAR_STRAT_NODE_BOUND_BATCHING(240100,1000)>))




