#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "../micro_benchmark_fixture.hpp"


#define LOCKING_INSERT(NAME,TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, LOCKING_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_insert_benchmark(SZ_TAXI_100M, SZ_TAXI_200M + SZ_TAXI_XL_INITIAL_POINTS_AMONT, state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, LOCKING_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(OPTIMAL_NUMBER_INSERT_THREADS)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});

LOCKING_INSERT(PMEM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE+PEAR_STRAT_BASIC_VERSION(1024)>))
LOCKING_INSERT(LEAF_PMEM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE+PEAR_STRAT_BASIC_VERSION(0)>))
LOCKING_INSERT(DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_OLD_MBR_UPDATE_ROUTINE+PEAR_STRAT_BASIC_VERSION(-1)>))

