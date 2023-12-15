#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "../micro_benchmark_fixture.hpp"

#define SCALABILITY_INSERT(NAME,TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_insert_benchmark_hyperthreading(SZ_TAXI_100M, SZ_TAXI_200M + SZ_TAXI_XL_INITIAL_POINTS_AMONT, state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), 1});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), 2});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), 4});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), 8});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), 16});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, SCALABILITY_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), 18});


SCALABILITY_INSERT(PMEM,(PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1024)>))
SCALABILITY_INSERT(LEAF_PMEM,(PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(3000000,1)>))

// Query: For X Threads query X rects of the same selectivty which are not overlapping


#define REGISTER_SCALABILITY_QUERY_BENCHMARK(NAME, BATCH_SIZE) \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE, 1});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE, 2});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE, 4});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE, 8});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE, 16});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE, 18});

#define SCALABILITY_QUERY(NAME, TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, SCALABILITY_QUERY_POINT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_point_query_benchmark_thread_binding(SZ_TAXI_200M, SZ_TAXI_QUERY_SIZE, state, *this);                     \
}                                                 \
REGISTER_SCALABILITY_QUERY_BENCHMARK(SCALABILITY_QUERY_POINT_ ## NAME, 0)\
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, SCALABILITY_QUERY_0_001_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_range_query_benchmark_thread_binding(SZ_TAXI_200M, micro_benchmark::get_pear_query_rects_180m_0_001(), state, *this);                     \
}                                                 \
REGISTER_SCALABILITY_QUERY_BENCHMARK(SCALABILITY_QUERY_0_001_ ## NAME, QUERY_001_BATCH_SIZE)

SCALABILITY_QUERY(PMEM,(PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1024)>))
SCALABILITY_QUERY(LEAF_PMEM,(PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_PARTIAL_PERSISTER_STATS+PEAR_STRAT_NODE_BOUND_BATCHING(3000000,1)>))
