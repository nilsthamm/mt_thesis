#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "../micro_benchmark_fixture.hpp"
#include "../micro_benchmark.hpp"

#define CPU_CACHE_ALIGNED_INSERT(NAME,TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, CPU_CACHE_ALIGNED_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_insert_benchmark_hyperthreading(SZ_TAXI_100M, SZ_TAXI_200M + SZ_TAXI_XL_INITIAL_POINTS_AMONT, state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, CPU_CACHE_ALIGNED_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT)), OPTIMAL_NUMBER_INSERT_THREADS});

CPU_CACHE_ALIGNED_INSERT(CPU_CACHE_ALIGNED, (PearTree<38, 12, float, PEAR_STRAT_BASIC_VERSION(1024)>))



#define CPU_CACHE_ALIGNED_QUERY(NAME, TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, CPU_CACHE_ALIGNED_QUERY_POINT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_point_query_benchmark_thread_binding(SZ_TAXI_200M, SZ_TAXI_QUERY_SIZE, state, *this);                     \
}                                                 \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, CPU_CACHE_ALIGNED_QUERY_POINT_ ## NAME)->Unit(benchmark::kMillisecond)->UseRealTime()->QUERY_ITERATIONS_AND_REPETITIONS->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,1,OPTIMAL_NUMBER_POINT_QUERY_THREADS});\
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, CPU_CACHE_ALIGNED_QUERY_RANGE_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_range_query_benchmark_thread_binding(SZ_TAXI_200M, micro_benchmark::get_pear_query_rects_180m_0_001(), state, *this);                     \
}                                                 \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, CPU_CACHE_ALIGNED_QUERY_RANGE_ ## NAME)->Unit(benchmark::kMillisecond)->UseRealTime()->QUERY_ITERATIONS_AND_REPETITIONS->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,1,OPTIMAL_NUMBER_POINT_QUERY_THREADS});


CPU_CACHE_ALIGNED_QUERY(CPU_CACHE_ALIGNED, (PearTree<38, 12, float, PEAR_STRAT_BASIC_VERSION(1024)>))