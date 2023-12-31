#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "../micro_benchmark_fixture.hpp"

#define NO_PREFETCHING_INSERT(NAME,TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_insert_benchmark(SZ_TAXI_100M, SZ_TAXI_200M + SZ_TAXI_XL_INITIAL_POINTS_AMONT, state, *this); \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(2)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(3)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(4)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(5)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(6)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(7)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(8)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(9)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(10)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(11)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(12)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(13)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(14)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(15)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(16)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(17)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(18)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});

NO_PREFETCHING_INSERT(PMEM,(PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_ENABLE_PREFETCHING + PEAR_STRAT_BASIC_VERSION(1024)>))


#define NO_PREFETCHING_QUERY(NAME, TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, NO_PREFETCHING_QUERY_POINT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_point_query_benchmark(SZ_TAXI_200M, SZ_TAXI_QUERY_SIZE, state, *this);                     \
}                                                 \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NO_PREFETCHING_QUERY_POINT_ ## NAME)->Unit(benchmark::kMillisecond)->Threads(OPTIMAL_NUMBER_POINT_QUERY_THREADS)->QUERY_ITERATIONS_AND_REPETITIONS->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,1});\

NO_PREFETCHING_QUERY(PMEM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_ENABLE_PREFETCHING + PEAR_STRAT_BASIC_VERSION(1024)>))