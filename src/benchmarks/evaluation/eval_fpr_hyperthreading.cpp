#include <benchmark/benchmark.h>

#include "fprtree.hpp"
#include "rectangle.hpp"

#include "../micro_benchmark_fixture.hpp"

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EVAL_FPR_INSERT, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_fpr_insert_benchmark(SZ_TAXI_100M, SZ_TAXI_200M + SZ_TAXI_XL_INITIAL_POINTS_AMONT, state, *this);
}
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(19)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(20)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(21)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(22)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(23)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(24)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(25)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(26)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(27)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(28)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(29)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(30)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(31)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(32)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(33)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(34)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(35)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EVAL_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(36)->Args({(SZ_TAXI_XL_INITIAL_POINTS_AMONT), ((SZ_TAXI_XL_ADDED_POINTS_AMONT))});

#define REGISTER_SCALABILITY_QUERY_BENCHMARK(NAME, BATCH_SIZE) \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(19)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(20)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(21)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(22)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(23)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(24)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(25)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(26)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(27)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(28)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(29)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(30)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(31)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(32)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(33)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(34)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(35)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});\
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, NAME)->Unit(benchmark::kMillisecond)->Threads(36)->QUERY_ITERATIONS_AND_REPETITIONS->UseRealTime()->Args({SZ_TAXI_XL_INITIAL_POINTS_AMONT+SZ_TAXI_XL_ADDED_POINTS_AMONT,BATCH_SIZE});


BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EVAL_FPR_QUERY_POINT, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_point_query_benchmark(SZ_TAXI_200M, SZ_TAXI_QUERY_SIZE, state, *this);
}
REGISTER_SCALABILITY_QUERY_BENCHMARK(EVAL_FPR_QUERY_POINT, 1)
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EVAL_FPR_QUERY_0_001, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_fpr_range_query_benchmark(SZ_TAXI_200M, micro_benchmark::get_pear_query_rects_180m_0_001(), state, *this);
}
REGISTER_SCALABILITY_QUERY_BENCHMARK(EVAL_FPR_QUERY_0_001, QUERY_001_BATCH_SIZE)