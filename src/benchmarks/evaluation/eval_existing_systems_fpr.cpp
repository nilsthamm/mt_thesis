#include <benchmark/benchmark.h>

#include "fprtree.hpp"
#include "rectangle.hpp"

#include "../micro_benchmark_fixture.hpp"



BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_INSERT, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_fpr_insert_benchmark(SZ_TAXI_100M, SZ_TAXI_100M, state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({0, 1500000});

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_POINT, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_point_query_benchmark(SZ_TAXI_100M, SZ_TAXI_QUERY_SIZE, state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_POINT)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(1500000)});

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_0_00001, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_fpr_range_query_benchmark(SZ_TAXI_100M, micro_benchmark::get_pear_query_rects_1_5m_0_00001(), state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_0_00001)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(1500000), QUERY_00001_BATCH_SIZE});

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_0_0001, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_fpr_range_query_benchmark(SZ_TAXI_100M, micro_benchmark::get_pear_query_rects_1_5m_0_0001(), state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_0_0001)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(1500000), QUERY_0001_BATCH_SIZE});

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_0_001, FPR_TREE_TYPE)(benchmark::State& state) {
    eval_fpr_range_query_benchmark(SZ_TAXI_100M, micro_benchmark::get_pear_query_rects_1_5m_0_001(), state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_FPR_QUERY_0_001)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(1500000), QUERY_001_BATCH_SIZE});
