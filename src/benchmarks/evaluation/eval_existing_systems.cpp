#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "../micro_benchmark_fixture.hpp"

#define EXSISTING_SYSTEMS_INSERT(NAME,TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_INSERT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_insert_benchmark_hyperthreading(SZ_TAXI_100M, SZ_TAXI_100M, state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_INSERT_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Args({(0), ((1500000))});

EXSISTING_SYSTEMS_INSERT(DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>))
EXSISTING_SYSTEMS_INSERT(PMEM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1024)>))


#define EXSISTING_SYSTEMS_QUERY(NAME, TREE_TYPE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_QUERY_POINT_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_point_query_benchmark(SZ_TAXI_100M, SZ_TAXI_QUERY_SIZE, state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_QUERY_POINT_ ## NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Args({(1500000), 1}); \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_QUERY_0_001_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    eval_range_query_benchmark(SZ_TAXI_100M, micro_benchmark::get_pear_query_rects_1_5m_0_001(), state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_QUERY_0_001_ ## NAME)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Args({(1500000), QUERY_001_BATCH_SIZE*1000});

EXSISTING_SYSTEMS_QUERY(DRAM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>))
EXSISTING_SYSTEMS_QUERY(PMEM, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1024)>))

template<size_t max_node_size,size_t min_node_size, size_t reinsert>
static void boost_benchmark(micro_benchmark::rect_float_type const * const initial_rects, micro_benchmark::rect_float_type const * const added_rects, benchmark::State& state, MicroBenchmarkFixture<PearTree<62,24, float, PEAR_STRAT_LEVEL_BOUND(2)>> const &fixture) {
    std::shared_ptr<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >> boost_tree;
    int initial_size = state.range(0);
    int added_size = state.range(1);
    boost_tree= std::make_shared<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >>();
    fixture.prepare_boost_tree(initial_rects, initial_size, boost_tree);
    std::vector<std::pair<micro_benchmark::boost_rect, int64_t> const *> boost_rects = std::vector<std::pair<micro_benchmark::boost_rect, int64_t> const *>();
    for (int i = 0; i < added_size; i++) {
        boost_rects.push_back(std::make_shared<std::pair<micro_benchmark::boost_rect, int64_t>>(std::make_pair(micro_benchmark::boost_rect(
                micro_benchmark::boost_point(added_rects[i].x_min, added_rects[i].y_min),
                micro_benchmark::boost_point(added_rects[i].x_max, added_rects[i].y_max)),
                                          (int64_t) i + initial_size)).get());
    }
    for(auto _: state) {
        state.SetItemsProcessed(initial_size+added_size);
        for (int i = 0; i < added_size; i++) {
            boost_tree->insert(*(boost_rects[i]));
        }
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_BOOST_INSERT, PearTree<62,24, float, PEAR_STRAT_LEVEL_BOUND(2)>)(benchmark::State& state) {
    boost_benchmark<46,16,0>(SZ_TAXI_100M, SZ_TAXI_100M, state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_BOOST_INSERT)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(0), (1500000)});


template<size_t max_node_size,size_t min_node_size, size_t reinsert>
static void boost_range_query_benchmark(micro_benchmark::rect_float_type const * const &rects, std::array<float,4> search_rects, benchmark::State& state, MicroBenchmarkFixture<PearTree<62,24, float, PEAR_STRAT_LEVEL_BOUND(2)>> const &fixture) {
    std::shared_ptr<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >> boost_tree;
    int initial_size = state.range(0);
    boost_tree= std::make_shared<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >>();
    fixture.prepare_boost_tree(rects, initial_size, boost_tree);
    size_t results = 0;
    std::vector<micro_benchmark::boost_value> result ;
    micro_benchmark::boost_rect search_rect = micro_benchmark::boost_rect(micro_benchmark::boost_point(search_rects[0],search_rects[1]),micro_benchmark::boost_point(search_rects[2],search_rects[3]));
    long const batch_size = state.range(1);
    for(auto _: state) {
        for(int i = 0; i < batch_size; i++) {
            result.clear();
            boost_tree->query(boost::geometry::index::covered_by(search_rect), std::back_inserter(result));
            results += result.size();
        }
    }
    state.SetItemsProcessed(results);
}

template<size_t max_node_size,size_t min_node_size, size_t reinsert>
static void boost_point_query_benchmark(micro_benchmark::rect_float_type const * const &rects, int search_size, benchmark::State& state, MicroBenchmarkFixture<PearTree<62,24, float, PEAR_STRAT_LEVEL_BOUND(2)>> const &fixture) {
    std::shared_ptr<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >> boost_tree;
    int initial_size = state.range(0);
    boost_tree= std::make_shared<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >>();
    fixture.prepare_boost_tree(rects, initial_size, boost_tree);
    std::vector<std::pair<micro_benchmark::boost_rect, int64_t>> boost_rects = std::vector<std::pair<micro_benchmark::boost_rect, int64_t>>();
    for (int i = 0; i < search_size; i++) {
        boost_rects.push_back(std::make_pair(micro_benchmark::boost_rect(
                micro_benchmark::boost_point(rects[i].x_min, rects[i].y_min),
                micro_benchmark::boost_point(rects[i].x_max, rects[i].y_max)),
                                          (int64_t) i ));
    }

    size_t results = 0;
    std::vector<micro_benchmark::boost_value> result ;
    long const batch_size = state.range(1);
    for(auto _: state) {
        for(int i = 0; i < search_size; i++) {
            result.clear();
            boost_tree->query(boost::geometry::index::covered_by(boost_rects[i].first) &&
                              boost::geometry::index::covers(boost_rects[i].first), std::back_inserter(result));
            results += result.size();
        }
    }
    state.SetItemsProcessed(state.threads*search_size*state.iterations());
}

BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_BOOST_QUERY_POINT, PearTree<62,24, float, PEAR_STRAT_LEVEL_BOUND(2)>)(benchmark::State& state) {
    boost_point_query_benchmark<PEAR_OPTIMAL_NODE_SIZE, 0>(SZ_TAXI_100M, SZ_TAXI_QUERY_SIZE,state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_BOOST_QUERY_POINT)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Args({(1500000),1});


BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_BOOST_QUERY_0_001, PearTree<62,24, float, PEAR_STRAT_LEVEL_BOUND(2)>)(benchmark::State& state) {
    boost_range_query_benchmark<PEAR_OPTIMAL_NODE_SIZE, 0>(SZ_TAXI_100M, {-73.9841f, 40.7429f, -73.9816f, 40.7466f},
                                                           state, *this);
}
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, EXSISTING_SYSTEMS_BOOST_QUERY_0_001)->Unit(benchmark::kMillisecond)->QUERY_ITERATIONS_AND_REPETITIONS->Args({(1500000),QUERY_001_BATCH_SIZE*1000});

