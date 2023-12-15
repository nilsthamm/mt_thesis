#include <benchmark/benchmark.h>

#include "macros/macros.hpp"

#include "pear_tree.hpp"
#include "test_universal_helpers.hpp"
#include "micro_benchmark_fixture.hpp"
#include "micro_benchmark.hpp"

#define QUERY_BENCHMARK_SZ_TAXI(NAME,TREE_TYPE, INITIAL_DATASET, INITIAL_SIZE, SEARCH_SIZE) \
BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, BM_QUERY_SZ_TAXI_ ## NAME, argument_type<void(TREE_TYPE)>::type)(benchmark::State& state) { \
    rtree_query_benchmark(INITIAL_DATASET, state, *this);                     \
} \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, BM_QUERY_SZ_TAXI_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(INITIAL_SIZE), (SEARCH_SIZE)});         \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, BM_QUERY_SZ_TAXI_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(2)->Args({(INITIAL_SIZE), ((SEARCH_SIZE)/2)});     \
BENCHMARK_REGISTER_F(MicroBenchmarkFixture, BM_QUERY_SZ_TAXI_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(9)->Args({(INITIAL_SIZE), ((SEARCH_SIZE)/9)});


template<typename tree_type>
static void rtree_query_benchmark(micro_benchmark::rect_float_type const * const &rects, benchmark::State& state, MicroBenchmarkFixture<tree_type> &fixture) {
    int initial_size = state.range(0);
    int search_size = state.range(1);
    if(state.threads == 1 || state.thread_index == 0) {
        fixture.prepare_tree(rects, initial_size);
        int count = fixture._tree->test_num_branches(0);
//        test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(*(fixture._tree));
        if (count != initial_size) {
            std::stringstream stream;
            stream << "Not the right number of leafes: " << count << std::endl;
            stream << "Expected: " << initial_size << std::endl;
            std::cout << stream.str();
//        throw std::runtime_error(stream.str());
        }
    }
    for(auto _: state) {
        state.SetItemsProcessed((search_size*(state.threads)));
        for (int i = 0; i < search_size; i++) {
            auto result = std::vector<typename tree_type::node_type::branch_type const*>();
            fixture._tree->equals_query(rects[i + (search_size * (state.thread_index))], result);
            if(result.size() == 0) {
                std::stringstream stream;
                stream << "Empty result for index " << i+(search_size*(state.thread_index)) << " : " << to_string(rects[i+(search_size*(state.thread_index))]) << std::endl;
                std::cout << stream.str();
//        throw std::runtime_error(stream.str());
            }
//            else if(result.size() != 1) {
//                std::cout << "More than 1 result (" << result.size() << ") for index " << i+(search_size*(state.thread_index)) << " : " << to_string(rects[i+(search_size*(state.thread_index))]) << std::endl;
//            }
        }
    }
    if(state.threads == 1 || state.thread_index == 0) {
        state.SetItemsProcessed((search_size*(state.threads)));
    }
}

//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_DRAM_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_LEAF_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_1_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_2_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_3_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(3)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_1024_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(4)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_DRAM_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_LEAF_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_1_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_2_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_3_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(3)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_1024_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(4)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_DRAM_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_LEAF_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_1_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_2_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_3_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(3)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_M_1024_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(4)>), SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)

QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_DRAM_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>), SZ_TAXI_100M, 100 * 1000 * 1000, 2 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_LEAF_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_1_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_2_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_3_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(3)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_1024_OPTIMAL_NODE_SIZE, (PearTree<PEAR_OPTIMAL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(4)>), SZ_TAXI_100M, 100 * 1000 * 1000, 2 * 1000 * 1000)

//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_DRAM_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_LEAF_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_1_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_2_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_3_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(3)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_1024_SMALL_NODE_SIZE, (PearTree<PEAR_SMALL_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(4)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_DRAM_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(-1)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_LEAF_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(0)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_1_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(1)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_2_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(2)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_3_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(3)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)
//QUERY_BENCHMARK_SZ_TAXI(MAX_PMEM_LEVEL_XL_1024_MAX_NODE_SIZE, (PearTree<PEAR_MAX_NODE_SIZE, float, PEAR_STRAT_BASIC_VERSION(4)>), SZ_TAXI_100M, 100 * 1000 * 1000, 20 * 1000 * 1000)


//#define QUERY_BENCHMARK_BOOST_SZ_TAXI(NAME, MAX_NODE_SIZE, MIN_NODE_SIZE, NUM_REINSERTS, INITIAL_DATASET, INITIAL_SIZE, SEARCH_SIZE) \
//BENCHMARK_TEMPLATE_DEFINE_F(MicroBenchmarkFixture, BM_BOOST_QUERY_SZ_TAXI_ ## NAME, PearTree<62,24, float, PEAR_STRAT_NO_1(2)>)(benchmark::State& state) { \
//    boost_query_benchmark<MAX_NODE_SIZE,MIN_NODE_SIZE,NUM_REINSERTS>(INITIAL_DATASET, state, *this);                     \
//} \
//BENCHMARK_REGISTER_F(MicroBenchmarkFixture, BM_BOOST_QUERY_SZ_TAXI_ ## NAME)->Unit(benchmark::kMillisecond)->INSERT_ITERATIONS_AND_REPETITIONS->Threads(1)->Args({(INITIAL_SIZE), (SEARCH_SIZE)});
//
//
//template<size_t max_node_size,size_t min_node_size, size_t reinsert>
//static void boost_query_benchmark(micro_benchmark::rect_float_type const * const &rects, benchmark::State& state, MicroBenchmarkFixture<PearTree<62,24, float, PEAR_STRAT_NO_1(2)>> const &fixture) {
//    std::shared_ptr<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >> boost_tree;
//    int initial_size = state.range(0);
//    int search_size = state.range(1);
//    boost_tree= std::make_shared<boost::geometry::index::rtree<micro_benchmark::boost_value , boost::geometry::index::rstar<max_node_size,min_node_size,reinsert> >>();
//    fixture.prepare_boost_tree(rects, initial_size, boost_tree);
//    std::vector<std::pair<micro_benchmark::boost_rect, int64_t>> boost_rects = std::vector<std::pair<micro_benchmark::boost_rect, int64_t>>();
//    for (int i = 0; i < search_size; i++) {
//        boost_rects.push_back(std::make_pair(micro_benchmark::boost_rect(
//                micro_benchmark::boost_point(rects[i].x_min, rects[i].y_min),
//                micro_benchmark::boost_point(rects[i].x_max, rects[i].y_max)),
//                                          (int64_t) i ));
//    }
//    for(auto _: state) {
//        state.SetItemsProcessed(search_size);
//        std::vector<micro_benchmark::boost_value> result;
//        for (int i = 0; i < search_size; i++) {
//            result.clear();
//            boost_tree->query(boost::geometry::index::covered_by(boost_rects[i].first)&&boost::geometry::index::covers(boost_rects[i].first), std::back_inserter(result));
//            if(result.size() < 0) {
//                std::stringstream stream;
//                stream << "Empty result for index " << i+(search_size*(state.thread_index)) << " : " << to_string(rects[i]) << std::endl;
////                throw std::runtime_error(stream.str());
//            }
////            else if(result.size() != 1) {
////                std::cout << "More than 1 result (" << result.size() << ") for index " << i+(search_size*(state.thread_index)) << " : " << to_string(rects[i+(search_size*(state.thread_index))]) << std::endl;
////            }
//        }
//    }
//}
//
//QUERY_BENCHMARK_BOOST_SZ_TAXI(NO_REINSERT_MAX_NODE_SIZE, 62, 24, 0 , SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_BOOST_SZ_TAXI(NO_REINSERT_OPTIMAL_NODE_SIZE, 46, 16, 0, SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_BOOST_SZ_TAXI(NO_REINSERT_SMALL_NODE_SIZE, 30, 8, 0, SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//
//QUERY_BENCHMARK_BOOST_SZ_TAXI(REINSERT_5_MAX_NODE_SIZE, 62, 24, 05, SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_BOOST_SZ_TAXI(REINSERT_5_OPTIMAL_NODE_SIZE, 46, 16, 5, SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//QUERY_BENCHMARK_BOOST_SZ_TAXI(REINSERT_5_SMALL_NODE_SIZE, 30, 8, 5, SZ_TAXI_10M, 20 * 1000 * 1000, 5 * 1000 * 1000)
//
