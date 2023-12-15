#include <thread>
#include <iostream>
#include <chrono>
#include <ctime>
#include <random>

#include "micro_benchmark.hpp"
#include "pear_tree.hpp"


//#define PEAR_TREE_TYPE PearTree<46,16, float, PEAR_STRAT_QUERY_STATS+PEAR_STRAT_BASIC_VERSION(-1)>
#define PEAR_TREE_TYPE PearTree<38,12, float, PEAR_STRAT_BASIC_VERSION(0)>

void insert_task(PEAR_TREE_TYPE* tree, Rectangle<float>* const rects, int const amount, int const offset, long const value_offset) {
    for (int i = offset; i < amount+offset; i++) {
        tree->insert(rects[i], (long) i+value_offset);
    }
}

void query_task(PEAR_TREE_TYPE* tree, Rectangle<float>* const rects, std::shared_ptr<std::vector<int>> indexes, int const amount, int const offset) {
    using branch_type = Branch<void>;
    for (int i = offset; i < amount+offset; i++) {
        auto result = std::vector<size_t>();
        tree->equals_query(rects[indexes->at(i)], result);
        bool found = false;
        for(auto const res : result) {
            if(res == i) {
                found = true;
            }
        }
        if(found) {
            std::stringstream stream;
            stream << "Empty result for index " << i << " : " << to_string(rects[indexes->at(i)]) << std::endl;
            throw std::runtime_error(stream.str());
            std::cerr << stream.str();
        }
    }
}

int main(int argc, char *argv[]) {
    using branch_type = Branch<void>;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for(int i : {18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35}) {
        CPU_SET(i, &cpuset);
    }

    Rectangle<float>* rects = micro_benchmark::get_sz_taxi_rects_200m();
    std::shared_ptr<std::vector<int>> indexes = std::make_shared<std::vector<int>>();
    PEAR_TREE_TYPE* tree = new PEAR_TREE_TYPE(0);

    std::vector<std::thread> insert_threads = std::vector<std::thread>();
    int num_insert_threads = 18;
    int inserts = 200*1000*1000;
    for(int i = 0; i < num_insert_threads; i++) {
        insert_threads.emplace_back(insert_task, tree, rects, inserts / num_insert_threads, (inserts / num_insert_threads) * i, (inserts/num_insert_threads)*i);
        int rc = pthread_setaffinity_np(insert_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i <inserts; i++) {
        indexes->push_back(i);
    }
//    auto rng = std::default_random_engine {};
//    std::shuffle(std::begin(*indexes), std::end(*indexes), rng);
    for(int i = 0; i < insert_threads.size(); i++) {
        insert_threads[i].join();
    }

//    if(tree->test_num_branches(0) != 10000000) {
//        std::cerr << "Error initial insert, actually inserted: " << tree->test_num_branches(0) << "\n";
//    } else {
//        std::cout << "Initial insert successful (number leaves)" << std::endl;
//    }
//    int count_0 = tree->test_num_branches(0);
//    int count_1 = tree->test_num_branches(1);
//    int count_2 = tree->test_num_branches(2);
//    int count_3 = tree->test_num_branches(3);
//    int count_4 = tree->test_num_branches(4);
//    int total_count = tree->get_dram_node_count();
////    int count_5 = tree->test_num_branches(5);
////
//    std::cout << "Branches on level 0/Nodes on level -1: " << count_0 << std::endl;
//    std::cout << "Branches on level 1/Nodes on level 0: " << count_1 << std::endl;
//    std::cout << "Branches on level 2/Nodes on level 1: " << count_2 << std::endl;
//    std::cout << "Branches on level 3/Nodes on level 2: " << count_3 << std::endl;
//    std::cout << "Branches on level 4/Nodes on level 3: " << count_4 << std::endl;
//    std::cout << "Total node count: " << total_count << std::endl;
//    std::cout << "Branches on level 5/Nodes on level 4: " << count_5 << std::endl;
//    tree->print_stats();
    std::vector<std::thread> threads = std::vector<std::thread>();
    int num_threads = 9;
    int total_searches = 2000000;
    for(int i = 0; i < num_threads; i++) {
        threads.emplace_back(query_task, tree, rects, indexes, (total_searches/20) / num_threads, ((total_searches/20) / num_threads) * i);
        int rc = pthread_setaffinity_np(threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    threads.clear();
    for(int i = 0; i < num_threads; i++) {
        threads.emplace_back(query_task, tree, rects, indexes, total_searches / num_threads, (total_searches / num_threads) * i);
        int rc = pthread_setaffinity_np(threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    for(int i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
//    tree->contains_query(Rectangle<float>(-73.9840698,40.7429008,-73.9835698,40.7434008), result);
//    auto result = std::vector<size_t>();
//    std::vector<std::pair<std::pair<float,float>,std::pair<float,float>>> points1k = std::vector<std::pair<std::pair<float,float>,std::pair<float,float>>>();
//    std::vector<std::pair<std::pair<float,float>,std::pair<float,float>>> points10k = std::vector<std::pair<std::pair<float,float>,std::pair<float,float>>>();
//    std::vector<std::pair<std::pair<float,float>,std::pair<float,float>>> points100k = std::vector<std::pair<std::pair<float,float>,std::pair<float,float>>>();
//    std::vector<std::pair<float,float>> start_points = {
//            {-74.02,40.70},
//            {-74.01,40.70},
//            {-74.0,40.72},
//            {-74.0,40.73},
//            {-74.0,40.74},
//            {-74.0,40.75},
//            {-73.99,40.66},
//            {-73.99,40.68},
//            {-73.99,40.66},
//            {-73.99,40.68},
//            {-73.99,40.71},
//            {-73.99,40.72},
//            {-73.99,40.73},
//            {-73.99,40.74},
//            {-73.99,40.75},
//            {-73.99,40.76},
//            {-73.98,40.71},
//            {-73.98,40.72},
//            {-73.98,40.73},
//            {-73.98,40.74},
//            {-73.98,40.75},
//            {-73.98,40.76},
//            {-73.97,40.72},
//            {-73.97,40.73},
//            {-73.97,40.76},
//            {-73.97,40.77},
//            {-73.96,40.74},
//            {-73.96,40.75},
//            {-73.96,40.76},
//            {-73.96,40.78},
//            {-73.96,40.79},
//            {-73.95,40.75},
//            {-73.95,40.76},
//            {-73.95,40.77},
//            {-73.95,40.79}
//            };
//    for(auto const point : start_points) {
//        std::cout << std::endl << "Finding query rects for: (" << point.first << ", " << point.second << ")" << std::endl;
//        bool found_100k = false;
//        bool found_10k = false;
//        bool found_1k = false;
//        std::pair<float,float> max_point_100k ;
//        int distance_to_100k = 90000;
//        std::pair<float,float> max_point_10k ;
//        int distance_to_10k = 9000;
//        std::pair<float,float> max_point_1k ;
//        int distance_to_1k = 900;
//        std::pair<float,float> current_max_point = point;
//        int last_count = 0;
//        while(last_count < 270000) {
//            result.clear();
//            tree->contains_query(Rectangle<float>(point.first,point.second,current_max_point.first,current_max_point.second), result);
//            int current_count = result.size();
//            if(std::abs(1800-current_count) < distance_to_1k) {
//                found_1k = true;
//                max_point_1k = current_max_point;
//                distance_to_1k = std::abs(1800-current_count);
//            } else if(std::abs(18000-current_count) < distance_to_10k) {
//                found_10k = true;
//                max_point_10k = current_max_point;
//                distance_to_10k = std::abs(18000-current_count);
//            } else if(std::abs(180000-current_count) < distance_to_100k) {
//                found_100k = true;
//                max_point_100k = current_max_point;
//                distance_to_100k = std::abs(180000-current_count);
//            }
//            last_count = current_count;
//            current_max_point = {current_max_point.first+0.00001f, current_max_point.second+0.00001f};
//        }
//        std::cout << std::endl;
//        if(found_1k) {
//            points1k.push_back({point,max_point_1k});
//            std::cout << "Point for 1k: (" << max_point_1k.first << ", " << max_point_1k.second << ")" << std::endl;
//            result.clear();
//            auto start = std::chrono::system_clock::now();
//            tree->contains_query(Rectangle<float>(point.first,point.second,max_point_1k.first,max_point_1k.second), result);
//            auto end = std::chrono::system_clock::now();
//            std::cout << result.size() <<  std::endl;
//            std::chrono::duration<double> elapsed_seconds = end-start;
//            std::time_t end_time = std::chrono::system_clock::to_time_t(end);
//            std::cout << "finished computation at " << std::ctime(&end_time)
//                      << "elapsed time: " << elapsed_seconds.count() << "s\n";
//        } else {
//            std::cout << "Point for 1k not found" << std::endl;
//        }
//        std::cout << std::endl;
//        if(found_10k) {
//            points10k.push_back({point,max_point_10k});
//            std::cout << "Point for 10k: (" << max_point_10k.first << ", " << max_point_10k.second << ")" << std::endl;
//            result.clear();
//            auto start = std::chrono::system_clock::now();
//            tree->contains_query(Rectangle<float>(point.first,point.second,max_point_10k.first,max_point_10k.second), result);
//            auto end = std::chrono::system_clock::now();
//            std::cout << result.size() <<  std::endl;
//            std::chrono::duration<double> elapsed_seconds = end-start;
//            std::time_t end_time = std::chrono::system_clock::to_time_t(end);
//            std::cout << "finished computation at " << std::ctime(&end_time)
//                      << "elapsed time: " << elapsed_seconds.count() << "s\n";
//        } else {
//            std::cout << "Point for 10k not found" << std::endl;
//        }
//        std::cout << std::endl;
//        if(found_100k) {
//            points100k.push_back({point,max_point_100k});
//            std::cout << "Point for 100k: (" << max_point_100k.first << ", " << max_point_100k.second << ")" << std::endl;
//            result.clear();
//            auto start = std::chrono::system_clock::now();
//            tree->contains_query(Rectangle<float>(point.first,point.second,max_point_100k.first,max_point_100k.second), result);
//            auto end = std::chrono::system_clock::now();
//            std::cout << result.size() <<  std::endl;
//            std::chrono::duration<double> elapsed_seconds = end-start;
//            std::time_t end_time = std::chrono::system_clock::to_time_t(end);
//            std::cout << "finished computation at " << std::ctime(&end_time)
//                      << "elapsed time: " << elapsed_seconds.count() << "s\n";
//        } else {
//            std::cout << "Point for 100k not found" << std::endl;
//        }
//    }
//
//    std::cout << std::endl << "1k array:" << std::endl;
//    std::cout << "{";
//    std::cout << "{ " << points1k[0].first.first << ", " << points1k[0].first.second << ", " << points1k[0].second.first << ", " << points1k[0].second.second << "}";
//    for(int i = 1; i < points1k.size(); i++) {
//        std::cout << "," << std::endl << "{ " << points1k[i].first.first << ", " << points1k[i].first.second << ", " << points1k[i].second.first << ", " << points1k[i].second.second << "}";
//    }
//    std::cout << "}";

//    for(auto const point_pair: points1k) {
//        std::cout << "result->emplace_back(" << point_pair.first.first << ", " << point_pair.first.second << ", " << point_pair.second.first << ", " << point_pair.second.second << ");"<< std::endl;
//    }
//
//    std::cout << std::endl << "10k array:" << std::endl;
//    for(auto const point_pair: points10k) {
//        std::cout << "result->emplace_back(" << point_pair.first.first << ", " << point_pair.first.second << ", " << point_pair.second.first << ", " << point_pair.second.second << ");"<< std::endl;
//    }
//
//    std::cout << std::endl << "100k array:" << std::endl;
//    for(auto const point_pair: points100k) {
//        std::cout << "result->emplace_back(" << point_pair.first.first << ", " << point_pair.first.second << ", " << point_pair.second.first << ", " << point_pair.second.second << ");"<< std::endl;
//    }

    return 0;
}