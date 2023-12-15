#include <memory>
#include <queue>

#include "catch.hpp"
#include "test_universal_helpers.hpp"
#include <macros/macros.hpp>
#include <per_tree_allocator.hpp>

#include "pear_tree.hpp"

#include "../benchmarks/micro_benchmark.hpp"



TEST_CASE("Simple test", "[BasicRTree]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<4,2, float, PEAR_STRAT_BASIC_VERSION(1024)>;
    tree_type test = tree_type(42u);
    std::set<long> values;
    for(int i = 1; i < 7; i++) {
        values.insert((long)i);
    };
    Rectangle<float> rects[7] = {
            Rectangle(2.0f,4.0f,3.0f,5.0f),
            Rectangle(4.0f,3.0f,5.0f,4.0f),
            Rectangle(5.0f,5.0f,6.0f,5.0f),
            Rectangle(3.0f,3.0f,6.0f,5.0f),
            Rectangle(1.0f,9.0f,4.0f,11.0f),
            Rectangle(5.0f,7.0f,6.0f,8.0f),
            Rectangle(5.0f,7.0f,6.0f,8.0f)
    };
    test.insert(Rectangle(2.0f,4.0f,3.0f,5.0f), 1L);
    test.insert(Rectangle(4.0f,3.0f,5.0f,4.0f), 2L);
    test.insert(Rectangle(5.0f,5.0f,6.0f,5.0f), 3L);
    test.insert(Rectangle(3.0f,3.0f,6.0f,5.0f), 4L);
    test.insert(Rectangle(1.0f,9.0f,4.0f,11.0f), 5L);
    test.insert(Rectangle(5.0f,7.0f,6.0f,8.0f), 6L);
    test.insert(Rectangle(5.0f,7.0f,6.0f,8.0f), 7L);
    test._root->check_for_values(values, rects, 7);
    REQUIRE(values.empty());
    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));

}
//
//TEST_CASE("Balanced Tree Level 1", "[2][1]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    BasicRTree<2,1,0, float> test = BasicRTree<2,1,0, float>();
//
//    test.insert(Rectangle(1.0f,1.0f,2.0f,2.0f), 1L);
//    test.insert(Rectangle(5.0f,3.0f,6.0f,4.0f), 4L);
//    test.insert(Rectangle(2.0f,1.0f,3.0f,2.0f), 2L);
//    test.insert(Rectangle(4.0f,3.0f,5.0f,4.0f), 3L);
//
//    REQUIRE(test_tree_MBR_fit(test));
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 4));
//    REQUIRE(test.test_max_level() == 1);
//}
//
//TEST_CASE("Balanced Tree 2 Level", "[4][2]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    using tree_type = BasicRTree<4,2,0, float>;
//    tree_type test = tree_type(42.0f);
//    test.insert(Rectangle(1.0f,1.0f,2.0f,2.0f), 1L);
//    test.insert(Rectangle(2.0f,1.0f,3.0f,2.0f), 2L);
//
//    test.insert(Rectangle(6.0f,2.0f,7.0f,3.0f), 5L);
//    test.insert(Rectangle(7.0f,2.0f,8.0f,3.0f), 6L);
//
//    test.insert(Rectangle(11.0f,3.0f,12.0f,4.0f), 9L);
//    test.insert(Rectangle(12.0f,3.0f,13.0f,4.0f), 10L);
//
//    test.insert(Rectangle(16.0f,4.0f,17.0f,5.0f), 13L);
//    test.insert(Rectangle(17.0f,4.0f,18.0f,5.0f), 14L);
//
//    test.insert(Rectangle(3.0f,1.0f,4.0f,2.0f), 3L);
//    test.insert(Rectangle(4.0f,1.0f,5.0f,2.0f), 4L);
//
//    test.insert(Rectangle(8.0f,2.0f,9.0f,3.0f), 7L);
//    test.insert(Rectangle(9.0f,2.0f,10.0f,3.0f), 8L);
//
//    test.insert(Rectangle(13.0f,3.0f,14.0f,4.0f), 11L);
//    test.insert(Rectangle(14.0f,3.0f,15.0f,4.0f), 12L);
//
//    test.insert(Rectangle(18.0f,4.0f,19.0f,5.0f), 15L);
//    test.insert(Rectangle(19.0f,4.0f,20.0f,5.0f), 16L);
//
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 16));
//    test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(test);
//    REQUIRE(test.test_max_level() == 1);
//}

TEST_CASE("Balanced Pear Tree 2 Level", "[4][2]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<4,2, float, PEAR_STRAT_LEVEL_BOUND((1024 * 1024))>;
    tree_type test = tree_type(42u);
    test.insert(Rectangle(1.0f,1.0f,2.0f,2.0f), 1L);
    REQUIRE(test.test_num_branches(0) == 1);
    test.insert(Rectangle(2.0f,1.0f,3.0f,2.0f), 2L);

    test.insert(Rectangle(6.0f,2.0f,7.0f,3.0f), 5L);
    test.insert(Rectangle(7.0f,2.0f,8.0f,3.0f), 6L);

    test.insert(Rectangle(11.0f,3.0f,12.0f,4.0f), 9L);
    REQUIRE(test.test_max_level() <= 1);
    test.insert(Rectangle(12.0f,3.0f,13.0f,4.0f), 10L);
    REQUIRE(test.test_max_level() <= 1);

    test.insert(Rectangle(16.0f,4.0f,17.0f,5.0f), 13L);
    test.insert(Rectangle(17.0f,4.0f,18.0f,5.0f), 14L);

    test.insert(Rectangle(3.0f,1.0f,4.0f,2.0f), 3L);
    test.insert(Rectangle(4.0f,1.0f,5.0f,2.0f), 4L);

    test.insert(Rectangle(8.0f,2.0f,9.0f,3.0f), 7L);
    test.insert(Rectangle(9.0f,2.0f,10.0f,3.0f), 8L);

    test.insert(Rectangle(13.0f,3.0f,14.0f,4.0f), 11L);
    test.insert(Rectangle(14.0f,3.0f,15.0f,4.0f), 12L);

    test.insert(Rectangle(18.0f,4.0f,19.0f,5.0f), 15L);
    test.insert(Rectangle(19.0f,4.0f,20.0f,5.0f), 16L);

    REQUIRE(test.test_max_level() >= 0);
    REQUIRE((test.test_num_branches(0) == 16));
    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));
    REQUIRE(test.test_max_level() == 1);
}

TEST_CASE("Balanced Pear Tree 3 Level", "[3][1]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<3,1, float, PEAR_STRAT_LEVEL_BOUND((1024 * 1024))>;
    tree_type test = tree_type(42u);

    for(int i = 0; i < 27; i++) {
        test.insert(Rectangle((float)(i+1+(i/3)),(float)((i/3)+1+((i/9)*5)),(float)(i+1+(i/3)+1.0f),(float)((i/3)+1+((i/9)*5)+1)), (long)(i+1));
    }

    REQUIRE(test.test_max_level() >= 1);
    REQUIRE(test.test_num_branches(0) == 27);
    REQUIRE(test.test_num_branches(1) == 9);
    REQUIRE(test.test_max_level() == 2);
    REQUIRE(test_universal_tree_MBR_fit<3,tree_type, typename tree_type::node_type,float>(test));
}

TEST_CASE("Large test pear tree", "[48][24]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<48,24, float, PEAR_STRAT_LEVEL_BOUND((1024 * 1024))>;
    tree_type test = tree_type(42u);

    auto const rects = micro_benchmark::get_rb_taxi_rects_10m();
    for (int i = 0; i <1000;i++) {
        test.insert(rects[i], (long)i);
    }

    REQUIRE(test.test_max_level() >= 1);
    REQUIRE(test.test_max_level() == 1);
    REQUIRE(test.test_num_branches(0) == 1000);
    REQUIRE(test_universal_tree_MBR_fit<48,tree_type, typename tree_type::node_type,float>(test));
}

TEST_CASE("Large test pear tree 2 levels", "[48][24]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<48,24, float, PEAR_STRAT_LEVEL_BOUND((1024 * 1024))>;
    tree_type test = tree_type(42u);

    auto const rects = micro_benchmark::get_rb_taxi_rects_10m();
    for (int i = 0; i <4000;i++) {
        test.insert(rects[i], (long)i);
    }

    REQUIRE(test.test_max_level() >= 1);
    REQUIRE(test.test_max_level() == 2);
    REQUIRE(test.test_num_branches(0) == 4000);
    REQUIRE(test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(test));
}

TEST_CASE("Large test pear tree 3 levels", "[48][24]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<48,24, float, PEAR_STRAT_BASIC_VERSION((1024))>;
    tree_type test = tree_type(42u);

    auto const rects = micro_benchmark::get_rb_taxi_rects_10m();
    for (int i = 0; i <120000;i++) {
        test.insert(rects[i], (long)i);
    }

    REQUIRE(test.test_max_level() >= 1);
    REQUIRE(test.test_max_level() == 3);
    REQUIRE(test.test_num_branches(0) == 120000);
    REQUIRE(test_universal_tree_MBR_fit<48,tree_type, typename tree_type::node_type,float>(test));
}

//TEST_CASE("Balanced Tree 3 Level", "[3][1]") {
//PearTreeAllocatorSingleton::getInstance().reset();
//    using tree_type = BasicRTree<3,1,0, float>;
//    tree_type test = tree_type();
//
//    for(int i = 0; i < 27; i++) {
//        test.insert(Rectangle((float)(i+1+(i/3)),(float)((i/3)+1+((i/9)*5)),(float)(i+1+(i/3)+1.0f),(float)((i/3)+1+((i/9)*5)+1)), (long)(i+1));
//    }
//
//    test_universal_tree_MBR_fit<tree_type::t_max_node_size,tree_type, typename tree_type::node_type,float>(test);
//    REQUIRE(test.test_num_branches(0) == 27);
//    REQUIRE(test.test_num_branches(1) == 9);
//    REQUIRE(test.test_max_level() == 2);
//}
//
//TEST_CASE("Special Tree 4/2 Level", "[4][2]") {
//PearTreeAllocatorSingleton::getInstance().reset();
//    BasicRTree<4,2,0, float> test = BasicRTree<4,2,0, float>();
//    test.insert(Rectangle(1.0f,1.0f,11.0f,11.0f), 1L);
//    test.insert(Rectangle(6.0f,1.0f,16.0f,11.0f), 2L);
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 2.0f));
//
//    test.insert(Rectangle(18.0f,1.0f,28.0f,11.0f), 3L);
//    test.insert(Rectangle(23.0f,1.0f,33.0f,11.0f), 4L);
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 4));
//
//    test.insert(Rectangle(16.0f,13.0f,17.0f,14.0f), 5L);
//    test.insert(Rectangle(16.0f,12.0f,17.0f,13.0f), 6L);
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 6));
//
//    test.insert(Rectangle(1.0f,13.0f,2.0f,14.0f), 7L);
//    test.insert(Rectangle(1.0f,12.0f,2.0f,13.0f), 8L);
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 8.0f));
//
//    test.insert(Rectangle(32.0f,13.0f,33.0f,14.0f), 9L);
//    test.insert(Rectangle(32.0f,12.0f,33.0f,13.0f), 10L);
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 10));
//
//    test.insert(Rectangle(17.0f,13.0f,18.0f,14.0f), 11L);
//    test.insert(Rectangle(17.0f,12.0f,18.0f,13.0f), 12L);
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 12.0f));
//
//    test.insert(Rectangle(2.0f,12.0f,4.0f,14.0f), 13L);
//
//    REQUIRE(test.test_max_level() >= 0);
//    REQUIRE((test.test_num_branches(0) == 13));
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE(test_tree_MBR_fit(test));
//    //TODO test structure (maybe with mbrs of leaf nodes?)
//}
//
//TEST_CASE("Special Tree 4/2 Level unoptimal", "[4][2]") {
//PearTreeAllocatorSingleton::getInstance().reset();
//    BasicRTree<4,2,0, float> test = BasicRTree<4,2,0, float>();
//    test.insert(Rectangle(1.0f,1.0f,11.0f,11.0f), 1L);
//    test.insert(Rectangle(6.0f,1.0f,16.0f,11.0f), 2L);
//
//    test.insert(Rectangle(18.0f,1.0f,28.0f,11.0f), 3L);
//    test.insert(Rectangle(23.0f,1.0f,33.0f,11.0f), 4L);
//
//    test.insert(Rectangle(15.0f,7.0f,16.0f,8.0f), 5L);
//    test.insert(Rectangle(15.0f,6.0f,16.0f,7.0f), 6L);
//    test.insert(Rectangle(15.0f,5.0f,16.0f,6.0f), 7L);
//    test.insert(Rectangle(15.0f,4.0f,16.0f,5.0f), 8L);
//
//    test.insert(Rectangle(18.0f,7.0f,19.0f,8.0f), 9L);
//    test.insert(Rectangle(18.0f,6.0f,19.0f,7.0f), 10L);
//    test.insert(Rectangle(18.0f,5.0f,19.0f,6.0f), 11L);
//    test.insert(Rectangle(18.0f,4.0f,19.0f,5.0f), 12L);
//
//    REQUIRE(test_tree_MBR_fit(test));
//    REQUIRE(test.test_max_level() >= 1);
//    REQUIRE((test.test_num_branches(0) == 12.0f));
//    REQUIRE(test.test_max_level() == 1);
//    //TODO test structure (maybe with mbrs of leaf nodes?)
//}
//
//TEST_CASE("Large test", "[49][24]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    BasicRTree<49,24,0, float> test = BasicRTree<49,24,0, float>();
//
//    auto const rects = micro_benchmark::get_rb_taxi_rects_10m();
//    for (int i = 0; i <1000;i++) {
//        test.insert(rects[i], (long)i);
//    }
//
//    REQUIRE(test_tree_MBR_fit(test));
//}