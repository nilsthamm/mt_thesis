#include <rectangle.hpp>
#include <pear_tree.hpp>

#include "test_universal_helpers.hpp"
#include "catch.hpp"

TEST_CASE("Delete test PMEM", "[Basic 2 1 Tree]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<2,1, float, PEAR_STRAT_BASIC_VERSION(1024)>;
    tree_type test = tree_type(0);
    test.insert(Rectangle(2.0f,4.0f,3.0f,5.0f), 1L);
    test.insert(Rectangle(4.0f,3.0f,5.0f,4.0f), 2L);
    test.insert(Rectangle(5.0f,5.0f,6.0f,5.0f), 3L);
    test.insert(Rectangle(4.0f,3.0f,6.0f,5.0f), 4L);
    test.insert(Rectangle(1.0f,9.0f,4.0f,11.0f), 5L);
    test.insert(Rectangle(5.0f,7.0f,6.0f,8.0f), 6L);
    test.insert(Rectangle(5.0f,7.0f,6.0f,8.0f), 7L);


    auto value_set = std::set<long>({1});
    test._root->check_for_values(value_set);

    REQUIRE(value_set.size() == 0);

    test.remove(Rectangle(2.0f,4.0f,3.0f,5.0f), 1L);

    REQUIRE(test_universal_tree_MBR_fit<2,tree_type, typename tree_type::node_type,float>(test));
    std::set<long> values = std::set<long>();
    for(long i = 2l; i < 8L; i++) {
        values.insert(i);
    }
    test._root->check_for_values(values);
    REQUIRE(values.empty());
}

TEST_CASE("Delete test DRAM", "[Basic 2 1 Tree]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<2,1, float, PEAR_STRAT_BASIC_VERSION(-1)>;
    tree_type test = tree_type(0);
    test.insert(Rectangle(0.0f,0.0f, 1.0f,1.0f), 1L);
    test.insert(Rectangle(0.0f,1.0f, 1.0f,2.0f), 2L);
    test.insert(Rectangle(1.0f,3.0f, 2.0f,3.0f), 3L);
    test.insert(Rectangle(1.0f,4.0f, 2.0f,4.0f), 4L);
    test.insert(Rectangle(1.0f,5.0f, 2.0f,5.0f), 5L);
    test.insert(Rectangle(2.0f,6.0f, 3.0f,6.0f), 6L);
    test.insert(Rectangle(2.0f,7.0f, 3.0f,7.0f), 7L);
    REQUIRE((test.test_num_branches(0) == 7));

    test.remove(Rectangle(0.0f,0.0f, 1.0f,1.0f), 1L);
    REQUIRE((test.test_num_branches(0) == 6));

    REQUIRE(test_universal_tree_MBR_fit<2,tree_type, typename tree_type::node_type,float>(test));
    std::set<long> values = std::set<long>();
    for(long i = 1l; i < 8L; i++) {
        values.insert(i);
    }
    test._root->check_for_values(values);
    REQUIRE(values.size() == 1);
    REQUIRE(*(values.begin()) == 1L);
}

//TEST_CASE("Single merge DRAM test", "[Basic 4 2 DRAM]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    using tree_type = PearTree<4,2, float, PEAR_STRAT_BASIC_VERSION(-1)>;
//    tree_type test = tree_type(0);
//    test.insert(Rectangle(0.0f,7.0f, 1.0f,8.0f), 1L);
//    test.insert(Rectangle(0.0f,8.0f, 1.0f,9.0f), 2L);
//    test.insert(Rectangle(0.0f, 9.0f, 1.0f, 10.0f), 3L);
//    test.insert(Rectangle(0.0f, 0.0f, 1.0f, 1.0f), 4L);
//    test.insert(Rectangle(0.0f, 1.0f, 1.0f, 2.0f), 5L);
//    test.insert(Rectangle(0.0f, 5.0f, 1.0f, 6.0f), 6L);
//    test.insert(Rectangle(0.0f, 4.0f, 1.0f, 5.0f), 7L);
//    test.insert(Rectangle(0.0f, 17.0f, 1.0f, 18.0f), 8L);
//    test.insert(Rectangle(0.0f, 18.0f, 1.0f, 19.0f), 9L);
//    test.insert(Rectangle(0.0f, 19.0f, 1.0f, 20.0f), 10L);
//    test.insert(Rectangle(0.0f, 14.0f, 1.0f, 16.0f), 11L);
//    test.insert(Rectangle(0.0f, 15.0f, 1.0f, 16.0f), 12L);
//
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE((test.test_num_branches(2) == 2));
//    REQUIRE((test.test_num_branches(1) == 5));
//    REQUIRE((test.test_num_branches(0) == 12));
//
//    test.remove(Rectangle(0.0f, 0.0f, 1.0f, 1.0f), 4L);
//
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE((test.test_num_branches(1) == 4));
//    REQUIRE((test.test_num_branches(0) == 11));
//    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));
//    std::set<long> values = std::set<long>();
//    for(long i = 1l; i < 13L; i++) {
//        if(i == 11L) continue;
//        values.insert(i);
//    }
//    test._root->check_for_values(values);
//    REQUIRE(values.empty());
//}

TEST_CASE("Single merge PMEM test", "[Basic 4 2 PMEM]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<4,2, float, PEAR_STRAT_BASIC_VERSION(1024)>;
    tree_type test = tree_type(0);
    test.insert(Rectangle(0.0f,0.0f, 1.0f,1.0f), 1L);
    test.insert(Rectangle(0.0f,1.0f, 1.0f,2.0f), 2L);
    test.insert(Rectangle(1.0f,3.0f, 2.0f,3.0f), 3L);
    test.insert(Rectangle(1.0f,4.0f, 2.0f,4.0f), 4L);
    test.insert(Rectangle(1.0f,5.0f, 2.0f,5.0f), 5L);
    test.insert(Rectangle(2.0f,6.0f, 3.0f,6.0f), 6L);
    test.insert(Rectangle(2.0f,7.0f, 3.0f,7.0f), 7L);

    REQUIRE(test.test_max_level() == 1);
    REQUIRE((test.test_num_branches(1) == 3));
    REQUIRE((test.test_num_branches(0) == 7));

    test.remove(Rectangle(1.0f,3.0f, 2.0f,3.0f), 3L);
    test.remove(Rectangle(1.0f,4.0f, 2.0f,4.0f), 4L);

    std::set<long> values = std::set<long>();
    for(long i = 1l; i < 8L; i++) {
        if(i == 3L || i == 4L) continue;
        values.insert(i);
    }
    test._root->check_for_values(values);
    REQUIRE(test.test_max_level() == 1);
    REQUIRE((test.test_num_branches(1) == 2));
    REQUIRE((test.test_num_branches(0) == 5));
    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));
    REQUIRE(values.empty());
}

//TEST_CASE("Single merge DRAM/PMEM test", "[Basic 4 2 DRAM/PMEM]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    using tree_type = PearTree<4,2, float, PEAR_STRAT_BASIC_VERSION(0)>;
//    tree_type test = tree_type(0);
//    test.insert(Rectangle(0.0f,0.0f, 1.0f,1.0f), 1L);
//    test.insert(Rectangle(0.0f,1.0f, 1.0f,2.0f), 2L);
//    test.insert(Rectangle(1.0f,3.0f, 2.0f,3.0f), 3L);
//    test.insert(Rectangle(1.0f,4.0f, 2.0f,4.0f), 4L);
//    test.insert(Rectangle(1.0f,5.0f, 2.0f,5.0f), 5L);
//    test.insert(Rectangle(2.0f,6.0f, 3.0f,6.0f), 6L);
//    test.insert(Rectangle(2.0f,7.0f, 3.0f,7.0f), 7L);
//
//    REQUIRE(test.test_max_level() == 1);
//    REQUIRE((test.test_num_branches(1) == 3));
//    REQUIRE((test.test_num_branches(0) == 7));
//
//    test.remove(Rectangle(1.0f,3.0f, 2.0f,3.0f), 3L);
//    test.remove(Rectangle(1.0f,4.0f, 2.0f,4.0f), 4L);
//
//    REQUIRE(test.test_max_level() == 1);
//    REQUIRE((test.test_num_branches(1) == 2));
//    REQUIRE((test.test_num_branches(0) == 5));
//    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));
//    std::set<long> values = std::set<long>();
//    for(long i = 1l; i < 8L; i++) {
//        if(i == 3L || i == 4L) continue;
//        values.insert(i);
//    }
//    test._root->check_for_values(values);
//    REQUIRE(values.empty());
//}

//TEST_CASE("Double merge DRAM test", "[Basic 4 2 DRAM]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    using tree_type = PearTree<4, 2, float, PEAR_STRAT_BASIC_VERSION(1024)>;
//    tree_type test = tree_type(0);
//    test.insert(Rectangle(0.0f,7.0f, 1.0f,8.0f), 1L);
//    test.insert(Rectangle(0.0f,8.0f, 1.0f,9.0f), 2L);
//    test.insert(Rectangle(0.0f, 9.0f, 1.0f, 10.0f), 3L);
//    test.insert(Rectangle(0.0f, 0.0f, 1.0f, 1.0f), 4L);
//    test.insert(Rectangle(0.0f, 1.0f, 1.0f, 2.0f), 5L);
//    test.insert(Rectangle(0.0f, 5.0f, 1.0f, 6.0f), 6L);
//    test.insert(Rectangle(0.0f, 4.0f, 1.0f, 5.0f), 7L);
//    test.insert(Rectangle(0.0f, 17.0f, 1.0f, 18.0f), 8L);
//    test.insert(Rectangle(0.0f, 18.0f, 1.0f, 19.0f), 9L);
//    test.insert(Rectangle(0.0f, 19.0f, 1.0f, 20.0f), 10L);
//    test.insert(Rectangle(0.0f, 14.0f, 1.0f, 16.0f), 11L);
//    test.insert(Rectangle(0.0f, 15.0f, 1.0f, 16.0f), 12L);
//
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE((test.test_num_branches(2) == 2));
//    REQUIRE((test.test_num_branches(1) == 5));
//    REQUIRE((test.test_num_branches(0) == 12));
//
//    test.remove(Rectangle(0.0f, 14.0f, 1.0f, 16.0f), 11L);
//
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE((test.test_num_branches(1) == 4));
//    REQUIRE((test.test_num_branches(0) == 11));
//    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));
//    std::set<long> values = std::set<long>();
//    for(long i = 1l; i < 13L; i++) {
//        if(i == 11L) continue;
//        values.insert(i);
//    }
//    test._root->check_for_values(values);
//    REQUIRE(values.empty());
//}
//
//TEST_CASE("Double merge PMEM test", "[Basic 4 2 PMEM]") {
//    PearTreeAllocatorSingleton::getInstance().reset();
//    using tree_type = PearTree<4, 2, float, PEAR_STRAT_BASIC_VERSION(1024)>;
//    tree_type test = tree_type(0);
//    test.insert(Rectangle(0.0f,7.0f, 1.0f,8.0f), 1L);
//    test.insert(Rectangle(0.0f,8.0f, 1.0f,9.0f), 2L);
//    test.insert(Rectangle(0.0f, 9.0f, 1.0f, 10.0f), 3L);
//    test.insert(Rectangle(2.0f, 0.0f, 3.0f, 1.0f), 4L);
//    test.insert(Rectangle(2.0f, 1.0f, 3.0f, 2.0f), 5L);
//    test.insert(Rectangle(0.0f, 4.0f, 1.0f, 5.0f), 6L);
//    test.insert(Rectangle(0.0f, 5.0f, 1.0f, 6.0f), 7L);
//    test.insert(Rectangle(0.0f, 17.0f, 1.0f, 18.0f), 8L);
//    test.insert(Rectangle(0.0f, 18.0f, 1.0f, 19.0f), 9L);
//    test.insert(Rectangle(0.0f, 19.0f, 1.0f, 20.0f), 10L);
//    test.insert(Rectangle(0.0f, 14.0f, 1.0f, 16.0f), 11L);
//    test.insert(Rectangle(0.0f, 15.0f, 1.0f, 16.0f), 12L);
//
//    REQUIRE((test.test_num_branches(1) == 5));
//    REQUIRE((test.test_num_branches(0) == 12));
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE((test.test_num_branches(2) == 2));
//
//    test.remove(Rectangle(0.0f, 14.0f, 1.0f, 16.0f), 11L);
//
//    REQUIRE(test.test_max_level() == 2);
//    REQUIRE((test.test_num_branches(1) == 4));
//    REQUIRE((test.test_num_branches(0) == 11));
//    REQUIRE(test_universal_tree_MBR_fit<4,tree_type, typename tree_type::node_type,float>(test));
//    std::set<long> values = std::set<long>();
//    for(long i = 1l; i < 13L; i++) {
//        if(i == 11L) continue;
//        values.insert(i);
//    }
//    test._root->check_for_values(values);
//    REQUIRE(values.empty());
//}