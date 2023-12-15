#include <rectangle.hpp>


#include "pear_tree.hpp"

#include "catch.hpp"

TEST_CASE("Query test", "[BasicRTree]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<4,2, float, PEAR_STRAT_LEVEL_BOUND((1024 * 1024))>;
    tree_type test = tree_type(42u);
    test.insert(Rectangle(2.0f,4.0f,3.0f,5.0f), 1L);
    test.insert(Rectangle(4.0f,3.0f,5.0f,4.0f), 2L);
    test.insert(Rectangle(5.0f,5.0f,6.0f,5.0f), 3L);
    test.insert(Rectangle(4.0f,3.0f,6.0f,5.0f), 4L);
    test.insert(Rectangle(1.0f,9.0f,4.0f,11.0f), 5L);
    test.insert(Rectangle(5.0f,7.0f,6.0f,8.0f), 6L);
    test.insert(Rectangle(5.0f,7.0f,6.0f,8.0f), 7L);

    auto result_1 = std::vector<typename tree_type::node_type::branch_type const*>();
    auto result_2 = std::vector<typename tree_type::node_type::branch_type const*>();
    auto result_3 = std::vector<typename tree_type::node_type::branch_type const*>();
    test.equals_query(Rectangle(20.0f, 20.0f, 21.0f, 21.0f), result_1);
//    test.intersect_query(Rectangle(5.0f,5.0f,6.0f,8.0f), result_2);
    test.equals_query(Rectangle(2.0f, 4.0f, 3.0f, 5.0f), result_3);

    REQUIRE(result_1.size() == 0);
//    REQUIRE(result_2.size() == 4);
    REQUIRE(result_3.size() == 1);
    std::vector<typename tree_type::node_type::branch_type const*> query_candidates_2 = std::vector<typename tree_type::node_type::branch_type const*>();
    query_candidates_2.emplace_back(new typename tree_type::node_type::branch_type(3L));
    query_candidates_2.emplace_back(new typename tree_type::node_type::branch_type(4L));
    query_candidates_2.emplace_back(new typename tree_type::node_type::branch_type(6L));
    query_candidates_2.emplace_back(new typename tree_type::node_type::branch_type(7L));
    std::vector<typename tree_type::node_type::branch_type const*> query_candidates_3 = std::vector<typename tree_type::node_type::branch_type const*>();
    query_candidates_3.emplace_back(new typename tree_type::node_type::branch_type(1L));
    int values_2_correct = true;
    int values_3_correct = true;

    for(int i = 0; i< result_2.size(); i++) {
        if(std::find_if(query_candidates_2.begin(),query_candidates_2.end(), [&](typename tree_type::node_type::branch_type const* branch_b){return result_2.at(i)->get_data() == branch_b->get_data();}) == query_candidates_2.end()) {
            values_2_correct = false;
            break;
        }
    }
    REQUIRE(values_2_correct);

    for(int i = 0; i< result_3.size(); i++) {
    if(std::find_if(query_candidates_3.begin(),query_candidates_3.end(), [&](typename tree_type::node_type::branch_type const* branch_b){return result_3.at(i)->get_data() == branch_b->get_data();}) == query_candidates_3.end()) {
            values_3_correct = false;
            break;
        }
    }
    REQUIRE(values_3_correct);

}