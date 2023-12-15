#include <rectangle.hpp>
#include <pear_recoverer.hpp>

#include "test_universal_helpers.hpp"
#include "catch.hpp"

//TEST_CASE("Simple recovery test", "[BasicRTree]") {
//    using branch_type = typename BasicNode<3,1,0, float>::branch_void_type;
//    PearTreeAllocatorSingleton::getInstance().reset();
//    PearTreeAllocatorSingleton::getInstance().persist_meta_data_for_recovery();
//    BasicRTree<3,1,0, float> test = BasicRTree<3,1,0, float>();
//    std::vector<branch_type const*> query_candidates = std::vector<branch_type const*>();
//    std::vector<Rectangle<float>> rect_candidates = std::vector<Rectangle<float>>();
////
//    for(int i = 0; i < 27; i++) {
//        query_candidates.emplace_back(new branch_type((long)(i+1)));
//        rect_candidates.emplace_back((float)(i+1+(i/3)),(float)((i/3)+1+((i/9)*5)),(float)(i+1+(i/3)+1.0f),(float)((i/3)+1+((i/9)*5)+1));
//        test.insert(Rectangle((float)(i+1+(i/3)),(float)((i/3)+1+((i/9)*5)),(float)(i+1+(i/3)+1.0f),(float)((i/3)+1+((i/9)*5)+1)), (long)(i+1));
//    }
//    PearTreeAllocatorSingleton::getInstance().restart();
//
//    auto recoverer = BasicRTreeRecoverer<3,1,0,float>();
//    BasicRTree<3,1,0, float>* recovered_tree = recoverer.recover_tree_object();
//    REQUIRE(test_tree_MBR_fit(*recovered_tree));
//    REQUIRE(recovered_tree->test_max_level() >= 1);
//    REQUIRE(recovered_tree->test_num_branches(0) == 27);
//    REQUIRE(recovered_tree->test_num_branches(1) == 9);
//    REQUIRE(recovered_tree->test_max_level() == 2);
//    bool values_correct = true;
//    for(int i = 0; i< 27; i++) {
//        auto result = std::vector<branch_type const*>();
//        recovered_tree->equals_query(rect_candidates[i], result);
//        REQUIRE(result.size() == 1);
//        if(std::find_if(query_candidates.begin(),query_candidates.end(), [&](branch_type const* branch_b){
//            return
//            result.at(0)->get_data()
//            ==
//            branch_b->get_data();
//        })
//        == query_candidates.end()) {
//            values_correct = false;
//            break;
//        }
//    }
//    REQUIRE(values_correct);
//}

TEST_CASE("PEAR basic recovery test", "[PearTree]") {
    PearTreeAllocatorSingleton::getInstance().reset();
    using tree_type = PearTree<3,1, float, PEAR_STRAT_BASIC_VERSION(0)>;
    using recoverer_type = PearRecoverer<3,1,float, PEAR_STRAT_BASIC_VERSION(0)>;
    tree_type* test = new tree_type(42u);

    for(int i = 0; i < 27; i++) {
        test->insert(Rectangle((float)(i+1+(i/3)),(float)((i/3)+1+((i/9)*5)),(float)(i+1+(i/3)+1.0f),(float)((i/3)+1+((i/9)*5)+1)), (long)(i+1));
    }
    delete test;
    tree_type* recovered_tree = recoverer_type::recover_tree_complete_level();
    REQUIRE(recovered_tree->test_max_level() >= 1);
    REQUIRE(recovered_tree->test_num_branches(0) == 27);
    REQUIRE(recovered_tree->test_num_branches(1) == 9);
    REQUIRE(recovered_tree->test_max_level() == 2);
    REQUIRE(test_universal_tree_MBR_fit<3,tree_type, typename tree_type::node_type,float>(*recovered_tree));
    PearTreeAllocatorSingleton::getInstance().reset();
}