#pragma once


struct recovery_stats {
    size_t touched_p_nodes;
    size_t recreated_v_nodes;
    size_t reinserted_p_nodes;
    size_t partial_level_p_nodes;
    int complete_persisted_level;
    int partial_persisted_level;

public:
    recovery_stats() : touched_p_nodes(0), recreated_v_nodes(0), reinserted_p_nodes(0), partial_level_p_nodes(0),complete_persisted_level(-1),partial_persisted_level(-1)  {}
};


struct query_stats {
    int touched_p_nodes = 0;
    int touched_v_nodes = 0;
    int touched_leafs = 0;
    int touched_inner_nodes = 0;
    int results_count = 0;
};