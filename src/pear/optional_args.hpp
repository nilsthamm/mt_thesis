#pragma once

#include "macros/pear_maros.hpp"

#include "custom_stats.hpp"

struct alignas(CACHE_LINE_SIZE) optional_args {
    // all args
    query_stats* q_stats;
    long branch_wait_counter;
    long node_wait_counter;
    long aborts_node_changed;
    long aborts_node_locked;

    optional_args() : q_stats(nullptr), branch_wait_counter(0), node_wait_counter(0), aborts_node_changed(0), aborts_node_locked(0) {}
    optional_args(query_stats* const stats_ptr) : q_stats(stats_ptr), branch_wait_counter(0), node_wait_counter(0), aborts_node_changed(0), aborts_node_locked(0) {}

};
