#pragma once

template<class Node, class Branch, class Rectangle>
struct path_entry{
    Node* node;
    Branch* branch;
    Rectangle* rectangle;
    uint32_t initial_version;
    bool last_level_before_pmem;

    path_entry() : node(NULL), initial_version(0), branch(NULL){}
    path_entry(Node* const n, uint32_t const i_version, Branch* const b, Rectangle* const rect) : node(n), initial_version(i_version), branch(b), rectangle(rect), last_level_before_pmem(false) {}
    path_entry(Node* const n, uint32_t const i_version, Branch* const b, Rectangle* const rect, bool const last_level_pmem) : node(n), initial_version(i_version), branch(b), rectangle(rect), last_level_before_pmem(last_level_pmem) {}
};