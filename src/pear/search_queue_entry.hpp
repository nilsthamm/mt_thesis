#pragma once

#include "macros/macros.hpp"

template <typename T>
struct search_queue_entry {
    T ptr;
    META_DATA_TYPE last_save_version;
    bool reinserted;

    search_queue_entry(T const pointer, META_DATA_TYPE const version) : ptr(pointer), last_save_version(version), reinserted(false) {}
    search_queue_entry(T const pointer, META_DATA_TYPE const version, bool const reinsert) : ptr(pointer), last_save_version(version), reinserted(reinsert) {}

    ~search_queue_entry() {
        ptr = nullptr;
    }
};