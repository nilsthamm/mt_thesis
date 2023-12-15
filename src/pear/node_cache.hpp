#pragma once


class NodeCacheSingelton {
public:
    NodeCacheSingelton(NodeCacheSingelton const&) =delete;
    void operator=(NodeCacheSingelton const&) = delete;

    static NodeCacheSingelton& get_instance() {
        static NodeCacheSingelton instance;

        return instance;
    }

    int register_a_node(void* const node_ptr) {
        // have atomic counter which maps to the array position
        // if counter reaches max size, get position with minimum epoch stamp (somehow needs to happen atomically)
        // or use min heap
        UNIMPLEMENTED_FUNCTION
    }

    void touch_a_node(int const position) {
        // update the epoch stamp at position
        // use some kind of atomically incremented long as epoch
    }

private:
    NodeCacheSingelton() {

    };
};