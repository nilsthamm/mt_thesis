#pragma once

#include "rectangle.hpp"
#include "macros/macros.hpp"

#include <bitset>


template<class Node>
struct Branch
{
    std::atomic<BRANCH_VERSION_TYPE> atomic_version;                            ///< Bounds
    Node* m_child;

    Node* get_child_node() const {
        return m_child;
    }

    size_t get_data() const {
        return reinterpret_cast<size_t>(m_child);
    }

    size_t get_version() const {
        return LOAD_META_RELAXED_INLINE(atomic_version);
    }
    static constexpr size_t VALUE_SIZE = sizeof(size_t);
    static constexpr size_t VERSION_SIZE = sizeof(BRANCH_VERSION_TYPE);
    static constexpr size_t BRANCH_SIZE = VALUE_SIZE + VERSION_SIZE;

    Branch() : m_child(reinterpret_cast<Node*>(NULL)) {
        static_assert(BRANCH_SIZE == sizeof(*this));
        STORE_META(atomic_version, 4)
    }

    Branch(const Branch&) = delete;
    Branch(Branch&&) = delete;
    Branch(size_t const value) : m_child(reinterpret_cast<Node*>(value)) {
        static_assert(BRANCH_SIZE == sizeof(*this));
        STORE_META(atomic_version, PEAR_MIN_BRANCH_UNRESERVED_VERSION)
    }

    Branch(BRANCH_VERSION_TYPE const v, Node* const value, bool const locked=false, bool const relaxed = false) : m_child(value) {
        static_assert(BRANCH_SIZE == sizeof(*this));
        META_DATA_TYPE initial_version = v;
        if(locked) {
            initial_version |= ~(UNLOCK_BIT_MASK);
        } else {
            initial_version &= UNLOCK_BIT_MASK;
        }
        if(relaxed) {
            STORE_META_RELAXED(atomic_version, initial_version)
        } else {
            STORE_META(atomic_version, initial_version)
        }
    }

    Branch(BRANCH_VERSION_TYPE const v, void* const child, bool const relaxed = false) : m_child(reinterpret_cast<Node*>(child)){
        static_assert(BRANCH_SIZE == sizeof(*this));
        if(relaxed) {
            STORE_META_RELAXED(atomic_version, v)
        } else {
            STORE_META(atomic_version, v)
        }
    }
    Branch(BRANCH_VERSION_TYPE const v, size_t const child, bool const relaxed = false) : m_child(reinterpret_cast<Node*>(child)){
        static_assert(BRANCH_SIZE == sizeof(*this));
        if(relaxed) {
            STORE_META_RELAXED(atomic_version, v)
        } else {
            STORE_META(atomic_version, v)
        }
    }

    bool operator==(Branch<Node> const b1) {
        return m_child == b1.m_child;
    }

    constexpr Branch<Node>& operator=(const Branch<Node>& other) {
        if (this == &other)
            return *this;

        m_child = other.m_child;
        STORE_META(atomic_version, 4)
        return *this;
    }
};


template<class Node, typename Pointtype>
std::string to_string(Branch<Node> const &branch, Rectangle<Pointtype> const  &m_rect, bool data_branch=false) {
    std::stringstream stream;
    const void * address = static_cast<const void*>(&branch);
    stream << "Branch {ID: " << address << " | " << to_string(m_rect) << "; ";
    if(data_branch) {
        // TODO(RTree interface)
        stream << "Value: " << branch.get_data() << "}";
    } else {
        // TODO(RTree interface)
        const void * address2 = static_cast<const void*>(&branch.m_child);
        stream << "Pointer (" << address2 << ") to level: " << branch.get_child_node()->_level << "}";
    }
    return stream.str();
}

template<class Node, typename Pointtype>
void to_string_cout(Branch<Node> const &branch, Rectangle<Pointtype> const  &m_rect, bool data_branch=false) {
    const void * address = static_cast<const void*>(&branch);
    std::bitset<64> branch_meta = branch.atomic_version.load();
    unsigned long branch_meta_without_bits = branch_meta.to_ulong()&(UINT64_MAX>>4);
    std::cout << "Branch (";
    if(branch_meta[63]) {
        // moved
        std::cout << "1|";
    } else {
        std::cout << "0|";
    }
    if(branch_meta[62]) {
        // deleted
        std::cout << "1|";
    } else {
        std::cout << "0|";
    }
    if(branch_meta[61]) {
        // added as subtree
        std::cout << "1|";
    } else {
        // added as subtree in _root
        std::cout << "0|";
    }
    if(branch_meta[60]) {
        std::cout << "1)";
    } else {
        std::cout << "0)";
    }
    std::cout << " {ID: " << address << " | ";
    to_string_cout(m_rect);
    std::cout << "; Meta: " << branch_meta_without_bits;

    std::cout  << "; ";
    if(data_branch) {
        // TODO(RTree interface)
        std::cout << "Value: " << branch.get_data() << "}";
    } else {
        // TODO(RTree interface)
        const void * address2 = static_cast<const void*>(&branch.m_child);
        std::cout << "Pointer (" << address2 << ") to level: " << branch.get_child_node()->get_level() << "}";
    }

}