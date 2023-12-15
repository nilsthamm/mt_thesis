#pragma once

#include "rectangle.hpp"


template<class Node, class Datatype>
struct FPRBranch
{
    Rectangle<float> m_rect;                                  ///< Bounds
    union
    {
        Node* m_child;                              ///< Child node
        Datatype m_data;                            ///< Data Id or Ptr
    };

    FPRBranch() : m_rect(INT32_MAX,INT32_MAX,INT32_MAX,INT32_MAX),m_data(INT64_MAX) {}
    FPRBranch(const int x_min, const int y_min, const int x_max, const int y_max, Datatype value) : m_rect(x_min,y_min,x_max,y_max),m_data(value) {}
};


template<class Node, class Datatype>
std::string to_string(FPRBranch<Node, Datatype> branch, bool data_branch=false) {
    std::stringstream stream;
    stream << "FPRBranch {" << to_string(branch.m_rect) << "; ";
    if(data_branch) {
        // TODO(RTree interface)
        stream << "Value: " << branch.m_data << "}";
    } else {
        // TODO(RTree interface)
        stream << "Pointer to level: " << branch.m_child->_level << "}";
    }
    return stream.str();
}