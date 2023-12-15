#pragma once

#include <algorithm>
#include <memory>
#include <sstream>
#include <iostream>
#include <iomanip>

template<typename T>
#ifdef UNALIGNED_BUILD
struct alignas(4) Rectangle {
#else
struct Rectangle {
#endif
    T x_min;
    T y_min;
    T x_max;
    T y_max;

    static constexpr size_t POINT_SIZE = sizeof(T)*2;
    static constexpr size_t RECTANGLE_SIZE = POINT_SIZE*2;

    Rectangle() : x_min(0),y_min(0),x_max(-1),y_max(-1) {}
    Rectangle(const Rectangle&) = default;
    Rectangle(Rectangle&&) = delete;

    Rectangle(const T x_min, const T y_min, const T x_max, const T y_max) : x_min(x_min),y_min(y_min), x_max(x_max), y_max(y_max) {
        static_assert(RECTANGLE_SIZE == sizeof(*this));
        #if IS_DEBUG
        if(x_min >= x_max)
            throw std::runtime_error("x_min not smaller than x_max");
        if(y_min >= y_max)
            throw std::runtime_error("y_min not smaller than x_max");
        #endif
    }

    Rectangle(std::shared_ptr<Rectangle const> const rect) : x_min(rect->x_min), y_min(rect->y_min), x_max(rect->x_max),
                                                       y_max(rect->y_max) {
        static_assert(RECTANGLE_SIZE == sizeof(*this));
        #if IS_DEBUG
        if(x_min >= x_max)
                throw std::runtime_error("x_min not smaller than x_max");
            if(y_min >= y_max)
                throw std::runtime_error("y_min not smaller than x_max");
        #endif
    }
//    Rectangle(Rectangle const &) = delete;

    inline bool operator==(Rectangle const &rectB) const {
        return x_min == rectB.x_min && y_min == rectB.y_min && x_max == rectB.x_max && y_max == rectB.y_max;
    }

    constexpr Rectangle<T>& operator=(const Rectangle<T>& other) {
        if (this == &other)
            return *this;

        x_min = other.x_min;
        y_min = other.y_min;
        x_max = other.x_max;
        y_max = other.y_max;
        return *this;
    }
};

template<typename T>
inline bool contains(Rectangle<T> const &rect_outer, Rectangle<T> const &recht_inner) {
    return rect_outer.x_min <= recht_inner.x_min
        && rect_outer.y_min <= recht_inner.y_min
        && rect_outer.x_max >= recht_inner.x_max
        && rect_outer.y_max >= recht_inner.y_max;
}

template<typename T>
// use no overlap instead of overlap to have a more readable bool expression
// Rects are inclusive min/max values
inline bool no_overlap(Rectangle<T> const &rectA, Rectangle<T> const &rectB) {
    return
            rectA.x_min > rectB.x_max || rectB.x_min > rectA.x_max ||
            rectA.y_min > rectB.y_max || rectB.y_min > rectA.y_max;
}

template<typename T>
inline bool overlap(const Rectangle<T> &rectA, const Rectangle<T> &rectB) {
    return
            rectA.x_min <= rectB.x_max && rectB.x_min <= rectA.x_max &&
            rectA.y_min <= rectB.y_max && rectB.y_min <= rectA.y_max;
}

template<typename T>
inline bool covers(const Rectangle<T> &rectA, const Rectangle<T> &rectB) {
    return
        rectA.x_min <= rectB.x_min && rectA.y_min <= rectB.y_min &&
        rectA.x_max >= rectB.x_max && rectA.y_max >= rectB.y_max;
}

template<typename T>
inline T area_inline(Rectangle<T> const &rect) {
    return std::abs((rect.x_max - rect.x_min) * (rect.y_max - rect.y_min));
}

template<typename T>
inline T area_2(Rectangle<T> const &rectA, Rectangle<T> const &rectB) {
    return std::abs((std::max(rectA.x_max,rectB.x_max) - std::min(rectA.x_min, rectB.x_min)) * (std::max(rectA.y_max,rectB.y_max) - std::min(rectA.y_min, rectB.y_min)));
}

template<typename T>
T overlap_area(Rectangle<T> const &rectA, Rectangle<T> const &rectB) {
    T x_overlap = std::max((T)0, std::min(rectA.x_max, rectB.x_max) - std::max(rectA.x_min, rectB.x_min));
    T y_overlap = std::max((T)0, std::min(rectA.y_max, rectB.y_max) - std::max(rectA.y_min, rectB.y_min));
    return std::abs(x_overlap*y_overlap);
}

template<typename T>
T overlap_area(Rectangle<T> const &rectA1, Rectangle<T> const &rectA2, Rectangle<T> const &rectB) {
    T x_overlap = std::max((T)0, std::min(std::max(rectA1.x_max,rectA2.x_max), rectB.x_max) - std::max(std::min(rectA1.x_min,rectA2.x_min), rectB.x_min));
    T y_overlap = std::max((T)0, std::min(std::max(rectA1.y_max,rectA2.y_max), rectB.y_max) - std::max(std::min(rectA1.y_min,rectA2.y_min), rectB.y_min));
    return std::abs(x_overlap*y_overlap);
}

template<typename T>
std::shared_ptr<Rectangle<T>> join(const Rectangle<T> &rectA, const Rectangle<T> &rectB) {
    return std::make_shared<Rectangle<T>>(std::min(rectA.x_min, rectB.x_min), std::min(rectA.y_min, rectB.y_min), std::max(rectA.x_max, rectB.x_max), std::max(rectA.y_max, rectB.y_max));
}

template<typename T>
inline void join(const Rectangle<T> &rectA, const Rectangle<T> &rectB, Rectangle<T> *const joined_rect) {
    new(joined_rect) Rectangle<T>(std::min(rectA.x_min, rectB.x_min), std::min(rectA.y_min, rectB.y_min), std::max(rectA.x_max, rectB.x_max), std::max(rectA.y_max, rectB.y_max));
}

template<typename T>
void float_to_hex(T f) {
    const unsigned char * pf = reinterpret_cast<const unsigned char*>(&f);
    std::cout << "0x";
    for (size_t i = 0; i != sizeof(T); ++i)
    {
        printf("%02x",pf[i]);
    }
    std::cout << std::dec;
}

template<typename T>
std::string to_string(Rectangle<T> const &rect) {

    std::stringstream stream;
    stream << "Rectangle<T> {Low Point: (" << rect.x_min << "), " << rect.y_min << ")); High Point: (" << rect.x_max << "), " << rect.y_max << "))}";
    return stream.str();
}

template<typename T>
void to_string_cout(Rectangle<T> const &rect) {

    std::cout << "Rectangle<T> {Low Point: (" << rect.x_min << "(";
    float_to_hex(rect.x_min);
    std::cout << "), " << rect.y_min << "(";
    float_to_hex(rect.y_min);
    std::cout << ")); High Point: (" << rect.x_max << "(";
    float_to_hex(rect.x_max);
    std::cout << "), " << rect.y_max << "(";
    float_to_hex(rect.y_max);
    std::cout << "))}";

}