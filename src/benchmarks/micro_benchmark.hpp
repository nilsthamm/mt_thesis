#pragma once
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry.hpp>
#include <iostream>
#include <fstream>
#include <sstream>

#include <rectangle.hpp>

#define SZ_TAXI_10M micro_benchmark::get_sz_taxi_rects_10m()
#define SZ_TAXI_SECOND_10M micro_benchmark::get_second_sz_taxi_rects_10m()
#define SZ_TAXI_100M micro_benchmark::get_sz_taxi_rects_200m()
#define SZ_TAXI_XL_INITIAL_POINTS_AMONT 100*1000*1000
#define SZ_TAXI_XL_ADDED_POINTS_AMONT 100*1000*1000
#define SZ_TAXI_QUERY_SIZE 500*1000
#define SZ_TAXI_200M micro_benchmark::get_sz_taxi_rects_200m()

#define OPTIMAL_NUMBER_INSERT_THREADS 18
#define OPTIMAL_NUMBER_POINT_QUERY_THREADS 36

#define FPR_TREE_TYPE FPRTree<56,24,INT32_MAX>
#define SEARCH_VECTOR_TYPE std::shared_ptr<std::vector<micro_benchmark::rect_float_type>>

#define NUMBER_REPETITIONS_INSERT 5
#define NUMBER_REPETITIONS_QUERY 5

#define INSERT_ITERATIONS_AND_REPETITIONS Iterations(1)->Repetitions(NUMBER_REPETITIONS_INSERT)
#define QUERY_ITERATIONS_AND_REPETITIONS Iterations(NUMBER_REPETITIONS_QUERY)->Repetitions(1)

#ifdef NDEBUG
#if NUMBER_REPETITIONS_INSERT != 5
#pragma message("WARNING: NUMBER_REPETITIONS_INSERT != 5")
#endif
#endif

#ifdef NDEBUG
#if NUMBER_REPETITIONS_QUERY != 5
#pragma message("WARNING: NUMBER_REPETITIONS_QUERY != 5")
#endif
#endif

template<typename T> struct argument_type;
template<typename T, typename U> struct argument_type<T(U)> { typedef U type; };

template <typename T>
constexpr T ipow(T num, unsigned int pow)
{
    return (pow >= sizeof(unsigned int)*8) ? 0 :
           pow == 0 ? 1 : num * ipow(num, pow-1);
}

namespace micro_benchmark {
    using rect_float_type = Rectangle<float>;
    static rect_float_type* sz_taxi_rects_10m;
    static rect_float_type* rb_taxi_rects_10m;
    static rect_float_type* sz_taxi_rects_100m;
    static rect_float_type* sz_taxi_rects_200m;
    static rect_float_type* rb_taxi_rects_100m;
    static rect_float_type* second_sz_taxi_rects_10m;
    static rect_float_type* second_rb_taxi_rects_10m;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_1_5m_0_00001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_1_5m_0_0001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_1_5m_0_001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_100m_0_00001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_100m_0_0001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_100m_0_001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_180m_0_00001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_180m_0_0001 = nullptr;
    static std::shared_ptr<std::vector<rect_float_type>> pear_query_rects_180m_0_001 = nullptr;

    typedef boost::geometry::model::point<float, 2, boost::geometry::cs::cartesian> boost_point;
    typedef boost::geometry::model::box<boost_point> boost_rect;
    typedef std::pair<boost_rect, uint64_t> boost_value;

    static rect_float_type* load_file_to_ptr(std::string const file_name, int const size) {
        rect_float_type* return_ptr = (rect_float_type*)malloc(sizeof(rect_float_type) * size);
        std::stringstream inputFilePath;
        inputFilePath << PROJECT_ROOT << file_name;
        std::string temp_str = inputFilePath.str();
        std::ifstream infile(inputFilePath.str().c_str(), std::ios::binary|std::ios::in);
        if (infile.is_open()) {
            infile.seekg(0, std::ios::beg);
            infile.read((char *)(return_ptr), sizeof(rect_float_type) * size);
        } else {
            inputFilePath << " not found";
            throw std::runtime_error(inputFilePath.str());
        }
        infile.close();
        return return_ptr;
    }

    static rect_float_type* get_sz_taxi_rects_10m() {
        if(sz_taxi_rects_10m == NULL) {
            sz_taxi_rects_10m = load_file_to_ptr("/data/taxi_data_2016_03_sz_10m", 10000000 * 2);
        }
        return sz_taxi_rects_10m;
    }

    static rect_float_type* get_rb_taxi_rects_10m() {
        if(rb_taxi_rects_10m == NULL) {
            rb_taxi_rects_10m = load_file_to_ptr("/data/taxi_data_2016_03_rb_10m", 10000000);
        }
        return rb_taxi_rects_10m;
    }

    static rect_float_type* get_sz_taxi_rects_100m() {
        if(sz_taxi_rects_100m == NULL) {
            sz_taxi_rects_100m = load_file_to_ptr("/data/taxi_data_2009_sz_100m", 100000000);
        }
        return sz_taxi_rects_100m;
    }

    static rect_float_type* get_sz_taxi_rects_200m() {
        if(sz_taxi_rects_200m == NULL) {
            sz_taxi_rects_200m = load_file_to_ptr("/data/taxi_data_2009_10_sz_200m", 200000000);
        }
        return sz_taxi_rects_200m;
    }

    static rect_float_type* get_rb_taxi_rects_100m() {
        if(rb_taxi_rects_100m == NULL) {
            rb_taxi_rects_100m = load_file_to_ptr("/data/taxi_data_2009_rb_100m", 100000000);
        }
        return rb_taxi_rects_100m;
    }

    static rect_float_type* get_second_sz_taxi_rects_10m() {
        if(second_sz_taxi_rects_10m == NULL) {
            second_sz_taxi_rects_10m = load_file_to_ptr("/data/taxi_data_2015_03_sz_10m", 10000000 * 2);
        }
        return second_sz_taxi_rects_10m;
    }

    static rect_float_type* get_second_rb_taxi_rects_10m() {
        if(second_rb_taxi_rects_10m == NULL) {
            second_rb_taxi_rects_10m = load_file_to_ptr("/data/taxi_data_2015_03_rb_10m", 10000000);
        }
        return second_rb_taxi_rects_10m;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_1_5m_0_00001() {
        if(pear_query_rects_1_5m_0_00001 != nullptr) {
            return pear_query_rects_1_5m_0_00001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-73.9841f, 40.7429f, -73.9838f, 40.7432f);
        pear_query_rects_1_5m_0_00001 = result;
        return pear_query_rects_1_5m_0_00001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_1_5m_0_0001() {
        if(pear_query_rects_1_5m_0_0001 != nullptr) {
            return pear_query_rects_1_5m_0_0001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-73.9841f, 40.7429f, -73.9834f, 40.7439f);
        pear_query_rects_1_5m_0_0001 = result;
        return pear_query_rects_1_5m_0_0001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_1_5m_0_001() {
        if(pear_query_rects_1_5m_0_001 != nullptr) {
            return pear_query_rects_1_5m_0_001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-73.9841f, 40.7429f, -73.9816f, 40.7466f);
        pear_query_rects_1_5m_0_001 = result;
        return pear_query_rects_1_5m_0_001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_100m_0_00001() {
        if(pear_query_rects_100m_0_00001 != nullptr) {
            return pear_query_rects_100m_0_00001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-74.02, 40.7, -74.0174, 40.7039);
        result->emplace_back(-74.01, 40.7, -74.0089, 40.7017);
        result->emplace_back(-74, 40.72, -73.9998, 40.7203);
        result->emplace_back(-74, 40.73, -73.9998, 40.7303);
        result->emplace_back(-74, 40.74, -73.9998, 40.7403);
        result->emplace_back(-74, 40.75, -73.9998, 40.7503);
        result->emplace_back(-73.99, 40.66, -73.989, 40.6616);
        result->emplace_back(-73.99, 40.68, -73.9893, 40.681);
        result->emplace_back(-73.99, 40.66, -73.989, 40.6616);
        result->emplace_back(-73.99, 40.68, -73.9893, 40.681);
        result->emplace_back(-73.99, 40.71, -73.9897, 40.7105);
        result->emplace_back(-73.99, 40.72, -73.9898, 40.7203);
        result->emplace_back(-73.99, 40.73, -73.9898, 40.7303);
        result->emplace_back(-73.99, 40.74, -73.9899, 40.7402);
        result->emplace_back(-73.99, 40.75, -73.9899, 40.7501);
        result->emplace_back(-73.99, 40.76, -73.9898, 40.7602);
        result->emplace_back(-73.98, 40.71, -73.9794, 40.7109);
        result->emplace_back(-73.98, 40.72, -73.9798, 40.7204);
        result->emplace_back(-73.98, 40.73, -73.9798, 40.7303);
        result->emplace_back(-73.98, 40.74, -73.9799, 40.7402);
        result->emplace_back(-73.98, 40.75, -73.9799, 40.7502);
        result->emplace_back(-73.98, 40.76, -73.9799, 40.7602);
        result->emplace_back(-73.97, 40.72, -73.9694, 40.721);
        result->emplace_back(-73.97, 40.73, -73.9696, 40.7307);
        result->emplace_back(-73.97, 40.76, -73.9699, 40.7602);
        result->emplace_back(-73.97, 40.77, -73.9698, 40.7703);
        result->emplace_back(-73.96, 40.74, -73.9595, 40.7407);
        result->emplace_back(-73.96, 40.75, -73.9597, 40.7504);
        result->emplace_back(-73.96, 40.76, -73.9598, 40.7603);
        result->emplace_back(-73.96, 40.78, -73.9598, 40.7803);
        result->emplace_back(-73.96, 40.79, -73.9596, 40.7907);
        result->emplace_back(-73.95, 40.75, -73.9497, 40.7505);
        result->emplace_back(-73.95, 40.76, -73.9496, 40.7605);
        result->emplace_back(-73.95, 40.77, -73.9497, 40.7704);
        result->emplace_back(-73.95, 40.79, -73.9497, 40.7904);
        pear_query_rects_100m_0_00001 = result;
        return pear_query_rects_100m_0_00001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_100m_0_0001() {
        if(pear_query_rects_100m_0_0001 != nullptr) {
            return pear_query_rects_100m_0_0001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-74.02, 40.7, -74.0168, 40.7049);
        result->emplace_back(-74.01, 40.7, -74.0083, 40.7026);
        result->emplace_back(-74, 40.72, -73.9994, 40.721);
        result->emplace_back(-74, 40.73, -73.9996, 40.7306);
        result->emplace_back(-74, 40.74, -73.9993, 40.741);
        result->emplace_back(-74, 40.75, -73.9994, 40.751);
        result->emplace_back(-73.99, 40.66, -73.987, 40.6644);
        result->emplace_back(-73.99, 40.68, -73.9878, 40.6833);
        result->emplace_back(-73.99, 40.66, -73.987, 40.6644);
        result->emplace_back(-73.99, 40.68, -73.9878, 40.6833);
        result->emplace_back(-73.99, 40.71, -73.9889, 40.7116);
        result->emplace_back(-73.99, 40.72, -73.9895, 40.7207);
        result->emplace_back(-73.99, 40.73, -73.9896, 40.7306);
        result->emplace_back(-73.99, 40.74, -73.9896, 40.7406);
        result->emplace_back(-73.99, 40.75, -73.9896, 40.7505);
        result->emplace_back(-73.99, 40.76, -73.9896, 40.7606);
        result->emplace_back(-73.98, 40.71, -73.9784, 40.7124);
        result->emplace_back(-73.98, 40.72, -73.9791, 40.7213);
        result->emplace_back(-73.98, 40.73, -73.9794, 40.731);
        result->emplace_back(-73.98, 40.74, -73.9794, 40.7408);
        result->emplace_back(-73.98, 40.75, -73.9796, 40.7506);
        result->emplace_back(-73.98, 40.76, -73.9796, 40.7605);
        result->emplace_back(-73.97, 40.72, -73.9682, 40.7227);
        result->emplace_back(-73.97, 40.73, -73.9686, 40.7321);
        result->emplace_back(-73.97, 40.76, -73.9696, 40.7606);
        result->emplace_back(-73.97, 40.77, -73.9693, 40.7711);
        result->emplace_back(-73.96, 40.74, -73.9585, 40.7422);
        result->emplace_back(-73.96, 40.75, -73.9591, 40.7514);
        result->emplace_back(-73.96, 40.76, -73.9594, 40.7609);
        result->emplace_back(-73.96, 40.78, -73.9595, 40.7808);
        result->emplace_back(-73.96, 40.79, -73.9585, 40.7922);
        result->emplace_back(-73.95, 40.75, -73.9487, 40.752);
        result->emplace_back(-73.95, 40.76, -73.9488, 40.7619);
        result->emplace_back(-73.95, 40.77, -73.9492, 40.7712);
        result->emplace_back(-73.95, 40.79, -73.9486, 40.792);
        pear_query_rects_100m_0_0001 = result;
        return pear_query_rects_100m_0_0001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_100m_0_001() {
        if(pear_query_rects_100m_0_001 != nullptr) {
            return pear_query_rects_100m_0_001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-74.02, 40.7, -74.0154, 40.7068);
        result->emplace_back(-74.01, 40.7, -74.0067, 40.705);
        result->emplace_back(-74, 40.72, -73.9981, 40.7229);
        result->emplace_back(-74, 40.73, -73.9983, 40.7326);
        result->emplace_back(-74, 40.74, -73.9982, 40.7427);
        result->emplace_back(-74, 40.75, -73.9979, 40.7531);
        result->emplace_back(-73.99, 40.66, -73.9819, 40.6722);
        result->emplace_back(-73.99, 40.68, -73.9837, 40.6895);
        result->emplace_back(-73.99, 40.66, -73.9819, 40.6722);
        result->emplace_back(-73.99, 40.68, -73.9837, 40.6895);
        result->emplace_back(-73.99, 40.71, -73.9869, 40.7146);
        result->emplace_back(-73.99, 40.72, -73.9884, 40.7224);
        result->emplace_back(-73.99, 40.73, -73.9886, 40.7321);
        result->emplace_back(-73.99, 40.74, -73.9888, 40.7418);
        result->emplace_back(-73.99, 40.75, -73.9887, 40.7519);
        result->emplace_back(-73.99, 40.76, -73.9886, 40.7622);
        result->emplace_back(-73.98, 40.71, -73.9758, 40.7164);
        result->emplace_back(-73.98, 40.72, -73.9775, 40.7238);
        result->emplace_back(-73.98, 40.73, -73.9778, 40.7333);
        result->emplace_back(-73.98, 40.74, -73.9785, 40.7422);
        result->emplace_back(-73.98, 40.75, -73.9789, 40.7517);
        result->emplace_back(-73.98, 40.76, -73.9789, 40.7616);
        result->emplace_back(-73.97, 40.72, -73.9643, 40.7285);
        result->emplace_back(-73.97, 40.73, -73.9655, 40.7368);
        result->emplace_back(-73.97, 40.76, -73.9688, 40.7618);
        result->emplace_back(-73.97, 40.77, -73.9678, 40.7732);
        result->emplace_back(-73.96, 40.74, -73.956, 40.7461);
        result->emplace_back(-73.96, 40.75, -73.9566, 40.7551);
        result->emplace_back(-73.96, 40.76, -73.9582, 40.7627);
        result->emplace_back(-73.96, 40.78, -73.9585, 40.7823);
        result->emplace_back(-73.96, 40.79, -73.9545, 40.7982);
        result->emplace_back(-73.95, 40.75, -73.9451, 40.7573);
        result->emplace_back(-73.95, 40.76, -73.9462, 40.7657);
        result->emplace_back(-73.95, 40.77, -73.9478, 40.7734);
        result->emplace_back(-73.95, 40.79, -73.9452, 40.7972);
        pear_query_rects_100m_0_001 = result;
        return pear_query_rects_100m_0_001;
    }



    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_180m_0_00001() {
        if(pear_query_rects_180m_0_00001 != nullptr) {
            return pear_query_rects_180m_0_00001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-74.02, 40.7, -74.0174, 40.7039);
        result->emplace_back(-74.01, 40.7, -74.0088, 40.7017);
        result->emplace_back(-74, 40.72, -73.9998, 40.7203);
        result->emplace_back(-74, 40.73, -73.9998, 40.7303);
        result->emplace_back(-74, 40.74, -73.9998, 40.7403);
        result->emplace_back(-74, 40.75, -73.9998, 40.7503);
        result->emplace_back(-73.99, 40.66, -73.989, 40.6616);
        result->emplace_back(-73.99, 40.68, -73.9893, 40.681);
        result->emplace_back(-73.99, 40.66, -73.989, 40.6616);
        result->emplace_back(-73.99, 40.68, -73.9893, 40.681);
        result->emplace_back(-73.99, 40.71, -73.9897, 40.7105);
        result->emplace_back(-73.99, 40.72, -73.9898, 40.7203);
        result->emplace_back(-73.99, 40.73, -73.9898, 40.7303);
        result->emplace_back(-73.99, 40.74, -73.9899, 40.7402);
        result->emplace_back(-73.99, 40.75, -73.9899, 40.7501);
        result->emplace_back(-73.99, 40.76, -73.9899, 40.7602);
        result->emplace_back(-73.98, 40.71, -73.9794, 40.7109);
        result->emplace_back(-73.98, 40.72, -73.9798, 40.7204);
        result->emplace_back(-73.98, 40.73, -73.9798, 40.7303);
        result->emplace_back(-73.98, 40.74, -73.9799, 40.7402);
        result->emplace_back(-73.98, 40.75, -73.9799, 40.7502);
        result->emplace_back(-73.98, 40.76, -73.9799, 40.7602);
        result->emplace_back(-73.97, 40.72, -73.9694, 40.7209);
        result->emplace_back(-73.97, 40.73, -73.9696, 40.7307);
        result->emplace_back(-73.97, 40.76, -73.9699, 40.7602);
        result->emplace_back(-73.97, 40.77, -73.9698, 40.7703);
        result->emplace_back(-73.96, 40.74, -73.9595, 40.7407);
        result->emplace_back(-73.96, 40.75, -73.9597, 40.7504);
        result->emplace_back(-73.96, 40.76, -73.9598, 40.7603);
        result->emplace_back(-73.96, 40.78, -73.9598, 40.7804);
        result->emplace_back(-73.96, 40.79, -73.9595, 40.7907);
        result->emplace_back(-73.95, 40.75, -73.9497, 40.7505);
        result->emplace_back(-73.95, 40.76, -73.9496, 40.7605);
        result->emplace_back(-73.95, 40.77, -73.9497, 40.7704);
        result->emplace_back(-73.95, 40.79, -73.9497, 40.7905);
        result->emplace_back(-73.96, 40.79, -73.9595, 40.7907);
        result->emplace_back(-73.95, 40.75, -73.9497, 40.7505);
        result->emplace_back(-73.95, 40.76, -73.9496, 40.7605);
        result->emplace_back(-73.95, 40.77, -73.9497, 40.7704);
        result->emplace_back(-73.95, 40.79, -73.9497, 40.7905);
        pear_query_rects_180m_0_00001 = result;
        return pear_query_rects_180m_0_00001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_180m_0_0001() {
        if(pear_query_rects_180m_0_0001 != nullptr) {
            return pear_query_rects_180m_0_0001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-74.02, 40.7, -74.0168, 40.7048);
        result->emplace_back(-74.01, 40.7, -74.0083, 40.7025);
        result->emplace_back(-74, 40.72, -73.9994, 40.721);
        result->emplace_back(-74, 40.73, -73.9996, 40.7306);
        result->emplace_back(-74, 40.74, -73.9993, 40.741);
        result->emplace_back(-74, 40.75, -73.9994, 40.7509);
        result->emplace_back(-73.99, 40.66, -73.987, 40.6644);
        result->emplace_back(-73.99, 40.68, -73.9878, 40.6833);
        result->emplace_back(-73.99, 40.66, -73.987, 40.6644);
        result->emplace_back(-73.99, 40.68, -73.9878, 40.6833);
        result->emplace_back(-73.99, 40.71, -73.9889, 40.7116);
        result->emplace_back(-73.99, 40.72, -73.9895, 40.7207);
        result->emplace_back(-73.99, 40.73, -73.9896, 40.7307);
        result->emplace_back(-73.99, 40.74, -73.9896, 40.7406);
        result->emplace_back(-73.99, 40.75, -73.9896, 40.7505);
        result->emplace_back(-73.99, 40.76, -73.9896, 40.7606);
        result->emplace_back(-73.98, 40.71, -73.9784, 40.7124);
        result->emplace_back(-73.98, 40.72, -73.9791, 40.7213);
        result->emplace_back(-73.98, 40.73, -73.9794, 40.731);
        result->emplace_back(-73.98, 40.74, -73.9794, 40.7408);
        result->emplace_back(-73.98, 40.75, -73.9796, 40.7506);
        result->emplace_back(-73.98, 40.76, -73.9797, 40.7605);
        result->emplace_back(-73.97, 40.72, -73.9682, 40.7226);
        result->emplace_back(-73.97, 40.73, -73.9686, 40.7322);
        result->emplace_back(-73.97, 40.76, -73.9696, 40.7606);
        result->emplace_back(-73.97, 40.77, -73.9693, 40.7711);
        result->emplace_back(-73.96, 40.74, -73.9585, 40.7422);
        result->emplace_back(-73.96, 40.75, -73.9591, 40.7514);
        result->emplace_back(-73.96, 40.76, -73.9594, 40.7609);
        result->emplace_back(-73.96, 40.78, -73.9594, 40.7808);
        result->emplace_back(-73.96, 40.79, -73.9585, 40.7923);
        result->emplace_back(-73.95, 40.75, -73.9487, 40.7519);
        result->emplace_back(-73.95, 40.76, -73.9488, 40.7618);
        result->emplace_back(-73.95, 40.77, -73.9492, 40.7712);
        result->emplace_back(-73.95, 40.79, -73.9486, 40.7921);
        result->emplace_back(-73.96, 40.76, -73.9594, 40.7609);
        result->emplace_back(-73.96, 40.78, -73.9594, 40.7808);
        result->emplace_back(-73.96, 40.79, -73.9585, 40.7923);
        result->emplace_back(-73.95, 40.75, -73.9487, 40.7519);
        result->emplace_back(-73.95, 40.76, -73.9488, 40.7618);
        result->emplace_back(-73.95, 40.77, -73.9492, 40.7712);
        result->emplace_back(-73.95, 40.79, -73.9486, 40.7921);
        pear_query_rects_180m_0_0001 = result;
        return pear_query_rects_180m_0_0001;
    }

    static std::shared_ptr<std::vector<rect_float_type>> get_pear_query_rects_180m_0_001() {
        if(pear_query_rects_180m_0_001 != nullptr) {
            return pear_query_rects_180m_0_001;
        }
        std::shared_ptr<std::vector<rect_float_type>> result = std::make_shared<std::vector<rect_float_type>>();
        result->emplace_back(-74.01, 40.7, -74.0067, 40.7049);
        result->emplace_back(-74, 40.72, -73.9981, 40.7229);
        result->emplace_back(-74, 40.73, -73.9983, 40.7326);
        result->emplace_back(-74, 40.74, -73.9982, 40.7427);
        result->emplace_back(-74, 40.75, -73.998, 40.753);
        result->emplace_back(-73.99, 40.66, -73.9819, 40.6722);
        result->emplace_back(-73.99, 40.68, -73.9836, 40.6895);
        result->emplace_back(-73.99, 40.66, -73.9819, 40.6722);
        result->emplace_back(-73.99, 40.68, -73.9836, 40.6895);
        result->emplace_back(-73.99, 40.71, -73.9869, 40.7146);
        result->emplace_back(-73.99, 40.72, -73.9884, 40.7224);
        result->emplace_back(-73.99, 40.73, -73.9886, 40.7321);
        result->emplace_back(-73.99, 40.74, -73.9888, 40.7418);
        result->emplace_back(-73.99, 40.75, -73.9888, 40.7518);
        result->emplace_back(-73.99, 40.76, -73.9886, 40.7621);
        result->emplace_back(-73.98, 40.71, -73.9758, 40.7163);
        result->emplace_back(-73.98, 40.72, -73.9775, 40.7238);
        result->emplace_back(-73.98, 40.73, -73.9778, 40.7333);
        result->emplace_back(-73.98, 40.74, -73.9785, 40.7422);
        result->emplace_back(-73.98, 40.75, -73.9789, 40.7517);
        result->emplace_back(-73.98, 40.76, -73.9789, 40.7616);
        result->emplace_back(-73.97, 40.72, -73.9643, 40.7285);
        result->emplace_back(-73.97, 40.73, -73.9655, 40.7368);
        result->emplace_back(-73.97, 40.76, -73.9688, 40.7618);
        result->emplace_back(-73.97, 40.77, -73.9678, 40.7733);
        result->emplace_back(-73.96, 40.74, -73.956, 40.7461);
        result->emplace_back(-73.96, 40.75, -73.9566, 40.755);
        result->emplace_back(-73.96, 40.76, -73.9582, 40.7627);
        result->emplace_back(-73.96, 40.78, -73.9584, 40.7823);
        result->emplace_back(-73.96, 40.79, -73.9544, 40.7984);
        result->emplace_back(-73.95, 40.75, -73.9453, 40.7571);
        result->emplace_back(-73.95, 40.76, -73.9462, 40.7657);
        result->emplace_back(-73.95, 40.77, -73.9477, 40.7734);
        result->emplace_back(-73.95, 40.79, -73.9451, 40.7973);
        result->emplace_back(-74.02, 40.7, -74.0157, 40.7065);
        result->emplace_back(-73.96, 40.78, -73.9584, 40.7823);
        result->emplace_back(-73.96, 40.79, -73.9544, 40.7984);
        result->emplace_back(-73.95, 40.75, -73.9453, 40.7571);
        result->emplace_back(-73.95, 40.76, -73.9462, 40.7657);
        result->emplace_back(-73.95, 40.77, -73.9477, 40.7734);
        result->emplace_back(-73.95, 40.79, -73.9451, 40.7973);
        result->emplace_back(-74.02, 40.7, -74.0157, 40.7065);
        pear_query_rects_180m_0_001 = result;
        return pear_query_rects_180m_0_001;
    }

//    static rect_float_type* load_rrs_to_ptr(std::string const file_name, int const size) {
//        rect_float_type* return_ptr = (rect_float_type*)malloc(sizeof(rect_float_type) * size);
//        std::stringstream inputFilePath;
//        inputFilePath << PROJECT_ROOT << file_name;
//        std::string temp_str = inputFilePath.str();
//        std::ifstream infile(inputFilePath.str().c_str(), std::ios::binary|std::ios::in);
//        if (infile.is_open()) {
//            infile.seekg(0, std::ios::beg);
//            int* x_min = (int*)malloc(sizeof(int));
//            int* y_min = (int*)malloc(sizeof(int));
//            int* x_max = (int*)malloc(sizeof(int));
//            int* y_max = (int*)malloc(sizeof(int));
//            for(int i = 0; i< size; i++) {
//                infile.read((char*)x_min,sizeof(int));
//                infile.read((char*)x_max,sizeof(int));
//                infile.read((char*)y_min,sizeof(int));
//                infile.read((char*)y_max,sizeof(int));
//                return_ptr[i] = rect_float_type(*x_min,*y_min,*x_max,*y_max);
//            }
//        } else {
//            inputFilePath << " not found";
//            throw std::runtime_error(inputFilePath.str());
//        }
//        return return_ptr;
//    }
}