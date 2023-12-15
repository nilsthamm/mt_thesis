#include <fstream>

#include "mixed_workload_benchmarks.hpp"



int main(int argc, char* argv[]) {
    std::ostream* out_stream;
    if(argc == 1) {
        out_stream = &(std::cout);
    } else {
        out_stream = new std::ofstream(argv[1]);
    }
    AvgWorkloadMeasurement::print_header(*out_stream);
    mixed_workload_benchmark<FPR_TREE_TYPE>(*out_stream);
};