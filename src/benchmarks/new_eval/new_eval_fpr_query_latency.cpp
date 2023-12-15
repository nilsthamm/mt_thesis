#include <thread>
#include <macros/macros.hpp>

#include "../micro_benchmark.hpp"
#include "new_eval_benchmarks.hpp"
#include "../evaluation/mixed_workload_measurement.hpp"



int main_thread(std::ostream* out_stream) {

    AvgWorkloadMeasurement::print_header(*out_stream);
    query_fpr_benchmark<FPR_TREE_TYPE>(*out_stream);

    return 0;
}

int main(int argc, char *argv[]) {

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for(int i : {18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35}) {
        CPU_SET(i, &cpuset);
    }
    std::ostream* out_stream;
    if(argc == 1) {
        out_stream = &(std::cout);
    } else {
        out_stream = new std::ofstream(argv[1]);
    }

    auto main_t = std::thread(main_thread, out_stream);
    int rc = pthread_setaffinity_np(main_t.native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    main_t.join();
    return 0;
}