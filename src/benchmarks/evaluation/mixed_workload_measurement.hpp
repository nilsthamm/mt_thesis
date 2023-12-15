#pragma once

#include <ostream>
#include <chrono>
#include <ctime>

class MixedWorkloadMeasurement {
public:
    virtual void start_event_measurement() = 0;
    virtual void stop_event_measurement() = 0;
    virtual void increase_counter(size_t const i) = 0;

    MixedWorkloadMeasurement(std::string const &name) : _name(name) {}

    std::string const _name;
};

class AvgWorkloadMeasurement : public MixedWorkloadMeasurement {
public:

    AvgWorkloadMeasurement(std::string const &name) : MixedWorkloadMeasurement(name) {}

    AvgWorkloadMeasurement(std::string const &name, std::vector<AvgWorkloadMeasurement*> measurements) : MixedWorkloadMeasurement(name) {
        for(auto const measurement : measurements) {
            _iterations += measurement->_iterations;
            _total_duration = std::max(_total_duration, measurement->_total_duration);
        }
    }

    void start_event_measurement() override {
        _start = std::chrono::high_resolution_clock::now();
    }

    void stop_event_measurement() override {
        _total_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() -_start).count();
    }

    void increase_counter(size_t const i) override {
        _iterations += i;
    }

    void print_result(std::ostream & out_stream) {
        out_stream << _name << ": ";
        // Time in ms
        out_stream << _total_duration/1000000.0;
        out_stream << "ms" <<std::endl;
    }

    static void print_header(std::ostream & out_stream) {
        out_stream << "Result, Total time, Total iterations, Time in ms, Time in s, Avg. time/iteration in ms, Iterations/second" << std::endl;
    }

    template<typename T>
    static void print_result_line(std::ostream & out_stream, std::string const name, T duration, T iterations) {
        out_stream << name;
        out_stream << ", ";

        out_stream << duration;
        out_stream << ", ";

        out_stream << iterations;
        out_stream << ", ";

        // Time in ms
        out_stream << duration/1000000.0;
        out_stream << ", ";
        // Time in s
        out_stream << duration/1000000000.0;
        out_stream << ", ";
        // Avg. time/iteration in ms
        out_stream << (double)duration/(1000000*iterations);
        out_stream << ", ";
        // Iterations/second
        out_stream << (double)iterations/(duration/1000000000.0);
        out_stream << std::endl;
    }

    static void join_measurement_list(std::vector<AvgWorkloadMeasurement> &measurements, std::ostream & out_stream, std::string const &prefix, bool const print_threads = false) {
        size_t total_duration = 0;
        size_t total_iterations = 0;
        size_t max_duration = 0;
        for(auto const measurement : measurements) {
            total_duration += measurement._total_duration;
            total_iterations += measurement._iterations;
            max_duration = std::max(max_duration, measurement._total_duration);
            if(print_threads) {
                AvgWorkloadMeasurement::print_result_line(out_stream, measurement._name, measurement._total_duration, measurement._iterations);
            }
        }
        std::stringstream stream;
        stream << prefix << "_AVG";
        AvgWorkloadMeasurement::print_result_line(out_stream, stream.str(), total_duration/(double)measurements.size(), total_iterations/(double)measurements.size());
    }

protected:
    std::chrono::high_resolution_clock::time_point _start;
    size_t _total_duration = 0;
    size_t _iterations = 0;
};