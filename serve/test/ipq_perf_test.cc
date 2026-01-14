#include "indexed_priority_queue.h"
#include <vector>
#include <chrono>
#include <random>
#include <iostream>
#include <iomanip>
#include <string>

template <typename IPQType, typename ValueGenerator>
void run_benchmark(std::string name, ValueGenerator gen_val) {
    const int N_ELEMENTS = 8000;
    const int N_OPERATIONS = 1000000; // 1 million operations

    IPQType ipq;
    std::mt19937 rng(42); // Same seed for fairness
    std::uniform_int_distribution<int> dist_op(0, 2);
    
    int current_max_id = N_ELEMENTS;
    
    std::cout << "--- Benchmarking: " << name << " ---" << std::endl;

    // 1. Init
    for (int i = 0; i < N_ELEMENTS; ++i) {
        ipq.insert(i, gen_val(rng));
    }
    
    // 2. Ops
    auto start_ops = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N_OPERATIONS; ++i) {
        int op = dist_op(rng);
        if (op == 0) { // Update (approx 1/3 probability)
            // Try to pick an ID that likely exists
            int range_start = std::max(0, current_max_id - N_ELEMENTS - 1000);
            int target_id = std::uniform_int_distribution<int>(range_start, current_max_id)(rng);
            
            if (ipq.contains(target_id)) {
                 ipq.update(target_id, gen_val(rng));
            } else if (!ipq.empty()) {
                 // Fallback to updating top to ensure update workload happens
                 auto top = ipq.peek();
                 ipq.update(top.first, gen_val(rng));
            }
        } else { // Pop & Insert (approx 2/3 probability)
            if (!ipq.empty()) {
                ipq.pop();
                ipq.insert(current_max_id++, gen_val(rng));
            }
        }
    }
    auto end_ops = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> ops_diff = end_ops - start_ops;
    
    double tps = N_OPERATIONS / ops_diff.count();
    double latency_ns = (ops_diff.count() * 1e9) / N_OPERATIONS;

    std::cout << "Total Time: " << ops_diff.count() << " s" << std::endl;
    std::cout << "Avg Latency: " << latency_ns << " ns" << std::endl;
    std::cout << "TPS: " << std::fixed << std::setprecision(2) << tps << " ops/sec" << std::endl;
    std::cout << std::endl;
}

int main() {
    std::cout << "Starting Comparative IPQ Performance Test..." << std::endl;
    std::cout << "Elements: 8000, Operations: 1,000,000" << std::endl;

    // 1. Double only
    {
        using IPQDouble = IPQ<int, double, std::greater<double>>;
        std::uniform_real_distribution<double> dist(0.0, 1000.0);
        run_benchmark<IPQDouble>("Value = double", [&](std::mt19937& r){ return dist(r); });
    }

    // 2. Pair <double, int>
    {
        using ValueType = std::pair<double, int>;
        using IPQPair = IPQ<int, ValueType, std::greater<ValueType>>;
        std::uniform_real_distribution<double> dist_d(0.0, 1000.0);
        std::uniform_int_distribution<int> dist_i(0, 100);
        run_benchmark<IPQPair>("Value = std::pair<double, int>", [&](std::mt19937& r){ 
            return std::make_pair(dist_d(r), dist_i(r)); 
        });
    }

    return 0;
}
