#pragma once

#include <unordered_map>
#include <vector>
#include <list>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <algorithm>
#include <mutex>
#include <iostream>
#include <string>
#include "common.h"
#include "btree_search.h"
#include "../metis_partitioner.h"

NewMetis metis_;

int main() {
    std::vector<int> graph_sizes = {1000, 5000, 10000, 20000, 50000};
    
    for(int size : graph_sizes) {
        std::vector<uint64_t> node_ids;
        for(uint64_t i = 0; i < size; ++i) {
            node_ids.push_back(i);
        }
        
        idx_t dominant_partition = metis_.build_internal_graph(node_ids);
        std::cout << "Graph Size: " << size << ", Dominant Partition: " << dominant_partition << std::endl;
    }
    return 0;
}