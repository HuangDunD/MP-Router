#pragma once // Prevents multiple inclusions of this header file

#include <vector>
#include <string>
#include <unordered_map>
#include <set>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm> // Required for std::max_element, std::max
#include <mutex>      // Required for std::mutex, std::lock_guard, std::unique_lock
#include <shared_mutex> // Required for std::shared_mutex, std::shared_lock
#include <numeric>    // Required for std::iota (optional)
#include <iterator>   // Required for std::inserter
#include <map>        // Included for potential ordered iteration if needed
#include <chrono>
#include <iomanip>
#include <ctime>
#include <atomic>     // Required for automatic partitioning counters
#include <random>
#include <cassert>

#include "region/region.h" // Assuming this path is correct
#include "log/Logger.h"
#include "config.h"
#include "threadpool.h"
#include "common.h"

// Include metis.h (ensure it's available in your include paths)
#include <metis.h>

// Enable automatic partitioning via preprocessor directive
#define ENABLE_AUTO_PARTITION

class NewMetis {
public:
    NewMetis(Logger* logger = nullptr) : num_partitions_(0),
                 logger_(logger),
                 gen_(rd()),
                 active_partition_map_ptr_(&partition_node_map) {
        if(logger_ == nullptr) {
            logger_ = new Logger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::INFO, partition_log_file_, 4096);
        }
        // remove the output_partition_file
        std::remove(partition_output_file_.c_str());
        // remove the change_rate_report_file_
        std::remove(change_rate_report_file_.c_str());
    }

    void set_thread_pool(ThreadPool *pool) {
        associated_thread_pool_ = pool;
    }

    void init_node_nums(int nums) {
        num_partitions_ = (nums > 0) ? static_cast<uint64_t>(nums) : 0;
    }

    // Returns the dominant PartitionIndex for the group, or -1 if none determined.
    idx_t build_internal_graph(std::unordered_map<uint64_t, node_id_t> &request_partition_node_map, node_id_t *metis_decision_node);

    // 不构建图，只是获取当前的分区结果
    void get_metis_partitioning_result(std::unordered_map<uint64_t, idx_t> &request_partition_node_map);
    node_id_t get_metis_partitioning_result(uint64_t request_partition_node);

    void build_link_between_nodes_in_graph(uint64_t from_node, uint64_t to_node);

    void partition_internal_graph(const std::string &output_partition_file, uint64_t ComputeNodeCount);

    void stabilize_partition_indices(
        idx_t nvtx,
        const std::vector<idx_t> &new_part_csr,
        const std::vector<uint64_t> &dense_to_original_id_snapshot,
        std::unordered_map<uint64_t, idx_t> &current_partition_node_map_ref // 传入引用以修改
    );

    struct Stats {
        int total_nodes_in_graph = 0;
        int total_edges_in_graph = 0;
        int total_edges_weight = 0;
        int cut_edges_weight = 0;
        std::atomic<uint64_t> total_partition_calls = 0;
        std::atomic<uint64_t> missing_node_decisions = 0;
        std::atomic<uint64_t> entire_affinity_decisions = 0;
        std::atomic<uint64_t> partial_affinity_decisions = 0;
        std::atomic<uint64_t> total_cross_partition_decisions = 0;
    };

    const Stats& get_stats() const {
        return stats_;
    }

    void reset_stats() {
        stats_.missing_node_decisions.store(0, std::memory_order_relaxed);
        stats_.entire_affinity_decisions.store(0, std::memory_order_relaxed);
        stats_.partial_affinity_decisions.store(0, std::memory_order_relaxed);
        stats_.total_cross_partition_decisions.store(0, std::memory_order_relaxed);
    }

private:
    // Internal Graph Representation
    std::set<uint64_t> active_nodes_;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t> > partition_graph_;
    std::unordered_map<uint64_t, uint64_t> partition_weight_;

    // Mapping and Partitioning Results
    // Stores OriginalRegionID -> PartitionIndex (from METIS)
    std::unordered_map<uint64_t, idx_t> partition_node_map;
    std::unordered_map<uint64_t, std::pair<idx_t, std::vector<int>> > pending_partition_node_map; // 在触发分区操作间隔中第一次出现悬而未决的顶点.
    
    // RCU-style fast read access: atomic pointer to current partition map
    std::atomic<std::unordered_map<uint64_t, idx_t>*> active_partition_map_ptr_;

    // Mutexes for thread safety
    mutable std::mutex graph_data_mutex_;
    mutable std::shared_mutex partition_map_mutex_;

    // Automatic Partitioning Members
    std::atomic<uint64_t> build_call_counter_{0};
    std::atomic<uint64_t> last_partition_milestone_{0};
    ThreadPool *associated_thread_pool_ = nullptr;
    std::string partition_output_file_ = "graph_partitions.csv";
    std::string change_rate_report_file_ = "partition_change_rate_report.txt";
    uint64_t num_partitions_;

    // statistics for monitoring
    Stats stats_;

    // Dense ID Mapping
    std::unordered_map<uint64_t, idx_t> regionid_to_denseid_map_;
    std::vector<uint64_t> regionid_to_dense_map_; // dense_id -> original_id
    std::atomic<idx_t> next_dense_id_{0};

    Logger* logger_ = nullptr;

    // enable partition, 这里引入一个允许分区的控制函数
    bool enable_partition = true;

    // random seed
    std::random_device rd;
    std::mt19937 gen_; // Mersenne Twister RNG
    std::uniform_real_distribution<double> distrib{0.0, 1.0}; // Uniform distribution [0.0, 1.0]

    // 稳定器，是评估上一次Metis和这一次Metis结果变化率
    std::vector<double> change_rates_history_;
    std::atomic<int> change_times;

    // Helper (not directly used by METIS call after snapshot, but for internal consistency if needed)
    // Must be called while holding graph_data_mutex_.
    uint64_t get_graph_size_unsafe() const {
        return next_dense_id_.load(std::memory_order_relaxed);
    }
};
