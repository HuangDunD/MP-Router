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

#include "region/region.h" // Assuming this path is correct


// Forward declarations
class ThreadPool; // Assume ThreadPool class is defined elsewhere

// Include metis.h (ensure it's available in your include paths)
#include <metis.h>

// Enable automatic partitioning via preprocessor directive
#define ENABLE_AUTO_PARTITION



class NewMetis {
public:
    NewMetis() : num_partitions_(0),
                 logger_(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::INFO, partition_log_file_, 4096) {
    }

    void set_thread_pool(ThreadPool *pool) {
        associated_thread_pool_ = pool;
    }

    void init_node_nums(int nums) {
        num_partitions_ = (nums > 0) ? static_cast<uint64_t>(nums) : 0;
    }

    // Returns the dominant PartitionIndex for the group, or -1 if none determined.
    idx_t build_internal_graph(const std::vector<uint64_t> &unique_mapped_ids_in_group);

    void partition_internal_graph(const std::string &output_partition_file,
                                  const std::string &log_file_path,
                                  uint64_t ComputeNodeCount);

private:
    // Internal Graph Representation
    std::set<uint64_t> active_nodes_;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t> > partition_graph_;
    std::unordered_map<uint64_t, uint64_t> partition_weight_;

    // Mapping and Partitioning Results
    // Stores OriginalRegionID -> PartitionIndex (from METIS)
    std::unordered_map<uint64_t, idx_t> partition_node_map;

    // Mutexes for thread safety
    mutable std::mutex graph_data_mutex_;
    mutable std::shared_mutex partition_map_mutex_;

    // Automatic Partitioning Members
    std::atomic<uint64_t> build_call_counter_{0};
    std::atomic<uint64_t> last_partition_milestone_{0};
    ThreadPool *associated_thread_pool_ = nullptr;
    std::string partition_output_file_ = "graph_partitions.csv";
    std::string partition_log_file_ = "partitioning_log.log";
    uint64_t num_partitions_;
    static const uint64_t PARTITION_INTERVAL = 1000;

    // Dense ID Mapping
    std::unordered_map<uint64_t, idx_t> regionid_to_denseid_map_;
    std::vector<uint64_t> regionid_to_dense_map_; // dense_id -> original_id
    std::atomic<idx_t> next_dense_id_{0};

    Logger logger_;

    // Helper (not directly used by METIS call after snapshot, but for internal consistency if needed)
    // Must be called while holding graph_data_mutex_.
    uint64_t get_graph_size_unsafe() const {
        return next_dense_id_.load(std::memory_order_relaxed);
    }
};


// ========================================================================
// MODIFIED build_internal_graph FUNCTION
// ========================================================================
inline idx_t NewMetis::build_internal_graph(const std::vector<uint64_t> &unique_mapped_ids_in_group) {
    if (unique_mapped_ids_in_group.empty()) {
        return -1; // Sentinel for empty input or no decision
    }

    // --- Automatic Partition Trigger Logic ---
    uint64_t current_call_count = 0;
    bool should_trigger_partition = false;
#ifdef ENABLE_AUTO_PARTITION
    if (associated_thread_pool_ != nullptr) {
        current_call_count = ++build_call_counter_;
        uint64_t current_milestone = (current_call_count / PARTITION_INTERVAL) * PARTITION_INTERVAL;

        if (current_milestone > 0) {
            uint64_t expected_last = last_partition_milestone_.load(std::memory_order_acquire);
            if (current_milestone > expected_last) {
                if (last_partition_milestone_.compare_exchange_strong(expected_last, current_milestone,
                                                                      std::memory_order_acq_rel)) {
                    should_trigger_partition = true;
                }
            }
        }
        // Trigger on the very first call, ensuring milestone is updated to prevent immediate re-trigger if interval is small
        if (current_call_count == 1) {
            should_trigger_partition = true;
            uint64_t expected_first_milestone = 0;
            // Only update if it's still 0, to ensure the first call claims a distinct milestone value
            // This helps if PARTITION_INTERVAL is very small and current_milestone could be 0.
            last_partition_milestone_.compare_exchange_strong(expected_first_milestone, PARTITION_INTERVAL,
                                                              std::memory_order_release);
        }
    }
#endif
    // --- End Auto Partition Trigger Check ---

    // --- Core Graph Modification (within graph_data_mutex_) ---
    {
        std::lock_guard<std::mutex> lock(graph_data_mutex_);

        for (const uint64_t &regionid: unique_mapped_ids_in_group) {
            active_nodes_.insert(regionid);
            partition_graph_.try_emplace(regionid);
            partition_weight_.try_emplace(regionid, 1);

            auto map_it = regionid_to_denseid_map_.find(regionid);
            if (map_it == regionid_to_denseid_map_.end()) {
                idx_t new_dense_id = next_dense_id_.fetch_add(1, std::memory_order_relaxed);
                regionid_to_denseid_map_[regionid] = new_dense_id;

                if (new_dense_id >= regionid_to_dense_map_.size()) {
                    // Resize in larger chunks to reduce frequency of reallocations
                    regionid_to_dense_map_.resize(new_dense_id + (regionid_to_dense_map_.size() / 2) + 100);
                }
                regionid_to_dense_map_[new_dense_id] = regionid;
            }
        }

        if (unique_mapped_ids_in_group.size() >= 2) {
            for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                for (size_t j = i + 1; j < unique_mapped_ids_in_group.size(); ++j) {
                    uint64_t u = unique_mapped_ids_in_group[i];
                    uint64_t v = unique_mapped_ids_in_group[j];
                    partition_graph_[u][v]++;
                    partition_graph_[v][u]++;
                }
            }
        }
    } // graph_data_mutex_ is released here

    // --- Submit Partition Task ---
#ifdef ENABLE_AUTO_PARTITION
    if (should_trigger_partition && associated_thread_pool_) {
        std::string outfile = this->partition_output_file_;
        std::string logfile = this->partition_log_file_;
        uint64_t nparts = this->num_partitions_;

        associated_thread_pool_->enqueue([this, outfile, logfile, nparts] {
            this->partition_internal_graph(outfile, logfile, nparts);
        });
    }
#endif
    // --- End Submit Partition Task ---

    idx_t final_partition_index_result = 0; // Default: no specific partition index determined
    bool decision_made = false;
    std::string cross_partition_log_message_str; {
        std::shared_lock<std::shared_mutex> lock(partition_map_mutex_);

        if (!partition_node_map.empty()) {
            // partition_counts now maps PartitionIndex (idx_t) to count (uint64_t)
            std::map<idx_t, uint64_t> partition_counts;
            uint64_t unmapped_count = 0;

            for (uint64_t region_id: unique_mapped_ids_in_group) {
                auto map_it = partition_node_map.find(region_id);
                if (map_it != partition_node_map.end()) {
                    partition_counts[map_it->second]++; // map_it->second is the PartitionIndex
                } else {
                    unmapped_count++;
                }
            }

            if (partition_counts.size() > 1) {
                decision_made = true;
                idx_t dominant_partition_idx = -1;
                uint64_t max_count = 0;

                std::stringstream counts_ss;
                counts_ss << "{ ";
                for (const auto &pair: partition_counts) {
                    // pair.first is PartitionIndex
                    counts_ss << "PartitionIndex " << pair.first << ": exist " << pair.second << " times; ";
                    if (pair.second > max_count) {
                        max_count = pair.second;
                        dominant_partition_idx = pair.first;
                    }
                }
                counts_ss << "}";
                final_partition_index_result = dominant_partition_idx;

                std::stringstream group_ss;
                group_ss << "[";
                for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                    group_ss << unique_mapped_ids_in_group[i] << (
                        i == unique_mapped_ids_in_group.size() - 1 ? "" : ", ");
                }
                group_ss << "]";

                cross_partition_log_message_str = "Cross-partition detected in " + group_ss.str() +
                                                  ". Counts per PartitionIndex: " + counts_ss.str() +
                                                  ". Choosing dominant PartitionIndex: " + std::to_string(
                                                      dominant_partition_idx) +
                                                  " based on max count (" + std::to_string(max_count) + ").";
                if (unmapped_count > 0) {
                    cross_partition_log_message_str += " Note: " + std::to_string(unmapped_count) +
                            " node(s) in the group were not found in the current partition map.";
                }
            } else if (partition_counts.size() == 1 && unmapped_count == 0) {
                decision_made = true;
                final_partition_index_result = partition_counts.begin()->first; // The only PartitionIndex present
                cross_partition_log_message_str = "Group maps entirely to PartitionIndex: " + std::to_string(
                                                      final_partition_index_result);
            } else if (partition_counts.empty() && unmapped_count > 0) {
                decision_made = true; // A decision that no mapping exists
                // final_partition_index_result remains -1
                std::stringstream group_ss;
                group_ss << "[";
                for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                    group_ss << unique_mapped_ids_in_group[i] << (
                        i == unique_mapped_ids_in_group.size() - 1 ? "" : ", ");
                }
                group_ss << "]";
                cross_partition_log_message_str = "Missing: None of the nodes in group " + group_ss.str() +
                                                  " found in the current partition map. Cannot determine dominant PartitionIndex.";
            }
            // If partition_node_map was not empty, but the group didn't match any of the above (e.g. all unmapped, but partition_counts empty),
            // decision_made might still be false, and final_partition_index_result will be -1.
        }
    } // partition_map_mutex_ (SHARED lock) is released here

    if (!cross_partition_log_message_str.empty()) {
        logger_.info(cross_partition_log_message_str);
    }

    return final_partition_index_result;
}


// ========================================================================
// MODIFIED partition_internal_graph FUNCTION
// ========================================================================
inline void NewMetis::partition_internal_graph(const std::string &output_partition_file,
                                               const std::string &log_file_path,
                                               uint64_t ComputeNodeCount) {
    std::ofstream log_stream(log_file_path, std::ios::app);
    if (!log_stream.is_open()) {
        std::cerr << "[Partition Error] Failed to open log file: " << log_file_path << std::endl;
    }
    logger_.info("Starting internal graph partitioning task (using DENSE ID snapshot).");

    std::vector<idx_t> xadj_csr;
    std::vector<idx_t> adjncy_csr;
    std::vector<idx_t> vwgt_csr;
    std::vector<idx_t> adjwgt_csr;
    std::vector<idx_t> part_csr;

    idx_t nvtx_for_metis = 0;
    const idx_t ncon_for_metis = 1;
    bool conversion_to_csr_successful = true;

    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t> > graph_snapshot;
    std::unordered_map<uint64_t, uint64_t> weight_snapshot;
    std::vector<uint64_t> dense_to_original_snapshot;
    std::unordered_map<uint64_t, idx_t> original_to_dense_snapshot;
    idx_t num_dense_ids_snapshot = 0; {
        std::lock_guard<std::mutex> lock(graph_data_mutex_);
        logger_.info("Acquired graph_data_mutex_ for creating graph snapshot.");

        num_dense_ids_snapshot = next_dense_id_.load(std::memory_order_relaxed);
        nvtx_for_metis = num_dense_ids_snapshot;

        if (nvtx_for_metis == 0) {
            logger_.info("No unique nodes mapped (nvtx_for_metis = 0 based on snapshot). Cannot partition.");
            std::ofstream out_part_empty(output_partition_file);
            if (out_part_empty.is_open()) {
                out_part_empty << "RegionID,TableID,InnerRegionID,PartitionIndex\n";
                out_part_empty.close();
            } else {
                logger_.error("Cannot open partition output file " + output_partition_file + " for empty graph.");
            } {
                std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                partition_node_map.clear();
            }
            return;
        }

        graph_snapshot = this->partition_graph_;
        weight_snapshot = this->partition_weight_;
        original_to_dense_snapshot = this->regionid_to_denseid_map_;

        if (num_dense_ids_snapshot <= this->regionid_to_dense_map_.size()) {
            dense_to_original_snapshot.assign(this->regionid_to_dense_map_.begin(),
                                              this->regionid_to_dense_map_.begin() + num_dense_ids_snapshot);
        } else {
            dense_to_original_snapshot = this->regionid_to_dense_map_;
            logger_.warning("num_dense_ids_snapshot (" + std::to_string(num_dense_ids_snapshot) +
                        ") > regionid_to_dense_map_ size (" + std::to_string(this->regionid_to_dense_map_.size()) +
                        "). Copying entire dense_map. Review synchronization.");
            if (this->regionid_to_dense_map_.size() < nvtx_for_metis) {
                nvtx_for_metis = this->regionid_to_dense_map_.size();
                logger_.info("Adjusted nvtx_for_metis to " + std::to_string(nvtx_for_metis) + " due to dense_map size.");
            }
        }
        if (nvtx_for_metis == 0 && num_dense_ids_snapshot > 0) {
            // Consistency check after adjustment
            logger_.info(
                "Error: nvtx_for_metis became 0 after dense_map size adjustment, but num_dense_ids_snapshot was > 0. Aborting."); {
                std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                partition_node_map.clear();
            }
            return;
        }

        logger_.info("Graph snapshot created. Releasing graph_data_mutex_.");
    }

    size_t total_degree_sum_csr = 0;
    if (nvtx_for_metis > 0) {
        // Only proceed if there are vertices
        for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
            if (dense_i < dense_to_original_snapshot.size()) {
                uint64_t original_id = dense_to_original_snapshot[dense_i];
                auto it = graph_snapshot.find(original_id);
                if (it != graph_snapshot.end()) {
                    total_degree_sum_csr += it->second.size();
                }
            } else {
                logger_.info(
                    "Error: dense_i " + std::to_string(dense_i) + " out of bounds for dense_to_original_snapshot (size "
                    +
                    std::to_string(dense_to_original_snapshot.size()) + ") during degree sum. Skipping.");
                conversion_to_csr_successful = false;
                break;
            }
        }
    } // else, total_degree_sum_csr remains 0, nvtx_for_metis is 0.

    if (!conversion_to_csr_successful) {
        logger_.info("Aborting partitioning due to error in degree sum calculation from snapshot."); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }

    logger_.info("Snapshot state (dense): nvtx_for_metis = " + std::to_string(nvtx_for_metis) +
                ", Total degree sum = " + std::to_string(total_degree_sum_csr));

    if (nvtx_for_metis == 0) {
        // Double check, could have been set to 0 if snapshot was inconsistent
        logger_.info("No vertices to partition after snapshot processing. Writing empty partition file.");
        std::ofstream out_part_empty(output_partition_file);
        if (out_part_empty.is_open()) {
            out_part_empty << "RegionID,TableID,InnerRegionID,PartitionIndex\n";
            out_part_empty.close();
        } {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }

    logger_.info("Starting conversion of snapshot to METIS CSR format.");

    try {
        xadj_csr.resize(nvtx_for_metis + 1);
        vwgt_csr.resize(nvtx_for_metis * ncon_for_metis); // ncon_for_metis is 1
        adjncy_csr.reserve(total_degree_sum_csr);
        adjwgt_csr.reserve(total_degree_sum_csr);
        part_csr.resize(nvtx_for_metis);
    } catch (const std::bad_alloc &e) {
        logger_.info("Memory allocation failed for CSR arrays from snapshot: " + std::string(e.what()));
        conversion_to_csr_successful = false;
    }

    if (conversion_to_csr_successful) {
        idx_t current_edge_ptr = 0;
        for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
            xadj_csr[dense_i] = current_edge_ptr;

            if (dense_i >= dense_to_original_snapshot.size()) {
                logger_.info(
                    "Error: Dense ID " + std::to_string(dense_i) +
                    " out of bounds in dense_to_original_snapshot during CSR population.");
                conversion_to_csr_successful = false;
                break;
            }
            uint64_t original_id = dense_to_original_snapshot[dense_i];

            auto weight_it = weight_snapshot.find(original_id);
            vwgt_csr[dense_i] = (weight_it != weight_snapshot.end()) ? static_cast<idx_t>(weight_it->second) : 1;
            // Since ncon_for_metis is 1

            auto neighbors_map_it = graph_snapshot.find(original_id);
            if (neighbors_map_it != graph_snapshot.end()) {
                const auto &neighbors = neighbors_map_it->second;
                std::map<idx_t, uint64_t> sorted_dense_neighbors;

                for (const auto &[neighbor_original_id, edge_weight]: neighbors) {
                    auto dense_id_it = original_to_dense_snapshot.find(neighbor_original_id);
                    if (dense_id_it != original_to_dense_snapshot.end()) {
                        sorted_dense_neighbors[dense_id_it->second] = edge_weight;
                    } else {
                        logger_.info(
                            "Warning: Neighbor " + std::to_string(neighbor_original_id) + " of node " + std::to_string(
                                original_id) +
                            " (dense " + std::to_string(dense_i) +
                            ") not found in original_to_dense_snapshot. Skipping edge.");
                    }
                }

                for (const auto &[neighbor_dense_id, edge_weight]: sorted_dense_neighbors) {
                    if (neighbor_dense_id >= nvtx_for_metis || neighbor_dense_id < 0) {
                        logger_.info(
                            "Error: Invalid neighbor dense ID " + std::to_string(neighbor_dense_id) + " (range 0 to " +
                            std::to_string(nvtx_for_metis - 1) + ") for node " + std::to_string(original_id) + ".");
                        conversion_to_csr_successful = false;
                        break;
                    }
                    idx_t current_edge_weight = (edge_weight <= 0) ? 1 : static_cast<idx_t>(edge_weight);
                    if (edge_weight <= 0) {
                        logger_.info(
                            "Warning: Non-positive edge weight (" + std::to_string(edge_weight) +
                            ") for edge involving original node " +
                            std::to_string(original_id) + ". Using weight 1.");
                    }
                    adjncy_csr.push_back(neighbor_dense_id);
                    adjwgt_csr.push_back(current_edge_weight);
                    current_edge_ptr++;
                }
            }
            if (!conversion_to_csr_successful) break;
        }
        if (conversion_to_csr_successful) {
            xadj_csr[nvtx_for_metis] = current_edge_ptr;
            if (current_edge_ptr != total_degree_sum_csr) {
                logger_.info("Warning: CSR edge pointer count (" + std::to_string(current_edge_ptr) +
                            ") does not match calculated total degree sum (" + std::to_string(total_degree_sum_csr) +
                            "). Mismatch in dense graph structure from snapshot.");
            } else {
                logger_.info(
                    "CSR conversion of snapshot successful. Total entries in adjncy/adjwgt: " + std::to_string(
                        current_edge_ptr));
            }
        }
    }

    if (!conversion_to_csr_successful) {
        logger_.info("Aborting partitioning due to CSR conversion failure from snapshot."); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }

    idx_t nWeights_metis = ncon_for_metis;
    idx_t nParts_metis = ComputeNodeCount;

    if (nParts_metis <= 0) {
        logger_.error(
            " Invalid number of partitions requested (" + std::to_string(nParts_metis) + ") for METIS. Aborting."); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }
    if (nParts_metis == 1) {
        logger_.warning(" Requested 1 partition. METIS call skipped, assigning all nodes to PartitionIndex 0.");
        std::ofstream out_part_one(output_partition_file);
        if (!out_part_one.is_open()) {
            logger_.error("Cannot open partition output file " + output_partition_file); {
                std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                partition_node_map.clear();
            }
            return;
        }
        out_part_one << "RegionID,TableID,InnerRegionID,PartitionIndex\n"; {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
            for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
                if (dense_i < dense_to_original_snapshot.size()) {
                    uint64_t original_id = dense_to_original_snapshot[dense_i];
                    Region region(original_id);
                    out_part_one << original_id << "," << region.getTableId() << "," << region.getInnerRegionId() <<
                            ",0\n";
                    partition_node_map[original_id] = 0; // Assign to PartitionIndex 0
                }
            }
        }
        out_part_one.close();
        logger_.info("Partition result (all to PartitionIndex 0) written. Router map populated.");
        return;
    }
    if (nParts_metis > nvtx_for_metis) {
        logger_.warning("Requested partitions (" + std::to_string(nParts_metis) +
                    ") > number of dense vertices (" + std::to_string(nvtx_for_metis) +
                    "). Reducing partitions to nvtx_for_metis.");
        nParts_metis = nvtx_for_metis;
        if (nParts_metis == 1) {
            logger_.info("Partitions reduced to 1. Assigning all nodes to PartitionIndex 0.");
            std::ofstream out_part_one_reduced(output_partition_file);
            if (!out_part_one_reduced.is_open()) {
                logger_.error("Cannot open partition output file " + output_partition_file); {
                    std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                    partition_node_map.clear();
                }
                return;
            }
            out_part_one_reduced << "RegionID,TableID,InnerRegionID,PartitionIndex\n"; {
                std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                partition_node_map.clear();
                for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
                    if (dense_i < dense_to_original_snapshot.size()) {
                        uint64_t original_id = dense_to_original_snapshot[dense_i];
                        Region region(original_id);
                        out_part_one_reduced << original_id << "," << region.getTableId() << "," << region.
                                getInnerRegionId() << ",0\n";
                        partition_node_map[original_id] = 0; // Assign to PartitionIndex 0
                    }
                }
            }
            out_part_one_reduced.close();
            logger_.info("Partition result (all to PartitionIndex 0 after reduction) written. Router map populated.");
            return;
        }
        if (nParts_metis <= 0 && nvtx_for_metis > 0) {
            logger_.error(
                "Cannot partition into " + std::to_string(nParts_metis) +
                " partitions after adjustment. Aborting."); {
                std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                partition_node_map.clear();
            }
            return;
        }
    }

    idx_t objval_metis;
    idx_t *vwgt_metis_ptr = vwgt_csr.empty() ? nullptr : vwgt_csr.data();
    idx_t *adjwgt_metis_ptr = adjwgt_csr.empty() ? nullptr : adjwgt_csr.data();

    logger_.info("Calling METIS_PartGraphKway with nparts = " + std::to_string(nParts_metis) +
                ", nvtx (dense snapshot) = " + std::to_string(nvtx_for_metis) +
                ", ncon = " + std::to_string(nWeights_metis) + "...");

    int metis_ret = METIS_PartGraphKway(
        &nvtx_for_metis,
        &nWeights_metis,
        xadj_csr.data(),
        adjncy_csr.data(),
        vwgt_metis_ptr,
        nullptr, // vsz
        adjwgt_metis_ptr,
        &nParts_metis,
        nullptr, // tpwgts
        nullptr, // ubvec
        nullptr, // options
        &objval_metis,
        part_csr.data()
    );

    if (metis_ret != METIS_OK) {
        logger_.info("METIS partitioning failed with error code: " + std::to_string(metis_ret)); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }
    logger_.info("METIS partitioning successful! Objective value (edge cut/volume): " + std::to_string(objval_metis));

    std::ofstream out_part_final(output_partition_file);
    if (!out_part_final.is_open()) {
        logger_.error("Cannot open partition output file " + output_partition_file); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }
    out_part_final << "RegionID,TableID,InnerRegionID,PartitionIndex\n";
    for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
        if (dense_i < dense_to_original_snapshot.size()) {
            uint64_t original_id = dense_to_original_snapshot[dense_i];
            idx_t partition_index_val = part_csr[dense_i]; // Value from METIS
            Region region(original_id);
            out_part_final << original_id << "," << region.getTableId() << "," << region.getInnerRegionId() << "," <<
                    partition_index_val << "\n";
        } else {
            logger_.error(
                "Dense ID " + std::to_string(dense_i) +
                " out of bounds in dense_to_original_snapshot during file writing.");
        }
    }
    out_part_final.close();
    logger_.info(
        "Partition results (RegionID,TableID,InnerRegionID,PartitionIndex) successfully written to " +
        output_partition_file); {
        std::unique_lock<std::shared_mutex> lock(partition_map_mutex_);
        logger_.info("Acquired partition_map_mutex_ for populating router rules.");
        partition_node_map.clear();

        for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
            if (dense_i >= dense_to_original_snapshot.size()) {
                logger_.error(
                    "Dense ID " + std::to_string(dense_i) +
                    " out of bounds in dense_to_original_snapshot during router rule creation.");
                continue;
            }
            uint64_t current_original_id = dense_to_original_snapshot[dense_i];
            idx_t assigned_partition_index = part_csr[dense_i];
            partition_node_map[current_original_id] = assigned_partition_index;
        }
        logger_.info("Router rules map (OriginalRegionID -> PartitionIndex) populated.");
    }

    logger_.info("Finished internal graph partitioning task.");
}
