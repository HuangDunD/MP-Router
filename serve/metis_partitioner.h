#include <vector>
#include <string>
#include <unordered_map>
#include <set>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm> // Required for std::max_element, std::max
#include <mutex>      // Required for std::mutex and std::lock_guard
#include <numeric>    // Required for std::iota (optional)
#include <iterator>   // Required for std::inserter
#include <map>        // Included for potential ordered iteration if needed, though unordered_map is used
#include <chrono>
#include <iomanip>
#include <ctime>
#include <atomic>     // Required for automatic partitioning counters

#include "region/region.h"


// Forward declarations
class ThreadPool; // Assume ThreadPool class is defined elsewhere

// Include metis.h (ensure it's available in your include paths)
#include <metis.h>


// Enable automatic partitioning via preprocessor directive
#define ENABLE_AUTO_PARTITION

// --- Logging Helper Function ---
inline void log_message(const std::string &message, std::ostream &log_stream) {
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    std::tm now_tm = *std::localtime(&now_c); // Use non-thread-safe localtime

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::stringstream ss;
    ss << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    std::string log_entry = "[" + ss.str() + "] " + message;

    if (log_stream.good()) {
        log_stream << log_entry << std::endl;
    } else {
        std::cerr << "[Logging Error] Log stream is not good for message: " << message << std::endl;
    }
}


class NewMetis {
public:
    NewMetis() = default;

    // --- Automatic Partitioning Setup Functions ---
    /**
     * @brief Sets the thread pool used for asynchronous partitioning tasks.
     * @param pool Pointer to an existing ThreadPool instance.
     */
    void set_thread_pool(ThreadPool *pool);

    /**
     * @brief Configures parameters for automatic partitioning.
     * @param output_file Path for the partition result file (CSV).
     * @param log_file Path for the partitioning log file.
     * @param num_parts The desired number of partitions for METIS.
     */
    void set_partition_parameters(std::string output_file, std::string log_file, uint64_t num_parts);

    // --- End Automatic Partitioning Setup ---

    /**
     * @brief Adds a group of unique, pre-mapped IDs to the internal graph representation.
     * Creates edges between all pairs within the group (fully connected subgraph).
     * If an edge already exists, its weight is incremented; otherwise, it's created with weight 1.
     * The graph is treated as undirected (weight for (u,v) and (v,u) are updated together).
     * Triggers asynchronous partitioning via the ThreadPool at specified intervals if ENABLE_AUTO_PARTITION is defined.
     * Performs a check for cross-partition access based on the *last completed* partitioning result.
     * This function is thread-safe.
     * @param unique_mapped_ids_in_group Vector of unique integer IDs appearing together.
     */
    uint64_t build_internal_graph(const std::vector<uint64_t> &unique_mapped_ids_in_group);

    /**
     * @brief Partitions the internally stored graph using METIS_PartGraphKway.
     * This function is thread-safe (acquires lock only for CSR conversion and map update).
     * Typically called asynchronously by build_internal_graph when auto-partitioning is enabled.
     * Considers edge weights stored internally.
     * Populates the partition_node_map based on the results.
     * @param output_partition_file Path for the partition result file (CSV: NodeID,PartitionID).
     * @param log_file_path Path for the log file where messages will be appended.
     * @param ComputeNodeCount The desired number of partitions (nParts for METIS).
     */
    void partition_internal_graph(const std::string &output_partition_file,
                                  const std::string &log_file_path,
                                  uint64_t ComputeNodeCount);

private:
    // Internal Graph Representation
    std::set<uint64_t> active_nodes_; // Stores IDs of nodes actually present
    // Adjacency list storing neighbors and edge weights (u -> {v1 -> weight1, v2 -> weight2, ...})
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t> > partition_graph_;
    std::unordered_map<uint64_t, uint64_t> partition_weight_; // Node weights (id -> weight), default 1
    // Map original node ID (0..max_id) to the primary node ID of its partition.
    // Updated AFTER successful partitioning. Requires graph_mutex_ for safe access.
    std::unordered_map<uint64_t, uint64_t> partition_node_map;

    // Mutex for thread safety protecting access to the graph members AND partition_node_map
    mutable std::mutex graph_mutex_; // mutable allows locking in const methods

    // --- Automatic Partitioning Members ---
    std::atomic<uint64_t> build_call_counter_{0}; // Counts calls to build_internal_graph
    std::atomic<uint64_t> last_partition_milestone_{0}; // Tracks the last milestone that triggered a partition
    ThreadPool *associated_thread_pool_ = nullptr; // Pointer to the thread pool for async tasks
    std::string partition_output_file_ = "graph_partitions.csv"; // Default output file
    std::string partition_log_file_ = "partitioning.log"; // Default log file
    uint64_t num_partitions_ = 8; // Default number of partitions
    static const uint64_t PARTITION_INTERVAL = 1000; // Trigger partition every 1000 calls


    // 新增的映射表和计数器
    std::unordered_map<uint64_t, idx_t> regionid_to_denseid_map_; // 从原始 Region ID 到稠密 ID 的映射
    std::vector<uint64_t> regionid_to_dense_map_; // 从稠密 ID 到原始 Region ID 的映射 (用于反向查找)
    std::atomic<idx_t> next_dense_id_{0}; // 用于生成稠密 ID 的原子计数器 (从 0 开始)


    // --- End Automatic Partitioning Members ---

    /**
     * @brief Helper to get graph vertex count (node count based on max ID).
     * **Must be called while holding graph_mutex_**.
     * @return uint64_t nvtx (0..max_id+1)
     */
    uint64_t get_graph_size_unsafe() const {
        // 图的大小现在是已分配的稠密 ID 的数量
        return next_dense_id_.load(std::memory_order_relaxed);
    }
};

// --- Inline Implementations ---

inline void NewMetis::set_thread_pool(ThreadPool *pool) {
    associated_thread_pool_ = pool;
}

inline void NewMetis::set_partition_parameters(std::string output_file, std::string log_file, uint64_t num_parts) {
    partition_output_file_ = std::move(output_file);
    partition_log_file_ = std::move(log_file);
    if (num_parts > 0) {
        num_partitions_ = num_parts;
    } else {
        std::cerr << "[Warning] Invalid number of partitions specified (" << num_parts << "). Using default: " <<
                num_partitions_ << std::endl;
    }
}

// ========================================================================
// MODIFIED build_internal_graph FUNCTION
// ========================================================================
inline uint64_t NewMetis::build_internal_graph(const std::vector<uint64_t> &unique_mapped_ids_in_group) {
    if (unique_mapped_ids_in_group.empty()) {
        return -1; // Nothing to do for empty input
    }

    // --- Automatic Partition Trigger Logic ---
    uint64_t current_call_count = 0; // Only incremented if auto-partitioning is enabled
    bool should_trigger_partition = false;
#ifdef ENABLE_AUTO_PARTITION // Check if the macro is defined
    if (associated_thread_pool_ != nullptr) {
        // Check if async partitioning is possible
        current_call_count = ++build_call_counter_; // Increment atomically
        uint64_t current_milestone = (current_call_count / PARTITION_INTERVAL) * PARTITION_INTERVAL;

        // Check if we've crossed a new milestone boundary
        if (current_milestone > 0) {
            uint64_t expected_last = last_partition_milestone_.load(std::memory_order_acquire);
            if (current_milestone > expected_last) {
                // Attempt to claim this milestone; only one thread should succeed
                if (last_partition_milestone_.compare_exchange_strong(expected_last, current_milestone,
                                                                      std::memory_order_acq_rel)) {
                    should_trigger_partition = true;
                    // Log triggering inside the function or let the partition function log its start
                    // std::cout << "[AutoTrigger] Build call count " << current_call_count
                    //           << " triggered partitioning for milestone " << current_milestone << "." << std::endl;
                }
            }
        }
    }
#endif
    // --- End Auto Partition Trigger Check ---


    // --- Core Graph Modification (within lock) ---
    {
        std::lock_guard<std::mutex> lock(graph_mutex_); // Scope for lock_guard

        // --- Add/Ensure Nodes Exist AND Handle ID Mapping ---
        for (const uint64_t &regionid: unique_mapped_ids_in_group) {
            // Use meaningful name
            // 1. Ensure node exists in graph structures (using original regionid)
            active_nodes_.insert(regionid);
            partition_graph_.try_emplace(regionid);
            partition_weight_.try_emplace(regionid, 1);

            // 2. Ensure mapping to dense ID exists
            auto map_it = regionid_to_denseid_map_.find(regionid);
            if (map_it == regionid_to_denseid_map_.end()) {
                // New regionid encountered, assign a dense ID
                idx_t new_dense_id = next_dense_id_.fetch_add(1, std::memory_order_relaxed);
                regionid_to_denseid_map_[regionid] = new_dense_id;

                // Update reverse map.
                if (new_dense_id >= regionid_to_dense_map_.size()) {
                    regionid_to_dense_map_.resize(new_dense_id + 1);
                }
                regionid_to_dense_map_[new_dense_id] = regionid; // Store original ID
            }
        }

        // --- Add/Update Edges (form a clique within the group) ---
        // This logic remains the same, operating on original regionids (u, v)
        if (unique_mapped_ids_in_group.size() >= 2) {
            for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                for (size_t j = i + 1; j < unique_mapped_ids_in_group.size(); ++j) {
                    uint64_t u = unique_mapped_ids_in_group[i]; // original regionid
                    uint64_t v = unique_mapped_ids_in_group[j]; // original regionid
                    partition_graph_[u][v]++; // Increment weight using original IDs
                    partition_graph_[v][u]++; // Symmetric update
                }
            }
        }
    } // graph_mutex_ is released here


    // --- Submit Partition Task (if triggered and pool exists) ---
#ifdef ENABLE_AUTO_PARTITION
    if (should_trigger_partition && associated_thread_pool_) {
        // Capture necessary parameters by value for the lambda
        std::string outfile = partition_output_file_;
        std::string logfile = partition_log_file_;
        uint64_t nparts = num_partitions_;

        // Enqueue the partitioning task to run asynchronously
        associated_thread_pool_->enqueue([this, outfile, logfile, nparts] {
            this->partition_internal_graph(outfile, logfile, nparts); // Call the partitioning method
        });
    }
#endif
    // --- End Submit Partition Task ---

    // ========================================================================
    // START: Cross-Partition Access Check and Logging (基于最新的已完成的分区结果)
    // ========================================================================
    {
        // New scope for lock guard to access partition_node_map safely
        std::lock_guard<std::mutex> lock(graph_mutex_); // Acquire lock

        // Check if partitioning has run at least once and the map is populated
        if (!partition_node_map.empty()) {
            std::map<uint64_t, uint64_t> partition_counts; // Map<PrimaryNodeID (Partition), Count>
            uint64_t unmapped_count = 0; // Count nodes in the group not found in the current map

            // Count occurrences of each primary partition ID within the input group
            for (uint64_t region_id: unique_mapped_ids_in_group) {
                auto map_it = partition_node_map.find(region_id);
                if (map_it != partition_node_map.end()) {
                    // Node found in the map, increment count for its primary partition ID
                    partition_counts[map_it->second]++;
                } else {
                    // Node from the input group is not (yet) in the partition map
                    // This can happen if the node was just added and partitioning hasn't completed
                    // or if the node ID was never part of a partitioning run.
                    unmapped_count++;
                }
            }

            // Check if the group accesses more than one partition (based on mapped nodes)
            if (partition_counts.size() > 1) {
                // Cross-partition access detected
                uint64_t dominant_partition_id = -1;
                uint64_t max_count = 0;

                std::stringstream counts_ss;
                counts_ss << "{ ";
                for (const auto &pair: partition_counts) {
                    counts_ss << "Partition(RegionID " << pair.first << "): exist " << pair.second << " times; ";
                    if (pair.second > max_count) {
                        max_count = pair.second;
                        dominant_partition_id = pair.first;
                    }
                }
                counts_ss << "}";

                // Log the decision
                std::ofstream log_stream(partition_log_file_, std::ios::app); // Open log file in append mode
                if (log_stream.is_open()) {
                    std::stringstream group_ss;
                    group_ss << "[";
                    for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                        group_ss << unique_mapped_ids_in_group[i] << (i == unique_mapped_ids_in_group.size() - 1
                                                                          ? ""
                                                                          : ", ");
                    }
                    group_ss << "]";

                    std::string log_msg = "Cross-partition detected in " + group_ss.str() +
                                          ". Counts per partition: " + counts_ss.str() +
                                          ". Choosing dominant partition (PegionID): " + std::to_string(
                                              dominant_partition_id) +
                                          " based on max count (" + std::to_string(max_count) + ").";
                    if (unmapped_count > 0) {
                        log_msg += " Note: " + std::to_string(unmapped_count) +
                                " node(s) in the group were not found in the current partition map.";
                    }
                    log_message(log_msg, log_stream);
                    log_stream.close(); // Close the log stream
                } else {
                    std::cerr << "[Logging Error] Could not open log file '" << partition_log_file_ <<
                            "' for cross-partition log." << std::endl;
                }

                return dominant_partition_id;
            } else if (partition_counts.size() == 1 && unmapped_count == 0) {
                // Optional: Log if all nodes map to the same partition (no cross-access)
                std::ofstream log_stream(partition_log_file_, std::ios::app);
                if (log_stream.is_open()) {
                    log_message(
                        "Group maps entirely to partition (PrimaryID): " + std::to_string(
                            partition_counts.begin()->first), log_stream);
                    log_stream.close();
                }
                return partition_counts.begin()->first;
            } else if (partition_counts.empty() && unmapped_count > 0) {
                // Optional: Log if none of the nodes were found in the map
                std::ofstream log_stream(partition_log_file_, std::ios::app);
                if (log_stream.is_open()) {
                    std::stringstream group_ss;
                    group_ss << "[";
                    for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                        group_ss << unique_mapped_ids_in_group[i] << (i == unique_mapped_ids_in_group.size() - 1
                                                                          ? ""
                                                                          : ", ");
                    }
                    group_ss << "]";
                    log_message(
                        "Missing :None of the nodes in group " + group_ss.str() +
                        " found in the current partition map. Cannot determine dominant partition.", log_stream);
                    log_stream.close();
                }
                return -1;
            }
        } // else: partition_node_map is empty, do nothing.
    } // graph_mutex_ is released here
    // ========================================================================
    // END: Cross-Partition Access Check
    // ========================================================================
    return static_cast<uint64_t>(-1);
}


// --- Implementation for partitioning the internal graph ---
// ========================================================================
// MODIFIED partition_internal_graph FUNCTION (to populate partition_node_map)
// ========================================================================
inline void NewMetis::partition_internal_graph(const std::string &output_partition_file,
                                               const std::string &log_file_path,
                                               uint64_t ComputeNodeCount) {
    // ComputeNodeCount is desired nParts
    // --- Open Log File (append mode) ---
    std::ofstream log_stream(log_file_path, std::ios::app);
    if (!log_stream.is_open()) {
        std::cerr << "[Partition Error] Failed to open log file: " << log_file_path << std::endl;
        // Continue without file logging, or return depending on requirements
    }
    // Updated log message
    log_message("Starting internal graph partitioning task (using dense IDs).", log_stream);

    // --- Variables needed for METIS CSR format ---
    std::vector<idx_t> xadj;
    std::vector<idx_t> adjncy;
    std::vector<idx_t> vwgt;
    std::vector<idx_t> adjwgt;
    idx_t nvtx_metis = 0; // Number of DENSE vertices for METIS (0..N-1)
    idx_t ncon_metis = 1; // Number of weights per vertex (constraints)
    bool conversion_success = true;
    std::vector<idx_t> part; // Output array for partition assignments (indexed by dense_id)

    // --- Convert Internal Graph to METIS CSR Format (within lock) ---
    {
        // Scope for lock_guard protecting graph data AND mapping tables
        std::lock_guard<std::mutex> lock(graph_mutex_); // LOCK acquired
        log_message("Acquired graph lock for CSR conversion.", log_stream);

        // Get DENSE vertex count (number of unique regions mapped)
        nvtx_metis = get_graph_size_unsafe();

        if (nvtx_metis <= 0) {
            // Updated log message
            log_message("No unique nodes mapped (nvtx = 0). Cannot partition.", log_stream);
            // Write an empty partition file for consistency
            std::ofstream outpartition(output_partition_file);
            if (outpartition.is_open()) {
                outpartition << "NodeID,PartitionID\n"; // Header only
                outpartition.close();
            } else {
                log_message("Error: Cannot open partition output file " + output_partition_file + " for empty graph.",
                            log_stream);
            }
            // Clear the partition map as the graph is effectively empty for partitioning
            partition_node_map.clear(); // Ensure map is clear
            log_message("Released graph lock (empty graph).", log_stream);
            return; // LOCK released by lock_guard destructor
        }

        // --- Calculate total degree sum needed for CSR arrays (optional but good for reserve) ---
        // Iterate through the mapped dense IDs to sum degrees based on original IDs
        size_t total_degree_sum_dense = 0;
        for (idx_t dense_i = 0; dense_i < nvtx_metis; ++dense_i) {
            // Safely get original ID (ensure dense_i is within bounds of the reverse map)
            if (dense_i < regionid_to_dense_map_.size()) {
                uint64_t original_id = regionid_to_dense_map_[dense_i];
                // Check if node exists in the actual graph adjacency list
                if (partition_graph_.count(original_id)) {
                    total_degree_sum_dense += partition_graph_.at(original_id).size();
                }
                // Else: node was mapped but has no edges (degree 0), add 0 to sum.
            } else {
                // This indicates an inconsistency if dense_i < nvtx_metis but not in map
                // This might happen if next_dense_id_ was incremented but the maps weren't updated yet? (Shouldn't happen if lock is held correctly)
                log_message(
                    "Warning: Dense ID " + std::to_string(dense_i) +
                    " not found in reverse map during degree calculation. Inconsistency possible.", log_stream);
            }
        }

        // Updated log message
        log_message("Graph state (dense): nvtx = " + std::to_string(nvtx_metis) +
                    ", Total degree sum = " + std::to_string(total_degree_sum_dense), log_stream);
        log_message("Starting conversion to METIS CSR format using dense IDs.", log_stream);

        // --- Allocate CSR structures (using nvtx_metis - dense count) ---
        try {
            xadj.resize(nvtx_metis + 1);
            vwgt.resize(nvtx_metis * ncon_metis); // Size based on dense count
            adjncy.reserve(total_degree_sum_dense); // Reserve based on calculated sum
            adjwgt.reserve(total_degree_sum_dense);
            part.resize(nvtx_metis); // Size based on dense count
        } catch (const std::bad_alloc &e) {
            log_message("Memory allocation failed for CSR arrays: " + std::string(e.what()), log_stream);
            conversion_success = false;
            // Clear the partition map on failure
            partition_node_map.clear(); // Ensure map is clear
        }

        if (conversion_success) {
            idx_t current_edge_ptr = 0; // Index into adjncy/adjwgt

            // --- Populate CSR Arrays (Iterate through DENSE IDs 0 to nvtx-1) ---
            for (idx_t dense_i = 0; dense_i < nvtx_metis; ++dense_i) {
                xadj[dense_i] = current_edge_ptr; // xadj index is dense_id

                // Get the original regionid corresponding to this dense ID
                // Add boundary check for safety
                if (dense_i >= regionid_to_dense_map_.size()) {
                    log_message(
                        "Error: Dense ID " + std::to_string(dense_i) +
                        " out of bounds in reverse map during CSR population.", log_stream);
                    conversion_success = false;
                    partition_node_map.clear();
                    break; // Exit loop on error
                }
                uint64_t original_id = regionid_to_dense_map_[dense_i];

                // Get vertex weight using original_id to look up in partition_weight_
                // Use dense_i as index for vwgt array
                vwgt[dense_i * ncon_metis + 0] = partition_weight_.count(original_id)
                                                     ? static_cast<idx_t>(partition_weight_.at(original_id))
                                                     : 1;
                // Default weight 1 if not found (should exist if node added correctly)

                // Get neighbors using original_id from partition_graph_
                if (partition_graph_.count(original_id)) {
                    const auto &neighbors = partition_graph_.at(original_id); // Neighbors are <original_id, weight>

                    // Optional: Sort neighbors based on their DENSE IDs for determinism
                    // Create a temporary map: <dense_id, weight>
                    std::map<idx_t, uint64_t> sorted_dense_neighbors;
                    for (const auto &[neighbor_original_id, edge_weight]: neighbors) {
                        // Find dense ID of the neighbor using the forward map
                        auto neighbor_it = regionid_to_denseid_map_.find(neighbor_original_id);
                        if (neighbor_it != regionid_to_denseid_map_.end()) {
                            // Store neighbor's dense ID and the original edge weight
                            sorted_dense_neighbors[neighbor_it->second] = edge_weight;
                        } else {
                            // Neighbor exists in graph structure but not in dense ID mapping - indicates an inconsistency.
                            log_message(
                                "Warning: Neighbor " + std::to_string(neighbor_original_id) + " of node " +
                                std::to_string(original_id) + " (dense " + std::to_string(dense_i) +
                                ") not found in dense ID map. Skipping edge.", log_stream);
                            // Skipping this edge:
                            continue;
                        }
                    }

                    // Add sorted dense neighbors to CSR arrays
                    for (const auto &[neighbor_dense_id, edge_weight]: sorted_dense_neighbors) {
                        // Basic validation on neighbor dense ID range
                        if (neighbor_dense_id < 0 || neighbor_dense_id >= nvtx_metis) {
                            log_message(
                                "Error: Invalid neighbor dense ID " + std::to_string(neighbor_dense_id) +
                                " found for node " + std::to_string(original_id) + " (dense " + std::to_string(dense_i)
                                + ").", log_stream);
                            conversion_success = false;
                            partition_node_map.clear();
                            break; // Exit inner neighbor loop
                        }
                        // Handle non-positive edge weights (assign weight 1)
                        idx_t weight_to_add = (edge_weight <= 0) ? 1 : static_cast<idx_t>(edge_weight);
                        if (edge_weight <= 0) {
                            log_message(
                                "Warning: Non-positive edge weight (" + std::to_string(edge_weight) +
                                ") found for edge involving original node " + std::to_string(original_id) +
                                ". Using weight 1 instead.", log_stream);
                        }

                        adjncy.push_back(neighbor_dense_id); // Add DENSE neighbor ID to adjncy
                        adjwgt.push_back(weight_to_add); // Add edge weight to adjwgt
                        current_edge_ptr++;
                    }
                    if (!conversion_success) break; // Exit outer loop if inner loop failed
                }
                // Else: Node represented by dense_i has no neighbors in partition_graph_ (degree 0)
            } // end for loop (dense_i < nvtx_metis)

            // Set final xadj entry only if conversion didn't fail mid-loop
            if (conversion_success) {
                xadj[nvtx_metis] = current_edge_ptr;
            }


            // --- Verification after loop ---
            // Compare current_edge_ptr with total_degree_sum_dense
            if (!conversion_success) {
                log_message("CSR conversion failed due to errors.", log_stream);
                // partition_node_map should have been cleared on error
            } else if (current_edge_ptr != total_degree_sum_dense) {
                // Updated log message
                log_message("Warning: CSR edge pointer count (" + std::to_string(current_edge_ptr) +
                            ") does not match calculated total degree sum (" + std::to_string(total_degree_sum_dense) +
                            "). Mismatch in dense graph structure.",
                            log_stream);
                // Continue, but be aware
            } else {
                // Updated log message
                log_message(
                    "CSR conversion successful using dense IDs. Total entries in adjncy/adjwgt: " + std::to_string(
                        current_edge_ptr), log_stream);
            }
        } // end if(conversion_success) before loop

        log_message("Released graph lock (before calling METIS).", log_stream);
    } // End of lock scope (graph_mutex_ released)


    // --- Check CSR Conversion Success ---
    if (!conversion_success) {
        log_message("Aborting partitioning due to CSR conversion failure.", log_stream);
        // partition_node_map should be clear already
        return;
    }
    if (nvtx_metis <= 0) {
        // This case handled earlier inside the lock
        // log_message("Skipping METIS call as nvtx is zero or negative after conversion check.", log_stream);
        return; // partition_node_map should be clear already
    }


    // --- Prepare and Call METIS ---
    idx_t nWeights = ncon_metis;
    idx_t nParts = ComputeNodeCount; // Use the input parameter

    // --- Validate nParts ---
    // Logic mostly unchanged, but comparisons use dense nvtx_metis
    if (nParts <= 0) {
        log_message("Error: Invalid number of partitions requested (" + std::to_string(nParts) + "). Aborting.",
                    log_stream);
        std::lock_guard<std::mutex> lock(graph_mutex_);
        partition_node_map.clear();
        return;
    }
    if (nParts == 1) {
        log_message("Warning: Requested 1 partition. METIS call skipped, assigning all nodes to partition 0.",
                    log_stream);
        // Create a dummy result file mapping ORIGINAL IDs to partition 0
        std::ofstream outpartition(output_partition_file);
        if (!outpartition.is_open()) {
            log_message("Error: Cannot open partition output file " + output_partition_file, log_stream);
            std::lock_guard<std::mutex> lock(graph_mutex_);
            partition_node_map.clear();
            return;
        }
        outpartition << "RegionID,PartitionIndex\n";
        uint64_t primary_id_for_part0 = 0; // Default primary ID
        {
            // Need lock to access reverse map and update partition_node_map
            std::lock_guard<std::mutex> lock(graph_mutex_);
            // Choose the original ID corresponding to dense ID 0 as the primary representative for partition 0
            if (nvtx_metis > 0 && !regionid_to_dense_map_.empty()) {
                primary_id_for_part0 = regionid_to_dense_map_[0];
            } // else remains 0 or handle error

            partition_node_map.clear(); // Clear previous results
            // Iterate through DENSE IDs to find all ORIGINAL IDs
            for (idx_t dense_i = 0; dense_i < nvtx_metis; ++dense_i) {
                if (dense_i < regionid_to_dense_map_.size()) {
                    // Safety check
                    uint64_t original_id = regionid_to_dense_map_[dense_i];
                    outpartition << original_id << ",0\n"; // Write original ID
                    partition_node_map[original_id] = primary_id_for_part0; // Map original ID to primary original ID
                }
            }
        }
        outpartition.close();
        log_message("Partition result (all 0) written to " + output_partition_file, log_stream);
        // Updated log message
        log_message(
            "Router rules map populated (all nodes map to primary original ID: " + std::to_string(primary_id_for_part0)
            + ").", log_stream);
        return;
    }
    // Check if nParts > number of dense vertices
    if (nParts > nvtx_metis) {
        log_message("Warning: Requested partitions (" + std::to_string(nParts) +
                    ") > number of dense vertices (" + std::to_string(nvtx_metis) +
                    "). Reducing partitions to nvtx.", log_stream);
        nParts = nvtx_metis;
        // Handle if nParts becomes 1 after adjustment (similar to above)
        if (nParts == 1) {
            // (Repeat or refactor the nParts==1 logic here)
            log_message("Partitions reduced to 1. Assigning all nodes to partition 0.", log_stream);
            std::ofstream outpartition(output_partition_file);
            if (!outpartition.is_open()) {
                /* log error, lock, clear partition_node_map, return */
            }
            outpartition << "RegionID,PartitionIndex\n";
            uint64_t primary_id_for_part0 = (nvtx_metis > 0 && regionid_to_dense_map_.size() > 0)
                                                ? regionid_to_dense_map_[0]
                                                : 0; {
                std::lock_guard<std::mutex> lock(graph_mutex_);
                partition_node_map.clear();
                for (idx_t dense_i = 0; dense_i < nvtx_metis; ++dense_i) {
                    if (dense_i < regionid_to_dense_map_.size()) {
                        uint64_t original_id = regionid_to_dense_map_[dense_i];
                        outpartition << original_id << ",0\n";
                        partition_node_map[original_id] = primary_id_for_part0;
                    }
                }
            }
            outpartition.close();
            log_message(
                "Partition result (all 0) written. Router map populated mapping all nodes to primary " + std::to_string(
                    primary_id_for_part0), log_stream);
            return;
        }
        if (nParts <= 0) {
            // Should not happen if nvtx_metis > 0
            log_message(
                "Error: Cannot partition into " + std::to_string(nParts) + " partitions after adjustment. Aborting.",
                log_stream);
            std::lock_guard<std::mutex> lock(graph_mutex_);
            partition_node_map.clear();
            return;
        }
    }

    idx_t objval; // Stores the edge-cut or communication volume

    // Get pointers to CSR data (不变)
    idx_t *vwgt_ptr = (vwgt.empty() ? nullptr : vwgt.data());
    idx_t *adjwgt_ptr = (adjwgt.empty() ? nullptr : adjwgt.data());

    // Updated log message
    log_message("Calling METIS_PartGraphKway with nparts = " + std::to_string(nParts) +
                ", nvtx (dense) = " + std::to_string(nvtx_metis) +
                ", ncon = " + std::to_string(nWeights) + "...", log_stream);

    // Ensure part vector is correctly sized (checked during allocation)
    // Call METIS_PartGraphKway (The call itself is unchanged, but inputs represent the dense graph)
    uint64_t metis_ret = METIS_PartGraphKway(
        &nvtx_metis, // Number of dense vertices
        &nWeights,
        xadj.data(), // CSR based on dense IDs
        adjncy.data(), // CSR based on dense IDs
        vwgt_ptr, // CSR based on dense IDs
        nullptr, // vsz
        adjwgt_ptr, // CSR based on dense IDs
        &nParts,
        nullptr, // tpwgts
        nullptr, // ubvec
        nullptr, // options
        &objval,
        part.data() // Output indexed by dense IDs
    );

    // --- Process METIS Results ---
    if (metis_ret != METIS_OK) {
        log_message("METIS partitioning failed with error code: " + std::to_string(metis_ret), log_stream);
        // ... (Log specific error types) ...
        // Clear map on failure
        std::lock_guard<std::mutex> lock(graph_mutex_);
        partition_node_map.clear();
        return;
    }
    log_message("METIS partitioning successful! Objective value (edge cut/volume): " + std::to_string(objval),
                log_stream);

    // --- Write Partition Results to File ---
    std::ofstream outpartition(output_partition_file);
    if (!outpartition.is_open()) {
        log_message("Error: Cannot open partition output file " + output_partition_file, log_stream);
        // Clear map before returning, even though METIS succeeded, output failed.
        std::lock_guard<std::mutex> lock(graph_mutex_);
        partition_node_map.clear();
        return;
    }
    // Header refers to ORIGINAL NodeID and its components
    // MODIFIED HEADER as requested
    outpartition << "RegionID,TableID,InnerRegionID,PartitionIndex\n";

    // Iterate through DENSE IDs to write ORIGINAL ID components and its partition index
    for (idx_t dense_i = 0; dense_i < nvtx_metis; ++dense_i) {
        if (dense_i < regionid_to_dense_map_.size()) {
            // Safety check
            uint64_t original_id = regionid_to_dense_map_[dense_i];
            idx_t partition_index = part[dense_i]; // Get the partition index assigned by METIS

            // --- NEW: Extract TableID and InnerRegionID from original_id ---
            Region region(original_id); // Create Region object from the 64-bit ID
            unsigned int table_id = region.getTableId();
            unsigned int inner_region_id = region.getInnerRegionId();


            // --- MODIFIED: Write the new format ---
            outpartition << original_id << ","
                    << table_id << ","
                    << inner_region_id << ","
                    << partition_index << "\n";
        } else {
            log_message(
                "Error: Dense ID " + std::to_string(dense_i) + " out of bounds in reverse map during file writing.",
                log_stream);
        }
    }
    outpartition.close();
    // Log message reflects the content change (optional)
    log_message(
        "Partition results (RegionID, TableID, InnerRegionID, PartitionIndex) successfully written to " +
        output_partition_file, log_stream);


    // --- Populate the partition_node_map (Router Rules Logic) ---
    {
        // Need lock to modify partition_node_map and access reverse map
        std::lock_guard<std::mutex> lock(graph_mutex_); // LOCK Acquired

        partition_node_map.clear(); // Clear any previous mapping (maps original_id -> primary_original_id)

        // Map affinity class (METIS partition index) to the chosen PRIMARY ORIGINAL ID for that class
        std::map<idx_t, uint64_t> affinity_to_primary_original_id_map;

        // Iterate through all DENSE nodes as partitioned by METIS
        for (idx_t dense_i = 0; dense_i < nvtx_metis; ++dense_i) {
            // Get original ID for this dense ID
            if (dense_i >= regionid_to_dense_map_.size()) {
                log_message(
                    "Error: Dense ID " + std::to_string(dense_i) +
                    " out of bounds in reverse map during router rule creation.", log_stream);
                continue; // Skip this dense ID if reverse map is inconsistent
            }
            uint64_t current_original_id = regionid_to_dense_map_[dense_i];
            idx_t affinity_class = part[dense_i]; // The partition METIS assigned (0..nParts-1)

            // Check if we've already assigned a primary original ID for this affinity class
            auto primary_it = affinity_to_primary_original_id_map.find(affinity_class);
            if (primary_it == affinity_to_primary_original_id_map.end()) {
                // This is the first time we encounter this affinity class.
                // Assign the current node's ORIGINAL ID as the primary for this class.
                uint64_t primary_node_for_class = current_original_id; // Use the original ID itself
                affinity_to_primary_original_id_map[affinity_class] = primary_node_for_class;
                // Map the current original ID to its chosen primary original ID
                partition_node_map[current_original_id] = primary_node_for_class;
            } else {
                // This affinity class already has a primary original ID assigned.
                // Map the current original ID to the existing primary original ID for its class
                partition_node_map[current_original_id] = primary_it->second;
            }
        }
        // Updated log message
        log_message("Router rules map (original_id -> primary_original_id) populated.", log_stream);

        // --- (Optional) Write Router Rules to Separate File ---
        // This part remains the same, as partition_node_map now correctly holds original IDs
        std::ofstream outpartition2("router_rules.csv");
        if (!outpartition2.is_open()) {
            log_message("Error: Cannot open partition output file router_rules.csv", log_stream);
        } else {
            // Header refers to original IDs
            outpartition2 << "RegionID,PrimaryRegionID\n";
            // Iterate through the generated map (original_id -> primary_original_id) and write it
            std::map<uint64_t, uint64_t> ordered_map(partition_node_map.begin(), partition_node_map.end());
            for (const auto &pair: ordered_map) {
                outpartition2 << pair.first << "," << pair.second << "\n";
            }
            outpartition2.close();
            log_message("Router rules map written to router_rules.csv", log_stream);
        }
    } // LOCK released

    log_message("Finished internal graph partitioning task.", log_stream);
} // End of partition_internal_graph
