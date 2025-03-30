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

// Forward declarations
class ThreadPool; // Assume ThreadPool class is defined elsewhere

// Include metis.h (ensure it's available in your include paths)
#include <metis.h>


// Enable automatic partitioning via preprocessor directive
#define ENABLE_AUTO_PARTITION

// --- Logging Helper Function ---
// Note: std::localtime is not guaranteed to be thread-safe.
// Consider using platform-specific thread-safe alternatives like
// localtime_s (Windows) or localtime_r (POSIX) for high concurrency.
inline void log_message(const std::string &message, std::ostream &log_stream) {
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
#ifdef _WIN32 // Handle Windows' localtime_s
        std::tm now_tm;
        localtime_s(&now_tm, &now_c);
#else // POSIX's localtime_r or fallback to potentially non-thread-safe localtime
    // For localtime_r (thread-safe):
    // std::tm now_tm;
    // localtime_r(&now_c, &now_tm);
    // Fallback:
    std::tm now_tm = *std::localtime(&now_c); // Use non-thread-safe localtime
#endif
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::stringstream ss;
    ss << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    std::string log_entry = "[" + ss.str() + "] " + message;

    // Output to console (optional, but often useful for debugging)
    //std::cout << log_entry << std::endl;

    // Output to log file stream
    if (log_stream.good()) {
        log_stream << log_entry << std::endl;
    } else {
        std::cerr << "[Logging Error] Log stream is not good for message: " << message << std::endl;
    }
}
// --- End Logging Helper ---


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
    void set_partition_parameters(std::string output_file, std::string log_file, int num_parts);

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
    void build_internal_graph(const std::vector<int> &unique_mapped_ids_in_group);

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
                                  int ComputeNodeCount);

private:
    // Internal Graph Representation
    std::set<int> active_nodes_; // Stores IDs of nodes actually present
    // Adjacency list storing neighbors and edge weights (u -> {v1 -> weight1, v2 -> weight2, ...})
    std::unordered_map<int, std::unordered_map<int, int> > partition_graph_;
    std::unordered_map<int, int> partition_weight_; // Node weights (id -> weight), default 1
    // Map original node ID (0..max_id) to the primary node ID of its partition.
    // Updated AFTER successful partitioning. Requires graph_mutex_ for safe access.
    std::unordered_map<int, int> partition_node_map;

    // Mutex for thread safety protecting access to the graph members AND partition_node_map
    mutable std::mutex graph_mutex_; // mutable allows locking in const methods

    // --- Automatic Partitioning Members ---
    std::atomic<int> build_call_counter_{0}; // Counts calls to build_internal_graph
    std::atomic<int> last_partition_milestone_{0}; // Tracks the last milestone that triggered a partition
    ThreadPool *associated_thread_pool_ = nullptr; // Pointer to the thread pool for async tasks
    std::string partition_output_file_ = "graph_partitions.csv"; // Default output file
    std::string partition_log_file_ = "partitioning.log"; // Default log file
    int num_partitions_ = 8; // Default number of partitions
    static const int PARTITION_INTERVAL = 1000; // Trigger partition every 1000 calls
    // --- End Automatic Partitioning Members ---

    /**
     * @brief Helper to get graph vertex count (node count based on max ID).
     * **Must be called while holding graph_mutex_**.
     * @return int nvtx (0..max_id+1)
     */
    int get_graph_size_unsafe() const {
        int max_id = active_nodes_.empty() ? -1 : *active_nodes_.rbegin();
        int nvtx = max_id + 1; // METIS needs the range 0 to max_id
        return nvtx > 0 ? nvtx : 0; // Ensure non-negative node count
    }
};

// --- Inline Implementations ---

inline void NewMetis::set_thread_pool(ThreadPool *pool) {
    associated_thread_pool_ = pool;
}

inline void NewMetis::set_partition_parameters(std::string output_file, std::string log_file, int num_parts) {
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
inline void NewMetis::build_internal_graph(const std::vector<int> &unique_mapped_ids_in_group) {
    if (unique_mapped_ids_in_group.empty()) {
        return; // Nothing to do for empty input
    }

    // --- Automatic Partition Trigger Logic ---
    int current_call_count = 0; // Only incremented if auto-partitioning is enabled
    bool should_trigger_partition = false;
#ifdef ENABLE_AUTO_PARTITION // Check if the macro is defined
    if (associated_thread_pool_ != nullptr) {
        // Check if async partitioning is possible
        current_call_count = ++build_call_counter_; // Increment atomically
        int current_milestone = (current_call_count / PARTITION_INTERVAL) * PARTITION_INTERVAL;

        // Check if we've crossed a new milestone boundary
        if (current_milestone > 0) {
            int expected_last = last_partition_milestone_.load(std::memory_order_acquire);
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
        // Scope for lock_guard
        std::lock_guard<std::mutex> lock(graph_mutex_);

        // --- Add/Ensure Nodes Exist ---
        for (const int &id: unique_mapped_ids_in_group) {
            active_nodes_.insert(id); // Add to set of active nodes
            partition_graph_.try_emplace(id); // Ensure outer map entry exists (creates empty inner map if new)
            partition_weight_.try_emplace(id, 1); // Ensure weight entry exists (sets to 1 if new)
        }

        // --- Add/Update Edges (form a clique within the group) ---
        // Increment weight for each pair (edge) in the group.
        if (unique_mapped_ids_in_group.size() >= 2) {
            for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                for (size_t j = i + 1; j < unique_mapped_ids_in_group.size(); ++j) {
                    int u = unique_mapped_ids_in_group[i];
                    int v = unique_mapped_ids_in_group[j];

                    // Increment weight for edge (u, v)
                    partition_graph_[u][v]++;

                    // Increment weight for edge (v, u) - Undirected graph needs symmetric update
                    partition_graph_[v][u]++;
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
        int nparts = num_partitions_;

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
    { // New scope for lock guard to access partition_node_map safely
        std::lock_guard<std::mutex> lock(graph_mutex_); // Acquire lock

        // Check if partitioning has run at least once and the map is populated
        if (!partition_node_map.empty()) {
            std::map<int, int> partition_counts; // Map<PrimaryNodeID (Partition), Count>
            int unmapped_count = 0; // Count nodes in the group not found in the current map

            // Count occurrences of each primary partition ID within the input group
            for (int node_id : unique_mapped_ids_in_group) {
                auto map_it = partition_node_map.find(node_id);
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
                int dominant_partition_id = -1;
                int max_count = 0;

                std::stringstream counts_ss;
                counts_ss << "{ ";
                for(const auto& pair : partition_counts) {
                    counts_ss << "Partition(PrimaryID " << pair.first << "): " << pair.second << " nodes; ";
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
                    for(size_t i=0; i<unique_mapped_ids_in_group.size(); ++i){
                        group_ss << unique_mapped_ids_in_group[i] << (i == unique_mapped_ids_in_group.size()-1 ? "" : ", ");
                    }
                    group_ss << "]";

                    std::string log_msg = "Cross-partition access detected for group " + group_ss.str() +
                                          ". Counts per partition: " + counts_ss.str() +
                                          ". Choosing dominant partition (PrimaryID): " + std::to_string(dominant_partition_id) +
                                          " based on max count (" + std::to_string(max_count) + ").";
                    if (unmapped_count > 0) {
                        log_msg += " Note: " + std::to_string(unmapped_count) + " node(s) in the group were not found in the current partition map.";
                    }
                    log_message(log_msg, log_stream);
                    log_stream.close(); // Close the log stream
                } else {
                     std::cerr << "[Logging Error] Could not open log file '" << partition_log_file_ << "' for cross-partition log." << std::endl;
                }

                // Here you would typically *use* the dominant_partition_id for routing or affinity decisions
                // For example: route_request_to_partition(dominant_partition_id);

            } else if (partition_counts.size() == 1 && unmapped_count == 0) {
                 // Optional: Log if all nodes map to the same partition (no cross-access)
                 std::ofstream log_stream(partition_log_file_, std::ios::app);
                 if (log_stream.is_open()) {
                     log_message("Group maps entirely to partition (PrimaryID): " + std::to_string(partition_counts.begin()->first), log_stream);
                     log_stream.close();
                 }
            } else if (partition_counts.empty() && unmapped_count > 0) {
                // Optional: Log if none of the nodes were found in the map
                 std::ofstream log_stream(partition_log_file_, std::ios::app);
                 if (log_stream.is_open()) {
                    std::stringstream group_ss;
                    group_ss << "[";
                    for(size_t i=0; i<unique_mapped_ids_in_group.size(); ++i){
                        group_ss << unique_mapped_ids_in_group[i] << (i == unique_mapped_ids_in_group.size()-1 ? "" : ", ");
                    }
                    group_ss << "]";
                    log_message("None of the nodes in group " + group_ss.str() + " found in the current partition map. Cannot determine dominant partition.", log_stream);
                    log_stream.close();
                 }
            }
            // Case: partition_counts.size() == 1 && unmapped_count > 0 is implicitly handled (treated as single partition access among mapped nodes)
            // Case: partition_counts.empty() && unmapped_count == 0 means input group was empty, already handled.

        } // else: partition_node_map is empty, do nothing.

    } // graph_mutex_ is released here
    // ========================================================================
    // END: Cross-Partition Access Check
    // ========================================================================
}


// --- Implementation for partitioning the internal graph ---
// ========================================================================
// MODIFIED partition_internal_graph FUNCTION (to populate partition_node_map)
// ========================================================================
inline void NewMetis::partition_internal_graph(const std::string &output_partition_file,
                                              const std::string &log_file_path,
                                              int ComputeNodeCount) {
    // --- Open Log File (append mode) ---
    std::ofstream log_stream(log_file_path, std::ios::app);
    if (!log_stream.is_open()) {
        std::cerr << "[Partition Error] Failed to open log file: " << log_file_path << std::endl;
        // Continue without file logging, or return depending on requirements
    }
    log_message("Starting internal graph partitioning task.", log_stream);

    // --- Variables needed for METIS CSR format ---
    std::vector<idx_t> xadj; // Pointers to start of adjacency list for each vertex
    std::vector<idx_t> adjncy; // Concatenated adjacency lists
    std::vector<idx_t> vwgt; // Vertex weights (concatenated)
    std::vector<idx_t> adjwgt; // Edge weights (concatenated, corresponding to adjncy)
    idx_t nvtx_metis = 0; // Number of vertices for METIS (0 to max_id)
    idx_t ncon_metis = 1; // Number of weights per vertex (constraints)
    bool conversion_success = true;
    size_t total_degree_sum = 0; // Will store the total number of entries needed in adjncy/adjwgt
    std::vector<idx_t> part; // Output array for partition assignments (declared earlier)

    // --- Convert Internal Graph to METIS CSR Format (within lock) ---
    {
        // Scope for lock_guard
        std::lock_guard<std::mutex> lock(graph_mutex_); // LOCK acquired
        log_message("Acquired graph lock for CSR conversion.", log_stream);

        nvtx_metis = static_cast<idx_t>(get_graph_size_unsafe()); // Get vertex count (0..max_id+1)

        if (nvtx_metis <= 0) {
            log_message(
                "Internal graph is empty or invalid (nvtx = " + std::to_string(nvtx_metis) + "). Cannot partition.",
                log_stream);
            // Write an empty partition file for consistency
            std::ofstream outpartition(output_partition_file);
            if (outpartition.is_open()) {
                outpartition << "NodeID,PartitionID\n"; // Header only
                outpartition.close();
            } else {
                log_message("Error: Cannot open partition output file " + output_partition_file + " for empty graph.",
                            log_stream);
            }
            // Clear the partition map as the graph is empty
            partition_node_map.clear(); // <-- Added clear here
            log_message("Released graph lock (empty graph).", log_stream);
            return; // LOCK released by lock_guard destructor
        }

        // --- Calculate total degree sum needed for CSR arrays ---
        total_degree_sum = 0;
        for (const auto &pair: partition_graph_) {
             if (active_nodes_.count(pair.first)) {
                 total_degree_sum += pair.second.size();
             }
        }

        log_message("Internal graph state: nvtx (0..max_id) = " + std::to_string(nvtx_metis) +
                    ", Total degree sum = " + std::to_string(total_degree_sum), log_stream);
        log_message("Starting conversion to METIS CSR format.", log_stream);

        // --- Allocate CSR structures ---
        try {
            xadj.resize(nvtx_metis + 1);
            vwgt.resize(nvtx_metis * ncon_metis);
            adjncy.reserve(total_degree_sum);
            adjwgt.reserve(total_degree_sum);
            part.resize(nvtx_metis); // Also resize the partition result vector here
        } catch (const std::bad_alloc &e) {
            log_message("Memory allocation failed for CSR arrays: " + std::string(e.what()), log_stream);
            conversion_success = false;
            // Clear the partition map on failure
            partition_node_map.clear(); // <-- Added clear here
        }

        if (conversion_success) {
            idx_t current_edge_ptr = 0; // Index into adjncy/adjwgt
            // --- Populate CSR Arrays ---
            for (idx_t i = 0; i < nvtx_metis; ++i) {
                xadj[i] = current_edge_ptr;
                int current_node_id = static_cast<int>(i);

                if (active_nodes_.count(current_node_id)) {
                    vwgt[i * ncon_metis + 0] = partition_weight_.count(current_node_id)
                                               ? static_cast<idx_t>(partition_weight_.at(current_node_id))
                                               : 1;

                    if (partition_graph_.count(current_node_id)) {
                        // Sort neighbors by ID for deterministic CSR? Optional but good practice.
                        // Using std::map temporarily for sorting, could optimize if needed.
                        std::map<int, int> sorted_neighbors(partition_graph_.at(current_node_id).begin(),
                                                             partition_graph_.at(current_node_id).end());

                        for (const auto &[neighbor_int_id, edge_weight]: sorted_neighbors) { // Iterate sorted
                            if (neighbor_int_id < 0 || static_cast<idx_t>(neighbor_int_id) >= nvtx_metis) {
                                log_message("Error: Invalid neighbor ID " + std::to_string(neighbor_int_id) +
                                            " found for node " + std::to_string(current_node_id) + ".", log_stream);
                                conversion_success = false;
                                partition_node_map.clear(); // <-- Added clear here
                                break;
                            }
                             if (edge_weight <= 0) {
                                log_message(
                                    "Warning: Non-positive edge weight (" + std::to_string(edge_weight) +
                                    ") found for edge (" +
                                    std::to_string(current_node_id) + ", " + std::to_string(neighbor_int_id) +
                                    "). Using weight 1 instead.", log_stream);
                                adjncy.push_back(static_cast<idx_t>(neighbor_int_id));
                                adjwgt.push_back(1);
                            } else {
                                adjncy.push_back(static_cast<idx_t>(neighbor_int_id));
                                adjwgt.push_back(static_cast<idx_t>(edge_weight));
                            }
                            current_edge_ptr++;
                        }
                         if (!conversion_success) break;
                    }
                } else {
                    vwgt[i * ncon_metis + 0] = 1; // Default weight 1 for gap nodes
                }
            } // end for loop (i < nvtx_metis)

            xadj[nvtx_metis] = current_edge_ptr;

            // --- Verification after loop ---
            if (!conversion_success) {
                log_message("CSR conversion failed due to errors.", log_stream);
                // partition_node_map already cleared on error
            } else if (current_edge_ptr != total_degree_sum) {
                log_message("Warning: CSR edge pointer count (" + std::to_string(current_edge_ptr) +
                            ") does not match calculated total degree sum (" + std::to_string(total_degree_sum) + "). "
                            "There might be an issue with graph structure or calculation during CSR population.",
                            log_stream);
                // Continue, but be aware
            } else {
                log_message(
                    "CSR conversion successful. Total entries in adjncy/adjwgt: " + std::to_string(current_edge_ptr),
                    log_stream);
            }
        } // end if(conversion_success) before loop

        log_message("Released graph lock (before calling METIS).", log_stream);
    } // End of lock scope (graph_mutex_ released)


    // --- Check if CSR Conversion was Successful ---
    if (!conversion_success) {
        log_message("Aborting partitioning due to CSR conversion failure.", log_stream);
        return; // partition_node_map should be clear already
    }
    if (nvtx_metis <= 0) {
        log_message("Skipping METIS call as nvtx is zero or negative after conversion check.", log_stream);
        return; // partition_node_map should be clear already
    }


    // --- Prepare and Call METIS ---
    idx_t nWeights = ncon_metis;
    idx_t nParts = ComputeNodeCount;

    // Validate nParts
    if (nParts <= 0) {
        log_message("Error: Invalid number of partitions requested (" + std::to_string(nParts) + "). Aborting.",
                    log_stream);
        // Clear map before returning
        std::lock_guard<std::mutex> lock(graph_mutex_);
        partition_node_map.clear();
        return;
    }
    if (nParts == 1) {
        log_message("Warning: Requested 1 partition. METIS call skipped, assigning all nodes to partition 0.",
                    log_stream);
        // Create a dummy result file with all nodes in partition 0
        std::ofstream outpartition(output_partition_file);
        if (!outpartition.is_open()) {
            log_message("Error: Cannot open partition output file " + output_partition_file, log_stream);
             // Clear map before returning
            std::lock_guard<std::mutex> lock(graph_mutex_);
            partition_node_map.clear();
            return;
        }
        outpartition << "NodeID,PartitionID\n";
        // Assign all nodes 0..nvtx-1 to partition 0
        { // Need lock to update partition_node_map
            std::lock_guard<std::mutex> lock(graph_mutex_);
            partition_node_map.clear(); // Clear previous results
            for (idx_t i = 0; i < nvtx_metis; ++i) {
                 outpartition << i << ",0\n"; // Output result for all nodes 0..max_id
                 partition_node_map[static_cast<int>(i)] = 0; // Map all nodes to primary ID 0
            }
        }
        outpartition.close();
        log_message("Partition result (all 0) written to " + output_partition_file, log_stream);
        log_message("Router rules map populated (all nodes map to primary 0).", log_stream); // Added log
        return;
    }
     if (nParts > nvtx_metis) {
        log_message("Warning: Requested partitions (" + std::to_string(nParts) +
                    ") > number of vertices (" + std::to_string(nvtx_metis) +
                    "). METIS requires nparts <= nvtx. Reducing partitions to nvtx.", log_stream);
        nParts = nvtx_metis;
        if (nParts <= 0) {
             log_message("Error: Cannot partition into " + std::to_string(nParts) + " partitions after adjustment. Aborting.", log_stream);
            // Clear map before returning
            std::lock_guard<std::mutex> lock(graph_mutex_);
            partition_node_map.clear();
            return;
        }
        // If nParts became 1 after adjustment, handle it like the nParts==1 case above
         if (nParts == 1) {
             log_message("Partitions reduced to 1. Assigning all nodes to partition 0.", log_stream);
             // (Duplicate the nParts==1 logic here or refactor)
             std::ofstream outpartition(output_partition_file);
             if (!outpartition.is_open()) { /*...*/ std::lock_guard<std::mutex> lock(graph_mutex_); partition_node_map.clear(); return; }
             outpartition << "NodeID,PartitionID\n";
             { std::lock_guard<std::mutex> lock(graph_mutex_);
                 partition_node_map.clear();
                 for (idx_t i = 0; i < nvtx_metis; ++i) { outpartition << i << ",0\n"; partition_node_map[static_cast<int>(i)] = 0; }
             }
             outpartition.close();
             log_message("Partition result (all 0) written to " + output_partition_file, log_stream);
             log_message("Router rules map populated (all nodes map to primary 0).", log_stream);
             return;
         }
    }


    idx_t objval; // Stores the edge-cut or communication volume calculated by METIS
    // std::vector<idx_t> part(nvtx_metis); // Moved declaration earlier

    idx_t *vwgt_ptr = (vwgt.empty() ? nullptr : vwgt.data());
    idx_t *adjwgt_ptr = (adjwgt.empty() ? nullptr : adjwgt.data());

    log_message("Calling METIS_PartGraphKway with nparts = " + std::to_string(nParts) +
                ", nvtx = " + std::to_string(nvtx_metis) +
                ", ncon = " + std::to_string(nWeights) +
                ", using vertex weights = " + (vwgt_ptr ? "Yes" : "No") +
                ", using edge weights = " + (adjwgt_ptr ? "Yes" : "No") + "...", log_stream);

    // Ensure part vector is correctly sized before passing to METIS
    if (part.size() != nvtx_metis) {
         log_message("Error: Partition result vector size mismatch. Aborting.", log_stream);
         std::lock_guard<std::mutex> lock(graph_mutex_); partition_node_map.clear(); return;
    }

    int metis_ret = METIS_PartGraphKway(
        &nvtx_metis,
        &nWeights,
        xadj.data(),
        adjncy.data(),
        vwgt_ptr,
        nullptr, // vsz
        adjwgt_ptr,
        &nParts,
        nullptr, // tpwgts
        nullptr, // ubvec
        nullptr, // options
        &objval,
        part.data() // Use part vector allocated earlier
    );

    // --- Process METIS Results ---
    if (metis_ret != METIS_OK) {
        log_message("METIS partitioning failed with error code: " + std::to_string(metis_ret), log_stream);
        if (metis_ret == METIS_ERROR_INPUT) {
            log_message(" -> METIS Input Error. Check CSR arrays, nvtx, nparts, weights.", log_stream);
        } else if (metis_ret == METIS_ERROR_MEMORY) {
            log_message(" -> METIS Memory Allocation Error.", log_stream);
        }
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
    outpartition << "NodeID,PartitionID\n"; // Write CSV header
    for (idx_t i = 0; i < nvtx_metis; ++i) {
        outpartition << i << "," << part[i] << "\n";
    }
    outpartition.close();
    log_message("Partition results successfully written to " + output_partition_file, log_stream);


    // --- Populate the partition_node_map (Router Rules Logic) ---
    { // Need lock to modify partition_node_map
        std::lock_guard<std::mutex> lock(graph_mutex_); // LOCK Acquired

        partition_node_map.clear(); // Clear any previous mapping
        std::map<int, int> affinity_to_primary_map; // Map<AffinityClass, PrimaryNodeID>
        int next_primary_id = 0;

        // Iterate through all nodes (0 to nvtx_metis - 1) as partitioned by METIS
        for (idx_t i = 0; i < nvtx_metis; ++i) {
            int current_node_id = static_cast<int>(i);
            int affinity_class = static_cast<int>(part[i]); // The partition METIS assigned

            // Check if we've already assigned a primary node for this affinity class
            auto primary_it = affinity_to_primary_map.find(affinity_class);
            if (primary_it == affinity_to_primary_map.end()) {
                // This is the first time we encounter this affinity class.
                // Assign the current node 'i' as the primary for this class (or use next_primary_id logic).

                // Option 1: Use the first node encountered in the partition as its primary ID
                // int primary_node_for_class = current_node_id;

                // Option 2: Use a sequential counter for primary IDs (0, 1, 2...)
                int primary_node_for_class = next_primary_id++;

                affinity_to_primary_map[affinity_class] = primary_node_for_class;
                partition_node_map[current_node_id] = primary_node_for_class;
            } else {
                // This affinity class already has a primary node assigned.
                partition_node_map[current_node_id] = primary_it->second; // Assign existing primary ID
            }
        }
        log_message("Router rules map (partition_node_map) populated.", log_stream);

         // --- (Optional) Write Router Rules to Separate File ---
        std::ofstream outpartition2("router_rules.csv");
        if (!outpartition2.is_open()) {
            log_message("Error: Cannot open partition output file router_rules.csv", log_stream);
            // Don't clear the map here, just log error.
        } else {
            outpartition2 << "NodeID,AssignedPrimaryPartitionNodeID\n"; // Write CSV header
            // Iterate through the generated map and write it
            // Using std::map for ordered output (optional)
            std::map<int, int> ordered_map(partition_node_map.begin(), partition_node_map.end());
            for (const auto& pair : ordered_map) {
                 outpartition2 << pair.first << "," << pair.second << "\n";
            }
            outpartition2.close();
            log_message("Router rules map written to router_rules.csv", log_stream);
        }

    } // LOCK released

    log_message("Finished internal graph partitioning task.", log_stream);
} // End of partition_internal_graph