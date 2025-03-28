#ifndef NEWMETIS_PARTITIONER_H
#define NEWMETIS_PARTITIONER_H

#include <vector>
#include <string>
#include <unordered_map>
#include <set>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm> // Required for std::max_element, std::max
#include <mutex>     // Required for std::mutex and std::lock_guard
#include <numeric>   // Required for std::iota (optional)
#include <iterator>  // Required for std::inserter

// Forward declarations
class ThreadPool; // Assume ThreadPool class is defined elsewhere

// Include metis.h (ensure it's available in your include paths)
#include <metis.h>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <atomic> // Required for automatic partitioning counters

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
    std::cout << log_entry << std::endl;

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
     * @brief Adds a group of unique, pre-mapped IDs to the internal graph representation,
     * creating edges between all pairs within the group (fully connected subgraph).
     * Triggers asynchronous partitioning via the ThreadPool at specified intervals if ENABLE_AUTO_PARTITION is defined.
     * This function is thread-safe.
     * @param unique_mapped_ids_in_group Vector of unique integer IDs appearing together.
     */
    void build_internal_graph(const std::vector<int> &unique_mapped_ids_in_group);

    /**
     * @brief Partitions the internally stored graph using METIS_PartGraphKway.
     * This function is thread-safe (acquires lock only for CSR conversion).
     * Typically called asynchronously by build_internal_graph when auto-partitioning is enabled.
     * @param output_partition_file Path for the partition result file (CSV: NodeID,PartitionID).
     * @param log_file_path Path for the log file where messages will be appended.
     * @param ComputeNodeCount The desired number of partitions (nParts for METIS).
     */
    void partition_internal_graph(const std::string &output_partition_file,
                                  const std::string &log_file_path,
                                  int ComputeNodeCount) const;

private:
    // Internal Graph Representation
    std::set<int> active_nodes_; // Stores IDs of nodes actually present
    // ! MT TODO here: add weights for edge. 
    std::unordered_map<int, std::set<int> > partition_graph_; // Adjacency list (u -> {v1, v2, ...})
    std::unordered_map<int, int> partition_weight_; // Node weights (id -> weight), default 1
    size_t num_edges_ = 0; // Cached count of unique undirected edges

    // Mutex for thread safety protecting access to the graph members above
    mutable std::mutex graph_mutex_; // mutable allows locking in const methods

    // --- Automatic Partitioning Members ---
    std::atomic<int> build_call_counter_{0}; // Counts calls to build_internal_graph
    std::atomic<int> last_partition_milestone_{0}; // Tracks the last milestone that triggered a partition
    ThreadPool *associated_thread_pool_ = nullptr; // Pointer to the thread pool for async tasks
    std::string partition_output_file_ = "graph_partitions.csv"; // Default output file
    std::string partition_log_file_ = "partitioning.log"; // Default log file
    int num_partitions_ = 8; // Default number of partitions
    static const int PARTITION_INTERVAL = 1; // Trigger partition every 1000 calls
    // --- End Automatic Partitioning Members ---

    /**
     * @brief Helper to get graph size (node count based on max ID, edge count)
     * **Must be called while holding graph_mutex_**.
     * @return std::pair<int, size_t> {nvtx (0..max_id+1), nedges}
     */
    std::pair<int, size_t> get_graph_size_unsafe() const {
        int max_id = active_nodes_.empty() ? -1 : *active_nodes_.rbegin();
        int nvtx = max_id + 1; // METIS needs the range 0 to max_id
        return {nvtx > 0 ? nvtx : 0, num_edges_}; // Ensure non-negative node count
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
                    std::cout << "[AutoTrigger] Build call count " << current_call_count
                            << " triggered partitioning for milestone " << current_milestone << "." << std::endl;
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
            partition_graph_.try_emplace(id); // Ensure adjacency list entry exists (creates empty set if new)
            partition_weight_.try_emplace(id, 1); // Ensure weight entry exists (sets to 1 if new)
        }

        // --- Add Edges (form a clique within the group) ---
        if (unique_mapped_ids_in_group.size() >= 2) {
            for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                for (size_t j = i + 1; j < unique_mapped_ids_in_group.size(); ++j) {
                    int u = unique_mapped_ids_in_group[i];
                    int v = unique_mapped_ids_in_group[j];

                    // Add edge (u, v) and (v, u) - std::set handles uniqueness
                    // Access using [] is safe here because try_emplace above ensured keys exist
                    partition_graph_[u].insert(v);
                    partition_graph_[v].insert(u);
                }
            }
        }

        // --- Recalculate Total Edge Count ---
        // Iterate through the adjacency list and sum degrees, then divide by 2
        size_t current_total_degree = 0;
        // Iterate only over nodes confirmed to be in the graph structure
        for (const auto &pair: partition_graph_) {
            // Only count degrees of nodes that are marked active
            // (though usually partition_graph_ keys should subset active_nodes_)
            if (active_nodes_.count(pair.first)) {
                current_total_degree += pair.second.size();
            }
        }
        num_edges_ = current_total_degree / 2; // Update the cached edge count
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
            // Log initiation from the worker thread (optional)
            // std::ofstream worker_log_stream(logfile, std::ios::app);
            // log_message("Worker thread starting triggered partition task.", worker_log_stream);

            this->partition_internal_graph(outfile, logfile, nparts); // Call the partitioning method

            // Optional: Log completion from the worker thread
            // log_message("Worker thread finished partition task.", worker_log_stream);
        });
    }
#endif
    // --- End Submit Partition Task ---
}


// --- Implementation for partitioning the internal graph ---
inline void NewMetis::partition_internal_graph(const std::string &output_partition_file,
                                               const std::string &log_file_path,
                                               int ComputeNodeCount) const {
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
    idx_t nedges_metis = 0; // Number of edges for METIS (from cache)
    idx_t ncon_metis = 1; // Number of weights per vertex (constraints)
    bool conversion_success = true;

    // --- Convert Internal Graph to METIS CSR Format (within lock) ---
    {
        // Scope for lock_guard
        std::lock_guard<std::mutex> lock(graph_mutex_);
        log_message("Acquired graph lock for CSR conversion.", log_stream);

        auto [graph_nvtx, graph_nedges] = get_graph_size_unsafe(); // Get size based on max_id and cached edges
        nvtx_metis = static_cast<idx_t>(graph_nvtx);
        nedges_metis = static_cast<idx_t>(graph_nedges);

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
            log_message("Released graph lock.", log_stream);
            return;
        }

        log_message("Internal graph state: nvtx (0..max_id) = " + std::to_string(nvtx_metis) +
                    ", nedges = " + std::to_string(nedges_metis), log_stream);
        log_message("Starting conversion to METIS CSR format.", log_stream);

        // --- Allocate CSR structures ---
        // Note: METIS idx_t is often int, but use the typedef for portability.
        try {
            xadj.resize(nvtx_metis + 1);
            vwgt.resize(nvtx_metis * ncon_metis);
            // Reserve space: need nedges * 2 because CSR stores each directed edge
            adjncy.reserve(nedges_metis * 2);
            adjwgt.reserve(nedges_metis * 2); // Assuming edge weight = 1 for all
        } catch (const std::bad_alloc &e) {
            log_message("Memory allocation failed for CSR arrays: " + std::string(e.what()), log_stream);
            conversion_success = false;
            // No need to break or return yet, let it finish the lock scope
        }


        if (conversion_success) {
            idx_t current_edge_ptr = 0; // Index into adjncy/adjwgt

            // --- Populate CSR Arrays ---
            // Iterate through the full potential node range [0, max_id]
            for (idx_t i = 0; i < nvtx_metis; ++i) {
                xadj[i] = current_edge_ptr;
                int current_node_id = static_cast<int>(i); // Convert idx_t back to int for map lookups

                // Check if the node 'i' is actually present in our graph
                if (active_nodes_.count(current_node_id)) {
                    // Node exists: set its weight
                    vwgt[i * ncon_metis + 0] = partition_weight_.count(current_node_id)
                                                   ? static_cast<idx_t>(partition_weight_.at(current_node_id))
                                                   : 1; // Default weight 1 if missing

                    // Add its neighbors to adjncy and their weights to adjwgt
                    if (partition_graph_.count(current_node_id)) {
                        const auto &neighbors = partition_graph_.at(current_node_id);
                        for (int neighbor_int_id: neighbors) {
                            // Basic validation: ensure neighbor ID is within expected range
                            if (neighbor_int_id < 0 || static_cast<idx_t>(neighbor_int_id) >= nvtx_metis) {
                                log_message("Error: Invalid neighbor ID " + std::to_string(neighbor_int_id) +
                                            " found for node " + std::to_string(current_node_id) + ".", log_stream);
                                conversion_success = false;
                                break; // Stop processing this node's neighbors
                            }
                            // Add neighbor and its edge weight (assuming 1)
                            adjncy.push_back(static_cast<idx_t>(neighbor_int_id));
                            adjwgt.push_back(1); // Edge weight = 1
                            current_edge_ptr++;
                        }
                        if (!conversion_success) break; // Stop processing outer loop if error occurred
                    }
                } else {
                    // Node 'i' is not active (a gap node)
                    // Set default weight and add no neighbors
                    vwgt[i * ncon_metis + 0] = 1; // Default weight 1
                }
            } // end for loop (i < nvtx_metis)

            // Final entry in xadj points to the end of adjncy/adjwgt
            xadj[nvtx_metis] = current_edge_ptr;

            // --- Verification after loop ---
            if (!conversion_success) {
                log_message("CSR conversion failed due to errors.", log_stream);
                // Let lock release naturally
            } else if (current_edge_ptr != nedges_metis * 2) {
                // This indicates an inconsistency between cached num_edges_ and actual structure
                log_message("Warning: CSR edge pointer count (" + std::to_string(current_edge_ptr) +
                            ") does not match expected edge count * 2 (" + std::to_string(nedges_metis * 2) + "). "
                            "There might be an issue with num_edges_ calculation.", log_stream);
                // Decide if this is fatal? METIS might still work if CSR structure itself is valid.
                // conversion_success = false; // Optionally mark as failure
            } else {
                log_message("CSR conversion successful. Total degree sum: " + std::to_string(current_edge_ptr),
                            log_stream);
            }
        } // end if(conversion_success) before loop

        log_message("Released graph lock (before calling METIS).", log_stream);
    } // End of lock scope (graph_mutex_ released)


    // --- Check if CSR Conversion was Successful ---
    if (!conversion_success) {
        log_message("Aborting partitioning due to CSR conversion failure.", log_stream);
        return;
    }
    if (nvtx_metis <= 0) {
        log_message("Skipping METIS call as nvtx is zero or negative after conversion check.", log_stream);
        return; // Already handled empty graph output earlier, but double-check
    }


    // --- Prepare and Call METIS ---
    idx_t nWeights = ncon_metis; // Number of constraints = number of weights per node
    idx_t nParts = ComputeNodeCount;

    // Validate nParts
    if (nParts <= 0) {
        log_message("Error: Invalid number of partitions requested (" + std::to_string(nParts) + "). Aborting.",
                    log_stream);
        return;
    }
    if (nParts == 1) {
        log_message("Warning: Requested 1 partition. METIS call skipped, assigning all nodes to partition 0.",
                    log_stream);
        // Create a dummy result file with all nodes in partition 0
        std::ofstream outpartition(output_partition_file);
        if (!outpartition.is_open()) {
            log_message("Error: Cannot open partition output file " + output_partition_file, log_stream);
            return;
        }
        outpartition << "NodeID,PartitionID\n";
        for (idx_t i = 0; i < nvtx_metis; ++i) {
            outpartition << i << ",0\n"; // Output result for all nodes 0..max_id
        }
        outpartition.close();
        log_message("Partition result (all 0) written to " + output_partition_file, log_stream);
        return;
    }
    if (nParts > nvtx_metis) {
        log_message("Warning: Requested partitions (" + std::to_string(nParts) +
                    ") > number of vertices (" + std::to_string(nvtx_metis) +
                    "). Reducing partitions to nvtx.", log_stream);
        nParts = nvtx_metis; // METIS requires nparts <= nvtx
    }


    idx_t objval; // Stores the edge-cut or communication volume calculated by METIS
    std::vector<idx_t> part(nvtx_metis); // Output array for partition assignments

    // METIS function expects pointers to the data
    // Pass actual data pointers if weights exist, otherwise nullptr
    idx_t *vwgt_ptr = (vwgt.empty() ? nullptr : vwgt.data()); // Check if vwgt was actually filled
    idx_t *adjwgt_ptr = (adjwgt.empty() ? nullptr : adjwgt.data()); // Check if adjwgt was actually filled
    // NOTE: Current logic assumes weights always exist (defaulting to 1),
    // so these pointers should typically be non-null unless allocation failed.
    // A more robust implementation might check fmt flags if read from a file.

    log_message("Calling METIS_PartGraphKway with nparts = " + std::to_string(nParts) + "...", log_stream);
    int metis_ret = METIS_PartGraphKway(
        &nvtx_metis, // Number of vertices
        &nWeights, // Number of balancing constraints (vertex weights)
        xadj.data(), // CSR graph structure (pointers)
        adjncy.data(), // CSR graph structure (adjacency lists)
        vwgt_ptr, // Vertex weights (can be nullptr for unweighted)
        nullptr, // vsz: Vertex sizes for communication volume (nullptr for default)
        adjwgt_ptr, // Edge weights (can be nullptr for unweighted)
        &nParts, // Number of desired partitions
        nullptr, // tpwgts: Target partition weights (nullptr for equal distribution)
        nullptr, // ubvec: Imbalance tolerance (nullptr for default 1.03)
        nullptr, // options: METIS options array (nullptr for defaults)
        &objval, // Output: Objective value (edge cut)
        part.data() // Output: Partition assignment for each vertex
    );

    // --- Process METIS Results ---
    if (metis_ret != METIS_OK) {
        log_message("METIS partitioning failed with error code: " + std::to_string(metis_ret), log_stream);
        // Potentially log more details based on the error code if needed
        return;
    }
    log_message("METIS partitioning successful! Objective value (edge cut): " + std::to_string(objval), log_stream);

    // --- Write Partition Results to File ---
    std::ofstream outpartition(output_partition_file);
    if (!outpartition.is_open()) {
        log_message("Error: Cannot open partition output file " + output_partition_file, log_stream);
        return;
    }
    outpartition << "NodeID,PartitionID\n"; // Write CSV header
    for (idx_t i = 0; i < nvtx_metis; ++i) {
        // Write the partition assignment for each node ID from 0 to max_id
        outpartition << i << "," << part[i] << "\n";
    }
    outpartition.close();
    log_message("Partition results successfully written to " + output_partition_file, log_stream);
    log_message("Finished internal graph partitioning task.", log_stream);
}

#endif // NEWMETIS_PARTITIONER_H
