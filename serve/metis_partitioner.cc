#include "metis_partitioner.h"

// ========================================================================
// MODIFIED build_internal_graph FUNCTION
// ========================================================================
int NewMetis::build_internal_graph(std::unordered_map<uint64_t, node_id_t> &request_partition_node_map, node_id_t *metis_decision_node) {
    // std::cout << "build internal graph called with " << unique_mapped_ids_in_group.size() << " unique IDs." << std::endl;
    if (request_partition_node_map.empty()) {
        return -1; // Sentinel for empty input or no decision
    }

    // --- Automatic Partition Trigger Logic ---
    uint64_t current_call_count = 0;
    bool should_trigger_partition = false;
#ifdef ENABLE_AUTO_PARTITION
    if (enable_partition && associated_thread_pool_ != nullptr) {
        current_call_count = ++build_call_counter_;
        uint64_t current_milestone = (current_call_count / PARTITION_INTERVAL) * PARTITION_INTERVAL;
        // std::cout<<"Build call count: " << current_call_count
        //          << ", Current milestone: " << current_milestone
        //          << ", Last partition milestone: " << last_partition_milestone_.load(std::memory_order_acquire)
        //          << std::endl;

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
    // add sample rate here
    double random_value = distrib(gen_); // Generate a random value between 0.0 and 1.0
    if (enable_partition && (random_value <= AffinitySampleRate)) {
        std::lock_guard<std::mutex> lock(graph_data_mutex_);

        std::vector<uint64_t> keys;
        for (const auto& itr: request_partition_node_map) {
            uint64_t regionid = itr.first;
            keys.push_back(regionid);
            active_nodes_.insert(regionid);
            partition_graph_.try_emplace(regionid);
            partition_weight_.try_emplace(regionid, 1); // 点的权重就是1

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

        if (keys.size() >= 2) {
            for (size_t i = 0; i < keys.size(); ++i) {
                for (size_t j = i + 1; j < keys.size(); ++j) {
                    uint64_t u = keys[i];
                    uint64_t v = keys[j];
                    partition_graph_[u][v]++;
                    partition_graph_[v][u]++;
                }
            }
        }
    } // graph_data_mutex_ is released here

    // --- Submit Partition Task ---
#ifdef ENABLE_AUTO_PARTITION
    if (enable_partition && should_trigger_partition && associated_thread_pool_) {
        std::string outfile = this->partition_output_file_;
        uint64_t nparts = this->num_partitions_;

        associated_thread_pool_->enqueue([this, outfile, nparts] {
            this->partition_internal_graph(outfile, nparts);
        });
    }
#endif
    // --- End Submit Partition Task ---

    idx_t final_partition_index_result = -1; // Default: no specific partition index determined
    int return_decision_type = 0; // 0: no decision, 1: entire affinity, 2: partial affinity, 3: cross-partition, -1: missing
    std::string cross_partition_log_message_str; {
        std::shared_lock<std::shared_mutex> lock(partition_map_mutex_);

        if (!partition_node_map.empty()) {
            // partition_counts now maps PartitionIndex (idx_t) to count (uint64_t)
            std::map<idx_t, uint64_t> partition_counts;
            uint64_t unmapped_count = 0;

            for (const auto& r: request_partition_node_map) {
                auto map_it = partition_node_map.find(r.first);
                if (map_it != partition_node_map.end()) {
                    partition_counts[map_it->second]++; // map_it->second is the PartitionIndex
                    request_partition_node_map[r.first] = map_it->second; // Update to PartitionIndex
                } else {
                    unmapped_count++;
                }
            }

            #if LOG_METIS_DECISION
            std::string group_str = "[";
            for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
                group_str += std::to_string(unique_mapped_ids_in_group[i]);
                if (i != unique_mapped_ids_in_group.size() - 1) group_str += ", ";
            }
            group_str += "]";
            #endif

            if (partition_counts.size() > 1) { 
                // 匹配到的图上的节点大于1, 取最大分布的亲和性作为路由节点
                return_decision_type = 3; // cross-partition
                std::vector<node_id_t> candidates;
                idx_t dominant_partition_idx = -1;
                uint64_t max_count = 0;
            
                std::string counts_str = "{ ";
                for (const auto &pair: partition_counts) {
                    counts_str += "PartitionIndex " + std::to_string(pair.first) + ": exist " + std::to_string(pair.second) + " times; ";
                    if (pair.second > max_count) {
                        candidates.clear();
                        max_count = pair.second;
                        candidates.push_back(pair.first);
                    } else if (pair.second == max_count) {
                        candidates.push_back(pair.first);
                    }
                    assert(!candidates.empty());
                    // 随机选择出现次数最多的节点
                    dominant_partition_idx = candidates[rand() % candidates.size()];
                }
                counts_str += "}";
                final_partition_index_result = dominant_partition_idx;

            #if LOG_METIS_DECISION
                cross_partition_log_message_str = "[Route Decision] Epoch: " + std::to_string(stats_.total_partition_calls) + " Cross-partition detected in " + group_str +
                                                  ". Counts per PartitionIndex: " + counts_str +
                                                  ". Choosing dominant PartitionIndex: " + std::to_string(
                                                      dominant_partition_idx) +
                                                  " based on max count (" + std::to_string(max_count) + ").";
                if (unmapped_count > 0) {
                    cross_partition_log_message_str += " Note: " + std::to_string(unmapped_count) +
                            " node(s) in the group were not found in the current partition map.";
                }
            #endif
                // for statistics
                stats_.total_cross_partition_decisions++;
            } else if (partition_counts.size() == 1 && unmapped_count == 0) {
                // 等于1且没有unmapped, 直接使用该PartitionIndex
                return_decision_type = 1; // entire affinity
                final_partition_index_result = partition_counts.begin()->first; // The only PartitionIndex present
            #if LOG_METIS_DECISION
                cross_partition_log_message_str = "[Route Decision] Epoch: " + std::to_string(stats_.total_partition_calls) + " Group maps entirely to PartitionIndex: " + std::to_string(
                                                      final_partition_index_result) + "<---" + group_str;
            #endif
                // for statistics
                stats_.entire_affinity_decisions++;
            } else if (partition_counts.size() == 1 && unmapped_count > 0) {
                // 等于1但有unmapped, 直接使用该PartitionIndex (部分匹配到)
                return_decision_type = 2; // partial affinity
                final_partition_index_result = partition_counts.begin()->first; // The only PartitionIndex present
            #if LOG_METIS_DECISION
                cross_partition_log_message_str = "[Route Decision] Epoch: " + std::to_string(stats_.total_partition_calls) + " Group partially maps to PartitionIndex: " + std::to_string(
                                                      final_partition_index_result)  + "<---" + group_str + 
                                                  ". Note: " + std::to_string(unmapped_count) +
                                                  " node(s) in the group were not found in the current partition map.";
            #endif
                // for statistics
                stats_.partial_affinity_decisions++;
            } else if (partition_counts.empty() && unmapped_count > 0) {
                // 都没有匹配到, 那么返回-1
                return_decision_type = -1; // missing
                // final_partition_index_result remains -1
            #if LOG_METIS_DECISION
                cross_partition_log_message_str = "[Route Decision] Epoch: " + std::to_string(stats_.total_partition_calls) + " Missing: None of the nodes in group " + group_str +
                                                  " found in the current partition map. Cannot determine dominant PartitionIndex.";
            #endif
                // for statistics
                stats_.missing_node_decisions++;
            } else {
                assert(false && "Unexpected state in partition_counts analysis.");
            }
        }
    } // partition_map_mutex_ (SHARED lock) is released here

#if LOG_METIS_DECISION
    if (!cross_partition_log_message_str.empty()) {
        logger_->info(cross_partition_log_message_str);
    }
#endif
    if (metis_decision_node != nullptr) {
        *metis_decision_node = final_partition_index_result;
    }
    return return_decision_type;
}

// 只查询当前分区的结果, 但不构建图
// 传入的request_partition_node_map 的key是图节点id, value是占位符, 函数会将value更新为对应的分区id
void NewMetis::get_metis_partitioning_result(std::unordered_map<uint64_t, idx_t> &request_partition_node_map) {
    std::shared_lock<std::shared_mutex> lock(partition_map_mutex_);
    for(auto& r: request_partition_node_map) {
        auto map_it = partition_node_map.find(r.first);
        if (map_it != partition_node_map.end()) {
            r.second = map_it->second; // Update to PartitionIndex
        } else {
            r.second = -1; // Indicate not found
        }
    }
}

node_id_t NewMetis::get_metis_partitioning_result(uint64_t request_partition_node) {
    std::shared_lock<std::shared_mutex> lock(partition_map_mutex_);
    auto map_it = partition_node_map.find(request_partition_node);
    if (map_it != partition_node_map.end()) {
        return map_it->second; // Return PartitionIndex
    } else {
        return -1; // Indicate not found
    }
}

// MODIFIED build_link_between_nodes_in_graph FUNCTION
void NewMetis::build_link_between_nodes_in_graph(uint64_t from_node, uint64_t to_node) {
    // try lock the partition_map_mutex_ for writing
    std::unique_lock<std::shared_mutex> lock(partition_map_mutex_); 
    // 首先先看下to_node是否已经有分区信息
    auto to_it = partition_node_map.find(to_node);
    if(to_it != partition_node_map.end()) return; // 已经有分区信息, 直接返回
    // 如果没有分区信息, 那么查看from_node的分区信息
    idx_t from_partition = partition_node_map.count(from_node) ? partition_node_map[from_node] : -1; 
    if (from_partition == -1) {
        // check pending_partition_node_map
        auto pending_it = pending_partition_node_map.find(from_node);
        if(pending_it != pending_partition_node_map.end() ) {
            from_partition = pending_it->second.first;
        }
    }
    if(from_partition == -1) { assert(false); } // 如果from_node没有分区信息, 则不进行任何处理, 按道理这种情况应该不会出现
    // 剩下的情况是to_it 不在partition_node_map中, 或者已经在里面了
    auto pending_to_it = pending_partition_node_map.find(to_node);
    if(pending_to_it != pending_partition_node_map.end()) {
        pending_to_it->second.second[from_partition]++; // 增加对应分区的计数
        idx_t now_idx = pending_to_it->second.first;
        if(from_partition != now_idx) {
            // 不同分区, 需要比较计数
            if(pending_to_it->second.second[from_partition] > pending_to_it->second.second[now_idx]) {
                // 更新为新的分区
                pending_to_it->second.first = from_partition;
            }
        }
    }
    else {
        // 不在pending_partition_node_map中, 直接插入
        std::vector<int> partition_counts(num_partitions_, 0);
        partition_counts[from_partition] = 1;
        pending_partition_node_map[to_node] = {from_partition, partition_counts};
    }
    return;
}

// ========================================================================
// MODIFIED partition_internal_graph FUNCTION
// ========================================================================
void NewMetis::partition_internal_graph(const std::string &output_partition_file,
                                               uint64_t ComputeNodeCount) {
    // std::cout<<"[Partition] Starting internal graph partitioning task (using DENSE ID snapshot)..." << std::endl;
    this->stats_.total_partition_calls++;
    if(enable_partition == false){
        // 理论上这种情况不应该出现
        logger_->info("Partitioning disabled, skipping this partitioning call.");
        return;
    }
    if(this->stats_.total_partition_calls > MetisWarmupRound){
        logger_->info("MetisWarmupRound reached, skipping further partitioning.");
        WarmupEnd = true; // indicate the end of warmup phase
        this->enable_partition = false;
    }
    logger_->info("Starting internal graph partitioning task (using DENSE ID snapshot).");

    std::vector<idx_t> xadj_csr;
    std::vector<idx_t> adjncy_csr;
    std::vector<idx_t> vwgt_csr;
    std::vector<idx_t> adjwgt_csr;
    std::vector<idx_t> part_csr;
    uint64_t total_edge_weight_sum = 0;
    idx_t nvtx_for_metis = 0;
    const idx_t ncon_for_metis = 1;
    bool conversion_to_csr_successful = true;

    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t> > graph_snapshot;
    std::unordered_map<uint64_t, uint64_t> weight_snapshot;
    std::vector<uint64_t> dense_to_original_snapshot;
    std::unordered_map<uint64_t, idx_t> original_to_dense_snapshot;
    idx_t num_dense_ids_snapshot = 0; {
        std::lock_guard<std::mutex> lock(graph_data_mutex_);
        logger_->info("Acquired graph_data_mutex_ for creating graph snapshot.");

        num_dense_ids_snapshot = next_dense_id_.load(std::memory_order_relaxed);
        nvtx_for_metis = num_dense_ids_snapshot;

        if (nvtx_for_metis == 0) {
            logger_->info("No unique nodes mapped (nvtx_for_metis = 0 based on snapshot). Cannot partition.");
            std::ofstream out_part_empty(output_partition_file);
            if (out_part_empty.is_open()) {
                out_part_empty << "RegionID,TableID,InnerRegionID,PartitionIndex\n";
                out_part_empty.close();
            } else {
                logger_->error("Cannot open partition output file " + output_partition_file + " for empty graph.");
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
            logger_->warning("num_dense_ids_snapshot (" + std::to_string(num_dense_ids_snapshot) +
                            ") > regionid_to_dense_map_ size (" + std::to_string(this->regionid_to_dense_map_.size()) +
                            "). Copying entire dense_map. Review synchronization.");
            if (this->regionid_to_dense_map_.size() < nvtx_for_metis) {
                nvtx_for_metis = this->regionid_to_dense_map_.size();
                logger_->info(
                    "Adjusted nvtx_for_metis to " + std::to_string(nvtx_for_metis) + " due to dense_map size.");
            }
        }
        if (nvtx_for_metis == 0 && num_dense_ids_snapshot > 0) {
            // Consistency check after adjustment
            logger_->info(
                "Error: nvtx_for_metis became 0 after dense_map size adjustment, but num_dense_ids_snapshot was > 0. Aborting."); {
                std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
                partition_node_map.clear();
            }
            return;
        }

        logger_->info("Graph snapshot created. Releasing graph_data_mutex_.");
    }

    size_t total_degree_sum_csr = 0; // Total number of edges (sum of degrees)
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
                logger_->info(
                    "Error: dense_i " + std::to_string(dense_i) + " out of bounds for dense_to_original_snapshot (size "
                    +
                    std::to_string(dense_to_original_snapshot.size()) + ") during degree sum. Skipping.");
                conversion_to_csr_successful = false;
                break;
            }
        }
    } // else, total_degree_sum_csr remains 0, nvtx_for_metis is 0.

    if (!conversion_to_csr_successful) {
        logger_->info("Aborting partitioning due to error in degree sum calculation from snapshot."); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }

    logger_->info("Snapshot state (dense): nvtx_for_metis = " + std::to_string(nvtx_for_metis) +
                 ", Total edges sum = " + std::to_string(total_degree_sum_csr));

    if (nvtx_for_metis == 0) {
        // Double check, could have been set to 0 if snapshot was inconsistent
        logger_->info("No vertices to partition after snapshot processing. Writing empty partition file.");
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

    logger_->info("Starting conversion of snapshot to METIS CSR format.");

    try {
        xadj_csr.resize(nvtx_for_metis + 1);
        vwgt_csr.resize(nvtx_for_metis * ncon_for_metis); // ncon_for_metis is 1
        adjncy_csr.reserve(total_degree_sum_csr);
        adjwgt_csr.reserve(total_degree_sum_csr);
        part_csr.resize(nvtx_for_metis);
    } catch (const std::bad_alloc &e) {
        logger_->info("Memory allocation failed for CSR arrays from snapshot: " + std::string(e.what()));
        conversion_to_csr_successful = false;
    }

    if (conversion_to_csr_successful) {
        idx_t current_edge_ptr = 0;
        for (idx_t dense_i = 0; dense_i < nvtx_for_metis; ++dense_i) {
            xadj_csr[dense_i] = current_edge_ptr;

            if (dense_i >= dense_to_original_snapshot.size()) {
                logger_->info(
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
                        total_edge_weight_sum += edge_weight;
                    } else {
                        logger_->info(
                            "Warning: Neighbor " + std::to_string(neighbor_original_id) + " of node " + std::to_string(
                                original_id) +
                            " (dense " + std::to_string(dense_i) +
                            ") not found in original_to_dense_snapshot. Skipping edge.");
                    }
                }

                for (const auto &[neighbor_dense_id, edge_weight]: sorted_dense_neighbors) {
                    if (neighbor_dense_id >= nvtx_for_metis || neighbor_dense_id < 0) {
                        logger_->info(
                            "Error: Invalid neighbor dense ID " + std::to_string(neighbor_dense_id) + " (range 0 to " +
                            std::to_string(nvtx_for_metis - 1) + ") for node " + std::to_string(original_id) + ".");
                        conversion_to_csr_successful = false;
                        break;
                    }
                    idx_t current_edge_weight = (edge_weight <= 0) ? 1 : static_cast<idx_t>(edge_weight);
                    if (edge_weight <= 0) {
                        logger_->info(
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
                logger_->info("Warning: CSR edge pointer count (" + std::to_string(current_edge_ptr) +
                             ") does not match calculated total edges sum (" + std::to_string(total_degree_sum_csr) +
                             "). Mismatch in dense graph structure from snapshot.");
            } else {
                //  输出边的数量
                logger_->info(
                    "CSR conversion of snapshot successful. Total entries in adjncy/adjwgt: " + std::to_string(
                        current_edge_ptr / 2) +
                    ". Total weight sum: " + std::to_string(total_edge_weight_sum / 2) + ".");
                this->stats_.total_edges_in_graph = current_edge_ptr / 2; 
                this->stats_.total_edges_weight = total_edge_weight_sum / 2;
            }
        }
    }

    if (!conversion_to_csr_successful) {
        logger_->info("Aborting partitioning due to CSR conversion failure from snapshot."); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }

    idx_t nWeights_metis = ncon_for_metis;
    idx_t nParts_metis = ComputeNodeCount;

    if (nParts_metis <= 0) {
        logger_->error(
            " Invalid number of partitions requested (" + std::to_string(nParts_metis) + ") for METIS. Aborting."); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }
    if (nParts_metis == 1) {
        logger_->warning(" Requested 1 partition. METIS call skipped, assigning all nodes to PartitionIndex 0.");
        std::ofstream out_part_one(output_partition_file);
        if (!out_part_one.is_open()) {
            logger_->error("Cannot open partition output file " + output_partition_file); {
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
        logger_->info("Partition result (all to PartitionIndex 0) written. Router map populated.");
        return;
    }
    if (nParts_metis > nvtx_for_metis) {
        logger_->warning("Requested partitions (" + std::to_string(nParts_metis) +
                        ") > number of dense vertices (" + std::to_string(nvtx_for_metis) +
                        "). Reducing partitions to nvtx_for_metis.");
        nParts_metis = nvtx_for_metis;
        if (nParts_metis == 1) {
            logger_->info("Partitions reduced to 1. Assigning all nodes to PartitionIndex 0.");
            std::ofstream out_part_one_reduced(output_partition_file);
            if (!out_part_one_reduced.is_open()) {
                logger_->error("Cannot open partition output file " + output_partition_file); {
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
            logger_->info("Partition result (all to PartitionIndex 0 after reduction) written. Router map populated.");
            return;
        }
        if (nParts_metis <= 0 && nvtx_for_metis > 0) {
            logger_->error(
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

    logger_->info("Calling METIS_PartGraphKway with nparts = " + std::to_string(nParts_metis) +
                 ", nvtx (dense snapshot) = " + std::to_string(nvtx_for_metis) +
                 ", ncon = " + std::to_string(nWeights_metis) + "...");

    idx_t options[METIS_NOPTIONS];
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT; // 通信体积最小化 METIS_OBJTYPE_VOL or METIS_OBJTYPE_CUT
    options[METIS_OPTION_CTYPE]    = METIS_CTYPE_SHEM;
    options[METIS_OPTION_IPTYPE]   = METIS_IPTYPE_GROW;
    options[METIS_OPTION_RTYPE]    = METIS_RTYPE_FM;
    options[METIS_OPTION_UFACTOR] = 100;               // 允许 10% 不平衡 (默认 30, 即 3%)
    options[METIS_OPTION_NCUTS] = 8;                   // 多次切割取最优
    options[METIS_OPTION_NITER] = 10;                  // 迭代次数
    options[METIS_OPTION_SEED] = 42;                   // 可复现性

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
        options, // options
        &objval_metis,
        part_csr.data()
    );

    if (metis_ret != METIS_OK) {
        logger_->info("METIS partitioning failed with error code: " + std::to_string(metis_ret)); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }
    logger_->info("METIS partitioning successful! Objective value (edge cut/volume): " + std::to_string(objval_metis));
    this->stats_.cut_edges_weight = objval_metis;
    this->stats_.total_nodes_in_graph = nvtx_for_metis;
    
    std::vector<uint64_t> partition_sizes(nParts_metis, 0);
    for (idx_t dense_i = 0; dense_i < part_csr.size(); ++dense_i) {
        idx_t assigned_partition = part_csr[dense_i];
        assert(assigned_partition >= 0 && assigned_partition < nParts_metis);
        partition_sizes[assigned_partition]++; 
    }
    std::string size_report = "Partition sizes (from METIS output): ";
    for (idx_t i = 0; i < nParts_metis; ++i) {
        size_report += "[PartitionIndex " + std::to_string(i) + ": " + std::to_string(partition_sizes[i]) + " nodes] ";
    }
    logger_->info(size_report);

    // 调用稳定化函数，它会修改 this->partition_node_map
    // 在调用前，this->partition_node_map 存储的是旧的分区结果
    // 传递 this->partition_node_map 的引用，它将作为旧分区信息被读取，并被新的稳定化结果覆盖
    this->stabilize_partition_indices(
        nvtx_for_metis,
        part_csr, // METIS 原始输出
        dense_to_original_snapshot,
        this->partition_node_map // 将被修改的路由映射
    );

    std::ofstream out_part_final(output_partition_file, std::ios::out | std::ios::app);
    if (!out_part_final.is_open()) {
        logger_->error("Cannot open partition output file " + output_partition_file); {
            std::unique_lock<std::shared_mutex> map_lock(partition_map_mutex_);
            partition_node_map.clear();
        }
        return;
    }
    out_part_final << "Epoch,RegionID,TableID,InnerRegionID,PartitionIndex\n"; {
        std::shared_lock<std::shared_mutex> lock(partition_map_mutex_); // 只需要读锁
        for (const auto &pair: partition_node_map) {
            uint64_t original_id = pair.first;
            idx_t assigned_partition_index = pair.second;
            Region region(original_id);
            out_part_final << stats_.total_partition_calls << "," << original_id << "," << region.getTableId() 
                << "," << region.getInnerRegionId() << "," << assigned_partition_index << "\n";
        }
    }
    out_part_final.close();
    logger_->info(
        "Partition results (RegionID,TableID,InnerRegionID,PartitionIndex) successfully written to " +
        output_partition_file);
}

void NewMetis::stabilize_partition_indices(
    idx_t nvtx,
    const std::vector<idx_t> &new_part_csr, // METIS's raw partition result (dense ID -> new_idx)
    const std::vector<uint64_t> &dense_to_original_id_snapshot, // dense ID -> original ID mapping
    std::unordered_map<uint64_t, idx_t> &current_partition_node_map_ref
    // Original ID -> old_idx, will be updated to stabilized new_idx
) {
    logger_->info(
        "Starting partition index stabilization process, ensuring indices are within [0, " + std::to_string(
            num_partitions_ - 1) + "].");

    // Create a copy of the OLD partition map to compare against.
    std::unordered_map<uint64_t, idx_t> old_partition_node_map_copy; {
        std::shared_lock<std::shared_mutex> lock(partition_map_mutex_); // Acquire read lock to copy
        old_partition_node_map_copy = current_partition_node_map_ref;
    }

    // Step 1: Collect information about existing (OLD) partitions
    std::map<idx_t, std::vector<uint64_t> > old_partition_to_nodes;
    for (const auto &pair: old_partition_node_map_copy) {
        uint64_t original_id = pair.first;
        idx_t old_partition_idx = pair.second;
        // 确保旧的索引本身就在合法范围内
        if (old_partition_idx < 0 || static_cast<uint64_t>(old_partition_idx) >= num_partitions_) {
            logger_->warning(
                "Old partition index " + std::to_string(old_partition_idx) + " for node " + std::to_string(original_id)
                + " is out of target range [0, " + std::to_string(num_partitions_ - 1) +
                "]. Adjusting to 0 for matching purposes.");
            old_partition_idx = 0; // 或者进行其他合理的调整，这里简单回退到0
        }
        old_partition_to_nodes[old_partition_idx].push_back(original_id);
    }
    logger_->info(
        "Collected information from " + std::to_string(old_partition_to_nodes.size()) +
        " existing partitions for matching.");

    // Step 2: Collect information about NEW partitions (from METIS's raw output)
    std::unordered_map<uint64_t, idx_t> temp_new_original_to_partition;
    std::map<idx_t, std::vector<uint64_t> > new_partition_to_nodes_metis_idx;
    // Map: METIS raw index -> List of Original IDs
    for (idx_t dense_i = 0; dense_i < nvtx; ++dense_i) {
        if (dense_i < dense_to_original_id_snapshot.size()) {
            uint64_t original_id = dense_to_original_id_snapshot[dense_i];
            idx_t new_partition_idx_metis = new_part_csr[dense_i];
            temp_new_original_to_partition[original_id] = new_partition_idx_metis;
            new_partition_to_nodes_metis_idx[new_partition_idx_metis].push_back(original_id);
        } else {
            logger_->error("Error: Dense ID " + std::to_string(dense_i) +
                          " out of bounds in dense_to_original_id_snapshot during new partition info collection. Skipping node.");
        }
    }
    logger_->info(
        "Collected information for " + std::to_string(new_partition_to_nodes_metis_idx.size()) +
        " new METIS partitions.");

    // Step 3: Calculate Overlap Matrix
    std::map<idx_t, std::map<idx_t, size_t> > overlap_matrix;
    // Map: Old Partition Index -> (New METIS Partition Index -> Overlap Count)
    for (const auto &old_part_pair: old_partition_to_nodes) {
        idx_t old_partition_idx = old_part_pair.first;
        const std::vector<uint64_t> &nodes_in_old_part = old_part_pair.second;

        for (uint64_t node_id: nodes_in_old_part) {
            auto it_new_part = temp_new_original_to_partition.find(node_id);
            if (it_new_part != temp_new_original_to_partition.end()) {
                idx_t new_partition_idx_metis = it_new_part->second;
                overlap_matrix[old_partition_idx][new_partition_idx_metis]++;
            }
        }
    }
    logger_->info("Overlap matrix calculated. Matrix size: " + std::to_string(overlap_matrix.size()) + " (rows).");

    // Step 4: Establish Best Mapping (Greedy Algorithm for maximum overlap)
    std::map<idx_t, idx_t> old_to_new_metis_mapping;
    // Map: Old Partition Index -> New METIS Partition Index (raw index)
    std::set<idx_t> assigned_old_partitions; // Track old partition indices that have been matched
    std::set<idx_t> assigned_new_metis_partitions; // Track new METIS partition indices that have been matched

    std::vector<std::tuple<size_t, idx_t, idx_t> > sorted_overlaps;
    for (const auto &old_pair: overlap_matrix) {
        idx_t old_idx = old_pair.first;
        for (const auto &new_pair: old_pair.second) {
            idx_t new_idx_metis = new_pair.first;
            size_t count = new_pair.second;
            sorted_overlaps.emplace_back(count, old_idx, new_idx_metis);
        }
    }
    std::sort(sorted_overlaps.rbegin(), sorted_overlaps.rend()); // Sort descending by overlap count

    for (const auto &entry: sorted_overlaps) {
        size_t count = std::get<0>(entry);
        idx_t old_idx = std::get<1>(entry);
        idx_t new_idx_metis = std::get<2>(entry);

        // If both the old partition and the new METIS partition haven't been assigned yet
        if (assigned_old_partitions.find(old_idx) == assigned_old_partitions.end() &&
            assigned_new_metis_partitions.find(new_idx_metis) == assigned_new_metis_partitions.end()) {
            // Only establish match if old_idx is within the target range [0, num_partitions_ - 1]
            if (static_cast<uint64_t>(old_idx) < num_partitions_) {
                old_to_new_metis_mapping[old_idx] = new_idx_metis;
                assigned_old_partitions.insert(old_idx);
                assigned_new_metis_partitions.insert(new_idx_metis);
                logger_->debug("Matched old partition " + std::to_string(old_idx) +
                              " to new METIS partition " + std::to_string(new_idx_metis) +
                              " with overlap " + std::to_string(count));
            } else {
                logger_->warning("Skipping match for old partition " + std::to_string(old_idx) +
                                " as it's outside target range [0, " + std::to_string(num_partitions_ - 1) + "].");
            }
        }
    }
    logger_->info(
        "Best partition index mapping established using greedy algorithm. Total matches: " + std::to_string(
            old_to_new_metis_mapping.size()) + ".");

    // Step 5: Construct the stabilized partition index mapping
    // Map: New METIS Partition Index -> Stabilized (Target) Partition Index
    std::map<idx_t, idx_t> new_metis_idx_to_stabilized_idx_map;
    std::set<idx_t> used_target_stabilized_indices;
    // Track which target stable indices (0 to num_partitions_-1) are already taken

    // First, populate `new_metis_idx_to_stabilized_idx_map` based on established matches
    for (const auto &pair: old_to_new_metis_mapping) {
        idx_t old_idx = pair.first; // This is the desired stable PartitionIndex (0 to num_partitions_-1)
        idx_t new_idx_metis = pair.second; // This is the raw METIS index

        new_metis_idx_to_stabilized_idx_map[new_idx_metis] = old_idx;
        used_target_stabilized_indices.insert(old_idx);
    }

    // Now, handle new METIS partitions that did not find a direct match.
    // Assign them a stable index from [0, num_partitions_ - 1] in a round-robin fashion,
    // prioritizing unused indices first, then reusing if necessary.
    idx_t current_round_robin_idx = 0; // Start round-robin from 0

    // Iterate through all METIS-assigned new partitions
    for (const auto &new_part_pair: new_partition_to_nodes_metis_idx) {
        idx_t new_idx_metis = new_part_pair.first;

        if (new_metis_idx_to_stabilized_idx_map.find(new_idx_metis) == new_metis_idx_to_stabilized_idx_map.end()) {
            // This METIS partition index was not matched to an old partition.
            // Assign it a stable index from [0, num_partitions_ - 1].

            // First, try to find an *unused* target index within the allowed range.
            idx_t assigned_stabilized_idx = -1;
            for (uint64_t i = 0; i < num_partitions_; ++i) {
                idx_t candidate_idx = (current_round_robin_idx + i) % num_partitions_;
                if (used_target_stabilized_indices.find(candidate_idx) == used_target_stabilized_indices.end()) {
                    assigned_stabilized_idx = candidate_idx;
                    break;
                }
            }

            if (assigned_stabilized_idx != -1) {
                // Found an unused target index
                new_metis_idx_to_stabilized_idx_map[new_idx_metis] = assigned_stabilized_idx;
                used_target_stabilized_indices.insert(assigned_stabilized_idx);
                current_round_robin_idx = (assigned_stabilized_idx + 1) % num_partitions_; // Advance round-robin
                logger_->debug("Assigned new METIS partition " + std::to_string(new_idx_metis) +
                              " to UNUSED stabilized index " + std::to_string(assigned_stabilized_idx) +
                              " (round-robin).");
            } else {
                // All target indices [0, num_partitions_ - 1] are currently "used" by a match.
                // We have to reuse one. Simply pick the next in round-robin sequence.
                // This will cause multiple METIS partitions to map to the same target index,
                // which is acceptable as long as the total number of partitions (num_partitions_) is the hard limit.
                assigned_stabilized_idx = current_round_robin_idx;
                new_metis_idx_to_stabilized_idx_map[new_idx_metis] = assigned_stabilized_idx;
                // No need to insert into used_target_stabilized_indices as it's already "used"
                current_round_robin_idx = (current_round_robin_idx + 1) % num_partitions_; // Advance round-robin
                logger_->warning("Assigned new METIS partition " + std::to_string(new_idx_metis) +
                                " to REUSED stabilized index " + std::to_string(assigned_stabilized_idx) +
                                " (round-robin). All target indices are currently occupied.");
            }
        }
    }

    logger_->info(
        "Stabilized partition index mapping created. Total target indices used (potentially reused): " +
        std::to_string(used_target_stabilized_indices.size()) + " out of " + std::to_string(num_partitions_) + ".");

    // Step 6: Construct the final stabilized `current_partition_node_map_ref` and calculate change rate
    uint64_t nodes_unchanged_count = 0;
    uint64_t nodes_existing_in_old_map = old_partition_node_map_copy.size(); {
        std::unique_lock<std::shared_mutex> lock(partition_map_mutex_); // Acquire exclusive write lock for router map
        current_partition_node_map_ref.clear(); // Clear the old map

        for (idx_t dense_i = 0; dense_i < nvtx; ++dense_i) {
            if (dense_i < dense_to_original_id_snapshot.size()) {
                uint64_t original_id = dense_to_original_id_snapshot[dense_i];
                idx_t original_new_partition_idx_metis = new_part_csr[dense_i]; // Raw index from METIS

                auto it_stabilized_idx_map = new_metis_idx_to_stabilized_idx_map.find(original_new_partition_idx_metis);
                idx_t final_assigned_partition_index;

                if (it_stabilized_idx_map != new_metis_idx_to_stabilized_idx_map.end()) {
                    final_assigned_partition_index = it_stabilized_idx_map->second;
                } else {
                    // This should ideally not happen if new_metis_idx_to_stabilized_idx_map
                    // is correctly populated with all raw METIS partition indices.
                    logger_->error(
                        "CRITICAL ERROR: METIS partition index " + std::to_string(original_new_partition_idx_metis) +
                        " for node " + std::to_string(original_id) +
                        " not found in stabilization map. Assigning to (0 % num_partitions_).");
                    final_assigned_partition_index = 0 % static_cast<idx_t>(num_partitions_);
                    // Fallback to 0 (within range)
                }

                // Final check to ensure the assigned index is within bounds [0, num_partitions_ - 1]
                if (static_cast<uint64_t>(final_assigned_partition_index) >= num_partitions_) {
                    logger_->error("CRITICAL ERROR: Stabilized index " + std::to_string(final_assigned_partition_index) +
                                  " for node " + std::to_string(original_id) + " is out of bounds [0, " +
                                  std::to_string(num_partitions_ - 1) + "]. Modulo correcting.");
                    final_assigned_partition_index %= static_cast<idx_t>(num_partitions_);
                }
                current_partition_node_map_ref[original_id] = final_assigned_partition_index;

                // Calculate unchanged nodes: only for nodes that existed in the previous partition map
                auto it_old_map = old_partition_node_map_copy.find(original_id);
                if (it_old_map != old_partition_node_map_copy.end()) {
                    if (it_old_map->second == final_assigned_partition_index) {
                        nodes_unchanged_count++;
                    }
                }
            } else {
                logger_->error("Dense ID " + std::to_string(dense_i) +
                              " out of bounds in dense_to_original_id_snapshot during final map creation. Skipping node for final map.");
            }
        }
    } // partition_map_mutex_ (UNIQUE lock) released

    // Log the change rate
    double change_rate = 0.0;
    if (nodes_existing_in_old_map > 0) {
        change_rate = static_cast<double>(nodes_existing_in_old_map - nodes_unchanged_count) /
                      nodes_existing_in_old_map;
        change_rates_history_.push_back(change_rate);
        change_times++;
        // if (change_times % 10 == 0) {
            double sum_change_rates = 0.0;
            // Sum the last 10 rates, or all if fewer than 10
            size_t start_idx = 0;
            if (change_rates_history_.size() > 10) {
                start_idx = change_rates_history_.size() - 10;
            }
            for (size_t i = start_idx; i < change_rates_history_.size(); ++i) {
                sum_change_rates += change_rates_history_[i];
            }
            double average_change_rate = sum_change_rates / (change_rates_history_.size() - start_idx);

            std::ofstream report_file(change_rate_report_file_, std::ios::app); // 以追加模式打开文件
            if (report_file.is_open()) {
                report_file << "--- Partition Change Rate Report ---\n";
                report_file << "Run: " << change_times << "\n"; // 使用 change_times 作为当前运行次数
                report_file << "Total successful partition runs: " << change_times << ".\n";
                report_file << "Average change rate over last " << (change_rates_history_.size() - start_idx) << " runs: "
                            << std::fixed << std::setprecision(4) << (average_change_rate * 100.0) << "%.\n";
                report_file << "------------------------------------\n";
                report_file.close();
                logger_->info("Partition change rate report written to " + change_rate_report_file_);
            } else {
                logger_->error("Failed to open partition change rate report file: " + change_rate_report_file_);
                // 失败时 fallback 到控制台输出
                std::cout << "--- Partition Change Rate Report ---" << std::endl;
                std::cout << "Total successful partition runs: " << change_times << "." << std::endl;
                std::cout << "Average change rate over last " << (change_rates_history_.size() - start_idx) << " runs: "
                          << std::fixed << std::setprecision(4) << (average_change_rate * 100.0) << "%." << std::endl;
                std::cout << "------------------------------------" << std::endl;
            }
        // }
    } else {
        logger_->info(
            "Partition index stabilization complete. No nodes existed in the previous partition map to compare for change rate (old map was empty).");
    }

    logger_->info("Router map updated with stabilized indices.");
}


