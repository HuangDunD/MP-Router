#pragma once

#include <cstdint>
#include <string>
#include <iostream>
#include "smart_router.h"
#include "config.h"

// RouterStatSnapshot: a lightweight snapshot of SmartRouter statistics
// Purpose: capture a point-in-time copy of counters (non-atomic types) so
// users can compute deltas between two snapshots.

struct RouterStatSnapshot {
    // cache sizes
    uint64_t hot_hash_entries = 0;
    uint64_t btree_bytes = 0;

    // lookup counts
    uint64_t hot_hit = 0;
    uint64_t hot_miss = 0;
    uint64_t btree_hit = 0;
    uint64_t btree_miss = 0;

    // evictions
    uint64_t evict_hot_entries = 0;
    
    // page changes
    int64_t change_page_cnt = 0;
    int64_t page_update_cnt = 0;

    // ownership changes
    int64_t ownership_changes = 0;
    std::vector<int> ownership_changes_per_txn_type = std::vector<int>(SYS_8_DECISION_TYPE_COUNT, 0); // for SYSTEM_MODE 8, 17 types of ownership changes

    // ownership transaction counters
    int64_t ownership_random_txns = 0;
    int64_t ownership_entirely_txns = 0;
    int64_t ownership_cross_txns = 0;

    // Metis statistics
    uint64_t total_nodes_in_graph = 0;
    uint64_t total_edges_weight = 0;
    uint64_t cut_edges_weight = 0;
    uint64_t missing_node_decisions = 0;
    uint64_t entire_affinity_decisions = 0;
    uint64_t partial_affinity_decisions = 0;
    uint64_t total_cross_partition_decisions = 0;
    uint64_t metis_query_successes = 0;
    uint64_t metis_query_missing = 0;
    
    // Metis and ownership combined statistics, for SYSTEM_MODE 8
    // for metis no decision
    uint64_t metis_no_decision = 0;
    // for metis missing
    uint64_t metis_missing_and_ownership_missing = 0;
    uint64_t metis_missing_and_ownership_entirely = 0;
    uint64_t metis_missing_and_ownership_cross = 0;
    // for metis entirely
    uint64_t metis_entirely_and_ownership_missing = 0;
    uint64_t metis_entirely_and_ownership_cross_equal = 0;
    uint64_t metis_entirely_and_ownership_cross_unequal = 0;
    uint64_t metis_entirely_and_ownership_entirely_equal = 0;
    uint64_t metis_entirely_and_ownership_entirely_unequal = 0;
    // for metis cross
    uint64_t metis_cross_and_ownership_missing = 0;
    uint64_t metis_cross_and_ownership_entirely_equal = 0;
    uint64_t metis_cross_and_ownership_entirely_unequal = 0;
    uint64_t metis_cross_and_ownership_cross_equal = 0; 
    uint64_t metis_cross_and_ownership_cross_unequal = 0; 
    // for metis partial
    uint64_t metis_partial_and_ownership_missing = 0;
    uint64_t metis_partial_and_ownership_entirely_equal = 0;
    uint64_t metis_partial_and_ownership_entirely_unequal = 0;
    uint64_t metis_partial_and_ownership_cross_equal = 0;
    uint64_t metis_partial_and_ownership_cross_unequal = 0; 
    // for time breakdown
    timespec snapshot_ts;
    double total_time_ms = 0.0;
    double fetch_txn_from_pool_ms = 0.0;
    double schedule_total_ms = 0.0;
    double push_txn_to_queue_ms = 0.0;
    double preprocess_txn_ms, wait_pending_txn_push_ms, wait_last_batch_finish_ms = 0.0;
    double preprocess_lookup_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double get_page_ownership_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double merge_global_txid_to_txn_map_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double compute_conflict_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double compute_union_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double ownership_retrieval_and_devide_unconflicted_txn_ms = 0.0; 
    double process_conflicted_txn_ms = 0.0;
    double merge_and_construct_ipq_ms = 0.0;
    double select_condidate_txns_ms = 0.0;
    double compute_transfer_page_ms = 0.0;
    double find_affected_txns_ms = 0.0;
    double decide_txn_schedule_ms = 0.0;
    double add_txn_dependency_ms = 0.0;
    double push_prioritized_txns_ms = 0.0;
    double fill_pipeline_bubble_ms = 0.0;
    double push_end_txns_ms = 0.0;
    double final_push_to_queues_ms = 0.0;

    std::vector<double> pop_txn_total_ms_per_node;
        std::vector<double> pop_txn_empty_total_ms_per_node;
        std::vector<double> pop_txn_dag_total_ms_per_node;
        std::vector<double> pop_txn_regular_total_ms_per_node;
    std::vector<double> wait_next_batch_total_ms_per_node;
    std::vector<double> sum_worker_thread_exec_time_ms_per_node;
    std::vector<double> sum_worker_thread_update_key_page_time_ms_per_node;
    std::vector<double> mark_done_total_ms_per_node;
    std::vector<double> log_debug_info_total_ms_per_node;

    // Helpers
    void print_snapshot() const {
        std::cout << "********** Smart Router page stats **********" << std::endl;
        std::cout << "Hot page hit: " << hot_hit << ", miss: " << hot_miss 
                  << ", hit ratio: " << (hot_hit + hot_miss > 0 ? (double)hot_hit / (hot_hit + hot_miss) * 100.0 : 0.0) << "%" << std::endl;
        std::cout << "Page ID changes: " << change_page_cnt << std::endl;
        std::cout << "Ownership changes: " << ownership_changes << std::endl;
        std::cout << "Page Operations count: " << page_update_cnt << std::endl; 
        std::cout << "Simulated page ownership changes ratio (cache fusion ratio): " 
                    << (page_update_cnt > 0 ? (double)ownership_changes / page_update_cnt * 100.0 : 0.0) << "%" << std::endl;
        std::cout << "Ownership-based routing txns: random " << ownership_random_txns 
                    << ", entire " << ownership_entirely_txns 
                    << ", cross " << ownership_cross_txns << std::endl;
        std::cout << "Metis total nodes in graph: " << total_nodes_in_graph << std::endl;
        std::cout << "Metis total edges weight: " << total_edges_weight << std::endl;
        std::cout << "Metis edge weight cut: " << cut_edges_weight << std::endl;
        std::cout << "Metis edge cut ratio: " << (total_edges_weight > 0 ? 1.0 * cut_edges_weight / total_edges_weight : 0.0) << std::endl;
        std::cout << "Metis cross node decisions: " << total_cross_partition_decisions << std::endl;
        std::cout << "Metis partial affinity decisions: " << partial_affinity_decisions << std::endl;
        std::cout << "Metis full affinity decisions: " << entire_affinity_decisions << std::endl;
        std::cout << "Metis missing decisions: " << missing_node_decisions << std::endl; 
        std::cout << "Metis query successes: " << metis_query_successes << std::endl;
        std::cout << "Metis query missing: " << metis_query_missing << std::endl;

        
        if(SYSTEM_MODE == 3 || SYSTEM_MODE == 9 || SYSTEM_MODE == 10) {
            // for SYSTEM_MODE 3
            std::cout << "Metis decisions (for SYSTEM_MODE 3):" << std::endl;
            std::cout << "  Metis No Decision Ownership Changes: " << ownership_changes_per_txn_type[0] << std::endl;
            std::cout << "  Metis Missing : " << missing_node_decisions << " Ownership Changes: " << ownership_changes_per_txn_type[1] << std::endl;
            std::cout << "  Metis Entirely : " << entire_affinity_decisions << " Ownership Changes: " << ownership_changes_per_txn_type[2] << std::endl;
            std::cout << "  Metis Partial : " << partial_affinity_decisions << " Ownership Changes: " << ownership_changes_per_txn_type[3] << std::endl;
            std::cout << "  Metis Cross : " << total_cross_partition_decisions << " Ownership Changes: " << ownership_changes_per_txn_type[4] << std::endl;
        }
        if(SYSTEM_MODE == 8) {
            // for SYSTEM_MODE 8
            // Metis and Ownership combined statistics
            std::cout << "Metis and Ownership combined decisions (for SYSTEM_MODE 8):" << std::endl;
            std::cout << "  Metis No Decision: " << metis_no_decision << " Ownership Changes: " << ownership_changes_per_txn_type[0] << std::endl;
            std::cout << "  Metis Missing & Ownership Missing: " << metis_missing_and_ownership_missing << " Ownership Changes: " << ownership_changes_per_txn_type[1] << std::endl;
            std::cout << "  Metis Missing & Ownership Entirely: " << metis_missing_and_ownership_entirely << " Ownership Changes: " << ownership_changes_per_txn_type[2] << std::endl;
            std::cout << "  Metis Missing & Ownership Cross: " << metis_missing_and_ownership_cross << " Ownership Changes: " << ownership_changes_per_txn_type[3] << std::endl;
            std::cout << "  Metis Entirely & Ownership Missing: " << metis_entirely_and_ownership_missing << " Ownership Changes: " << ownership_changes_per_txn_type[4] << std::endl;
            std::cout << "  Metis Entirely & Ownership Cross Equal: " << metis_entirely_and_ownership_cross_equal << " Ownership Changes: " << ownership_changes_per_txn_type[5] << std::endl;
            std::cout << "  Metis Entirely & Ownership Cross Unequal: " << metis_entirely_and_ownership_cross_unequal << " Ownership Changes: " << ownership_changes_per_txn_type[6] << std::endl;
            std::cout << "  Metis Entirely & Ownership Entirely Equal: " << metis_entirely_and_ownership_entirely_equal << " Ownership Changes: " << ownership_changes_per_txn_type[7] << std::endl;
            std::cout << "  Metis Entirely & Ownership Entirely Unequal: " << metis_entirely_and_ownership_entirely_unequal << " Ownership Changes: " << ownership_changes_per_txn_type[8] << std::endl;
            std::cout << "  Metis Cross & Ownership Missing: " << metis_cross_and_ownership_missing << " Ownership Changes: " << ownership_changes_per_txn_type[9] << std::endl;
            std::cout << "  Metis Cross & Ownership Entirely Equal: " << metis_cross_and_ownership_entirely_equal << " Ownership Changes: " << ownership_changes_per_txn_type[10] << std::endl;
            std::cout << "  Metis Cross & Ownership Entirely Unequal: " << metis_cross_and_ownership_entirely_unequal << " Ownership Changes: " << ownership_changes_per_txn_type[11] << std::endl;
            std::cout << "  Metis Cross & Ownership Cross Equal: " << metis_cross_and_ownership_cross_equal << " Ownership Changes: " << ownership_changes_per_txn_type[12] << std::endl;
            std::cout << "  Metis Cross & Ownership Cross Unequal: " << metis_cross_and_ownership_cross_unequal << " Ownership Changes: " << ownership_changes_per_txn_type[13] << std::endl;
            std::cout << "  Metis Partial & Ownership Missing: " << metis_partial_and_ownership_missing << " Ownership Changes: " << ownership_changes_per_txn_type[14] << std::endl;
            std::cout << "  Metis Partial & Ownership Entirely Equal: " << metis_partial_and_ownership_entirely_equal << " Ownership Changes: " << ownership_changes_per_txn_type[15] << std::endl;
            std::cout << "  Metis Partial & Ownership Entirely Unequal: " << metis_partial_and_ownership_entirely_unequal << " Ownership Changes: " << ownership_changes_per_txn_type[16] << std::endl;
            std::cout << "  Metis Partial & Ownership Cross Equal: " << metis_partial_and_ownership_cross_equal << " Ownership Changes: " << ownership_changes_per_txn_type[17] << std::endl;
            std::cout << "  Metis Partial & Ownership Cross Unequal: " << metis_partial_and_ownership_cross_unequal << " Ownership Changes: " << ownership_changes_per_txn_type[18] << std::endl;
        }
        std::cout << "*********************************************" << std::endl;

        std::cout << "----- Time Statistics (ms)-----:" << std::endl;
        std::cout << "Total Time Elapsed: " << total_time_ms << " ms" << std::endl;
        std::cout << "[Router Thread Time Breakdown] " << std::endl;
        std::cout << "  Fetch Txn From Pool Time: " <<fetch_txn_from_pool_ms << " ms" << std::endl;
        std::cout << "  Schedule Batch Total Time: " << schedule_total_ms << " ms" << std::endl;
        std::cout << "    Preprocess Txn Time: " << preprocess_txn_ms << " ms" << std::endl;
        std::cout << "      Preprocess Lookup Time: " << preprocess_lookup_ms << " ms" << std::endl;
        std::cout << "      Merge Global Txid To Txn Map Time: " << merge_global_txid_to_txn_map_ms << " ms" << std::endl;
        std::cout << "      Compute Conflict Time: " << compute_conflict_ms << " ms" << std::endl;
        std::cout << "      Compute Union Time: " << compute_union_ms << " ms" << std::endl;
        std::cout << "    Wait Pending Txn Push Time: " << wait_pending_txn_push_ms << " ms" << std::endl;
        std::cout << "    Wait Last Batch Finish Time: " << wait_last_batch_finish_ms << " ms" << std::endl;
        std::cout << "    Get Page Ownership Time: " << get_page_ownership_ms << " ms" << std::endl;
        std::cout << "    Ownership Retrieval And Devide Unconflicted Txn Time: " 
                  << ownership_retrieval_and_devide_unconflicted_txn_ms << " ms" << std::endl;
        std::cout << "    Process Conflicted Txn Time: " << process_conflicted_txn_ms << " ms" << std::endl;
        std::cout << "      Merge And Construct IPQ Time: " << merge_and_construct_ipq_ms << " ms" << std::endl;
        std::cout << "      Select Condidate Txns Time: " << select_condidate_txns_ms << " ms" << std::endl;
        std::cout << "      Compute Transfer Page Time: " << compute_transfer_page_ms << " ms" << std::endl;
        std::cout << "      Find Affected Txns Time: " << find_affected_txns_ms << " ms" << std::endl;
        std::cout << "      Decide Txn Schedule Time: " << decide_txn_schedule_ms << " ms" << std::endl;
        std::cout << "      Add Txn Dependency Time: " << add_txn_dependency_ms << " ms" << std::endl;
        std::cout << "      Push Prioritized Txns Time: " << push_prioritized_txns_ms << " ms" << std::endl;
        std::cout << "      Fill Pipeline Bubble Time: " << fill_pipeline_bubble_ms << " ms" << std::endl;
        std::cout << "      Push End Txns Time: " << push_end_txns_ms << " ms" << std::endl;
        std::cout << "      Final Push To Queues Time: " << final_push_to_queues_ms << " ms" << std::endl;
        std::cout << "  Push Txn To Queue Time: " << push_txn_to_queue_ms << " ms" << std::endl;

        std::cout << "[Worker Thread Time Breakdown] " << std::endl;
        for(int i=0; i< ComputeNodeCount; i++) {
            std::cout << "Node " << i << ":" << std::endl;
            std::cout << "  Average Pop Txn From Queue Time: " << pop_txn_total_ms_per_node[i] / worker_threads << " ms" << std::endl; 
            std::cout << "    Average Pop Empty Time: " << pop_txn_empty_total_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "    Average Pop DAG Time: " << pop_txn_dag_total_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "    Average Pop Regular Time: " << pop_txn_regular_total_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "  Average Wait Next Batch Time: " << wait_next_batch_total_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "  Average Worker Thread Exec Time: " << sum_worker_thread_exec_time_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "    Average Worker Thread Update Key Page Time: " << sum_worker_thread_update_key_page_time_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "  Average Mark Done Time: " << mark_done_total_ms_per_node[i] / worker_threads << " ms" << std::endl;
            std::cout << "  Average Log Debug Info Time: " << log_debug_info_total_ms_per_node[i] / worker_threads << " ms" << std::endl;
        }
        std::cout << "------------------------------------------" << std::endl;
        return;
    }
};

// Take a snapshot from a SmartRouter instance. This reads atomic fields using
// relaxed loads and copies non-atomic fields under the SmartRouter's mutex when possible.
inline RouterStatSnapshot take_router_snapshot(SmartRouter* router) {
    RouterStatSnapshot snap;
    if (router == nullptr) return snap;
    // SmartRouter exposes get_stats() which returns a reference. We'll copy values.
    SmartRouter::Stats &s = router->get_stats();
    // Non-atomic fields (copy directly)
    snap.hot_hash_entries = s.hot_hash_entries;
    snap.btree_bytes = s.btree_bytes;
    snap.hot_hit = s.hot_hit;
    snap.hot_miss = s.hot_miss;
    snap.btree_hit = s.btree_hit;
    snap.btree_miss = s.btree_miss;
    snap.evict_hot_entries = s.evict_hot_entries;
    // Atomic fields -- read using load
    snap.change_page_cnt = s.change_page_cnt.load(std::memory_order_relaxed);
    snap.page_update_cnt = s.page_update_cnt.load(std::memory_order_relaxed);

    snap.ownership_changes = router->get_ownership_changes();
    snap.ownership_changes_per_txn_type = router->get_ownership_changes_per_txn_type();

    snap.ownership_random_txns = s.ownership_random_txns.load(std::memory_order_relaxed);
    snap.ownership_entirely_txns = s.ownership_entirely_txns.load(std::memory_order_relaxed);
    snap.ownership_cross_txns = s.ownership_cross_txns.load(std::memory_order_relaxed);

    // Metis stats
    const NewMetis::Stats& ms = router->get_metis_stats();
    snap.total_nodes_in_graph = ms.total_nodes_in_graph;
    snap.total_edges_weight = ms.total_edges_weight;
    snap.cut_edges_weight = ms.cut_edges_weight;
    snap.missing_node_decisions = ms.missing_node_decisions.load(std::memory_order_relaxed);
    snap.entire_affinity_decisions = ms.entire_affinity_decisions.load(std::memory_order_relaxed);
    snap.partial_affinity_decisions = ms.partial_affinity_decisions.load(std::memory_order_relaxed);
    snap.total_cross_partition_decisions = ms.total_cross_partition_decisions.load(std::memory_order_relaxed);
    snap.metis_query_successes = ms.metis_query_successes.load(std::memory_order_relaxed);
    snap.metis_query_missing = ms.metis_query_missing.load(std::memory_order_relaxed);

    // Metis and ownership combined stats
    snap.metis_no_decision = s.metis_no_decision.load(std::memory_order_relaxed);
    snap.metis_missing_and_ownership_missing = s.metis_missing_and_ownership_missing.load(std::memory_order_relaxed);
    snap.metis_missing_and_ownership_entirely = s.metis_missing_and_ownership_entirely.load(std::memory_order_relaxed);
    snap.metis_missing_and_ownership_cross = s.metis_missing_and_ownership_cross.load(std::memory_order_relaxed);
    snap.metis_entirely_and_ownership_missing = s.metis_entirely_and_ownership_missing.load(std::memory_order_relaxed);
    snap.metis_entirely_and_ownership_cross_equal = s.metis_entirely_and_ownership_cross_equal.load(std::memory_order_relaxed);
    snap.metis_entirely_and_ownership_cross_unequal = s.metis_entirely_and_ownership_cross_unequal.load(std::memory_order_relaxed);
    snap.metis_entirely_and_ownership_entirely_equal = s.metis_entirely_and_ownership_entirely_equal.load(std::memory_order_relaxed);
    snap.metis_entirely_and_ownership_entirely_unequal = s.metis_entirely_and_ownership_entirely_unequal.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_missing = s.metis_cross_and_ownership_missing.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_entirely_equal = s.metis_cross_and_ownership_entirely_equal.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_entirely_unequal = s.metis_cross_and_ownership_entirely_unequal.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_cross_equal = s.metis_cross_and_ownership_cross_equal.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_cross_unequal = s.metis_cross_and_ownership_cross_unequal.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_missing = s.metis_partial_and_ownership_missing.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_entirely_equal = s.metis_partial_and_ownership_entirely_equal.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_entirely_unequal = s.metis_partial_and_ownership_entirely_unequal.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_cross_equal = s.metis_partial_and_ownership_cross_equal.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_cross_unequal = s.metis_partial_and_ownership_cross_unequal.load(std::memory_order_relaxed);

    // time breakdown
    clock_gettime(CLOCK_MONOTONIC, &snap.snapshot_ts);
    router->sum_worker_thread_stat_time(); // update the sum
    if(SYSTEM_MODE >= 0 && SYSTEM_MODE <= 8 || SYSTEM_MODE == 13 || (SYSTEM_MODE >= 23 && SYSTEM_MODE <= 25)) {
        // 对于这些模式, 是使用多线程router的，因此需要计算平均
        router->Record_time_ms(false, true, true);
    } else if(SYSTEM_MODE == 11) {
        router->Record_time_ms(false, false, true);
    }
    
    SmartRouter::TimeBreakdown& tdb = router->get_time_breakdown();
    snap.fetch_txn_from_pool_ms = tdb.fetch_txn_from_pool_ms;
    snap.schedule_total_ms = tdb.schedule_total_ms;
    snap.preprocess_txn_ms = tdb.preprocess_txn_ms;
    snap.merge_global_txid_to_txn_map_ms = tdb.merge_global_txid_to_txn_map_ms;
    snap.get_page_ownership_ms = tdb.get_page_ownership_ms;
    snap.preprocess_lookup_ms = tdb.preprocess_lookup_ms;
    snap.compute_conflict_ms = tdb.compute_conflict_ms;
    snap.compute_union_ms = tdb.compute_union_ms;
    snap.wait_pending_txn_push_ms = tdb.wait_pending_txn_push_ms;
    snap.wait_last_batch_finish_ms = tdb.wait_last_batch_finish_ms;
    snap.ownership_retrieval_and_devide_unconflicted_txn_ms = tdb.ownership_retrieval_and_devide_unconflicted_txn_ms;
    snap.merge_and_construct_ipq_ms = tdb.merge_and_construct_ipq_ms;
    snap.process_conflicted_txn_ms = tdb.process_conflicted_txn_ms;
    snap.select_condidate_txns_ms = tdb.select_condidate_txns_ms;
    snap.compute_transfer_page_ms = tdb.compute_transfer_page_ms;
    snap.find_affected_txns_ms = tdb.find_affected_txns_ms;
    snap.decide_txn_schedule_ms = tdb.decide_txn_schedule_ms;
    snap.add_txn_dependency_ms = tdb.add_txn_dependency_ms;
    snap.push_prioritized_txns_ms = tdb.push_prioritized_txns_ms;
    snap.fill_pipeline_bubble_ms = tdb.fill_pipeline_bubble_ms;
    snap.push_end_txns_ms = tdb.push_end_txns_ms;
    snap.final_push_to_queues_ms = tdb.final_push_to_queues_ms;
    snap.pop_txn_total_ms_per_node = tdb.pop_txn_total_ms_per_node;
    snap.pop_txn_empty_total_ms_per_node = tdb.pop_txn_empty_total_ms_per_node;
    snap.pop_txn_dag_total_ms_per_node = tdb.pop_txn_dag_total_ms_per_node;
    snap.pop_txn_regular_total_ms_per_node = tdb.pop_txn_regular_total_ms_per_node;
    snap.wait_next_batch_total_ms_per_node = tdb.wait_next_batch_total_ms_per_node;
    snap.sum_worker_thread_exec_time_ms_per_node = tdb.sum_worker_thread_exec_time_ms_per_node;
    snap.sum_worker_thread_update_key_page_time_ms_per_node = tdb.sum_worker_thread_update_key_page_time_ms_per_node;
    snap.push_txn_to_queue_ms = tdb.push_txn_to_queue_ms;
    snap.mark_done_total_ms_per_node = tdb.mark_done_total_ms_per_node;
    snap.log_debug_info_total_ms_per_node = tdb.log_debug_info_total_ms_per_node;
    return snap;
}

// Compute difference: b - a (i.e., stats that occurred between snapshot a and b)
inline RouterStatSnapshot diff_snapshot(const RouterStatSnapshot &a, const RouterStatSnapshot &b) {
    RouterStatSnapshot d;
    // const, 这些属性不需要计算差值
    d.hot_hash_entries = b.hot_hash_entries;
    d.btree_bytes = b.btree_bytes;
    d.hot_hit = b.hot_hit;
    d.hot_miss = b.hot_miss;
    d.btree_hit = b.btree_hit;
    d.btree_miss = b.btree_miss;
    d.evict_hot_entries = b.evict_hot_entries;

    d.total_nodes_in_graph = b.total_nodes_in_graph;
    d.total_edges_weight = b.total_edges_weight;
    d.cut_edges_weight = b.cut_edges_weight;

    // -----------------
    // 需要计算差值的属性
    // smart router, and ownership simulation
    d.change_page_cnt = b.change_page_cnt - a.change_page_cnt;
    d.page_update_cnt = b.page_update_cnt - a.page_update_cnt;

    // ownership changes
    d.ownership_changes = b.ownership_changes - a.ownership_changes;
    for(size_t i=0; i<b.ownership_changes_per_txn_type.size(); i++) {
        d.ownership_changes_per_txn_type[i] = b.ownership_changes_per_txn_type[i] - a.ownership_changes_per_txn_type[i];
    }

    // ownership transaction counters
    d.ownership_random_txns = b.ownership_random_txns - a.ownership_random_txns;
    d.ownership_entirely_txns = b.ownership_entirely_txns - a.ownership_entirely_txns;
    d.ownership_cross_txns = b.ownership_cross_txns - a.ownership_cross_txns;

    // Metis stats
    d.missing_node_decisions = (b.missing_node_decisions >= a.missing_node_decisions) ? (b.missing_node_decisions - a.missing_node_decisions) : 0;
    d.entire_affinity_decisions = (b.entire_affinity_decisions >= a.entire_affinity_decisions) ? (b.entire_affinity_decisions - a.entire_affinity_decisions) : 0;
    d.partial_affinity_decisions = (b.partial_affinity_decisions >= a.partial_affinity_decisions) ? (b.partial_affinity_decisions - a.partial_affinity_decisions) : 0;
    d.total_cross_partition_decisions = (b.total_cross_partition_decisions >= a.total_cross_partition_decisions) ? (b.total_cross_partition_decisions - a.total_cross_partition_decisions) : 0;
    d.metis_query_successes = (b.metis_query_successes >= a.metis_query_successes) ? (b.metis_query_successes - a.metis_query_successes) : 0;
    d.metis_query_missing = (b.metis_query_missing >= a.metis_query_missing) ? (b.metis_query_missing - a.metis_query_missing) : 0;

    // Metis and ownership combined stats
    d.metis_no_decision = (b.metis_no_decision >= a.metis_no_decision) ? (b.metis_no_decision - a.metis_no_decision) : 0;
    d.metis_missing_and_ownership_missing = (b.metis_missing_and_ownership_missing >= a.metis_missing_and_ownership_missing) ? 
                                            (b.metis_missing_and_ownership_missing - a.metis_missing_and_ownership_missing) : 0;
    d.metis_missing_and_ownership_entirely = (b.metis_missing_and_ownership_entirely >= a.metis_missing_and_ownership_entirely) ? 
                                            (b.metis_missing_and_ownership_entirely - a.metis_missing_and_ownership_entirely) : 0;
    d.metis_missing_and_ownership_cross = (b.metis_missing_and_ownership_cross >= a.metis_missing_and_ownership_cross) ? 
                                            (b.metis_missing_and_ownership_cross - a.metis_missing_and_ownership_cross) : 0;
    d.metis_entirely_and_ownership_missing = (b.metis_entirely_and_ownership_missing >= a.metis_entirely_and_ownership_missing) ? 
                                            (b.metis_entirely_and_ownership_missing - a.metis_entirely_and_ownership_missing) : 0;
    d.metis_entirely_and_ownership_cross_equal = (b.metis_entirely_and_ownership_cross_equal >= a.metis_entirely_and_ownership_cross_equal) ? 
                                            (b.metis_entirely_and_ownership_cross_equal - a.metis_entirely_and_ownership_cross_equal) : 0;
    d.metis_entirely_and_ownership_cross_unequal = (b.metis_entirely_and_ownership_cross_unequal >= a.metis_entirely_and_ownership_cross_unequal) ? 
                                            (b.metis_entirely_and_ownership_cross_unequal - a.metis_entirely_and_ownership_cross_unequal) : 0;
    d.metis_entirely_and_ownership_entirely_equal = (b.metis_entirely_and_ownership_entirely_equal >= a.metis_entirely_and_ownership_entirely_equal) ? 
                                            (b.metis_entirely_and_ownership_entirely_equal - a.metis_entirely_and_ownership_entirely_equal) : 0;
    d.metis_entirely_and_ownership_entirely_unequal = (b.metis_entirely_and_ownership_entirely_unequal >= a.metis_entirely_and_ownership_entirely_unequal) ? 
                                            (b.metis_entirely_and_ownership_entirely_unequal - a.metis_entirely_and_ownership_entirely_unequal) : 0;
    d.metis_cross_and_ownership_missing = (b.metis_cross_and_ownership_missing >= a.metis_cross_and_ownership_missing) ? 
                                            (b.metis_cross_and_ownership_missing - a.metis_cross_and_ownership_missing) : 0;
    d.metis_cross_and_ownership_entirely_equal = (b.metis_cross_and_ownership_entirely_equal >= a.metis_cross_and_ownership_entirely_equal) ? 
                                            (b.metis_cross_and_ownership_entirely_equal - a.metis_cross_and_ownership_entirely_equal) : 0;
    d.metis_cross_and_ownership_entirely_unequal = (b.metis_cross_and_ownership_entirely_unequal >= a.metis_cross_and_ownership_entirely_unequal) ? 
                                            (b.metis_cross_and_ownership_entirely_unequal - a.metis_cross_and_ownership_entirely_unequal) : 0;
    d.metis_cross_and_ownership_cross_equal = (b.metis_cross_and_ownership_cross_equal >= a.metis_cross_and_ownership_cross_equal) ? 
                                            (b.metis_cross_and_ownership_cross_equal - a.metis_cross_and_ownership_cross_equal) : 0;
    d.metis_cross_and_ownership_cross_unequal = (b.metis_cross_and_ownership_cross_unequal >= a.metis_cross_and_ownership_cross_unequal) ? 
                                            (b.metis_cross_and_ownership_cross_unequal - a.metis_cross_and_ownership_cross_unequal) : 0;
    d.metis_partial_and_ownership_missing = (b.metis_partial_and_ownership_missing >= a.metis_partial_and_ownership_missing) ? 
                                            (b.metis_partial_and_ownership_missing - a.metis_partial_and_ownership_missing) : 0;
    d.metis_partial_and_ownership_entirely_equal = (b.metis_partial_and_ownership_entirely_equal >= a.metis_partial_and_ownership_entirely_equal) ? 
                                            (b.metis_partial_and_ownership_entirely_equal - a.metis_partial_and_ownership_entirely_equal) : 0;
    d.metis_partial_and_ownership_entirely_unequal = (b.metis_partial_and_ownership_entirely_unequal >= a.metis_partial_and_ownership_entirely_unequal) ? 
                                            (b.metis_partial_and_ownership_entirely_unequal - a.metis_partial_and_ownership_entirely_unequal) : 0;
    d.metis_partial_and_ownership_cross_equal = (b.metis_partial_and_ownership_cross_equal >= a.metis_partial_and_ownership_cross_equal) ? 
                                            (b.metis_partial_and_ownership_cross_equal - a.metis_partial_and_ownership_cross_equal) : 0;
    d.metis_partial_and_ownership_cross_unequal = (b.metis_partial_and_ownership_cross_unequal >= a.metis_partial_and_ownership_cross_unequal) ? 
                                            (b.metis_partial_and_ownership_cross_unequal - a.metis_partial_and_ownership_cross_unequal) : 0;
    
    // time breakdown
    d.total_time_ms = (b.snapshot_ts.tv_sec - a.snapshot_ts.tv_sec) * 1000.0 + (b.snapshot_ts.tv_nsec - a.snapshot_ts.tv_nsec) / 1e6;
    d.fetch_txn_from_pool_ms = b.fetch_txn_from_pool_ms - a.fetch_txn_from_pool_ms;
    d.schedule_total_ms = b.schedule_total_ms - a.schedule_total_ms;
    d.preprocess_txn_ms = b.preprocess_txn_ms - a.preprocess_txn_ms;
    d.merge_global_txid_to_txn_map_ms = b.merge_global_txid_to_txn_map_ms - a.merge_global_txid_to_txn_map_ms;
    d.get_page_ownership_ms = b.get_page_ownership_ms - a.get_page_ownership_ms;
    d.preprocess_lookup_ms = b.preprocess_lookup_ms - a.preprocess_lookup_ms;
    d.compute_conflict_ms = b.compute_conflict_ms - a.compute_conflict_ms;
    d.compute_union_ms = b.compute_union_ms - a.compute_union_ms;
    d.wait_pending_txn_push_ms = b.wait_pending_txn_push_ms - a.wait_pending_txn_push_ms;
    d.wait_last_batch_finish_ms = b.wait_last_batch_finish_ms - a.wait_last_batch_finish_ms;
    d.ownership_retrieval_and_devide_unconflicted_txn_ms = b.ownership_retrieval_and_devide_unconflicted_txn_ms - a.ownership_retrieval_and_devide_unconflicted_txn_ms;
    d.merge_and_construct_ipq_ms = b.merge_and_construct_ipq_ms - a.merge_and_construct_ipq_ms;
    d.process_conflicted_txn_ms = b.process_conflicted_txn_ms - a.process_conflicted_txn_ms;
    d.select_condidate_txns_ms = b.select_condidate_txns_ms - a.select_condidate_txns_ms;
    d.compute_transfer_page_ms = b.compute_transfer_page_ms - a.compute_transfer_page_ms;
    d.find_affected_txns_ms = b.find_affected_txns_ms - a.find_affected_txns_ms;
    d.decide_txn_schedule_ms = b.decide_txn_schedule_ms - a.decide_txn_schedule_ms;
    d.add_txn_dependency_ms = b.add_txn_dependency_ms - a.add_txn_dependency_ms;
    d.push_prioritized_txns_ms = b.push_prioritized_txns_ms - a.push_prioritized_txns_ms;
    d.fill_pipeline_bubble_ms = b.fill_pipeline_bubble_ms - a.fill_pipeline_bubble_ms;
    d.push_end_txns_ms = b.push_end_txns_ms - a.push_end_txns_ms;
    d.final_push_to_queues_ms = b.final_push_to_queues_ms - a.final_push_to_queues_ms;
    for(int i=0; i< ComputeNodeCount; i++) {
        d.pop_txn_total_ms_per_node.push_back( b.pop_txn_total_ms_per_node[i] - a.pop_txn_total_ms_per_node[i] );
        d.pop_txn_empty_total_ms_per_node.push_back( b.pop_txn_empty_total_ms_per_node[i] - a.pop_txn_empty_total_ms_per_node[i] );
        d.pop_txn_dag_total_ms_per_node.push_back( b.pop_txn_dag_total_ms_per_node[i] - a.pop_txn_dag_total_ms_per_node[i] );
        d.pop_txn_regular_total_ms_per_node.push_back( b.pop_txn_regular_total_ms_per_node[i] - a.pop_txn_regular_total_ms_per_node[i] );
        d.wait_next_batch_total_ms_per_node.push_back( b.wait_next_batch_total_ms_per_node[i] - a.wait_next_batch_total_ms_per_node[i] );
        d.sum_worker_thread_exec_time_ms_per_node.push_back( b.sum_worker_thread_exec_time_ms_per_node[i] - a.sum_worker_thread_exec_time_ms_per_node[i] );
        d.sum_worker_thread_update_key_page_time_ms_per_node.push_back( b.sum_worker_thread_update_key_page_time_ms_per_node[i] - a.sum_worker_thread_update_key_page_time_ms_per_node[i] );
        d.mark_done_total_ms_per_node.push_back( b.mark_done_total_ms_per_node[i] - a.mark_done_total_ms_per_node[i] );
        d.log_debug_info_total_ms_per_node.push_back( b.log_debug_info_total_ms_per_node[i] - a.log_debug_info_total_ms_per_node[i] );
    }
    d.push_txn_to_queue_ms = b.push_txn_to_queue_ms - a.push_txn_to_queue_ms;
    return d;
}

inline void print_snapshot(const RouterStatSnapshot &snap, const std::string &label = "snapshot") {
    std::cout << "[" << label << "] " << std::endl;
    snap.print_snapshot();
}

inline void print_diff_snapshot(const RouterStatSnapshot &a, const RouterStatSnapshot &b, const std::string &label = "delta") {
    RouterStatSnapshot d = diff_snapshot(a, b);
    std::cout << "[" << label << "] " << std::endl;
    d.print_snapshot();
    std::cout << std::endl;
}
