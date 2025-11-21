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
    uint64_t hot_hash_bytes = 0;
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
    double total_time_ms = 0.0;
    double fetch_txn_from_pool_ms = 0.0;
    double schedule_total_ms = 0.0;
    double preprocess_txn_ms, wait_last_batch_finish_ms = 0.0;
    double merge_global_txid_to_txn_map_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double compute_conflict_ms = 0.0; // 这部分属于preprocess_txn_ms的一部分
    double ownership_retrieval_and_devide_unconflicted_txn_ms, process_conflicted_txn_ms = 0.0;
    double sum_worker_thread_exec_time_ms = 0.0;

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

        std::cout << "-----SmartRouter Time Statistics (ms)-----:" << std::endl;
        std::cout << "  Total Time: " << total_time_ms << " ms" << std::endl;
        std::cout << "  Fetch Txn From Pool Time: " <<fetch_txn_from_pool_ms << " ms" << std::endl;
        std::cout << "  Schedule Batch Total Time: " << schedule_total_ms << " ms" << std::endl;
        std::cout << "    Preprocess Txn Time: " << preprocess_txn_ms << " ms" << std::endl;
        std::cout << "      Merge Global Txid To Txn Map Time: " << merge_global_txid_to_txn_map_ms << " ms" << std::endl;
        std::cout << "      Compute Conflict Time: " << compute_conflict_ms << " ms" << std::endl;
        std::cout << "    Wait Last Batch Finish Time: " << wait_last_batch_finish_ms << " ms" << std::endl;
        std::cout << "    Ownership Retrieval And Devide Unconflicted Txn Time: " 
                  << ownership_retrieval_and_devide_unconflicted_txn_ms << " ms" << std::endl;
        std::cout << "    Process Conflicted Txn Time: " << process_conflicted_txn_ms << " ms" << std::endl;
        // std::cout << "  Sum Worker Thread Exec Time: " << sum_worker_thread_exec_time_ms << " ms" << std::endl;
        std::cout << "  Average Worker Thread Exec Time: " << sum_worker_thread_exec_time_ms / worker_threads << " ms" << std::endl;
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
    snap.hot_hash_bytes = s.hot_hash_bytes;
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
    router->sum_worker_thread_exec_time(); // update the sum
    if(SYSTEM_MODE >= 0 && SYSTEM_MODE <= 8) {
        // 对于这些模式, 是使用多线程router的，因此需要计算平均
        router->Record_time_ms();
    }
    SmartRouter::TimeBreakdown& tdb = router->get_time_breakdown();
    snap.total_time_ms = tdb.total_time_ms;
    snap.fetch_txn_from_pool_ms = tdb.fetch_txn_from_pool_ms;
    snap.schedule_total_ms = tdb.schedule_total_ms;
    snap.preprocess_txn_ms = tdb.preprocess_txn_ms;
    snap.merge_global_txid_to_txn_map_ms = tdb.merge_global_txid_to_txn_map_ms;
    snap.compute_conflict_ms = tdb.compute_conflict_ms;
    snap.wait_last_batch_finish_ms = tdb.wait_last_batch_finish_ms;
    snap.ownership_retrieval_and_devide_unconflicted_txn_ms = tdb.ownership_retrieval_and_devide_unconflicted_txn_ms;
    snap.process_conflicted_txn_ms = tdb.process_conflicted_txn_ms;
    snap.sum_worker_thread_exec_time_ms = tdb.sum_worker_thread_exec_time_ms;
    return snap;
}

// Compute difference: b - a (i.e., stats that occurred between snapshot a and b)
inline RouterStatSnapshot diff_snapshot(const RouterStatSnapshot &a, const RouterStatSnapshot &b) {
    RouterStatSnapshot d;
    // const, 这些属性不需要计算差值
    d.hot_hash_bytes = b.hot_hash_bytes;
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
    d.total_time_ms = b.total_time_ms - a.total_time_ms;
    d.fetch_txn_from_pool_ms = b.fetch_txn_from_pool_ms - a.fetch_txn_from_pool_ms;
    d.schedule_total_ms = b.schedule_total_ms - a.schedule_total_ms;
    d.preprocess_txn_ms = b.preprocess_txn_ms - a.preprocess_txn_ms;
    d.merge_global_txid_to_txn_map_ms = b.merge_global_txid_to_txn_map_ms - a.merge_global_txid_to_txn_map_ms;
    d.compute_conflict_ms = b.compute_conflict_ms - a.compute_conflict_ms;
    d.wait_last_batch_finish_ms = b.wait_last_batch_finish_ms - a.wait_last_batch_finish_ms;
    d.ownership_retrieval_and_devide_unconflicted_txn_ms = b.ownership_retrieval_and_devide_unconflicted_txn_ms - a.ownership_retrieval_and_devide_unconflicted_txn_ms;
    d.process_conflicted_txn_ms = b.process_conflicted_txn_ms - a.process_conflicted_txn_ms;
    d.sum_worker_thread_exec_time_ms = b.sum_worker_thread_exec_time_ms - a.sum_worker_thread_exec_time_ms;
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
