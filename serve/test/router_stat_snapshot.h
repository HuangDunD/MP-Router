#pragma once

#include <cstdint>
#include <string>
#include <iostream>
#include "smart_router.h"

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
    uint64_t metis_cross_and_ownership_entirely = 0;
    uint64_t metis_cross_and_ownership_cross_equal = 0; 
    uint64_t metis_cross_and_ownership_cross_unequal = 0; 
    // for metis partial
    uint64_t metis_partial_and_ownership_missing = 0;
    uint64_t metis_partial_and_ownership_entirely = 0;
    uint64_t metis_partial_and_ownership_cross_equal = 0;
    uint64_t metis_partial_and_ownership_cross_unequal = 0; 

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

        
        if(SYSTEM_MODE != 8) {
            std::cout << "*********************************************" << std::endl;
            return;
        }
        // for SYSTEM_MODE 8
        // Metis and Ownership combined statistics
        std::cout << "Metis and Ownership combined decisions (for SYSTEM_MODE 8):" << std::endl;
        std::cout << "  Metis No Decision: " << metis_no_decision << std::endl;
        std::cout << "  Metis Missing & Ownership Missing: " << metis_missing_and_ownership_missing << std::endl;   
        std::cout << "  Metis Missing & Ownership Entirely: " << metis_missing_and_ownership_entirely << std::endl;
        std::cout << "  Metis Missing & Ownership Cross: " << metis_missing_and_ownership_cross << std::endl;
        std::cout << "  Metis Entirely & Ownership Missing: " << metis_entirely_and_ownership_missing << std::endl;
        std::cout << "  Metis Entirely & Ownership Cross Equal: " << metis_entirely_and_ownership_cross_equal << std::endl;
        std::cout << "  Metis Entirely & Ownership Cross Unequal: " << metis_entirely_and_ownership_cross_unequal << std::endl;
        std::cout << "  Metis Entirely & Ownership Entirely Equal: " << metis_entirely_and_ownership_entirely_equal << std::endl;
        std::cout << "  Metis Entirely & Ownership Entirely Unequal: " << metis_entirely_and_ownership_entirely_unequal << std::endl;
        std::cout << "  Metis Cross & Ownership Missing: " << metis_cross_and_ownership_missing << std::endl;
        std::cout << "  Metis Cross & Ownership Entirely: " << metis_cross_and_ownership_entirely << std::endl;
        std::cout << "  Metis Cross & Ownership Cross Equal: " << metis_cross_and_ownership_cross_equal << std::endl;
        std::cout << "  Metis Cross & Ownership Cross Unequal: " << metis_cross_and_ownership_cross_unequal << std::endl;
        std::cout << "  Metis Partial & Ownership Missing: " << metis_partial_and_ownership_missing << std::endl;
        std::cout << "  Metis Partial & Ownership Entirely: " << metis_partial_and_ownership_entirely << std::endl;
        std::cout << "  Metis Partial & Ownership Cross Equal: " << metis_partial_and_ownership_cross_equal << std::endl;
        std::cout << "  Metis Partial & Ownership Cross Unequal: " << metis_partial_and_ownership_cross_unequal << std::endl;
        std::cout << "*********************************************" << std::endl;
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
    snap.metis_cross_and_ownership_entirely = s.metis_cross_and_ownership_entirely.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_cross_equal = s.metis_cross_and_ownership_cross_equal.load(std::memory_order_relaxed);
    snap.metis_cross_and_ownership_cross_unequal = s.metis_cross_and_ownership_cross_unequal.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_missing = s.metis_partial_and_ownership_missing.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_entirely = s.metis_partial_and_ownership_entirely.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_cross_equal = s.metis_partial_and_ownership_cross_equal.load(std::memory_order_relaxed);
    snap.metis_partial_and_ownership_cross_unequal = s.metis_partial_and_ownership_cross_unequal.load(std::memory_order_relaxed);
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
    d.metis_cross_and_ownership_entirely = (b.metis_cross_and_ownership_entirely >= a.metis_cross_and_ownership_entirely) ? 
                                            (b.metis_cross_and_ownership_entirely - a.metis_cross_and_ownership_entirely) : 0;
    d.metis_cross_and_ownership_cross_equal = (b.metis_cross_and_ownership_cross_equal >= a.metis_cross_and_ownership_cross_equal) ? 
                                            (b.metis_cross_and_ownership_cross_equal - a.metis_cross_and_ownership_cross_equal) : 0;
    d.metis_cross_and_ownership_cross_unequal = (b.metis_cross_and_ownership_cross_unequal >= a.metis_cross_and_ownership_cross_unequal) ? 
                                            (b.metis_cross_and_ownership_cross_unequal - a.metis_cross_and_ownership_cross_unequal) : 0;
    d.metis_partial_and_ownership_missing = (b.metis_partial_and_ownership_missing >= a.metis_partial_and_ownership_missing) ? 
                                            (b.metis_partial_and_ownership_missing - a.metis_partial_and_ownership_missing) : 0;
    d.metis_partial_and_ownership_entirely = (b.metis_partial_and_ownership_entirely >= a.metis_partial_and_ownership_entirely  ) ? 
                                            (b.metis_partial_and_ownership_entirely - a.metis_partial_and_ownership_entirely) : 0;
    d.metis_partial_and_ownership_cross_equal = (b.metis_partial_and_ownership_cross_equal >= a.metis_partial_and_ownership_cross_equal) ? 
                                            (b.metis_partial_and_ownership_cross_equal - a.metis_partial_and_ownership_cross_equal) : 0;
    d.metis_partial_and_ownership_cross_unequal = (b.metis_partial_and_ownership_cross_unequal >= a.metis_partial_and_ownership_cross_unequal) ? 
                                            (b.metis_partial_and_ownership_cross_unequal - a.metis_partial_and_ownership_cross_unequal) : 0;
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
