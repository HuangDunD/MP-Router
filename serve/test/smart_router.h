// Copyright 2025
// Author: huangdund
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
#include <atomic>

#include "common.h"
#include "btree_search.h"
#include "metis_partitioner.h"
#include "threadpool.h"
#include "log/Logger.h"
#include "ownership_table.h"
#include "txn_queue.h"
#include "config.h"

// SmartRouter: 一个针对 hot-key hash cache 设有严格内存预算的事务路由器。
// 它维护：
//  1) 一个 hot-key -> page_id 的 hash cache（LRU 管理），受内存预算限制。
//  2) 一个 B+tree 非叶子缓存，存储 key 范围作为提示，大小不限。
// 驱逐仅应用于 hot-key hash cache，优先移除低价值（LRU）条目。
//
// 线程安全性由实现（.cc）决定（如 RW-locks）。
// 所有大小均指内存中元数据大小（不包括数据页）。

struct DataItemKey {
    table_id_t table_id;
    itemkey_t key;

    bool operator==(const DataItemKey &other) const {
        return table_id == other.table_id && key == other.key;
    }
};

struct DataItemKeyHash {
    std::size_t operator()(const DataItemKey &k) const {
        std::size_t h1 = std::hash<table_id_t>()(k.table_id);
        std::size_t h2 = std::hash<itemkey_t>()(k.key);
        // 组合两个 hash，避免简单相加冲突率高
        return h1 ^ (h2 << 1);
    }
};

class SmartRouter {
public:
    struct Config {
        std::size_t partition_nums = ComputeNodeCount; // 分区数量，通常等于计算节点数量

        std::size_t hot_hash_cap_bytes = 640ULL * 1024ULL * 1024ULL; // 默认 64 MB, 作为 hot hash 的内存预算
        int thread_pool_size = 4; // 线程池大小
        std::string log_file = "smart_router_metis.log"; // 日志文件
    };

	enum class MetisOwnershipDecisionType {
		MetisNoDecision = 0,
		MetisMissingAndOwnershipMissing,
		MetisMissingAndOwnershipEntirely,
		MetisMissingAndOwnershipCross,
		MetisEntirelyAndOwnershipMissing,
		MetisEntirelyAndOwnershipCrossEqual,
		MetisEntirelyAndOwnershipCrossUnequal,
		MetisEntirelyAndOwnershipEntirelyEqual,
		MetisEntirelyAndOwnershipEntirelyUnequal,
		MetisCrossAndOwnershipMissing,
		MetisCrossAndOwnershipEntirelyEqual,
        MetisCrossAndOwnershipEntirelyUnequal,
		MetisCrossAndOwnershipCrossEqual,
		MetisCrossAndOwnershipCrossUnequal,
		MetisPartialAndOwnershipMissing,
		MetisPartialAndOwnershipEntirelyEqual,
        MetisPartialAndOwnershipEntirelyUnequal,
		MetisPartialAndOwnershipCrossEqual,
		MetisPartialAndOwnershipCrossUnequal
	};

    struct Stats {
        // 当前大小
        std::size_t hot_hash_bytes = 0;
        std::size_t btree_bytes = 0; // 追踪但不受大小限制
        // 查找计数
        std::uint64_t hot_hit = 0;
        std::uint64_t hot_miss = 0;
        std::uint64_t btree_hit = 0; // 范围提示命中
        std::uint64_t btree_miss = 0; // 无提示，需访问
        // 驱逐计数
        std::uint64_t evict_hot_entries = 0;
        // 页面更新计数
        std::atomic<int> change_page_cnt = 0;
        std::atomic<int> page_update_cnt = 0;
        // Ownership 事务计数
        std::atomic<int> ownership_random_txns = 0;
        std::atomic<int> ownership_entirely_txns = 0;
        std::atomic<int> ownership_cross_txns = 0;

        // for SYSTEM_MODE 8
        // for metis no decision
        std::atomic<int> metis_no_decision = 0;
        // for metis missing
        std::atomic<int> metis_missing_and_ownership_missing = 0;
        std::atomic<int> metis_missing_and_ownership_entirely = 0;
        std::atomic<int> metis_missing_and_ownership_cross = 0;
        // for metis entirely
        std::atomic<int> metis_entirely_and_ownership_missing = 0;
        std::atomic<int> metis_entirely_and_ownership_cross_equal = 0;
        std::atomic<int> metis_entirely_and_ownership_cross_unequal = 0;
        std::atomic<int> metis_entirely_and_ownership_entirely_equal = 0;
        std::atomic<int> metis_entirely_and_ownership_entirely_unequal = 0;
        // for metis cross
        std::atomic<int> metis_cross_and_ownership_missing = 0;
        std::atomic<int> metis_cross_and_ownership_entirely_equal = 0;
         std::atomic<int> metis_cross_and_ownership_entirely_unequal = 0;
        std::atomic<int> metis_cross_and_ownership_cross_equal = 0; 
        std::atomic<int> metis_cross_and_ownership_cross_unequal = 0;
        // for metis partial
        std::atomic<int> metis_partial_and_ownership_missing = 0;
        std::atomic<int> metis_partial_and_ownership_entirely_equal = 0;
        std::atomic<int> metis_partial_and_ownership_entirely_unequal = 0;
        std::atomic<int> metis_partial_and_ownership_cross_equal = 0;
        std::atomic<int> metis_partial_and_ownership_cross_unequal = 0; 
    };

    void reset_Metis_Router_txn_statistics() {
        reset_txn_statistics();
        metis_->reset_stats();
    }

    // hot hash 层的热键条目
    class HotEntry {
    public:
        page_id_t page = 0; // 初始化page字段
        std::uint64_t freq = 0;
        node_id_t key_access_last_node = -1; // 最近访问的节点ID，-1表示未设置
        uint64_t last_access_time = 0; // 最近访问时间（毫秒级）
        // LRU 列表迭代器
        std::list<DataItemKey>::iterator lru_it;

        HotEntry(){};
        HotEntry(page_id_t p, std::uint64_t f, std::list<DataItemKey>::iterator it)
        : page(p), freq(f), lru_it(it) {}
    };

public:
    explicit SmartRouter(const Config &cfg, TxnPool* txn_pool, std::vector<TxnQueue*> txn_queue, int worker_threads,
            BtreeIndexService *btree_service, NewMetis* metis = nullptr, Logger* logger_ptr = nullptr)
        : cfg_(cfg),
          txn_pool_(txn_pool),
          txn_queues_(txn_queue),
          db_con_worker_threads(worker_threads),
          logger(logger_ptr),
          btree_service_(btree_service),
          metis_(metis),
          threadpool(cfg.thread_pool_size, *logger), 
          routed_txn_cnt_per_node(MaxComputeNodeCount), 
          batch_finished_flags(MaxComputeNodeCount, 0)
    {
        metis_->set_thread_pool(&threadpool);
        metis_->init_node_nums(cfg.partition_nums);
        ownership_table_ = new OwnershipTable(logger);

        // for logging access key
        #if LOG_ACCESS_KEY
            // open the access key log file
            access_key_log_file.open(access_log_file_name, std::ios::out | std::ios::trunc);
            if (!access_key_log_file.is_open()) {
                std::cerr << "Failed to open access key log file." << std::endl;
                return -1;
            }
        #else 
            // delete existing log file if any
            std::remove(access_log_file_name.c_str());
        #endif

        // start the router thread
        if(SYSTEM_MODE <= 8){
            for(int i=0; i<worker_threads; i++) {
                std::thread router_thread([this, i]() {
                    std::string thread_name = "SmartRouter_" + std::to_string(i);
                    pthread_setname_np(pthread_self(), thread_name.c_str());
                    this->run_router_worker();
                });
                pthread_setname_np(router_thread.native_handle(), ("SmartRouter_" + std::to_string(i)).c_str());
                router_thread.detach();
            }
        }
        else if (SYSTEM_MODE >= 9) {
            // SYSTEM_MODE 9 的 SmartRouter 线程启动逻辑（如果有不同的话）
            std::thread router_thread([this]() {
                std::string thread_name = "SmartRouter";
                pthread_setname_np(pthread_self(), thread_name.c_str());
                this->run_router_batch_worker();
            });
            router_thread.detach();
        }
        else {
            std::cerr << "Unsupported SYSTEM_MODE for SmartRouter: " << SYSTEM_MODE << std::endl;
            assert(false);
        }
    }

    ~SmartRouter() {
        #if LOG_ACCESS_KEY
            // 转到 vector 便于排序
            std::vector<std::pair<int, long long>> vec(key_freq.begin(), key_freq.end());

            // 按 value 从大到小排序
            std::sort(vec.begin(), vec.end(),
                    [](auto &a, auto &b) { return a.second > b.second; });

            // 输出前 50 个
            int topN = 50;
            if (vec.size() < topN) topN = vec.size();
            for (int i = 0; i < topN; i++) {
                std::cout << "Key: " << vec[i].first
                        << "  Count: " << vec[i].second << "\n";
            }
            
            // 计算总访问次数
            long long total = 0;
            for (auto &p : vec) total += p.second;

            auto calc_ratio = [&](double percent) {
                size_t topN = std::max<size_t>(1, size_t(vec.size() * percent));
                long long sum = 0;
                for (size_t i = 0; i < topN && i < vec.size(); i++) sum += vec[i].second;
                return double(sum) / total * 100.0;
            };

            double r1  = calc_ratio(0.01);
            double r10 = calc_ratio(0.10);
            double r50 = calc_ratio(0.50);

            std::cout << "前 1% key 占总访问比例:  " << r1  << "%\n";
            std::cout << "前10% key 占总访问比例:  " << r10 << "%\n";
            std::cout << "前50% key 占总访问比例:  " << r50 << "%\n";
        #endif
    }

    // 可能Update SQL执行之后数据页所在的位置, 根据returning ctid 进行更新key-page映射
    // 如果key不存在, 则不进行任何操作
    inline void update_key_page(TxnQueueEntry* txn, std::vector<table_id_t>& table_ids, std::vector<itemkey_t>& keys, 
            std::vector<page_id_t> ctid_ret_pages, node_id_t routed_node_id) { // txn_type for SYSTEM_MODE 8
        // 这个地方可能ctid_ret_pages的数量不等于keys, 因为这个事务可能触发了回滚, 此时需要将table_ids, keys截断一下
        if(table_ids.size() != ctid_ret_pages.size() || keys.size() != ctid_ret_pages.size()) {
            std::cerr << "Warning: Mismatched sizes in update_key_page. table_ids: " << table_ids.size() 
                      << ", keys: " << keys.size() << ", ctid_ret_pages: " << ctid_ret_pages.size() << std::endl;
            size_t min_size = std::min({table_ids.size(), keys.size(), ctid_ret_pages.size()});
            table_ids.resize(min_size);
            keys.resize(min_size);
            ctid_ret_pages.resize(min_size);
        }
        // assert(table_ids.size() == keys.size() && keys.size() == ctid_ret_pages.size());
        for(size_t i=0; i<table_ids.size(); i++) {
            std::lock_guard<std::mutex> lock(hot_mutex_);
            auto it = hot_key_map.find({table_ids[i], keys[i]});
            if (it != hot_key_map.end()) {
                auto original_page = it->second.page;
                if(it->second.page != ctid_ret_pages[i]){ // 只有在page变化时才更新
                    // 这个地方应该是访问了原来的页面和新的页面, 都变成了这个节点的所有                    
                    ownership_table_->set_owner(txn, table_ids[i], keys[i], ctid_ret_pages[i], routed_node_id);
                    ownership_table_->set_owner(txn, table_ids[i], keys[i], original_page, routed_node_id); 
                    it->second.page = ctid_ret_pages[i];
                    // 毫秒级时间戳
                    it->second.last_access_time = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()
                        ).count()
                    );
                    stats_.change_page_cnt++;
                #if LOG_PAGE_UPDATE
                    logger->info("Key (table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                                    ") page changed from " + std::to_string(original_page) + " to " + std::to_string(ctid_ret_pages[i]) + 
                                    " at node " + std::to_string(routed_node_id));
                #endif 
                }
                else{
                    // 仅访问了原来的页面, 仍然是这个节点的所有权
                    ownership_table_->set_owner(txn, table_ids[i], keys[i], original_page, routed_node_id); 
                    stats_.page_update_cnt++;
                }
                // 更新 last_node
                it->second.key_access_last_node = routed_node_id;
            }
        }
    };

    // init key-page mapping when load data
    inline void initial_key_page(table_id_t table_id, itemkey_t key, page_id_t page) {
        std::lock_guard<std::mutex> lock(hot_mutex_);
        auto it = hot_key_map.find({table_id, key});
        if (it == hot_key_map.end()) {
            // 插入新条目
            hot_lru_.push_front({table_id, key});
            HotEntry entry;
            entry.page = page;
            entry.freq = 1;
            entry.lru_it = hot_lru_.begin();
            hot_key_map.emplace(DataItemKey{table_id, key}, std::move(entry));
            stats_.hot_hash_bytes += hot_entry_size_model_();
            // std::cout << "Initialized hot key: (table_id=" << table_id << ", key=" << key << ") -> page " << page << std::endl;
            // 检查是否超预算, 超预算则驱逐
            while (stats_.hot_hash_bytes > cfg_.hot_hash_cap_bytes && !hot_lru_.empty()) {
                DataItemKey evict_key = hot_lru_.back();
                auto evict_it = hot_key_map.find(evict_key);
                if (evict_it != hot_key_map.end()) {
                    stats_.hot_hash_bytes -= hot_entry_size_model_();
                    stats_.evict_hot_entries++;
                    hot_key_map.erase(evict_it);
                }
                hot_lru_.pop_back();
                std::cout << "Evicted hot key: (table_id=" << evict_key.table_id << ", key=" << evict_key.key << ")" <<
                        std::endl;
            }
        }
    }
  
    // ******************* METIS ******************
    // 执行分区操作，返回分区结果
    struct SmartRouterResult {
        bool success = false;
        int smart_router_id = -1;
        std::string error_message;
        size_t keys_processed = 0;
        // for SYSTEM_MODE 3
        int sys_3_decision_type = -1; // 0: metis no decision, 1: metis missing, 2: metis entirely, 3: metis partial, 4: metis cross
        // for SYSTEM_MODE 8
        int sys_8_decision_type = -1; // 0: metis no decision, 1: metis missing and ownership missing, 2: metis missing and ownership entirely, 3: metis missing and ownership cross
                                      // 4: metis entirely and ownership missing, 5: metis entirely and ownership cross equal, 6: metis entirely and ownership cross unequal
                                      // 7: metis entirely and ownership entirely equal, 8: metis entirely and ownership entirely unequal
                                      // 9: metis cross and ownership missing, 10: metis cross and ownership entirely, 11: metis cross and ownership cross equal
                                      // 12: metis cross and ownership cross unequal, 13: metis partial and ownership missing, 14: metis partial and ownership entirely
                                      // 15: metis partial and ownership cross equal, 16: metis partial and ownership cross unequal
    };

    void getKeyOriginalPages(std::vector<table_id_t>& table_ids, std::vector<itemkey_t>& keys, std::vector<page_id_t>& original_pages) {
        original_pages.clear();
        std::lock_guard<std::mutex> lock(hot_mutex_);
        for(size_t i=0; i<keys.size(); i++) {
            auto it = hot_key_map.find({table_ids[i], keys[i]});
            if (it != hot_key_map.end()) {
                original_pages.push_back(it->second.page);
            }
            else {
                assert(false); // 这里不应该找不到
            }
        }
    }

    // 根据table_ids和keys进行路由，返回目标节点ID
    SmartRouterResult get_route_primary(tx_id_t tx_id, std::vector<table_id_t> &table_ids, std::vector<itemkey_t> &keys, 
            std::vector<pqxx::connection *> &thread_conns) {
        SmartRouterResult result;
        if (table_ids.size() != keys.size() || table_ids.empty()) {
            result.error_message = "Mismatched or empty table_ids and keys";
            return result;
        }

        std::unordered_map<uint64_t, node_id_t> page_to_node_map; // 高32位存table_id，低32位存page_id, for SYSTEM_MODE 3
        std::unordered_map<node_id_t, int> node_count_basedon_key_access_last; // 基于key_access_last的计数, for SYSTEM_MODE 5
        std::unordered_map<uint64_t, node_id_t> table_key_id;  // 高32位存table_id，低32位存key, for SYSTEM_MODE 6
        std::unordered_map<node_id_t, int> node_count_basedon_page_access_last; // 基于page_access_last的计数, for SYSTEM_MODE 7
        std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 找到ownership对应的节点的映射关系, for SYSTEM_MODE 8
        std::string debug_info;
        page_to_node_map.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            if(SYSTEM_MODE == 3) {
                auto entry = lookup(table_ids[i], keys[i], thread_conns);
                // 计算page id
                if (entry.page == kInvalidPageId) {
                    result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                        ", key=" + std::to_string(keys[i]) + ")";
                    assert(false); // 这里不应该失败
                }
                // 方法1: 直接使用page_id
                uint64_t table_page = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
                page_to_node_map[table_page] = -1; // 初始化
                // 方法2: 每GroupPageAffinitySize个页面作为一个region, 这样可以减少图分区的大小
                // table_page_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | (entry.page / GroupPageAffinitySize));
                // 方法3: 直接对page_id做hash, 这样可以减少图分区的大小, 这会使得负载变得更加均衡
                // std::hash<page_id_t> h;
                // int region_id = h(entry.page) % (cfg_.partition_nums * 1000); // 10倍分区数的region
                // table_page_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | region_id);
            }
            else if(SYSTEM_MODE == 5) {
                auto entry = lookup(table_ids[i], keys[i], thread_conns);
                // 如果是模式5，则统计key_access_last_node出现的次数, 按照上次key访问节点进行路由
                if (entry.key_access_last_node != -1) {
                    node_count_basedon_key_access_last[entry.key_access_last_node]++;
                }
            }
            else if(SYSTEM_MODE == 6) {
                // 计算key id
                uint64_t table_key = (static_cast<uint64_t>(table_ids[i]) << 32) | keys[i];
                table_key_id[table_key] = -1; // 初始化
            }
            else if(SYSTEM_MODE == 7) {
                auto entry = lookup(table_ids[i], keys[i], thread_conns);
                // 计算page id
                if (entry.page == kInvalidPageId) {
                    result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                        ", key=" + std::to_string(keys[i]) + ")";
                    assert(false); // 这里不应该失败
                }
                auto last_node = ownership_table_->get_owner(table_ids[i], entry.page);
                if (last_node != -1) {
                    node_count_basedon_page_access_last[last_node]++;
                }
                debug_info += "(table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                              ", page=" + std::to_string(entry.page) + ", last_node=" + std::to_string(last_node) + "); ";
            }
            else if(SYSTEM_MODE == 8) {
                // 计算page id
                auto entry = lookup(table_ids[i], keys[i], thread_conns);
                if (entry.page == kInvalidPageId) {
                    result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                        ", key=" + std::to_string(keys[i]) + ")";
                    assert(false); // 这里不应该失败
                }
                uint64_t table_page_id_val = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
                page_to_node_map[table_page_id_val] = -1; // 初始化
                auto last_node = ownership_table_->get_owner(table_ids[i], entry.page);
                if (last_node != -1) {
                    page_ownership_to_node_map[table_page_id_val] = last_node;
                    node_count_basedon_page_access_last[last_node]++;
                }
                debug_info += "(table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                              ", page=" + std::to_string(entry.page) + ", last_node=" + std::to_string(last_node) + "); ";
            }
            else { 
                assert(false); // unknown mode
            }
        }

        try {
            if (SYSTEM_MODE == 3) {
                // 基于page Metis的结果进行分区
                // 去重优化
                result.keys_processed = page_to_node_map.size();
                int ret_code = metis_->build_internal_graph(page_to_node_map, &result.smart_router_id);
                if(ret_code == 0) {
                    result.sys_3_decision_type = 0; // metis no decision
                } else if (ret_code == -1) {
                    result.sys_3_decision_type = 1; // metis missing
                }
                else if (ret_code == 1) {
                    result.sys_3_decision_type = 2; // metis entirely
                }
                else if (ret_code == 2) {
                    result.sys_3_decision_type = 3; // metis partial
                }
                else if (ret_code == 3) {
                    result.sys_3_decision_type = 4; // metis cross
                }
                // result.smart_router_id = 0;
            }
            else if (SYSTEM_MODE == 5) {
                // !如果是模式5，则结合key_access_last_node的计数结果进行调整
                int max_count = 0;
                node_id_t candidate_node = -1;
                for (const auto& [node, count] : node_count_basedon_key_access_last) {
                    if (count > max_count) {
                        max_count = count;
                        candidate_node = node;
                    }
                }
                if (candidate_node != -1) {
                    if (candidate_node != result.smart_router_id) {
                        // !直接选择出现次数最多的节点
                        result.smart_router_id = candidate_node;
                    }
                }
                else {
                    // 在运行前面一段时间, 因为初始化时没有last_node, 导致这里没有任何候选节点
                    // 那就随机选择一个
                    result.smart_router_id = rand() % cfg_.partition_nums;
                }
            }
            else if (SYSTEM_MODE == 6) {
                // SYSTEM_MODE 6: 按照key做亲和性划分
                // 去重优化
                result.keys_processed = table_key_id.size();
                metis_->build_internal_graph(table_key_id, &result.smart_router_id);
            }
            else if (SYSTEM_MODE == 7) {
                // SYSTEM_MODE 7: 按照page做亲和性划分, 结合page_access_last_node
                if(node_count_basedon_page_access_last.empty()) {
                    this->stats_.ownership_random_txns++;
                    result.smart_router_id = rand() % cfg_.partition_nums;
                    logger->info("[SmartRouter Random] found no candidate node based on page access last node, randomly selected node " + 
                                std::to_string(result.smart_router_id));
                }
                else if (node_count_basedon_page_access_last.size() == 1) {
                    this->stats_.ownership_entirely_txns++;
                    // 只有一个候选节点, 直接选择
                    result.smart_router_id = node_count_basedon_page_access_last.begin()->first;
                    logger->info("[SmartRouter Entirely] " + debug_info + " based on page access last node directly to node " + 
                                std::to_string(result.smart_router_id));
                }
                else if (node_count_basedon_page_access_last.size() > 1) {
                    this->stats_.ownership_cross_txns++;
                    int max_count = 0;
                    node_id_t candidate_node = -1;
                    std::vector<node_id_t> candidates;
                    for (const auto& [node, count] : node_count_basedon_page_access_last) {
                        if (count > max_count) {
                            max_count = count;
                            candidates.clear();
                            candidates.push_back(node);
                        } else if (count == max_count) {
                            candidates.push_back(node);
                        }
                    }
                    assert(!candidates.empty());
                    // 随机选择出现次数最多的节点
                    result.smart_router_id = candidates[rand() % candidates.size()];
                    logger->info("[SmartRouter Cross] " + debug_info + " based on page access last node with count (" 
                                + std::to_string(max_count) + ") to node " + std::to_string(result.smart_router_id));
                }
            }
            else if(SYSTEM_MODE == 8) {
                // 基于page Metis的结果进行分区, 同时返回page到node的映射
                result.keys_processed = page_to_node_map.size();
                node_id_t metis_decision_node;
                int ret_code = metis_->build_internal_graph(page_to_node_map, &metis_decision_node);
                // page_ownership_to_node_map 是ownership_table_中记录的page到node的映射
                // page_to_node_map 是metis分区后得到的page到node的映射
                // 这里进行对比, 看看两者是否一致, 综合考虑page_to_node_map和ownership_table_的信息, 进行调整
                if  (ret_code == 0){ 
                    // no decision
                    assert(!page_to_node_map.empty());
                    result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisNoDecision);
                }
                else if (ret_code == -1) {
                    // missing, 没有任何page被映射到节点
                    // 这个时候再检查一下ownership_table_的信息
                    assert(!page_to_node_map.empty()); 
                    if(node_count_basedon_page_access_last.empty()) {
                        // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                        result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipMissing);
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        result.smart_router_id = page_ownership_to_node_map.begin()->second;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipEntirely);
                    }
                    else if(node_count_basedon_page_access_last.size() > 1) {
                        // ownership_table_中有多个page的映射信息
                        // 基于page_ownership_to_node_map进行计数, 选择出现次数最多的节点
                        int max_count = 0;
                        node_id_t candidate_node = -1;
                        std::vector<node_id_t> candidates;
                        for (const auto& [node, count] : node_count_basedon_page_access_last) {
                            if (count > max_count) {
                                max_count = count;
                                candidates.clear();
                                candidates.push_back(node);
                            } else if (count == max_count) {
                                candidates.push_back(node);
                            }
                        }
                        assert(!candidates.empty());
                        // 随机选择出现次数最多的节点
                        node_id_t min_txn_node = -1;
                        size_t min_txn_count = SIZE_MAX;
                        for (int i=0; i<candidates.size(); i++) {
                            auto node = candidates[i];
                            if (routed_txn_cnt_per_node[node].load() < min_txn_count) {
                                min_txn_count = routed_txn_cnt_per_node[node].load();
                                min_txn_node = node;
                            }
                        }
                        assert(min_txn_node != -1);
                        result.smart_router_id = min_txn_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipCross);
                    }
                    else assert(false); // 不可能出现的情况
                }
                else if (ret_code == 1) {
                    // entire affinity, 所有page都映射到同一个节点
                    assert(!page_to_node_map.empty());
                    assert(metis_decision_node != -1);
                    if(node_count_basedon_page_access_last.empty()) {
                        // ownership_table_中没有任何page的映射信息, 即涉及到的页面
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipMissing);
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        auto ownership_node = page_ownership_to_node_map.begin()->second;
                        if(ownership_node == metis_decision_node) {
                            result.smart_router_id = metis_decision_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyEqual);
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyUnequal);
                        }
                    }
                    else if(node_count_basedon_page_access_last.size() > 1) {
                        // ownership_table_中有多个page的映射信息
                        int max_count = 0;
                        node_id_t candidate_node = -1;
                        std::vector<node_id_t> candidates;
                        for (const auto& [node, count] : node_count_basedon_page_access_last) {
                            if (count > max_count) {
                                max_count = count;
                                candidates.clear();
                                candidates.push_back(node);
                            } else if (count == max_count) {
                                candidates.push_back(node);
                            }
                        }
                        assert(!candidates.empty());
                        int i;
                        for(i=0; i<candidates.size(); i++) {
                            if (candidates[i] == metis_decision_node) {
                                candidate_node = candidates[i];
                                break;
                            }
                        }
                        if(i >= candidates.size()) {
                            // candidates中没有metis_decision_node, 那就随机选择一个
                            candidate_node = candidates[rand() % candidates.size()];
                        }
                        if(candidate_node == metis_decision_node) {
                            result.smart_router_id = metis_decision_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossEqual);
                        }
                        else {
                            // !pay attention: 这里对于candidates 中有可能包含metis_decision_node的情况, 这种情况需要选择metis_decision_node
                            result.smart_router_id = candidate_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossUnequal);
                        }
                    }
                    else assert(false); // 不可能出现的情况
                }
                else if (ret_code == 2) {
                    // partial affinity, 多个page映射到多个节点
                    assert(!page_to_node_map.empty());
                    // 这个时候再检查一下ownership_table_的信息
                    if(node_count_basedon_page_access_last.empty()) {
                        // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipMissing);
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        auto ownership_node = page_ownership_to_node_map.begin()->second;
                        if(ownership_node == metis_decision_node) {
                            // 两者一致
                            result.smart_router_id = metis_decision_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyEqual);
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyUnequal);
                        }
                    }
                    else if(node_count_basedon_page_access_last.size() > 1) {
                        // ownership_table_中有多个page的映射信息
                        int max_count = 0;
                        node_id_t candidate_node = -1;
                        std::vector<node_id_t> candidates;
                        for (const auto& [node, count] : node_count_basedon_page_access_last) {
                            if (count > max_count) {
                                max_count = count;
                                candidates.clear();
                                candidates.push_back(node);
                            } else if (count == max_count) {
                                candidates.push_back(node);
                            }
                        }
                        assert(!candidates.empty());
                        // 找到routed_txn_cnt_per_node中最小的节点
                        node_id_t min_txn_node = -1;
                        size_t min_txn_count = SIZE_MAX;
                        for (int i=0; i<candidates.size(); i++) {
                            auto node = candidates[i];
                            if (routed_txn_cnt_per_node[node].load() < min_txn_count) {
                                min_txn_count = routed_txn_cnt_per_node[node].load();
                                min_txn_node = node;
                            }
                        }
                        assert(min_txn_node != -1);
                        node_id_t ownership_node = min_txn_node;
                        if(ownership_node == metis_decision_node) {
                            result.smart_router_id = metis_decision_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossEqual);
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossUnequal);
                        }
                    }
                    else assert(false); // 不可能出现的情况
                }
                else if (ret_code == 3) {
                    // cross affinity, 多个page映射到多个节点, 并且有些page没有映射到节点
                    assert(!page_to_node_map.empty());
                    // 这个时候再检查一下ownership_table_的信息
                    if(node_count_basedon_page_access_last.empty()) {
                        // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipMissing);
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        auto ownership_node = page_ownership_to_node_map.begin()->second;
                        if(ownership_node == metis_decision_node) {
                            // 两者一致
                            result.smart_router_id = metis_decision_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyEqual);
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node; 
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyUnequal);
                        }
                    }
                    else if(node_count_basedon_page_access_last.size() > 1) {
                        // ownership_table_中有多个page的映射信息
                        int max_count = 0;
                        node_id_t candidate_node = -1;
                        std::vector<node_id_t> candidates;
                        for (const auto& [node, count] : node_count_basedon_page_access_last) {
                            if (count > max_count) {
                                max_count = count;
                                candidates.clear();
                                candidates.push_back(node);
                            } else if (count == max_count) {
                                candidates.push_back(node);
                            }
                        }
                        assert(!candidates.empty());
                        // 找到routed_txn_cnt_per_node中最小的节点
                        node_id_t min_txn_node = -1;
                        size_t min_txn_count = SIZE_MAX;
                        for (int i=0; i<candidates.size(); i++) {
                            auto node = candidates[i];
                            if (routed_txn_cnt_per_node[node].load() < min_txn_count) {
                                min_txn_count = routed_txn_cnt_per_node[node].load();
                                min_txn_node = node;
                            }
                        }
                        assert(min_txn_node != -1);
                        node_id_t ownership_node = min_txn_node;
                        if(ownership_node == metis_decision_node) {
                            result.smart_router_id = metis_decision_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossEqual);
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossUnequal);
                        }
                    }
                    else assert(false); // 不可能出现的情况
                }
                else assert(false);
            }
            else {
                assert(false);
            }

            if(result.smart_router_id == -1) {
                // 如果没有决定, 则随机选择一个节点
                result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
            }
            result.success = true;
        }
        catch (const std::exception &e) {
            result.error_message = std::string("Exception during partitioning: ") + e.what();
        }
        return result;
    }
    
    // ! core code, propose the transaction scheduling
    // 批量对事务进行路由
    std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> get_route_primary_batch_2phase(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch,
            std::vector<pqxx::connection *> &thread_conns) {
        
        assert(SYSTEM_MODE == 9); // 仅支持模式9
        std::vector<SmartRouterResult> results;
        results.reserve(txn_batch->size());
        // 这几个变量是对整个batch, 记录一些基本的信息
        std::unordered_map<tx_id_t, std::vector<uint64_t>> txn_to_pages_map; // 记录每个事务涉及的页面列表
        std::unordered_map<uint64_t, tx_id_t> page_to_txn_map; // 记录每个页面对应的事务ID
        std::unordered_map<uint64_t, node_id_t> page_to_node_map; // 记录每个页面对应的节点ID（Metis分区结果）
        std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 记录每个页面对应的节点ID（Ownership表结果）

        std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> ready_txn_queues = 
            std::make_unique<std::vector<std::queue<TxnQueueEntry*>>>(ComputeNodeCount);
        std::vector<std::queue<TxnQueueEntry*>> tmp_routed_txn_queues(ComputeNodeCount);


        std::vector<int> partition_txn_count(ComputeNodeCount, 0);
        std::vector<int> global_txn_count(ComputeNodeCount, 0);

        for (auto& txn : *txn_batch) { 
            auto txn_type = txn->txn_type;
            auto tx_id = txn->tx_id;
            itemkey_t account1 = txn->accounts[0];
            itemkey_t account2 = txn->accounts[1];
            std::vector<itemkey_t> accounts_keys;
            std::vector<table_id_t> table_ids = smallbank_->get_table_ids_by_txn_type(txn_type); 
            assert(!table_ids.empty());
            smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
            assert(table_ids.size() == accounts_keys.size());

            // 获取涉及的页面列表
            std::unordered_map<uint64_t, node_id_t> table_page_ids; // 高32位存table_id，低32位存page_id
            for (size_t i = 0; i < accounts_keys.size(); ++i) {
                auto entry = lookup(table_ids[i], accounts_keys[i], thread_conns);
                // 计算page id
                if (entry.page == kInvalidPageId) {
                    assert(false); // 这里不应该失败
                }
                uint64_t table_page_id = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
                table_page_ids[table_page_id] = -1; // 初始化
                page_to_txn_map[table_page_id] = tx_id;
                txn_to_pages_map[tx_id].push_back(table_page_id);
            }
            // 让Metis构建图
            node_id_t metis_decision_node;
            int metis_ret = metis_->build_internal_graph(table_page_ids, &metis_decision_node);

            if (SYSTEM_MODE == 9) {
                // SYSTEM_MODE 9的思想是类似于Chimera，将节点内的事务先都做了，然后再做跨节点的事务
                if (metis_ret == 1){
                    // 事务访问的页面在metis图分区中全部映射到同一个节点
                    txn->txn_decision_type = 2; // metis entirely
                    (*ready_txn_queues)[metis_decision_node].push(txn);
                    partition_txn_count[metis_decision_node]++;
                }
                else {
                    if(metis_decision_node == -1) {
                        // metis没有给出决策, 随机选择一个节点
                        metis_decision_node = rand() % cfg_.partition_nums;
                    }
                    if(metis_ret == 0) txn->txn_decision_type = 0; // metis no decision
                    else if(metis_ret == -1) txn->txn_decision_type = 1; // metis missing
                    else if(metis_ret == 2) txn->txn_decision_type = 3; // metis partial
                    else if(metis_ret == 3) txn->txn_decision_type = 4; // metis cross
                    // 否则给他push到metis_decision_node节点上，该信息是根据metis的分区结果选择出来的最匹配的那一个
                    tmp_routed_txn_queues[metis_decision_node].push(txn);
                    global_txn_count[metis_decision_node]++;
                }
            }
        }
        
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            // 合并tmp_routed_txn_queues到txn_queues_
            while(!tmp_routed_txn_queues[node_id].empty()) {
                (*ready_txn_queues)[node_id].push(tmp_routed_txn_queues[node_id].front());
                tmp_routed_txn_queues[node_id].pop();
            }
        }
        return ready_txn_queues;
    }

    struct SchedulingCandidateTxn {
        TxnQueueEntry* txn;
        std::vector<uint64_t> involved_pages;
        std::unordered_map<node_id_t, double> node_benefit_map;
        node_id_t will_route_node; // 最终决定路由到的节点
    };

    std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> get_route_primary_batch_schedule(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch,
            std::vector<pqxx::connection *> &thread_conns);

    std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> get_route_primary_batch_schedule_v2(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch,
        std::vector<pqxx::connection *> &thread_conns);

    // 这个是路由层的主循环, 他不断从txn_pool中取出事务进行路由
    // 进行的路由决策会放入txn_queue中，供执行层消费
    void run_router_worker() {
        assert(SYSTEM_MODE >=0 && SYSTEM_MODE <=8); 
        // for routing needed db connections, each routing thread has its own connections
        std::vector<pqxx::connection*> thread_conns_vec;
        for(int i=0; i<ComputeNodeCount; i++) {
            pqxx::connection* conn = new pqxx::connection(DBConnection[i]);
            thread_conns_vec.push_back(conn);
        }
        while (true) {
            TxnQueueEntry* txn_entry = txn_pool_->fetch_txn_from_poolfront();
            if (txn_entry == nullptr) {
                // 说明事务池已经运行完成
                for(auto txn_queue : txn_queues_) {
                    txn_queue->set_finished();
                }
                break;
            }
            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            itemkey_t account1 = txn_entry->accounts[0];
            itemkey_t account2 = txn_entry->accounts[1];

            // Init the routed node id
            int routed_node_id = 0; // Default node ID
            
            // ! decide the routed_node_id based on SYSTEM_MODE
            // keys 只和 account1/account2有关，不能静态化，但可以用局部变量，每次只构造一份
            std::vector<itemkey_t> keys; 
            smallbank_->get_keys_by_txn_type(txn_type, account1, account2, keys);
            // table_ids 静态化后只需引用
            const std::vector<table_id_t>& table_ids = TABLE_IDS_ARR[txn_type < 6 ? txn_type : 0];
            #if LOG_ACCESS_KEY
                // 写日志记录一下
                std::unique_lock<std::mutex> lock(log_mutex);
                if(access_key_log_file.is_open()) {
                    int i=0;
                    for(i=0; i<table_ids.size()-1; i++) access_key_log_file << "{" << table_ids[i] << ":" << keys[i] << "},";
                    access_key_log_file << "{" << table_ids[i] << ":" << keys[i] << "}" << std::endl;
                    access_key_log_file.flush();
                }
                for (auto key: keys) key_freq[key]++;
                lock.unlock();
            #endif

            if(SYSTEM_MODE == 0) {
                routed_node_id = rand() % 2; // Randomly select node ID for system mode 0
            }
            else if(SYSTEM_MODE == 1){
                if(txn_type == 0 || txn_type == 1) {
                    int node1 = account1 / (smallbank_->get_account_count() / ComputeNodeCount); // Range partitioning
                    int node2 = account2 / (smallbank_->get_account_count() / ComputeNodeCount); // Range partitioning
                    if(node1 == node2) {
                        routed_node_id = node1;
                    }
                    else {
                        // randomly pick one
                        routed_node_id = (rand() % 2 == 0) ? node1 : node2;
                    }
                }
                else {
                    routed_node_id = account1 / (smallbank_->get_account_count() / ComputeNodeCount); // Range partitioning
                }
            }
            else if(SYSTEM_MODE == 2) {
                // get page_id from checking_page_map
                routed_node_id = rand() % ComputeNodeCount; // Fallback to random node if not found
            }
            else if(SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 6 || SYSTEM_MODE == 7 || SYSTEM_MODE == 8) {
                SmartRouter::SmartRouterResult result = this->get_route_primary(tx_id, const_cast<std::vector<table_id_t>&>(table_ids), keys, thread_conns_vec);
                if(result.success) {
                    routed_node_id = result.smart_router_id;
                    if(SYSTEM_MODE == 3) txn_entry->txn_decision_type = result.sys_3_decision_type; 
                    else if(SYSTEM_MODE == 8) txn_entry->txn_decision_type = result.sys_8_decision_type; 
                }
                else {
                    // fallback to random
                    routed_node_id = rand() % ComputeNodeCount;
                    std::cerr << "Warning: SmartRouter get_route_primary failed: " << result.error_message << std::endl;
                }
            }
            else if(SYSTEM_MODE == 4) {
                routed_node_id = 0; // All to node 0 for single
            }
            else assert(false); // unknown mode

            // 将事务放入对应的TxnQueue中
            txn_queues_[routed_node_id]->push_txn(txn_entry);
            routed_txn_cnt_per_node[routed_node_id]++;
        }
        std::cout << "Router worker thread finished." << std::endl;
    }

    // !层次与run_router_worker并列，用于批量路由事务
    void run_router_batch_worker() {
        assert(SYSTEM_MODE == 9 || SYSTEM_MODE == 10); 
        // for routing needed db connections, each routing thread has its own connections
        std::vector<pqxx::connection*> thread_conns_vec;
        for(int i=0; i<ComputeNodeCount; i++) {
            pqxx::connection* conn = new pqxx::connection(DBConnection[i]);
            thread_conns_vec.push_back(conn);
        }
        while (true) {
            logger->info("Router Worker: Batch ID " + std::to_string(batch_id) + " processing started.");
            auto txn_batch = txn_pool_->fetch_batch_txns_from_pool(BatchRouterProcessSize);
            if (txn_batch == nullptr || txn_batch->empty()) {
                // 说明事务池已经运行完成
                for(auto txn_queue : txn_queues_) {
                    txn_queue->set_finished();
                }
                break;
            }
            assert(txn_batch->size() == BatchRouterProcessSize);
            
            std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> reorder_route_queues;
            if(SYSTEM_MODE == 9) {
                reorder_route_queues = this->get_route_primary_batch_2phase(txn_batch, thread_conns_vec);
                assert(reorder_route_queues && reorder_route_queues->size() == ComputeNodeCount);
            }
            else if(SYSTEM_MODE == 10) {
                reorder_route_queues = this->get_route_primary_batch_schedule(txn_batch, thread_conns_vec);
                assert(reorder_route_queues && reorder_route_queues->size() == ComputeNodeCount);
            }
            else assert(false); 

            // 开几个线程把reorder_route_queues合并到txn_queues_，运用线程池
            for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                auto node_q = std::move((*reorder_route_queues)[node_id]);
                
                threadpool.enqueue([this, node_id, q = std::move(node_q)]() mutable {
                    txn_queues_[node_id]->set_process_batch_id(batch_id);
                    // 合并reorder_route_queues到txn_queues_
                    while (!q.empty()) {
                        // std::cout << "Routing txn " << q.front()->tx_id << " to node " << node_id << std::endl;
                        // 将事务放入对应的TxnQueue中
                        txn_queues_[node_id]->push_txn(q.front());
                        q.pop();
                        routed_txn_cnt_per_node[node_id]++;
                    }
                    // 把该批的事务都分发完成，设置batch处理完成标志
                    txn_queues_[node_id]->set_batch_finished();
                });
            }
            // 等待所有db connector线程完成该批次的路由
            for(int i=0; i<ComputeNodeCount; i++) {
                std::unique_lock<std::mutex> lock(batch_mutex); 
                batch_cv.wait(lock, [this, i]() { 
                    if(batch_finished_flags[i] >= db_con_worker_threads) return true; 
                    else return false;
                });
                // std::cout << "Batch Router Worker: Node " << i << " finished batch " << batch_id << std::endl;
            }
            
            // 说明该计算节点的所有线程已经跑完事务了, 重置该节点的batch完成标志
            std::unique_lock<std::mutex> lock(batch_mutex); 
            for(int i=0; i<ComputeNodeCount; i++) {
                // 设置txn_queues_可以开始处理下一批次
                txn_queues_[i]->set_process_batch_id(batch_id);
                // 设置batch_finished_flags, 之后db connector线程可以开始处理下一批次
                batch_finished_flags[i] = 0;
                batch_cv.notify_all();
            }
            batch_id ++;
        }
        std::cout << "Router worker thread finished." << std::endl; 
    }
    
    // 计算节点通知
    void notify_batch_finished(node_id_t compute_node_id) { 
        std::lock_guard<std::mutex> lock(batch_mutex);
        // 累计该计算节点完成的线程数
        if(++batch_finished_flags[compute_node_id] >= db_con_worker_threads) {
            batch_cv.notify_all();
        }
    }

    // 计算节点等待
    void wait_for_next_batch(node_id_t compute_node_id, int con_batch_id) {
        std::unique_lock<std::mutex> lock(batch_mutex);
        batch_cv.wait(lock, [this, compute_node_id, con_batch_id]() { 
            // 如果该计算节点的所有线程还没有完成该批次的处理，则继续等待
            // 如果batch_finished_flags被重置为0，说明可以开始下一批次的处理
            if(batch_finished_flags[compute_node_id] == 0 || con_batch_id < batch_id) return true; 
            else return false; 
        });
    }


    // ---------- metis -----------
    // 设置METIS分区数量
    void set_partition_count(int count) {
        if (count > 0) {
            metis_->init_node_nums(count);
        }
    }

    Stats& get_stats() {
        return stats_;
    }

    const NewMetis::Stats& get_metis_stats() const {
        return metis_->get_stats();
    }

    int get_ownership_changes() const {
        return ownership_table_->get_ownership_changes();
    }

    std::vector<int> get_ownership_changes_per_txn_type() const {
        return ownership_table_->get_ownership_changes_per_txn_type();
    }

    std::vector<uint64_t> get_routed_txn_cnt_per_node() {
        std::vector<uint64_t> result;
        for (int i = 0; i < ComputeNodeCount; i++) {
            result.push_back(routed_txn_cnt_per_node[i].load(std::memory_order_relaxed));
        }
        return result;
    }

private:
    // 重置事务/路由相关的统计信息（线程安全）
    void reset_txn_statistics() {
        stats_.change_page_cnt.store(0, std::memory_order_relaxed);
        stats_.page_update_cnt.store(0, std::memory_order_relaxed);
        stats_.ownership_random_txns.store(0, std::memory_order_relaxed);
        stats_.ownership_entirely_txns.store(0, std::memory_order_relaxed);
        stats_.ownership_cross_txns.store(0, std::memory_order_relaxed);
    }

    // 大小模型 — 根据实际结构开销调整
    static std::size_t hot_entry_size_model_() {
        return sizeof(HotEntry);
    }

    // 查找 key。若在 hot hash 中找到，立即返回 page。
    // 否则可能查 B+tree 提示（在 .cc 实现），未命中返回 std::nullopt。
    inline HotEntry lookup(table_id_t table_id, itemkey_t key, std::vector<pqxx::connection *> &thread_conns) {
        std::lock_guard<std::mutex> lock(hot_mutex_);
        auto it = hot_key_map.find({table_id, key});
        if (it != hot_key_map.end()) {
            // hot hash 命中
            stats_.hot_hit++;
            it->second.freq++;
            // 更新 LRU 列表, 把当前的key移动到前端
            if (it->second.lru_it != hot_lru_.begin()) {
                hot_lru_.splice(hot_lru_.begin(), hot_lru_, it->second.lru_it);
            }
            return it->second;
        } else {
            // hot hash 未命中
            stats_.hot_miss++;
            // 在B+树中查找所在的页面
            BtreeNode *return_node = nullptr;
            page_id_t btree_page = btree_service_->get_page_id_by_key(table_id, key, thread_conns[0], &return_node);
            HotEntry entry;
            if (btree_page != kInvalidPageId) {
                // 更新 hot_key_map
                entry = insert_or_victim_hot(table_id, key, btree_page);
            }
            insert_batch_bnode(table_id, return_node);
            return entry;
        }
    }
    
    // 插入映射。如果存储满了, 会在预算内驱逐。
    inline HotEntry insert_or_victim_hot(table_id_t table_id, itemkey_t key, page_id_t page) {
        // 当前的key一定不会在map中存在
        assert(hot_key_map.find({table_id, key}) == hot_key_map.end());
        // 插入新条目
        hot_lru_.push_front({table_id, key});
        auto [it, ok] = hot_key_map.emplace(
            DataItemKey{table_id, key},
            HotEntry{page, 1, hot_lru_.begin()}
        );
        if (!ok) assert(false); // 不应该发生

        stats_.hot_hash_bytes += hot_entry_size_model_();
        std::cout << "Inserted hot key: (table_id=" << table_id << ", key=" << key << ") -> page " << page << std::endl;
        // 检查是否超预算, 超预算则驱逐
        while (stats_.hot_hash_bytes > cfg_.hot_hash_cap_bytes && !hot_lru_.empty()) {
            const auto & evict_key = hot_lru_.back();
            auto evict_it = hot_key_map.find(evict_key);
            if (evict_it != hot_key_map.end()) {
                stats_.hot_hash_bytes -= hot_entry_size_model_();
                stats_.evict_hot_entries++;
                hot_key_map.erase(evict_it);
            }
            hot_lru_.pop_back();
            std::cout << "Evicted hot key: (table_id=" << evict_key.table_id << ", key=" << evict_key.key << ")" <<
                    std::endl;
        }
        return hot_key_map.at({table_id, key});
    }

    inline void insert_batch_bnode(table_id_t table_id, BtreeNode *return_node) {
        if (return_node == nullptr) return;
        if (stats_.hot_hash_bytes > cfg_.hot_hash_cap_bytes * 0.9) return; // 热点缓存快满了就不插入了
        // 批量插入B+树的非叶子节点
        for (size_t i = 0; i < return_node->keys.size(); i++) {
            itemkey_t key = return_node->keys[i];
            page_id_t page = return_node->values[i];
            if (key == -1) continue; // 跳过无效键
            auto it = hot_key_map.find({table_id, key});
            if (it == hot_key_map.end()) {
                // 插入新条目
                hot_lru_.push_front({table_id, key});
                HotEntry entry;
                entry.page = page;
                entry.freq = 1;
                entry.lru_it = hot_lru_.begin();
                hot_key_map.emplace(DataItemKey{table_id, key}, entry);
                stats_.hot_hash_bytes += hot_entry_size_model_();
                std::cout << "Inserted hot key: (table_id=" << table_id << ", key=" << key << ") -> page " << page <<
                        std::endl;
            }
        }
    }

    inline void log_metis_ownership_based_router_result(SmartRouterResult &result, const std::string &debug_info, const MetisOwnershipDecisionType router_decision_type) {
        result.sys_8_decision_type = (int)router_decision_type;
        if(router_decision_type == MetisOwnershipDecisionType::MetisNoDecision) {
            this->stats_.metis_no_decision++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis No Decision] found no candidate node based on metis, randomly selected node " + 
					std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisMissingAndOwnershipMissing) {
            this->stats_.metis_missing_and_ownership_missing++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Missing + Ownership Missing] found no candidate node based on ownership table, randomly selected node " + 
					std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisMissingAndOwnershipEntirely) {
            this->stats_.metis_missing_and_ownership_entirely++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Missing + Ownership Entirely] " + debug_info + 
					" based on ownership table directly to node " + std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisMissingAndOwnershipCross) {
            this->stats_.metis_missing_and_ownership_cross++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Missing + Ownership Cross] " + debug_info + 
					" based on ownership table with count to node " + std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisEntirelyAndOwnershipMissing) {
            this->stats_.metis_entirely_and_ownership_missing++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Entirely + Ownership Missing] found no candidate node based on ownership table, selected metis node " + 
					std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossEqual) {
            this->stats_.metis_entirely_and_ownership_cross_equal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Entirely + Ownership Cross Equal] " + debug_info + 
					" both metis and ownership table to node " + std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossUnequal) {
            this->stats_.metis_entirely_and_ownership_cross_unequal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Entirely + Ownership Cross Unequal] " + debug_info + 
					" metis to node " + std::to_string(result.smart_router_id) + 
					" but ownership table to node " + std::to_string(result.smart_router_id) + 
					", selected ownership node");
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyEqual) {
            this->stats_.metis_entirely_and_ownership_entirely_equal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Entirely + Ownership Entirely Equal] " + debug_info + 
					" both metis and ownership table to node " + std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyUnequal) {
            this->stats_.metis_entirely_and_ownership_entirely_unequal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Entirely + Ownership Entirely Unequal] " + debug_info + 
					" metis to node " + std::to_string(result.smart_router_id) + 
					" but ownership table to node " + std::to_string(result.smart_router_id) + 
					", selected ownership node");
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisCrossAndOwnershipMissing) {
            this->stats_.metis_cross_and_ownership_missing++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Cross + Ownership Missing] found no candidate node based on ownership table, selected metis node " + 
					std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyEqual) {
            this->stats_.metis_cross_and_ownership_entirely_equal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Cross + Ownership Entirely Equal] " + debug_info + 
					" both metis and ownership table to node " + std::to_string(result.smart_router_id));
		}
        else if(router_decision_type == MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyUnequal) {
            this->stats_.metis_cross_and_ownership_entirely_unequal++;
            if(WarmupEnd)
            logger->info("[SmartRouter Metis Cross + Ownership Entirely Unequal] " + debug_info + 
                    " metis to node " + std::to_string(result.smart_router_id) + 
                    " but ownership table to node " + std::to_string(result.smart_router_id) + 
                    ", selected ownership node");
        }
		else if(router_decision_type == MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossEqual) {
            this->stats_.metis_cross_and_ownership_cross_equal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Cross + Ownership Cross Equal] " + debug_info + 
					" both metis and ownership table to node " + std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossUnequal) {
            this->stats_.metis_cross_and_ownership_cross_unequal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Cross + Ownership Cross Unequal] " + debug_info + 
					" metis to node " + std::to_string(result.smart_router_id) + 
					" but ownership table to node " + std::to_string(result.smart_router_id) + 
					", selected ownership node");
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisPartialAndOwnershipMissing) {
            this->stats_.metis_partial_and_ownership_missing++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Partial + Ownership Missing] found no candidate node based on ownership table, selected metis node " + 
					std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyEqual) {
            this->stats_.metis_partial_and_ownership_entirely_equal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Partial + Ownership Entirely Equal] " + debug_info + 
					" both metis and ownership table to node " + std::to_string(result.smart_router_id));
		}
        else if(router_decision_type == MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyUnequal) {
            this->stats_.metis_partial_and_ownership_entirely_unequal++;
            if(WarmupEnd)
            logger->info("[SmartRouter Metis Partial + Ownership Entirely Unequal] " + debug_info + 
                    " based on ownership table directly to node " + std::to_string(result.smart_router_id));
        }
		else if(router_decision_type == MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossEqual) {
            this->stats_.metis_partial_and_ownership_cross_equal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Partial + Ownership Cross Equal] " + debug_info + 
					" both metis and ownership table to node " + std::to_string(result.smart_router_id));
		}
		else if(router_decision_type == MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossUnequal) {
            this->stats_.metis_partial_and_ownership_cross_unequal++;
            if(WarmupEnd)
			logger->info("[SmartRouter Metis Partial + Ownership Cross Unequal] " + debug_info + 
					" metis to node " + std::to_string(result.smart_router_id) + 
					" but ownership table to node " + std::to_string(result.smart_router_id) + 
					", selected ownership node");
		}
    }

private:
    // 配置
    Config cfg_{};
    Logger* logger;
    int db_con_worker_threads;

    // 一级缓存: hot hash (key -> HotEntry)
    std::unordered_map<DataItemKey, HotEntry, DataItemKeyHash> hot_key_map;
    std::list<DataItemKey> hot_lru_; // 前端为最新，后端为最旧
    std::mutex hot_mutex_;

    // 二级缓存: B+ 树的非叶子节点, 在路由层通过维护B+树的中间节点，通过pageinspect插件访问B+树的叶子节点获取key->page的映射
    BtreeIndexService *btree_service_;

    // 页面所有权表
    OwnershipTable* ownership_table_;

    //for metis
    ThreadPool threadpool;
    NewMetis* metis_;

    // 统计数据
    Stats stats_{};

    SmallBank* smallbank_ = nullptr;

    // 事务池
    TxnPool* txn_pool_ = nullptr;
    // 事务队列
    std::vector<TxnQueue*> txn_queues_;

    std::vector<std::atomic<int>> routed_txn_cnt_per_node;

    // log access key
    std::mutex log_mutex;
    std::string access_log_file_name = "access_key.log"; // 日志文件路径
    std::ofstream access_key_log_file;
    std::unordered_map<int, long long> key_freq;

    // for batch routing
    std::mutex batch_mutex;
    std::condition_variable batch_cv;
    std::atomic<int> batch_id{0};
    std::vector<int> batch_finished_flags;
};
