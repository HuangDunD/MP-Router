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

#include "common.h"
#include "btree_search.h"
#include "metis_partitioner.h"
#include "threadpool.h"
#include "log/Logger.h"
#include "ownership_table.h"

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
        int thread_pool_size = 16; // 线程池大小
        std::string log_file = "smart_router_metis.log"; // 日志文件
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
        std::atomic<int> metis_cross_and_ownership_entirely = 0;
        std::atomic<int> metis_cross_and_ownership_cross_equal = 0; 
        std::atomic<int> metis_cross_and_ownership_cross_unequal = 0;
        // for metis partial
        std::atomic<int> metis_partial_and_ownership_missing = 0;
        std::atomic<int> metis_partial_and_ownership_entirely = 0;
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
    explicit SmartRouter(const Config &cfg, BtreeIndexService *btree_service, 
            NewMetis* metis = nullptr, Logger* logger_ptr = nullptr)
        : cfg_(cfg),
          logger(logger_ptr),
          btree_service_(btree_service),
          metis_(metis),
          pool(cfg.thread_pool_size, *logger)
    {
        metis_->set_thread_pool(&pool);
        metis_->init_node_nums(cfg.partition_nums);
        ownership_table_ = new OwnershipTable(logger);
    }

    ~SmartRouter() = default; // 使用 = default

    // 可能Update SQL执行之后数据页所在的位置, 根据returning ctid 进行更新key-page映射
    // 如果key不存在, 则不进行任何操作
    inline void update_key_page(std::vector<table_id_t>& table_ids, std::vector<itemkey_t>& keys, 
            std::vector<page_id_t> ctid_ret_pages, node_id_t routed_node_id, int txn_type = -1) { // txn_type for SYSTEM_MODE 8
        assert(table_ids.size() == keys.size() && keys.size() == ctid_ret_pages.size());
        for(size_t i=0; i<table_ids.size(); i++) {
            std::lock_guard<std::mutex> lock(hot_mutex_);
            auto it = hot_key_map.find({table_ids[i], keys[i]});
            if (it != hot_key_map.end()) {
                auto original_page = it->second.page;
                if(it->second.page != ctid_ret_pages[i]){ // 只有在page变化时才更新
                    // 这个地方应该是访问了原来的页面和新的页面, 都变成了这个节点的所有                    
                    ownership_table_->set_owner(table_ids[i], keys[i], ctid_ret_pages[i], routed_node_id, txn_type);
                    ownership_table_->set_owner(table_ids[i], keys[i], original_page, routed_node_id, txn_type); 
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
                    ownership_table_->set_owner(table_ids[i], keys[i], original_page, routed_node_id, txn_type); 
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

        std::vector<uint64_t> table_page_id; // 高32位存table_id，低32位存page_id, for SYSTEM_MODE 3
        std::unordered_map<node_id_t, int> node_count_basedon_key_access_last; // 基于key_access_last的计数, for SYSTEM_MODE 5
        std::vector<uint64_t> table_key_id;  // 高32位存table_id，低32位存key, for SYSTEM_MODE 6
        std::unordered_map<node_id_t, int> node_count_basedon_page_access_last; // 基于page_access_last的计数, for SYSTEM_MODE 7
        std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 找到ownership对应的节点的映射关系, for SYSTEM_MODE 8
        std::string debug_info;
        table_page_id.reserve(keys.size());
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
                table_page_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | entry.page);
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
                table_key_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | keys[i]); 
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
                table_page_id.push_back(table_page_id_val);
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
                std::sort(table_page_id.begin(), table_page_id.end());
                table_page_id.erase(std::unique(table_page_id.begin(), table_page_id.end()), table_page_id.end());
                result.keys_processed = table_page_id.size();
                metis_->build_internal_graph(table_page_id, &result.smart_router_id);
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
                std::sort(table_key_id.begin(), table_key_id.end());
                table_key_id.erase(std::unique(table_key_id.begin(), table_key_id.end()), table_key_id.end());
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
                std::sort(table_page_id.begin(), table_page_id.end());
                table_page_id.erase(std::unique(table_page_id.begin(), table_page_id.end()), table_page_id.end());
                result.keys_processed = table_page_id.size();
                node_id_t metis_decision_node;
                std::unordered_map<uint64_t, node_id_t> page_to_node_map;
                int ret_code = metis_->build_internal_graph(table_page_id, &metis_decision_node, &page_to_node_map);
                // page_ownership_to_node_map 是ownership_table_中记录的page到node的映射
                // page_to_node_map 是metis分区后得到的page到node的映射
                // 这里进行对比, 看看两者是否一致, 综合考虑page_to_node_map和ownership_table_的信息, 进行调整
                if  (ret_code == 0){ 
                    // no decision
                    assert(page_to_node_map.empty());
                    this->stats_.metis_no_decision++;
                    result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
                    result.sys_8_decision_type = 0;
                    if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                        logger->info("[SmartRouter Metis No Decision] found no candidate node based on metis, randomly selected node " + 
                                std::to_string(result.smart_router_id));
                }
                else if (ret_code == -1) {
                    // missing, 没有任何page被映射到节点
                    // 这个时候再检查一下ownership_table_的信息
                    assert(page_to_node_map.empty()); // 因为ret_code == -1, 所以page_to_node_map应该是空的
                    if(node_count_basedon_page_access_last.empty()) {
                        // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                        this->stats_.metis_missing_and_ownership_missing++;
                        result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
                        result.sys_8_decision_type = 1;
                        if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                            logger->info("[SmartRouter Metis Missing + Ownership Missing] found no candidate node based on ownership table, randomly selected node " + 
                                    std::to_string(result.smart_router_id));
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        this->stats_.metis_missing_and_ownership_entirely++;
                        result.smart_router_id = page_ownership_to_node_map.begin()->second;
                        result.sys_8_decision_type = 2;
                        if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                            logger->info("[SmartRouter Metis Missing + Ownership Entirely] " + debug_info + 
                                    " based on ownership table directly to node " + std::to_string(result.smart_router_id));
                    }
                    else if(node_count_basedon_page_access_last.size() > 1) {
                        // ownership_table_中有多个page的映射信息
                        this->stats_.metis_missing_and_ownership_cross++;
                        result.sys_8_decision_type = 3;
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
                        result.smart_router_id = candidates[rand() % candidates.size()];
                        if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                            logger->info("[SmartRouter Metis Missing + Ownership Cross] " + debug_info + 
                                    " based on ownership table with count (" + std::to_string(max_count) + 
                                    ") to node " + std::to_string(result.smart_router_id));
                    }
                    else assert(false); // 不可能出现的情况
                }
                else if (ret_code == 1) {
                    // entire affinity, 所有page都映射到同一个节点
                    assert(!page_to_node_map.empty());
                    assert(metis_decision_node != -1);
                    if(node_count_basedon_page_access_last.empty()) {
                        // ownership_table_中没有任何page的映射信息, 即涉及到的页面
                        this->stats_.metis_entirely_and_ownership_missing++;
                        result.smart_router_id = metis_decision_node;
                        result.sys_8_decision_type = 4;
                        if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                            logger->info("[SmartRouter Metis Entirely + Ownership Missing] found no candidate node based on ownership table, selected metis node " + 
                                    std::to_string(result.smart_router_id));
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        auto ownership_node = page_ownership_to_node_map.begin()->second;
                        if(ownership_node == metis_decision_node) {
                            this->stats_.metis_entirely_and_ownership_entirely_equal++;
                            result.smart_router_id = metis_decision_node;
                            result.sys_8_decision_type = 7;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Entirely + Ownership Entirely Equal] " + debug_info + 
                                        " both metis and ownership table to node " + std::to_string(result.smart_router_id));
                        }
                        else {
                            this->stats_.metis_entirely_and_ownership_entirely_unequal++;
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            result.sys_8_decision_type = 8;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Entirely + Ownership Entirely Unequal] " + debug_info + 
                                        " metis to node " + std::to_string(metis_decision_node) + 
                                        " but ownership table to node " + std::to_string(ownership_node) + 
                                        ", selected ownership node");
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
                            this->stats_.metis_entirely_and_ownership_cross_equal++;
                            result.smart_router_id = metis_decision_node;
                            result.sys_8_decision_type = 5;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Entirely + Ownership Cross Equal] " + debug_info + 
                                        " both metis and ownership table to node " + std::to_string(result.smart_router_id));
                        }
                        else {
                            this->stats_.metis_entirely_and_ownership_cross_unequal++;
                            // !pay attention: 这里对于candidates 中有可能包含metis_decision_node的情况, 这种情况需要选择metis_decision_node
                            result.smart_router_id = candidate_node;
                            result.sys_8_decision_type = 6;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Entirely + Ownership Cross Unequal] " + debug_info + 
                                        " metis to node " + std::to_string(metis_decision_node) + 
                                        " but ownership table to node " + std::to_string(candidate_node) + 
                                        ", selected ownership node");
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
                        this->stats_.metis_partial_and_ownership_missing++;
                        result.smart_router_id = metis_decision_node;
                        result.sys_8_decision_type = 13;
                        if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                            logger->info("[SmartRouter Metis Partial + Ownership Missing] found no candidate node based on ownership table, selected metis node " + 
                                    std::to_string(result.smart_router_id));
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        auto ownership_node = page_ownership_to_node_map.begin()->second;
                        this->stats_.metis_partial_and_ownership_entirely++;
                        if(ownership_node == metis_decision_node) {
                            // 两者一致
                            result.smart_router_id = metis_decision_node;
                            result.sys_8_decision_type = 14;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Partial + Ownership Entirely Equal] " + debug_info + 
                                        " both metis and ownership table to node " + std::to_string(result.smart_router_id));
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            result.sys_8_decision_type = 14;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Partial + Ownership Entirely Unequal] " + debug_info + 
                                        " metis to node " + std::to_string(metis_decision_node) + 
                                        " but ownership table to node " + std::to_string(ownership_node) + 
                                        ", selected ownership node");
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
                        node_id_t ownership_node = candidates[rand() % candidates.size()];
                        if(ownership_node == metis_decision_node) {
                            this->stats_.metis_partial_and_ownership_cross_equal++;
                            result.smart_router_id = metis_decision_node;
                            result.sys_8_decision_type = 15;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Partial + Ownership Cross Equal] " + debug_info + 
                                        " both metis and ownership table to node " + std::to_string(result.smart_router_id));
                        }
                        else {
                            this->stats_.metis_partial_and_ownership_cross_unequal++;
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            result.sys_8_decision_type = 16;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Partial + Ownership Cross Unequal] " + debug_info + 
                                        " metis to node " + std::to_string(metis_decision_node) + 
                                        " but ownership table to node " + std::to_string(ownership_node) + 
                                        ", selected ownership node");
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
                        this->stats_.metis_cross_and_ownership_missing++;
                        result.smart_router_id = metis_decision_node;
                        result.sys_8_decision_type = 9;
                        if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                            logger->info("[SmartRouter Metis Cross + Ownership Missing] found no candidate node based on ownership table, selected metis node " + 
                                    std::to_string(result.smart_router_id));
                    }
                    else if(node_count_basedon_page_access_last.size() == 1) {
                        // ownership_table_中只有一个page的映射信息
                        auto ownership_node = page_ownership_to_node_map.begin()->second;
                        this->stats_.metis_cross_and_ownership_entirely++;
                        if(ownership_node == metis_decision_node) {
                            // 两者一致
                            result.smart_router_id = metis_decision_node;
                            result.sys_8_decision_type = 10;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Cross + Ownership Entirely Equal] " + debug_info + 
                                        " both metis and ownership table to node " + std::to_string(result.smart_router_id));
                        }
                        else {
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            result.sys_8_decision_type = 10;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Cross + Ownership Entirely Unequal] " + debug_info + 
                                        " metis to node " + std::to_string(metis_decision_node) + 
                                        " but ownership table to node " + std::to_string(ownership_node) + 
                                        ", selected ownership node");
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
                        node_id_t ownership_node = candidates[rand() % candidates.size()];
                        if(ownership_node == metis_decision_node) {
                            this->stats_.metis_cross_and_ownership_cross_equal++;
                            result.smart_router_id = metis_decision_node;
                            result.sys_8_decision_type = 11;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Cross + Ownership Cross Equal] " + debug_info + 
                                        " both metis and ownership table to node " + std::to_string(result.smart_router_id));
                        }
                        else {
                            this->stats_.metis_cross_and_ownership_cross_unequal++;
                            // !两者不一致, 优先选择ownership_node
                            result.smart_router_id = ownership_node;
                            result.sys_8_decision_type = 12;
                            if(WarmupEnd) // 仅在完成warmup阶段后记录日志
                                logger->info("[SmartRouter Metis Cross + Ownership Cross Unequal] " + debug_info + 
                                        " metis to node " + std::to_string(metis_decision_node) + 
                                        " but ownership table to node " + std::to_string(ownership_node) + 
                                        ", selected ownership node");
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

private:
    // 配置
    Config cfg_{};
    Logger* logger;

    // 一级缓存: hot hash (key -> HotEntry)
    std::unordered_map<DataItemKey, HotEntry, DataItemKeyHash> hot_key_map;
    std::list<DataItemKey> hot_lru_; // 前端为最新，后端为最旧
    std::mutex hot_mutex_;

    // 二级缓存: B+ 树的非叶子节点, 在路由层通过维护B+树的中间节点，通过pageinspect插件访问B+树的叶子节点获取key->page的映射
    BtreeIndexService *btree_service_;

    // 页面所有权表
    OwnershipTable* ownership_table_;

    //for metis
    ThreadPool pool;
    NewMetis* metis_;

    // 统计数据
    Stats stats_{};
};
