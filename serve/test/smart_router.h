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

        std::size_t hot_hash_cap_bytes = 64ULL * 1024ULL * 1024ULL; // 默认 64 MB, 作为 hot hash 的内存预算
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
    };

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
            std::vector<page_id_t> ctid_ret_pages, node_id_t routed_node_id) {
        assert(table_ids.size() == keys.size() && keys.size() == ctid_ret_pages.size());
        for(size_t i=0; i<table_ids.size(); i++) {
        std::lock_guard<std::mutex> lock(hot_mutex_);
        auto it = hot_key_map.find({table_ids[i], keys[i]});
        if (it != hot_key_map.end()) {
            auto original_page = it->second.page;
            if(it->second.page != ctid_ret_pages[i]){ // 只有在page变化时才更新
                // 这个地方应该是访问了原来的页面和新的页面, 都变成了这个节点的所有权
                ownership_table_->set_owner(table_ids[i], ctid_ret_pages[i], routed_node_id);
                ownership_table_->set_owner(table_ids[i], original_page, routed_node_id); 
                it->second.page = ctid_ret_pages[i];
                // 毫秒级时间戳
                it->second.last_access_time = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count()
                );
                stats_.change_page_cnt++;
            }
            else{
                // 仅访问了原来的页面, 仍然是这个节点的所有权
                ownership_table_->set_owner(table_ids[i], original_page, routed_node_id); 
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
    SmartRouterResult get_route_primary(std::vector<table_id_t> &table_ids, std::vector<itemkey_t> &keys, 
            std::vector<pqxx::connection *> &thread_conns) {
        SmartRouterResult result;
        if (table_ids.size() != keys.size() || table_ids.empty()) {
            result.error_message = "Mismatched or empty table_ids and keys";
            return result;
        }

        std::vector<uint64_t> table_page_id; // 高32位存table_id，低32位存page_id, for SYSTEM_MODE 3
        std::unordered_map<node_id_t, int> node_count_basedon_key_access_last; // 基于key_access_last的计数, for SYSTEM_MODE 5
        std::vector<uint64_t> table_key_id;  // 高32位存table_id，低32位存key, for SYSTEM_MODE 6
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
                table_page_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | entry.page);
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
                result.smart_router_id = metis_->build_internal_graph(table_page_id);
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
                result.smart_router_id = metis_->build_internal_graph(table_key_id);
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

private:
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
