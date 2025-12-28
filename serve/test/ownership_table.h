// 这部分的代码主要是想模拟RAC层所有权的交互以便于调试优化
#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <cstdint>
#include <mutex>
#include <cassert>
#include <atomic>
#include "common.h"
#include "txn_queue.h"
class OwnershipEntry {
public:
    OwnershipEntry() {}
    std::mutex mutex;
    // node_id_t owner; // -1表示无主
    std::vector<node_id_t> owners; // 支持shared ownership
    bool mode = false; // false表示shared, true表示exclusive
};

class OwnershipTable {
public:
    OwnershipTable(Logger* logger) : logger(logger), ownership_changes_per_txn_type(SYS_8_DECISION_TYPE_COUNT) {
        // 初始化
        table_.clear();
        table_.resize(MAX_DB_TABLE_NUM);
        for(table_id_t i = 0; i < MAX_DB_TABLE_NUM; i++) {
            table_[i].resize(MAX_DB_PAGE_NUM);
            for(page_id_t j = 0; j < MAX_DB_PAGE_NUM; j++) {
                table_[i][j] = new OwnershipEntry();
                table_[i][j]->mode = 1; // default exclusive ownership
                // randomly assign owner
                node_id_t owner = rand() % ComputeNodeCount;
                table_[i][j]->owners.push_back(owner);
            }
        }
        // ownership_changes_per_txn_type 已在初始化列表中构造
        // 0: metis no decision, 1: metis missing and ownership missing, 2: metis missing and ownership entirely, 3: metis missing and ownership cross
                                      // 4: metis entirely and ownership missing, 5: metis entirely and ownership cross equal, 6: metis entirely and ownership cross unequal
                                      // 7: metis entirely and ownership entirely equal, 8: metis entirely and ownership entirely unequal
                                      // 9: metis cross and ownership missing, 10: metis cross and ownership entirely, 11: metis cross and ownership cross equal
                                      // 12: metis cross and ownership cross unequal, 13: metis partial and ownership missing, 14: metis partial and ownership entirely
                                      // 15: metis partial and ownership cross equal, 16: metis partial and ownership cross unequal
    }

    // 同时返回所有者和加锁模式（false=exclusive, true=shared）
    std::pair<std::vector<node_id_t>, bool> get_owner(table_id_t table_id, page_id_t page_id) const {
        if (table_id < 0 || table_id >= MAX_DB_TABLE_NUM) assert(false);
        if (page_id < 0 || page_id >= MAX_DB_PAGE_NUM) assert(false);
        const auto& entry = table_[table_id][page_id];
        std::unique_lock lock(entry->mutex);
        return {entry->owners, entry->mode};
    }

    std::pair<std::vector<node_id_t>, bool> get_owner(uint64_t table_page_id) const {
        table_id_t table_id = static_cast<table_id_t>(table_page_id >> 32);
        page_id_t page_id = static_cast<page_id_t>(table_page_id & 0xFFFFFFFF);
        return std::move(get_owner(table_id, page_id));
    }

    // 设置页面所有权, 如果所有权发生变化返回true，否则返回false
    bool set_owner(TxnQueueEntry* txn, table_id_t table_id, itemkey_t access_key, bool rw, page_id_t page_id, node_id_t owner) {
        if (table_id < 0 || table_id >= MAX_DB_TABLE_NUM) assert(false);
        if (page_id < 0 || page_id >= MAX_DB_PAGE_NUM) assert(false);
        auto& entry = table_[table_id][page_id];
        int txn_decision_type = txn ? txn->txn_decision_type : -1;
        int txn_id = txn ? txn->tx_id : -1;

        std::unique_lock lock(entry->mutex);
        if(!rw) {
            // shared ownership mode
            // 检查是否已经存在该owner
            for(auto existing_owner : entry->owners) {
                if(existing_owner == owner) {
                    // 已经存在该owner，不需要变更
        #if LOG_OWNERSHIP_CHANGE
                    uint64_t table_page_id = (static_cast<uint64_t>(table_id) << 32) | static_cast<uint64_t>(page_id);
                    if(WarmupEnd) // 只在正式阶段记录
                        logger->info("Txn id: "  + std::to_string(txn_id) + " txn_type: " + std::to_string(txn->txn_decision_type) +
                                "# Ownership not change (shared): (table_id=" + std::to_string(table_id) + ", access_key=" + std::to_string(access_key) + ", page_id=" + std::to_string(page_id) + 
                                ") --> " + std::to_string(table_page_id) + " owner remains: " + std::to_string(existing_owner));
        #endif
                    return false;   
                }
            }
            // 添加新的owner
            entry->owners.push_back(owner);
            entry->mode = false; // shared mode
            ownership_changes++;
            if(txn_decision_type >=0) {
                ownership_changes_per_txn_type[txn_decision_type]++;
            }
        #if LOG_OWNERSHIP_CHANGE
            uint64_t table_page_id = (static_cast<uint64_t>(table_id) << 32) | static_cast<uint64_t>(page_id);
            if(WarmupEnd) // 只在正式阶段记录
                logger->info("Txn id: " + std::to_string(txn_id) + " txn_type: " + std::to_string(txn->txn_decision_type) +
                        "! Ownership changed (shared): (table_id=" + std::to_string(table_id) + ", access_key=" + std::to_string(access_key) + ", page_id=" + std::to_string(page_id) + 
                        ") --> " + std::to_string(table_page_id) + " new owner added: " + std::to_string(owner));
        #endif
            return true;
        }
        else {
            // exclusive ownership mode
            if(entry->owners.size() == 1 && entry->owners[0] == owner && entry->mode == true) {
                // 已经是该owner且是exclusive模式，不需要变更
        #if LOG_OWNERSHIP_CHANGE
                uint64_t table_page_id = (static_cast<uint64_t>(table_id) << 32) | static_cast<uint64_t>(page_id);
                if(WarmupEnd) // 只在正式阶段记录
                    logger->info("Txn id: "  + std::to_string(txn_id) + " txn_type: " + std::to_string(txn->txn_decision_type) +
                            "# Ownership not change (exclusive): (table_id=" + std::to_string(table_id) + ", access_key=" + std::to_string(access_key) + ", page_id=" + std::to_string(page_id) + 
                            ") --> " + std::to_string(table_page_id) + " owner remains: " + std::to_string(entry->owners[0]));
        #endif
                return false;
            }
            // exclusive模式，清空现有owners
            entry->owners.clear();
            entry->mode = true; // exclusive mode
            entry->owners.push_back(owner);
            ownership_changes++;
            if(txn_decision_type >=0) {
                ownership_changes_per_txn_type[txn_decision_type]++;
            }
        #if LOG_OWNERSHIP_CHANGE
            uint64_t table_page_id = (static_cast<uint64_t>(table_id) << 32) | static_cast<uint64_t>(page_id);
            if(WarmupEnd) // 只在正式阶段记录
                logger->info("Txn id: " + std::to_string(txn_id) + " txn_type: " + std::to_string(txn->txn_decision_type) +
                        "! Ownership changed (exclusive): (table_id=" + std::to_string(table_id) + ", access_key=" + std::to_string(access_key) + ", page_id=" + std::to_string(page_id) + 
                        ") --> " + std::to_string(table_page_id) + " new owner: " + std::to_string(owner));
        #endif
            return true;
        }
    }

    // 获取所有权变更次数
    int get_ownership_changes() const {
        return ownership_changes.load();
    }

    std::vector<int> get_ownership_changes_per_txn_type() const {
        std::vector<int> result(ownership_changes_per_txn_type.size());
        for(size_t i=0; i<ownership_changes_per_txn_type.size(); i++) {
            result[i] = ownership_changes_per_txn_type[i].load();
        }
        return result;
    }

private:
    std::vector<std::vector<OwnershipEntry*>> table_;
    std::atomic<int> ownership_changes;
    std::vector<std::atomic<int>> ownership_changes_per_txn_type; // 按事务类型统计的所有权变更次数, for sys_decision_type
    Logger* logger;
};