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

class OwnershipEntry {
public:
    OwnershipEntry() : owner(-1) {}
    std::mutex mutex;
    node_id_t owner; // -1表示无主
};

class OwnershipTable {
public:
    OwnershipTable(Logger* logger) : logger(logger) {
        // 初始化
        table_.clear();
        table_.resize(MAX_DB_TABLE_NUM);
        for(table_id_t i = 0; i < MAX_DB_TABLE_NUM; i++) {
            table_[i].resize(MAX_DB_PAGE_NUM);
            for(page_id_t j = 0; j < MAX_DB_PAGE_NUM; j++) {
                table_[i][j] = new OwnershipEntry();
            }
        }
    }
    
    // 查询页面所有者，未找到返回-1
    node_id_t get_owner(table_id_t table_id, page_id_t page_id) const {
        if (table_id < 0 || table_id >= MAX_DB_TABLE_NUM) assert(false);
        if (page_id < 0 || page_id >= MAX_DB_PAGE_NUM) assert(false);
        const auto& entry = table_[table_id][page_id];
        std::unique_lock lock(entry->mutex);
        return entry->owner;
    }

    node_id_t get_owner(uint64_t table_page_id) const {
        table_id_t table_id = static_cast<table_id_t>(table_page_id >> 32);
        page_id_t page_id = static_cast<page_id_t>(table_page_id & 0xFFFFFFFF);
        return get_owner(table_id, page_id);
    }

    // 设置页面所有权, 如果所有权发生变化返回true，否则返回false
    bool set_owner(table_id_t table_id, itemkey_t access_key, page_id_t page_id, node_id_t owner) {
        if (table_id < 0 || table_id >= MAX_DB_TABLE_NUM) assert(false);
        if (page_id < 0 || page_id >= MAX_DB_PAGE_NUM) assert(false);
        auto& entry = table_[table_id][page_id];
        std::unique_lock lock(entry->mutex);
        if (entry->owner == owner) {
        #if LOG_OWNERSHIP_CHANGE
            uint64_t table_page_id = (static_cast<uint64_t>(table_id) << 32) | static_cast<uint64_t>(page_id);
            if(WarmupEnd) // 只在正式阶段记录
                logger->info("# Ownership not change: (table_id=" + std::to_string(table_id) + ", access_key=" + std::to_string(access_key) + ", page_id=" + std::to_string(page_id) + 
                        ") --> " + std::to_string(table_page_id) + " owner remains: " + std::to_string(entry->owner));
        #endif
            return false;
        }
        if(entry->owner != -1) {
            ownership_changes++;
        #if LOG_OWNERSHIP_CHANGE
            uint64_t table_page_id = (static_cast<uint64_t>(table_id) << 32) | static_cast<uint64_t>(page_id);
            if(WarmupEnd) // 只在正式阶段记录
                logger->info("! Ownership changed: (table_id=" + std::to_string(table_id) + ", access_key=" + std::to_string(access_key) + ", page_id=" + std::to_string(page_id) + 
                        ") --> " + std::to_string(table_page_id) + " original owner: " + std::to_string(entry->owner) + ", new owner: " + std::to_string(owner));
        #endif
        }
        entry->owner = owner;
        return true;
    }

    // 获取所有权变更次数
    int get_ownership_changes() const {
        return ownership_changes.load();
    }

private:
    std::vector<std::vector<OwnershipEntry*>> table_;
    std::atomic<int> ownership_changes;
    Logger* logger;
};