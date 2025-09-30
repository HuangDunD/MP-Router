// 这部分的代码主要是想模拟RAC层所有权的交互以便于调试优化
#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <cstdint>
#include <mutex>
#include <cassert>
#include "common.h"

class OwnershipEntry {
public:
    OwnershipEntry() : owner(-1) {}
    std::mutex mutex;
    node_id_t owner; // -1表示无主
};

class OwnershipTable {
public:
    OwnershipTable() {
        // 初始化
        table_.clear();
        table_.resize(MAX_DB_TABLE_NUM);
        for(table_id_t i = 0; i < MAX_DB_TABLE_NUM; i++) {
            table_[i].resize(MAX_DB_PAGE_NUM);
            for(page_id_t j = 0; j < MAX_DB_PAGE_NUM; j++) {
                table_[i][j].owner = -1;
            }
        }
    }
    
    // 查询页面所有者，未找到返回-1
    node_id_t get_owner(table_id_t table_id, page_id_t page_id) const {
        if (table_id < 0 || table_id >= MAX_DB_TABLE_NUM) assert(false);
        if (page_id < 0 || page_id >= MAX_DB_PAGE_NUM) assert(false);
        const auto& entry = table_[table_id][page_id];
        std::unique_lock lock(entry.mutex);
        return entry.owner;
    }

    // 设置页面所有权, 如果所有权发生变化返回true，否则返回false
    bool set_owner(table_id_t table_id, page_id_t page_id, node_id_t owner) {
        if (table_id < 0 || table_id >= MAX_DB_TABLE_NUM) assert(false);
        if (page_id < 0 || page_id >= MAX_DB_PAGE_NUM) assert(false);
        auto& entry = table_[table_id][page_id];
        std::unique_lock lock(entry.mutex);
        if (entry.owner == owner) return false;
        entry.owner = owner;
        return true;
    }

private:
    std::vector<std::vector<OwnershipEntry>> table_;
};