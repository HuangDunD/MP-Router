// Author: huangdund
// Year: 2025

#pragma once
#include <atomic>
#include <vector>
#include "common.h"

enum class TxnScheduleType {
    NONE = -1,
    UNCONFLICT = 0,
    SCHEDULE_PRIOR = 1,
    OWNERSHIP_OK = 2
};
struct TxnQueueEntry {
    tx_id_t tx_id;
    int txn_type;
    std::vector<uint64_t> accounts; // for smallbank, store involved account ids, the table id is generated based on txn_type
    std::vector<uint64_t> keys; // for ycsb, store involved record keys
    
    int txn_decision_type = -1; // init to -1, and will be set during routing
    std::vector<page_id_t> accessed_page_ids; // the page ids this txn will access, set during routing
    int combine_txn_count = 0; // 这个字段的意思表示，pop事务执行的时候连带多少个事务(包括他自己)一起pop到一个工作线程执行，这样可以避免一些死锁的问题
    std::atomic<bool> done{false}; // 由执行线程标记完成；内存回收交由 TIT 管理
    std::vector<tx_id_t> dependencies; // 路由层决定的这些事务的前序事务，也就是执行这些事务之前检查一下TIT表中以来的事务状态是否已经done，否则拖延一小段时间再执行
    uint64_t first_exec_time = 0; // 判定为前面的依赖的事务还没执行完成, 稍微延后一会

    // 拓扑图相关
    std::vector<TxnQueueEntry*> after_txns; // 依赖当前事务的后续事务列表
    std::atomic<int> ref = 0; // 引用计数, 表示前序依赖事务数量
    TxnScheduleType schedule_type = TxnScheduleType::NONE; // 0: unconflict, 1: schedule_prior, 2: ownership_ok_back 
    int group_id;
    int batch_id;
    std::vector<int> dependency_group_id;
};