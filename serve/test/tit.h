// Copyright 2025
// Author: huangdund 

#pragma once
#include <queue> 
#include <mutex>
#include <list>
#include <condition_variable>

#include <atomic>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>
#include "txn_entry.h"
#include "Logger.h"

class SlidingTransactionInforTable {
public:
    SlidingTransactionInforTable(Logger* logger_ptr, size_t txnTableSize = 1000000)
        : txnTableSize_(txnTableSize),
          slots_(txnTableSize),
          logger_(logger_ptr) {}

    enum class TxnStatus { Empty, InProgress, Done, Evicted };

    // 将事务指针写入环形表。若覆盖的槽位中旧指针已完成，则删除；
    // 若未完成，则移入延迟删除集合，等待其 mark_done 后再删除。
    void push(TxnQueueEntry* entry) {
        if (!entry) return;
        // std::cout << "Pushing " << entry->tx_id << "." << std::endl;
        // 使用 tx_id 做环形槽定位
        size_t idx = static_cast<size_t>(entry->tx_id % txnTableSize_);
        Slot& slot = slots_[idx];

        // 先拿到旧值，再写入新值
        TxnQueueEntry* old_ptr = slot.ptr.load(std::memory_order_acquire);
        tx_id_t old_id = slot.tx_id.load(std::memory_order_acquire);

        // 正式写入新指针与 tx_id
        slot.ptr.store(entry, std::memory_order_release);
        slot.tx_id.store(entry->tx_id, std::memory_order_release);

        // 处理旧指针生命周期
        if (old_ptr) {
            // 如果旧指针的事务已完成，则回收；否则放入延迟集合
            if (old_id == entry->tx_id) {
                // 相同槽、相同 id 的情况理论上不会发生（tx_id 唯一）
                assert(false);
            }
            bool was_done = old_ptr->done.load(std::memory_order_acquire);
            if (was_done) {
                // 已完成，可安全删除
                delete old_ptr;
            } else {
                // 未完成，放入延迟集合，等 mark_done 时删除
                std::lock_guard<std::mutex> lk(defer_mutex_);
                deferred_.insert(old_ptr);
                logger_->warning("Transaction ID " + std::to_string(old_id) + " deferred for deletion.");
            }
        }
    }

    // 将某个事务标记为完成。若它仍在表中，则仅置位完成标记；
    // 若它已被环形覆盖而进入延迟集合，则在此处删除并移除。
    void mark_done(TxnQueueEntry* entry) {
        if (!entry) return;
        // std::cout << "Marking done " << entry->tx_id << "." << std::endl;
        // 直接按 tx_id 定位槽位
        size_t idx = static_cast<size_t>(entry->tx_id % txnTableSize_);
        Slot& slot = slots_[idx];
        TxnQueueEntry* cur = slot.ptr.load(std::memory_order_acquire);
        tx_id_t cur_id = slot.tx_id.load(std::memory_order_acquire);


        if (cur == entry && cur_id == entry->tx_id) {
            // 仍在表中：不立即删除，等待下一次覆盖回收
            entry->done.store(true, std::memory_order_release);
            return;
        }

        // 已被覆盖：从延迟集合中删除并释放
        std::lock_guard<std::mutex> lk(defer_mutex_);
        auto it = deferred_.find(entry);
        if (it != deferred_.end()) {
            deferred_.erase(it);
            delete entry;
        }
    }

    // 获取当前容量
    size_t capacity() const { return txnTableSize_; }

    // 通过 tx_id 查询状态（按照 tx_id % N 定位槽位 if match，否则 Evicted）
    TxnStatus get_status_by_tx_id(tx_id_t txid) const {
        size_t idx = static_cast<size_t>(txid % txnTableSize_);
        const Slot& slot = slots_[idx];
        tx_id_t cur_id = slot.tx_id.load(std::memory_order_acquire);
        if (cur_id != txid) return TxnStatus::Evicted;
        auto* p = slot.ptr.load(std::memory_order_acquire);
        if (!p) return TxnStatus::Empty;
        bool d = p->done.load(std::memory_order_acquire);
        return d ? TxnStatus::Done : TxnStatus::InProgress;
    }

    bool check_dependency_txn(TxnQueueEntry* txn_entry){ 
        bool wait = false; 
        for (auto dep_tx_id : txn_entry->dependencies) {
            auto status = get_status_by_tx_id(dep_tx_id);
            assert(status != TxnStatus::Empty);
            if (status == TxnStatus::InProgress) {
                wait = true;
                logger_->warning("Transaction: " + std::to_string(txn_entry->tx_id) + " is waiting due to dependency in progress " + std::to_string(dep_tx_id));
                break;
            }
        }
        return wait;
    }

private:
    struct Slot {
        std::atomic<TxnQueueEntry*> ptr{nullptr};
        std::atomic<tx_id_t> tx_id{0};
    };

    size_t txnTableSize_;
    std::vector<Slot> slots_;               // 槽位数组

    // 已被覆盖但尚未完成的指针，待完成后删除
    std::unordered_set<TxnQueueEntry*> deferred_;
    std::mutex defer_mutex_;

    Logger* logger_;
};