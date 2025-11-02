// Author: huangdund
// Year: 2025

#pragma once
#include <queue> 
#include <mutex>
#include <list>
#include <condition_variable>
#include "common.h"

struct TxnQueueEntry {
    tx_id_t tx_id;
    int txn_type;
    std::vector<uint64_t> accounts; // for smallbank, store involved account ids, the table id is generated based on txn_type

    int txn_decision_type = -1; // init to -1, and will be set during routing
};

// for every compute node db connections, we have a txn queue to store incoming txns
class TxnQueue {
public:
    TxnQueue(node_id_t node_id) : node_id_(node_id) {}
    ~TxnQueue() = default;
    
    TxnQueueEntry* pop_txn() {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() {
            return !txn_queue_.empty() || finished_ || batch_finished_;
        });
        if (finished_ && txn_queue_.empty()) {
            return nullptr; // indicate finished
        }
        if (batch_finished_ && txn_queue_.empty()) {
            return nullptr; // indicate batch finished
        }
        TxnQueueEntry* entry = txn_queue_.front();
        txn_queue_.pop();
        current_queue_size_--;
        return entry;
    }

    void push_txn(TxnQueueEntry* entry) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        txn_queue_.push(entry);
        current_queue_size_++;
        queue_cv_.notify_one();
    }

    int size() {
        return current_queue_size_.load();
    }

    void set_finished() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        finished_ = true;
        queue_cv_.notify_all();
    }

    void set_process_batch_id(int batch_id) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        assert(current_queue_size_ == 0); // only set new batch id when queue is empty
        process_batch_id_ = batch_id;
        batch_finished_ = false;
    }

    void set_batch_finished() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        batch_finished_ = true;
        queue_cv_.notify_all();
    }

    bool is_batch_finished() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return batch_finished_;
    }

    bool is_finished() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return finished_;
    }

private:
    std::queue<TxnQueueEntry*> txn_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    node_id_t node_id_; // the compute node id this queue belongs to
    std::atomic<int> current_queue_size_ = 0;
    bool finished_ = false;

    int process_batch_id_ = 0;
    bool batch_finished_ = false;
};


// the txn pool, receive txns from clients and dispatch to txn queues of compute nodes
class TxnPool {
public:
    TxnPool(int max_pool_size) : max_pool_size_(max_pool_size){}
    ~TxnPool() = default;

    void receive_txn_from_client(TxnQueueEntry* entry) {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        pool_cv_.wait(lock, [this]() {
            return current_pool_size_ < max_pool_size_;
        });
        txn_pool_.push_back(entry);
        current_pool_size_++;
        pool_cv_.notify_one();
    }

    TxnQueueEntry* fetch_txn_from_poolfront() {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        pool_cv_.wait(lock, [this]() {
            return !txn_pool_.empty() || stop_; // 如果停止标志被设置，也要退出等待
        });
        if (stop_ && txn_pool_.empty()) {
            return nullptr; // 如果停止且池为空，返回空指针
        }
        if(current_pool_size_-- >= max_pool_size_) {
            pool_cv_.notify_one();
        }
        TxnQueueEntry* entry = txn_pool_.front();
        txn_pool_.pop_front();
        return entry;
    }

    std::unique_ptr<std::vector<TxnQueueEntry*>> fetch_batch_txns_from_pool(int batch_size) {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        pool_cv_.wait(lock, [this, batch_size]() {
            return txn_pool_.size() >= batch_size || stop_; // 如果停止标志被设置，也要退出等待
        }); 
        if (stop_ && txn_pool_.size() < batch_size) {
            return {}; // 如果停止且池中事务不足，返回空向量
        }
        std::unique_ptr<std::vector<TxnQueueEntry*>> batch_txns = 
            std::make_unique<std::vector<TxnQueueEntry*>>();
        for (int i = 0; i < batch_size; i++) {
            current_pool_size_--;
            TxnQueueEntry* entry = txn_pool_.front();
            txn_pool_.pop_front();
            batch_txns->push_back(entry);
        }
        if(current_pool_size_ + batch_size >= max_pool_size_) {
            pool_cv_.notify_one();
        }
        return batch_txns;
    }

    void stop_pool() {
        stop_ = true;
        pool_cv_.notify_all();
    }

private: 
    
private:
    SmallBank* smallbank_; // pointer to the SmallBank instance

    const int max_pool_size_; // batch process txn size 
    std::atomic<int> current_pool_size_{0};
    std::list<TxnQueueEntry*> txn_pool_; 
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;

    // for reordering txns based on smart router's static affinity partitioning and dynamic ownership table
    std::unordered_map<uint64_t, std::list<TxnQueueEntry*>> partitioned_txn_map_; // table page id (table id & page id) to txn list

    bool stop_ = false;
    // SmartRouter* smart_router_; 
};
