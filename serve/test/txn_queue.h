// Author: huangdund
// Year: 2025

#pragma once
#include <queue> 
#include <mutex>
#include <list>
#include <condition_variable>
#include "common.h"
#include "smallbank.h"
#include "tit.h"
#include "txn_entry.h"
#include "Logger.h"

// for every compute node db connections, we have a txn queue to store incoming txns
class TxnQueue {
public:
    TxnQueue(SlidingTransactionInforTable* _tit, Logger* log, node_id_t node_id, int max_queue_size) : 
        tit(_tit), logger_(log), node_id_(node_id), max_queue_size_(max_queue_size) {}
    ~TxnQueue() = default;
    
    std::list<TxnQueueEntry*> pop_txn() {
        std::list<TxnQueueEntry*> batch_entries; // ret
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() {
            return !txn_queue_.empty() || finished_ || batch_finished_;
        });
        if (finished_ && txn_queue_.empty()) {
            return {}; // indicate finished
        }
        if (batch_finished_ && txn_queue_.empty()) {
            return {}; // indicate batch finished
        }
        TxnQueueEntry* entry = txn_queue_.front();
        txn_queue_.pop_front();
        current_queue_size_--;
        batch_entries.push_back(entry);
        if(entry->combine_next_txn_count > 0) {
            for(int i = 0; i < entry->combine_next_txn_count; i++) {
                assert(!txn_queue_.empty());
                TxnQueueEntry* next_entry = txn_queue_.front();
                txn_queue_.pop_front();
                current_queue_size_--;
                batch_entries.push_back(next_entry);
            }
        }
        // else{
        else if(current_queue_size_ > worker_threads * BatchExecutorPOPTxnSize) {
            // 如果队列中还有很多事务，一次可以拿多个事务交给一个线程处理
            // 如果队列中事务不多，就不要一次拿太多，避免某个线程拿走太多事务，其他线程饿死
            // pop batch txn to reduce mutex lock/unlock overhead
            for(int i = 0; i < BatchExecutorPOPTxnSize - 1; i++) {
                if(txn_queue_.empty()) break;
                TxnQueueEntry* next_entry = txn_queue_.front();
                txn_queue_.pop_front();
                current_queue_size_--;
                batch_entries.push_back(next_entry);
            }
        }
        if(current_queue_size_ >= max_queue_size_ * 0.8) {
            queue_cv_.notify_one();
        }
        lock.unlock(); // 释放锁
        // add dependency check here, 这里只建议依赖吧, 先统计输出一下, 感觉改成类似确定性的思路不好做
        assert(tit != nullptr);
        for (auto& txn_entry : batch_entries) {
            if(tit->check_dependency_txn(txn_entry)) {
                // need wait, defer a little time
                txn_entry->advise_exe_time = 
            }

        }
        return batch_entries;
    }

    void push_txn(TxnQueueEntry* entry) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() {
            return current_queue_size_ < max_queue_size_;
        });
        txn_queue_.push_back(entry);
        current_queue_size_++;
        queue_cv_.notify_one();
    }

    void push_txn_front(std::vector<TxnQueueEntry*> entries) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });
        auto first_entry = entries[0];
        first_entry->combine_next_txn_count = entries.size() - 1;
        // 从后往前放
        for(int i = entries.size() - 1; i >=0; i--) {
            txn_queue_.push_front(entries[i]);
            current_queue_size_++;
        }
        queue_cv_.notify_one();
    }

    void push_txn_back_batch(std::vector<TxnQueueEntry*> entries) {
        if(entries.empty()) return;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });
        for(int i = 0; i < entries.size(); i++) {
            txn_queue_.push_back(entries[i]);
            current_queue_size_++;
        }
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

    int get_process_batch_id() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return process_batch_id_;
    }
    
private:
    std::deque<TxnQueueEntry*> txn_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    
    std::deque<TxnQueueEntry*> delayed_txn_queue_;
    std::mutex delayed_queue_mutex_;

    SlidingTransactionInforTable* tit;
    node_id_t node_id_; // the compute node id this queue belongs to
    std::atomic<int> current_queue_size_ = 0;
    int max_queue_size_; // max queue size
    bool finished_ = false;

    int process_batch_id_ = -1; // the batch id this queue is processing
    bool batch_finished_ = false;

    Logger* logger_; 
};


// the txn pool, receive txns from clients and dispatch to txn queues of compute nodes
class TxnPool {
public:
    TxnPool(int max_pool_size, SlidingTransactionInforTable* tit) : max_pool_size_(max_pool_size), tit(tit){}
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

    void receive_txn_from_client_batch(std::vector<TxnQueueEntry*> entry) {
        int size = entry.size();
        std::unique_lock<std::mutex> lock(pool_mutex_);
        pool_cv_.wait(lock, [this, size]() {
            return current_pool_size_ + size < max_pool_size_;
        });
        for(int i = 0; i < size; i++){
            current_pool_size_++;
            txn_pool_.push_back(entry[i]);
        }
        pool_cv_.notify_one();
    }

    // !not used, because we always fetch batch txns, fetch single txn may cause high mutex overhead
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
        tit->push(entry); // 即将调度这个事务， 从池中取出，放入事务信息表
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
            tit->push(entry); // 即将调度这个事务， 从池中取出，放入事务信息表
        }
        if(current_pool_size_ <= max_pool_size_ * 0.6) {
            pool_cv_.notify_all();
        }
        return batch_txns;
    }

    void stop_pool() {
        stop_ = true;
        pool_cv_.notify_all();
    }

    int size() {
        return current_pool_size_.load();
    }
    
private:
    SmallBank* smallbank_; // pointer to the SmallBank instance
    SlidingTransactionInforTable *tit; // pointer to the SlidingTransactionInforTable instance

    const int max_pool_size_; // batch process txn size 
    std::atomic<int> current_pool_size_{0};
    std::list<TxnQueueEntry*> txn_pool_; 
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;

    bool stop_ = false;
    // SmartRouter* smart_router_; 
};
