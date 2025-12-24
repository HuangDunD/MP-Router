// Author: huangdund
// Year: 2025

#pragma once
#include <queue> 
#include <mutex>
#include <list>
#include <condition_variable>
#include <thread>
#include <chrono>
#include "common.h"
#include "config.h"
#include "tit.h"
#include "txn_entry.h"
#include "Logger.h"

// the shared txn queue, some txns may be equal for every compute nodes, and this queue is for workload balance 
class SharedTxnQueue {
public:
    SharedTxnQueue(SlidingTransactionInforTable* _tit, Logger* log, int max_queue_size) : 
        tit(_tit), logger_(log), max_queue_size_(max_queue_size) {}
    ~SharedTxnQueue() = default;

    void push_txn(TxnQueueEntry* entry) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() {
            return current_queue_size_ < max_queue_size_;
        });
        txn_queue_.push_back(entry); 
        ++current_queue_size_;
        queue_cv_.notify_one();
    }

    std::list<TxnQueueEntry*> pop_txn() {
        std::list<TxnQueueEntry*> batch_entries; // ret
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if(current_queue_size_ == 0) return {}; 
        auto entry = txn_queue_.front();
        txn_queue_.pop_front();
        current_queue_size_--;
        batch_entries.push_back(entry);
        return batch_entries;
    }

    int size(){
        return current_queue_size_; 
    }

    bool empty(){
        return current_queue_size_ == 0;
    }

private:
    std::deque<TxnQueueEntry*> txn_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    SlidingTransactionInforTable* tit;
    std::atomic<int> current_queue_size_ = 0;
    int max_queue_size_; // max queue size
    Logger* logger_; // 
};

// for every compute node db connections, we have a txn queue to store incoming txns
class TxnQueue {
public:
    TxnQueue(SlidingTransactionInforTable* _tit, SharedTxnQueue* shared_txn_queue, Logger* log, node_id_t node_id, int max_queue_size) : 
        tit(_tit), shared_txn_queue_(shared_txn_queue), logger_(log), node_id_(node_id), max_queue_size_(max_queue_size) {}
    ~TxnQueue() = default;
    
    std::list<TxnQueueEntry*> pop_txn() {
        int call_id = rand();
        std::list<TxnQueueEntry*> batch_entries; // ret
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() {
            return !txn_queue_.empty() || finished_ || batch_finished_;
        });
        if(txn_queue_.empty() && (finished_ || batch_finished_)) {
            // indicate finished or batch finished, pop one from shared_queue, if shared_queue is empty, will returen {}
            batch_entries = std::move(shared_txn_queue_->pop_txn()); 
            // if(WarmupEnd)
            //     logger_->info("Compute Node " + std::to_string(node_id_) + 
            //                 " popped " + std::to_string(batch_entries.size()) 
            //                 + " txns from shared_txn_queue. ");
            return batch_entries;
        }
        assert(!txn_queue_.empty());
        auto it = txn_queue_.begin();
        assert(it != txn_queue_.end());
        if(it->front() != nullptr && it->front()->combine_txn_count > 0) {
            // 绑定到一个线程的事务
            batch_entries = std::move(*it);
            txn_queue_.pop_front();
            current_queue_size_ -= static_cast<int>(batch_entries.size());
            // 校验批大小与combine计数一致：size == combine_txn_count
            assert(!batch_entries.empty());
            assert(batch_entries.front() != nullptr);
            assert(static_cast<int>(batch_entries.size()) == batch_entries.front()->combine_txn_count);
            schedule_txn_cnt -= static_cast<int>(batch_entries.size());
            schedule_txn_vec_cnt -= 1;
            logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) + " Popping combined txn batch of size " + 
                            std::to_string(batch_entries.size()) + " from txn queue of compute node " + std::to_string(node_id_) +
                            ", current queue size: " + std::to_string(current_queue_size_) + 
                            ", schedule_txn_cnt: " + std::to_string(schedule_txn_cnt) +
                            ", schedule_txn_vec_cnt: " + std::to_string(schedule_txn_vec_cnt) +
                            ", regular_txn_cnt: " + std::to_string(regular_txn_cnt) +
                            ", regular_txn_vec_cnt: " + std::to_string(regular_txn_vec_cnt));
        }
        // else if (current_queue_size_ < worker_threads * BatchExecutorPOPTxnSize) {
        else if (current_queue_size_ < 0) {
            // 队列中事务不多，一次只拿一个事务
            assert(!it->empty());
            auto* e = it->front();
            assert(e != nullptr);
            batch_entries.push_back(e);
            it->pop_front();
            if(it->empty()) {
                txn_queue_.pop_front();
            }
            current_queue_size_--;
        }
        else {
            logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) + " Popping regular txn batch of size " + 
                            std::to_string(batch_entries.size()) + " from txn queue of compute node " + std::to_string(node_id_) + 
                            ", current queue size: " + std::to_string(current_queue_size_));
            // 队列中事务较多，一次拿一个批次
            batch_entries = std::move(*it);
            txn_queue_.pop_front();
            current_queue_size_ -= static_cast<int>(batch_entries.size());
            assert(!batch_entries.empty());
            assert(batch_entries.front() != nullptr);
            regular_txn_cnt -= static_cast<int>(batch_entries.size());
            regular_txn_vec_cnt -= 1;
            logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) + " Popping regular txn batch of size " + 
                            std::to_string(batch_entries.size()) + " from txn queue of compute node " + std::to_string(node_id_) +
                            ", current queue size: " + std::to_string(current_queue_size_) + 
                            ", schedule_txn_cnt: " + std::to_string(schedule_txn_cnt) +
                            ", schedule_txn_vec_cnt: " + std::to_string(schedule_txn_vec_cnt) +
                            ", regular_txn_cnt: " + std::to_string(regular_txn_cnt) +
                            ", regular_txn_vec_cnt: " + std::to_string(regular_txn_vec_cnt));
        }

        // 通知可能阻塞的生产者线程
        if(current_queue_size_ <= max_queue_size_ * 0.8) {
            queue_cv_.notify_one();
        }
        lock.unlock(); // !释放锁
        // add dependency check here, 这里只建议依赖吧, 先统计输出一下, 感觉改成类似确定性的思路不好做
        assert(tit != nullptr);        
        // for (auto& txn_entry : batch_entries) {
        //     tit->check_dependency_txn(txn_entry, call_id);
        // }
        // for (auto& txn_entry : batch_entries) {
        //     tit->mark_enter_executor(txn_entry);
        // }
        return batch_entries;
    }

    void push_txn(TxnQueueEntry* entry) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() {
            return current_queue_size_ < max_queue_size_;
        });
        if (txn_queue_.empty()) {
            txn_queue_.emplace_back(1, entry);
        } 
        else{
            auto it = txn_queue_.end() - 1;
            if((*it).size() < BatchExecutorPOPTxnSize) {
                it->push_back(entry);
            }
            else txn_queue_.emplace_back(1, entry); // 构造包含单元素的批
        }
        ++current_queue_size_;
        queue_cv_.notify_one();
    }

    // push 绑定到单线程的事务到队列前端
    void push_txn_front(std::vector<TxnQueueEntry*> entries) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });
        assert(!entries.empty());
        auto first_entry = entries[0];
        assert(first_entry != nullptr);
        first_entry->combine_txn_count = static_cast<int>(entries.size());
        txn_queue_.emplace_front(entries.begin(), entries.end());
        current_queue_size_ += static_cast<int>(entries.size());
        schedule_txn_cnt += static_cast<int>(entries.size());
        schedule_txn_vec_cnt += 1;
        queue_cv_.notify_one();
    }

    // push 绑定到指定位置的事务到队列指定位置
    void push_txn_front_pos(std::vector<TxnQueueEntry*> entries, int pos) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (entries.empty()) return;

        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });

        auto first_entry = entries[0];
        assert(first_entry != nullptr);
        first_entry->combine_txn_count = static_cast<int>(entries.size());

        // 在指定位置插入一个批次（用移动避免拷贝）
        if (pos < 0) pos = 0;
        pos = pos > static_cast<int>(txn_queue_.size()) ? static_cast<int>(txn_queue_.size()) : pos;
        std::list<TxnQueueEntry*> batch(entries.begin(), entries.end());
        auto it = txn_queue_.begin() + pos;

        txn_queue_.insert(it, std::move(batch));
        current_queue_size_ += static_cast<int>(entries.size());
        queue_cv_.notify_one();
    }

    void push_txn_back_batch(std::vector<TxnQueueEntry*> entries) {
        if(entries.empty()) return;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });
        int i = 0;
        while(i < entries.size()) {
            std::list<TxnQueueEntry*> batch;
            for(int j = 0; j < BatchExecutorPOPTxnSize && i < entries.size(); j++, i++) {
                // construct a batch
                batch.push_back(entries[i]);
            }
            current_queue_size_ += static_cast<int>(batch.size());
            regular_txn_cnt += static_cast<int>(batch.size());
            regular_txn_vec_cnt += 1;
            txn_queue_.emplace_back(std::move(batch)); // 构造包含单元素的批
        }
        queue_cv_.notify_one();
    }

    // push 绑定到距离首部pos位置之后的事务到队列随机位置
    void push_txn_back_after_pos_rand(std::vector<TxnQueueEntry*> entries, double pos_ratio = 0.0){
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (entries.empty()) return;

        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });

        auto first_entry = entries[0];
        assert(first_entry != nullptr);
        first_entry->combine_txn_count = static_cast<int>(entries.size());

        // 在指定位置插入一个批次（用移动避免拷贝）
        if (pos_ratio < 0.0) pos_ratio = 0.0;
        int pos = static_cast<int>(pos_ratio * txn_queue_.size());
        pos = pos >= static_cast<int>(txn_queue_.size()) ? static_cast<int>(txn_queue_.size()) - 1 : pos;
        // 生成随机位置
        int rand_pos = pos + (rand() % (static_cast<int>(txn_queue_.size()) - pos + 1));
        if(rand_pos < 0) rand_pos = 0;
        if(rand_pos > static_cast<int>(txn_queue_.size())) rand_pos = static_cast<int>(txn_queue_.size());
        std::list<TxnQueueEntry*> batch(entries.begin(), entries.end());
        auto it = txn_queue_.begin() + rand_pos;

        txn_queue_.insert(it, std::move(batch));
        current_queue_size_ += static_cast<int>(entries.size());
        queue_cv_.notify_one();
    }

    // push 绑定到距离尾端pos位置的事务到队列指定位置
    void push_txn_back_pos(std::vector<TxnQueueEntry*> entries, int pos){ 
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (entries.empty()) return;

        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });

        auto first_entry = entries[0];
        assert(first_entry != nullptr);
        first_entry->combine_txn_count = static_cast<int>(entries.size());

        // 在指定位置插入一个批次（用移动避免拷贝）
        if (pos < 0) pos = 0;
        pos = pos > static_cast<int>(txn_queue_.size()) ? static_cast<int>(txn_queue_.size()) : pos;
        std::list<TxnQueueEntry*> batch(entries.begin(), entries.end());
        auto it = txn_queue_.end() - pos;

        txn_queue_.insert(it, std::move(batch));
        current_queue_size_ += static_cast<int>(entries.size());
        queue_cv_.notify_one();
    }

    void push_txn_into_shared_queue(TxnQueueEntry* entry) {
        shared_txn_queue_->push_txn(entry);
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
        // assert(current_queue_size_ == 0); // only set new batch id when queue is empty
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

    bool is_shared_queue_empty() {
        return shared_txn_queue_->empty();
    }

private:
    std::deque<std::list<TxnQueueEntry*>> txn_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    SharedTxnQueue* shared_txn_queue_;
    
    SlidingTransactionInforTable* tit;
    node_id_t node_id_; // the compute node id this queue belongs to
    std::atomic<int> current_queue_size_ = 0;
    int max_queue_size_; // max queue size
    bool finished_ = false;

    int process_batch_id_ = -1; // the batch id this queue is processing
    bool batch_finished_ = false;

    Logger* logger_; 

    // for batch debug
    int regular_txn_cnt = 0;
    int regular_txn_vec_cnt = 0;
    int schedule_txn_cnt = 0;
    int schedule_txn_vec_cnt = 0;
};

class MiniTxnPool{
public:
    MiniTxnPool(int max_pool_size, SlidingTransactionInforTable* tit) : max_pool_size_(max_pool_size), tit(tit){}
    ~MiniTxnPool() = default;

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
            return current_pool_size_ + size < max_pool_size_ || stop_;
        });
        if(stop_) return;
        for(int i = 0; i < size; i++){
            txn_pool_.push_back(entry[i]);
        }
        current_pool_size_ += size;
        pool_cv_.notify_one();
    }

    std::unique_ptr<std::vector<TxnQueueEntry*>> fetch_batch_txns_from_pool(int batch_size) {
        std::unique_ptr<std::vector<TxnQueueEntry*>> batch_txns = 
            std::make_unique<std::vector<TxnQueueEntry*>>();
        {
            std::unique_lock<std::mutex> lock(pool_mutex_);
            pool_cv_.wait(lock, [this, batch_size]() {
                return txn_pool_.size() >= batch_size || stop_; // 如果停止标志被设置，也要退出等待
            }); 
            if (stop_ && txn_pool_.size() < batch_size) {
                return {}; // 如果停止且池中事务不足，返回空向量
            }
            for (int i = 0; i < batch_size; i++) {
                TxnQueueEntry* entry = txn_pool_.front();
                txn_pool_.pop_front();
                batch_txns->push_back(entry);
            }
        }
        current_pool_size_ -= batch_size;
        // 在lock之外更新tit
        for (auto& entry : *batch_txns) {
            tit->push(entry); // 即将调度这个事务， 从池中取出，放入事务信息表
        }
        if(current_pool_size_ <= max_pool_size_ * 0.8) {
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
    SlidingTransactionInforTable *tit; // pointer to the SlidingTransactionInforTable instance

    const int max_pool_size_; // batch process txn size 
    std::atomic<int> current_pool_size_{0};
    std::list<TxnQueueEntry*> txn_pool_; 
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;

    bool stop_ = false;
};

// the txn pool, receive txns from clients and dispatch to txn queues of compute nodes
class TxnPool {
public:
    TxnPool(int num_sub_pool, int max_pool_size, SlidingTransactionInforTable* tit):num_sub_pool_(num_sub_pool) {
        pools = new MiniTxnPool*[num_sub_pool];
        for(int i = 0; i < num_sub_pool; i++){
            pools[i] = new MiniTxnPool(max_pool_size, tit);
        }
    }
    ~TxnPool() = default;

    void receive_txn_from_client(TxnQueueEntry* entry, int gen_thread_id) {
        pools[gen_thread_id % num_sub_pool_]->receive_txn_from_client(entry);
    }

    void receive_txn_from_client_batch(std::vector<TxnQueueEntry*> entry, int gen_thread_id) {
        pools[gen_thread_id % num_sub_pool_]->receive_txn_from_client_batch(std::move(entry));
    }

    std::unique_ptr<std::vector<TxnQueueEntry*>> fetch_batch_txns_from_pool(int batch_size, int thread_id) {
        return pools[thread_id % num_sub_pool_]->fetch_batch_txns_from_pool(batch_size);
    }

    void stop_pool() {
        stop_ = true;
        for(int i = 0; i < num_sub_pool_; i++){
            pools[i]->stop_pool();
        }
    }

    int size() {
        int total_size = 0;
        for(int i = 0; i < num_sub_pool_; i++){
            total_size += pools[i]->size();
        }
        return total_size;
    }
    
private:
    int num_sub_pool_;
    MiniTxnPool** pools; // 由于单个事务池的开销可能会很大, 这里做一个分区
    bool stop_ = false;
};
