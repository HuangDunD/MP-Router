// Author: huangdund
// Year: 2025

#pragma once
#include <queue> 
#include <mutex>
#include <list>
#include <unordered_map>
#include <unordered_set>
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

class DAGTxnQueue {
public:
    DAGTxnQueue(SlidingTransactionInforTable* _tit, Logger* log) : 
        tit(_tit), logger_(log) {}
    ~DAGTxnQueue() = default;

    // 将一批 DAG-ready 事务推入优先队列（批次为一个不可拆分单元）
    // 优先级定义：entries 中所有事务的 after_txns 数量之和，越大优先级越高
    void push_ready_batch(std::vector<TxnQueueEntry*> entries) {
        if (entries.empty()) return;
        // 计算优先级（后继事务总数）
        int priority = 0;
        for (auto* e : entries) {
            if (!e) continue;
            // 优先使用 Group 中的后续事务数量作为优先级
            if (!e->notification_groups.empty()) {
                for(const auto& group : e->notification_groups) {
                    priority += static_cast<int>(group->after_txns.size());
                }
            } 
        }
        
        priority += entries.size(); // 再加上批次大小, 越大越应该优先处理
        // 组装批次（保留 combine 语义以便下游保持一致）
        std::list<TxnQueueEntry*> batch_list(entries.begin(), entries.end());
        if (batch_list.front() != nullptr) {
            batch_list.front()->combine_txn_count = static_cast<int>(batch_list.size());
        }

        std::unique_lock<std::mutex> lock(queue_mutex_);
        Batch b;
        b.priority = priority;
        b.seq = seq_counter_++;
        b.entries = std::move(batch_list);
        pq_.push(std::move(b));
        current_queue_size_ += static_cast<int>(entries.size());
        queue_cv_.notify_one();
    }

    // 弹出最高优先级的一个完整批次
    std::list<TxnQueueEntry*> pop_ready_batch() {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        // 若存在 inflight 批次，则将其余量一次性弹出
        if (inflight_active_) {
            auto batch_entries = std::move(inflight_.entries);
            inflight_active_ = false;
            current_queue_size_ -= static_cast<int>(batch_entries.size());
            if (!batch_entries.empty() && batch_entries.front() != nullptr) {
                int expected = static_cast<int>(batch_entries.size());
                batch_entries.front()->combine_txn_count = expected;
            }
            return batch_entries;
        }
        if (pq_.empty()) return {};
        Batch top = std::move(const_cast<Batch&>(pq_.top()));
        pq_.pop();
        auto batch_entries = std::move(top.entries);
        current_queue_size_ -= static_cast<int>(batch_entries.size());
        // 校验 combine 一致性
        assert(!batch_entries.empty());
        assert(batch_entries.front() != nullptr);
        assert(batch_entries.front()->combine_txn_count == static_cast<int>(batch_entries.size()));
        return batch_entries;
    }

    // 弹出最高优先级批次的一部分（chunk) 
    // out_is_last 为 true 表示该批次已全部取完
    std::list<TxnQueueEntry*> pop_ready_chunk(bool* out_is_last = nullptr) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        // 若当前没有在处理的 top 批次，则取出队首批次作为 inflight，不重新入队
        if (!inflight_active_) {
            if (pq_.empty()) {
                if (out_is_last) *out_is_last = false;
                return {};
            }
            inflight_ = std::move(const_cast<Batch&>(pq_.top()));
            pq_.pop();
            inflight_active_ = true;
            chunk_size_ = std::max(1, static_cast<int>(inflight_.entries.size()) / worker_threads);
        }

        // 从 inflight_ 批次前端切分 chunk
        std::list<TxnQueueEntry*> chunk;
        int taken = 0;
        while (taken < chunk_size_ && !inflight_.entries.empty()) {
            TxnQueueEntry* e = inflight_.entries.front();
            inflight_.entries.pop_front();
            chunk.push_back(e);
            taken++;
        }
        // 设置 chunk 的 combine
        assert(!chunk.empty());
        chunk.front()->combine_txn_count = static_cast<int>(chunk.size());
        // 更新剩余 inflight 的 combine（若还有剩余）
        if (!inflight_.entries.empty() && inflight_.entries.front() != nullptr) {
            inflight_.entries.front()->combine_txn_count = static_cast<int>(inflight_.entries.size());
        }
        // 若 inflight 耗尽，标记完成
        bool finished_inflight = inflight_.entries.empty();
        if (finished_inflight) {
            inflight_active_ = false;
        }
        if (out_is_last) *out_is_last = finished_inflight;
        current_queue_size_ -= static_cast<int>(chunk.size());
        return chunk;
    }

    // 查看当前最高优先级批次大小（仅用于决策，不弹出）
    int top_batch_size() {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (inflight_active_) {
            return static_cast<int>(inflight_.entries.size());
        }
        if (pq_.empty()) return 0;
        const Batch& top = pq_.top();
        return static_cast<int>(top.entries.size());
    }

    int size() { return current_queue_size_; }
    bool empty() { return current_queue_size_ == 0; }
    bool is_inflight_active() { return inflight_active_; }

private:
    struct Batch {
        int priority = 0;         // 越大越优先
        uint64_t seq = 0;         // 先进先出用于同优先级打破平局
        std::list<TxnQueueEntry*> entries; // 批次条目
    };
    struct BatchCmp {
        bool operator()(const Batch& a, const Batch& b) const {
            if (a.priority == b.priority) return a.seq > b.seq; // seq 小的先出
            return a.priority < b.priority; // priority 大的先出
        }
    };

    std::priority_queue<Batch, std::vector<Batch>, BatchCmp> pq_;
    uint64_t seq_counter_ = 0;
    Batch inflight_;
    bool inflight_active_ = false;
    int chunk_size_ = 0;

    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    SlidingTransactionInforTable* tit;
    std::atomic<int> current_queue_size_ = 0;
    Logger* logger_;
};

// 这里存储着等待的事务集合
class PendingTxnSet{
public: 
    PendingTxnSet(SlidingTransactionInforTable* _tit, Logger* log) : 
        tit(_tit), logger_(log), pending_txn_cnt_per_node_(ComputeNodeCount) {
            for (auto &cnt : pending_txn_cnt_per_node_) cnt = 0;
        }
    ~PendingTxnSet() = default;

    // 禁止拷贝/赋值，避免 vector<atomic<int>> 的复制导致编译错误
    PendingTxnSet(const PendingTxnSet&) = delete;
    PendingTxnSet& operator=(const PendingTxnSet&) = delete;

    bool add_pendingtxn_on_node(TxnQueueEntry* tx_entry, int node_id) {
        std::unique_lock<std::mutex> lock(pending_mutex_);
        // double check if ref = 0 already
        if(tx_entry->ref.load(std::memory_order_acquire) == 0) {
            return false;
        }
        pending_target_node_[tx_entry] = node_id;
        pending_txn_cnt_per_node_[node_id]++;
        return true;
    }

    int get_pending_txn_count() {
        std::unique_lock<std::mutex> lock(pending_mutex_);
        int total = 0;
        for (const auto& count : pending_txn_cnt_per_node_) {
            total += count;
        }
        return total;
    }

    std::vector<int> get_pending_txns_ids() {
        std::unique_lock<std::mutex> lock(pending_mutex_);
        std::vector<int> ids;
        ids.reserve(pending_target_node_.size());
        for (const auto& pair : pending_target_node_) {
            ids.push_back(pair.first->tx_id);
        }
        return ids;
    }

    int get_pending_txn_cnt_on_node(int node_id){
        std::unique_lock<std::mutex> lock(pending_mutex_);
        return pending_txn_cnt_per_node_[node_id];
    }

    void wait_for_pending_txn_empty(){
        std::unique_lock<std::mutex> lock(pending_mutex_);
        pending_cv_.wait(lock, [this]() { 
            return pending_target_node_.size() == 0;
        });
        for(int cnt : pending_txn_cnt_per_node_) {
            assert(cnt == 0);
        }
    }

    std::vector<std::vector<TxnQueueEntry*>> self_check_ref_is_zero(){
        std::vector<std::vector<TxnQueueEntry*>> ready_to_push;
        ready_to_push.resize(ComputeNodeCount);
        std::lock_guard<std::mutex> lk(pending_mutex_);
        for (auto it = pending_target_node_.begin(); it != pending_target_node_.end(); ) {
            auto txn = it->first;
            int node_id = it->second;
            if(txn->ref.load(std::memory_order_acquire) == 0) {
                // ready to push
                ready_to_push[node_id].emplace_back(txn);
                it = pending_target_node_.erase(it); // 安全地擦除当前迭代器
                pending_txn_cnt_per_node_[node_id]--;
            } else {
                ++it;
            }
        }
        pending_cv_.notify_all();
        return ready_to_push;
    }

    std::vector<std::vector<TxnQueueEntry*>> pop_dag_ready_txns(std::vector<TxnQueueEntry*>& entries) {
        std::vector<std::vector<TxnQueueEntry*>> to_schedule;
        to_schedule.resize(ComputeNodeCount);
        if (entries.empty()) return to_schedule;
        bool notify = false;
        {
            std::lock_guard<std::mutex> lk(pending_mutex_);
            if( pending_target_node_.empty() ) {
                // 快速判断
                // 没有pending的事务，直接返回
                return to_schedule;
            }
            for(auto entry: entries) {
                assert(entry != nullptr);
                auto it = pending_target_node_.find(entry);
                if(it == pending_target_node_.end()){
                    // not in dag_not_ready_txns_, 可能已经被调度或者在当前还不着急调度(在ownership_ok队列中)
                    continue;
                }
                node_id_t node_id = it->second;
                pending_txn_cnt_per_node_[node_id]--;
                pending_target_node_.erase(it);
                if(pending_target_node_.size() < 100) notify = true;
                to_schedule[node_id].push_back(entry);
            }
        }
        if (notify) {
            pending_cv_.notify_all();
        }
        #if LOG_DEPENDENCY
        logger_->info("[PendingTxnSet] Pop DAG-ready pending txns cnt: " + 
            [&]() {
                std::string s;
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                    int cnt = static_cast<int>(to_schedule[node_id].size());
                    s += "Node " + std::to_string(node_id) + ": " + std::to_string(cnt) + "; ";
                }
                return s;
            }()
            + " now pending txn cnt: " + [&]() {
                std::string s;
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                    int cnt = pending_txn_cnt_per_node_[node_id];
                    s += "Node " + std::to_string(node_id) + ": " + std::to_string(cnt) + "; ";
                }
                return s;
            }()
        );
        #endif
        return to_schedule;
    }

    void clean_pendingtxn_on_node() {
        // 进行自检, 将ref = 0 的事务清理
    }

private:
    std::unordered_map<TxnQueueEntry*, node_id_t> pending_target_node_;
    std::vector<int> pending_txn_cnt_per_node_;
    std::condition_variable pending_cv_;
    std::mutex pending_mutex_;

    SlidingTransactionInforTable* tit;
    Logger* logger_;
};

// for every compute node db connections, we have a txn queue to store incoming txns
class TxnQueue {
public:
    TxnQueue(SlidingTransactionInforTable* _tit, SharedTxnQueue* shared_txn_queue, PendingTxnSet* pending_txn_queue, Logger* log, node_id_t node_id, int max_queue_size) : 
        tit(_tit), shared_txn_queue_(shared_txn_queue), pending_txn_queue_(pending_txn_queue), logger_(log), node_id_(node_id), max_queue_size_(max_queue_size) {
            dag_txn_queue_ = new DAGTxnQueue(_tit, logger_);
        }
    ~TxnQueue() = default;
    
    std::list<TxnQueueEntry*> pop_txn(int* ret_call_id = nullptr) {
        int call_id = rand();
        if(ret_call_id != nullptr) *ret_call_id = call_id;
        std::list<TxnQueueEntry*> batch_entries; // ret
        std::unique_lock<std::mutex> lock(queue_mutex_);
        
        // 计时
        struct timespec start_time, end_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        queue_cv_.wait(lock, [this]() {
            return !txn_queue_.empty() || !dag_txn_queue_->empty() || finished_ || batch_finished_;
        });
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        double wait_time = (end_time.tv_sec - start_time.tv_sec) * 1000 + (end_time.tv_nsec - start_time.tv_nsec) / 1e6;
        if(wait_time > 100) {
            logger_->info("[TxnQueue Pop] Node id: " + std::to_string(node_id_) 
                + " call_id: " + std::to_string(call_id) + " Waited for " + std::to_string(wait_time) + " ms");
        }
        
        if(txn_queue_.empty() && dag_txn_queue_->empty() && (finished_ || batch_finished_)) {
            // indicate finished or batch finished, pop one from shared_queue, if shared_queue is empty, will returen {}
            batch_entries = std::move(shared_txn_queue_->pop_txn()); 
        #if LOG_QUEUE_STATUS
            logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) + 
                            " Popping from shared txn queue of compute node " + std::to_string(node_id_) +
                            ", popped size: " + std::to_string(batch_entries.size()));
        #endif
            return batch_entries;
        }

        if(!dag_txn_queue_->empty()){
            // 优先取 DAG-ready，若过大则分块，并与 regular 批次做混合以降低冲突
            bool inflight_active = dag_txn_queue_->is_inflight_active();
            // if(inflight_active || dag_txn_queue_->top_batch_size() > worker_threads * 3) {
            if(inflight_active || dag_txn_queue_->top_batch_size() > 10000) {
                // !get the inflight chunk
                bool last_chunk = false;
                auto dag_chunk = std::move(dag_txn_queue_->pop_ready_chunk(&last_chunk));
                assert(!dag_chunk.empty());
                assert(dag_chunk.front() != nullptr); 
                if (last_chunk) {
                    // 完全消费一个 DAG 批次（向量）
                    schedule_txn_vec_cnt -= 1;
                }
                int dag_sz = static_cast<int>(dag_chunk.size());
                schedule_txn_cnt -= dag_sz;

                // 从 txn_queue_ 中尝试混合多个 regular 批次，使用 splice 扁平化条目
                std::list<TxnQueueEntry*> reg_part;
                int popped_vec_cnt = 0;
                for (int i = 0; i < 3 && !txn_queue_.empty(); ++i) {
                    // 取队首一个批次并将条目移动到 reg_part
                    auto& front_batch = txn_queue_.front();
                    reg_part.splice(reg_part.end(), front_batch);
                    txn_queue_.pop_front();
                    popped_vec_cnt += 1;
                }

                int reg_sz = static_cast<int>(reg_part.size());
                regular_txn_cnt -= reg_sz;
                regular_txn_vec_cnt -= popped_vec_cnt;

                // 统计与日志
                current_queue_size_ -= static_cast<int>(dag_chunk.size() + reg_part.size());
            #if LOG_QUEUE_STATUS
                logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) +
                            " Popping mixed DAG+regular batch size " + std::to_string(dag_sz + reg_sz) +
                            " (dag=" + std::to_string(dag_sz) + ", regular=" + std::to_string(reg_sz) +
                            ") from txn queue of compute node " + std::to_string(node_id_) +
                            ", current queue size: " + std::to_string(current_queue_size_) +
                            ", schedule_txn_cnt: " + std::to_string(schedule_txn_cnt) +
                            ", schedule_txn_vec_cnt: " + std::to_string(schedule_txn_vec_cnt) +
                            ", regular_txn_cnt: " + std::to_string(regular_txn_cnt) +
                            ", regular_txn_vec_cnt: " + std::to_string(regular_txn_vec_cnt));
            #endif

                // ! Fix: Notify waiting threads (producers or other consumers)
                if(current_queue_size_ <= max_queue_size_ * 0.8 || !txn_queue_.empty() || !dag_txn_queue_->empty()) {
                    queue_cv_.notify_all();
                }

                lock.unlock(); // !释放锁
                // 最后在锁外部将 dag_chunk 和 reg_part 随机打散合并返回
                if (reg_part.empty()) {
                    // 只有 DAG chunk，直接返回
                    return std::move(dag_chunk);
                }
                if (dag_chunk.empty()) {
                    // 理论上不会发生；若发生直接返回 regular 部分
                    return std::move(reg_part);
                }

                // 随机交替合并两个 list，避免总是首尾拼接
                std::list<TxnQueueEntry*> mixed;
                int dag_rem = static_cast<int>(dag_chunk.size());
                int reg_rem = static_cast<int>(reg_part.size());
                while (!dag_chunk.empty() || !reg_part.empty()) {
                    if (dag_chunk.empty()) {
                        TxnQueueEntry* e = reg_part.front();
                        reg_part.pop_front();
                        mixed.push_back(e);
                        reg_rem--;
                        continue;
                    }
                    if (reg_part.empty()) {
                        TxnQueueEntry* e = dag_chunk.front();
                        dag_chunk.pop_front();
                        mixed.push_back(e);
                        dag_rem--;
                        continue;
                    }
                    int total_rem = dag_rem + reg_rem;
                    int pick = rand() % total_rem;
                    if (pick < dag_rem) {
                        TxnQueueEntry* e = dag_chunk.front();
                        dag_chunk.pop_front();
                        mixed.push_back(e);
                        dag_rem--;
                    } else {
                        TxnQueueEntry* e = reg_part.front();
                        reg_part.pop_front();
                        mixed.push_back(e);
                        reg_rem--;
                    }
                }
                // 设置合并批次的 combine 计数
                if (!mixed.empty() && mixed.front() != nullptr) {
                    mixed.front()->combine_txn_count = static_cast<int>(mixed.size());
                }
                return std::move(mixed);
            }
            else {
                // !get the full dag batch
                batch_entries = std::move(dag_txn_queue_->pop_ready_batch());
                assert(!batch_entries.empty());
                assert(batch_entries.front() != nullptr);
                current_queue_size_ -= static_cast<int>(batch_entries.size());
                schedule_txn_cnt -= static_cast<int>(batch_entries.size());
                schedule_txn_vec_cnt -= 1;
            #if LOG_QUEUE_STATUS
                logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) + 
                                " Popping full DAG txn batch of size " + std::to_string(batch_entries.size()) + 
                                " from txn queue of compute node " + std::to_string(node_id_) +
                                ", current queue size: " + std::to_string(current_queue_size_) + 
                                ", schedule_txn_cnt: " + std::to_string(schedule_txn_cnt) +
                                ", schedule_txn_vec_cnt: " + std::to_string(schedule_txn_vec_cnt));
            #endif
                return std::move(batch_entries);
            }
        } else {
            // !get the regular ready txn
            assert(!txn_queue_.empty());
            batch_entries = std::move(txn_queue_.front());
            assert(!batch_entries.empty());
            assert(batch_entries.front() != nullptr);
            txn_queue_.pop_front();
            current_queue_size_ -= static_cast<int>(batch_entries.size());
            regular_txn_cnt -= static_cast<int>(batch_entries.size());
            regular_txn_vec_cnt -= 1;
        #if LOG_QUEUE_STATUS
            logger_->info("[TxnQueue Pop] call_id: " + std::to_string(call_id) + " Popping regular txn batch of size " + 
                            std::to_string(batch_entries.size()) + " from txn queue of compute node " + std::to_string(node_id_) +
                            ", current queue size: " + std::to_string(current_queue_size_) + 
                            ", schedule_txn_cnt: " + std::to_string(schedule_txn_cnt) +
                            ", schedule_txn_vec_cnt: " + std::to_string(schedule_txn_vec_cnt) +
                            ", regular_txn_cnt: " + std::to_string(regular_txn_cnt) +
                            ", regular_txn_vec_cnt: " + std::to_string(regular_txn_vec_cnt));
        #endif
        }

        // 通知可能阻塞的生产者线程或消费者线程
        if(current_queue_size_ <= max_queue_size_ * 0.8 || !txn_queue_.empty() || !dag_txn_queue_->empty()) {
            queue_cv_.notify_all();
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
        return std::move(batch_entries);
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
    void push_txn_dag_ready(std::vector<TxnQueueEntry*> entries, int type = 0) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        assert(!entries.empty());
        int size = static_cast<int>(entries.size());
        current_queue_size_ += size;
        schedule_txn_cnt += size;
        schedule_txn_vec_cnt += 1;
        dag_txn_queue_->push_ready_batch(std::move(entries));
    #if LOG_QUEUE_STATUS
        logger_->info("[TxnQueue Push Front] Pushed combined txn batch of size " + 
                        std::to_string(size) + " to front of txn queue of compute node " + std::to_string(node_id_) + 
                        ", push type: " + std::to_string(type) +
                        ", current queue size: " + std::to_string(current_queue_size_) + 
                        ", schedule_txn_cnt: " + std::to_string(schedule_txn_cnt) +
                        ", schedule_txn_vec_cnt: " + std::to_string(schedule_txn_vec_cnt) +
                        ", regular_txn_cnt: " + std::to_string(regular_txn_cnt) +
                        ", regular_txn_vec_cnt: " + std::to_string(regular_txn_vec_cnt));
    #endif
        queue_cv_.notify_one();
    }

    // // push 绑定到指定位置的事务到队列指定位置
    // void push_txn_front_pos(std::vector<TxnQueueEntry*> entries, int pos) {
    //     std::unique_lock<std::mutex> lock(queue_mutex_);
    //     if (entries.empty()) return;

    //     queue_cv_.wait(lock, [this, &entries]() {
    //         return current_queue_size_ + entries.size() < max_queue_size_;
    //     });

    //     auto first_entry = entries[0];
    //     assert(first_entry != nullptr);
    //     first_entry->combine_txn_count = static_cast<int>(entries.size());

    //     // 在指定位置插入一个批次（用移动避免拷贝）
    //     if (pos < 0) pos = 0;
    //     pos = pos > static_cast<int>(txn_queue_.size()) ? static_cast<int>(txn_queue_.size()) : pos;
    //     std::list<TxnQueueEntry*> batch(entries.begin(), entries.end());
    //     auto it = txn_queue_.begin() + pos;

    //     txn_queue_.insert(it, std::move(batch));
    //     current_queue_size_ += static_cast<int>(entries.size());
    //     queue_cv_.notify_one();
    // }

    void push_txn_back_batch(std::list<TxnQueueEntry*>& entries) {
        if(entries.empty()) return;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });
        
        assert(entries.size() <= BatchExecutorPOPTxnSize);
        current_queue_size_ += static_cast<int>(entries.size());
        regular_txn_cnt += static_cast<int>(entries.size());
        regular_txn_vec_cnt += 1;
        txn_queue_.emplace_back(std::move(entries)); // 构造包含单元素的批
        queue_cv_.notify_one();
    }

    void push_txn_back_batch(std::vector<TxnQueueEntry*> entries) {
        if(entries.empty()) return;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this, &entries]() {
            return current_queue_size_ + entries.size() < max_queue_size_;
        });
        
        int i = 0;
        // 1. Try to fill the last batch first
        if (!txn_queue_.empty()) {
            auto& last_batch = txn_queue_.back();
            while (last_batch.size() < BatchExecutorPOPTxnSize && i < entries.size()) {
                last_batch.push_back(entries[i]);
                i++;
                current_queue_size_++;
                regular_txn_cnt++;
            }
        }

        // 2. Create new batches for remaining entries
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

    // // push 绑定到距离首部pos位置之后的事务到队列随机位置
    // void push_txn_back_after_pos_rand(std::vector<TxnQueueEntry*> entries, double pos_ratio = 0.0){
    //     std::unique_lock<std::mutex> lock(queue_mutex_);
    //     if (entries.empty()) return;

    //     queue_cv_.wait(lock, [this, &entries]() {
    //         return current_queue_size_ + entries.size() < max_queue_size_;
    //     });

    //     auto first_entry = entries[0];
    //     assert(first_entry != nullptr);
    //     first_entry->combine_txn_count = static_cast<int>(entries.size());

    //     // 在指定位置插入一个批次（用移动避免拷贝）
    //     if (pos_ratio < 0.0) pos_ratio = 0.0;
    //     int pos = static_cast<int>(pos_ratio * txn_queue_.size());
    //     pos = pos >= static_cast<int>(txn_queue_.size()) ? static_cast<int>(txn_queue_.size()) - 1 : pos;
    //     // 生成随机位置
    //     int rand_pos = pos + (rand() % (static_cast<int>(txn_queue_.size()) - pos + 1));
    //     if(rand_pos < 0) rand_pos = 0;
    //     if(rand_pos > static_cast<int>(txn_queue_.size())) rand_pos = static_cast<int>(txn_queue_.size());
    //     std::list<TxnQueueEntry*> batch(entries.begin(), entries.end());
    //     auto it = txn_queue_.begin() + rand_pos;

    //     txn_queue_.insert(it, std::move(batch));
    //     current_queue_size_ += static_cast<int>(entries.size());
    //     queue_cv_.notify_one();
    // }

    // // push 绑定到距离尾端pos位置的事务到队列指定位置
    // void push_txn_back_pos(std::vector<TxnQueueEntry*> entries, int pos){ 
    //     std::unique_lock<std::mutex> lock(queue_mutex_);
    //     if (entries.empty()) return;

    //     queue_cv_.wait(lock, [this, &entries]() {
    //         return current_queue_size_ + entries.size() < max_queue_size_;
    //     });

    //     auto first_entry = entries[0];
    //     assert(first_entry != nullptr);
    //     first_entry->combine_txn_count = static_cast<int>(entries.size());

    //     // 在指定位置插入一个批次（用移动避免拷贝）
    //     if (pos < 0) pos = 0;
    //     pos = pos > static_cast<int>(txn_queue_.size()) ? static_cast<int>(txn_queue_.size()) : pos;
    //     std::list<TxnQueueEntry*> batch(entries.begin(), entries.end());
    //     auto it = txn_queue_.end() - pos;

    //     txn_queue_.insert(it, std::move(batch));
    //     current_queue_size_ += static_cast<int>(entries.size());
    //     queue_cv_.notify_one();
    // }

    int get_pending_txn_cnt_on_node(int node_id) {
        return pending_txn_queue_->get_pending_txn_cnt_on_node(node_id);
    }

    // shared txn queue operations
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
    DAGTxnQueue* dag_txn_queue_;
    PendingTxnSet* pending_txn_queue_;
    
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
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            double now_ms = ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
            for (int i = 0; i < batch_size; i++) {
                TxnQueueEntry* entry = txn_pool_.front();
                txn_pool_.pop_front();
                entry->fetch_time = now_ms;
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
