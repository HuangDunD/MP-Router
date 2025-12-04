// 这是一个记录历史工作负载的类
// Author: huangdund
// Year: 2025

#pragma once
#include <queue> 
#include <mutex>
#include <list>
#include <condition_variable>

#include <atomic>
#include <vector>

class SlidingWindowLoadTracker {
public:
    SlidingWindowLoadTracker(int nodeCount, size_t windowSize = 30000)
        : nodeCount_(nodeCount),
          windowSize_(windowSize),
          pos_(0),
          window_(windowSize),
          load_(nodeCount)
    {
        // 初始化 window 为 -1（表示空槽）
        for (auto &x : window_) x.store(-1, std::memory_order_relaxed);

        for (auto &l : load_) l.store(0, std::memory_order_relaxed);
    }

    // 记录一次事务路由
    void record(int node_id)
    {
        size_t idx = pos_.fetch_add(1, std::memory_order_relaxed);
        idx %= windowSize_;

        int old_node = window_[idx].exchange(node_id, std::memory_order_relaxed);

        // 更新计数器
        if (old_node >= 0)
            load_[old_node].fetch_sub(1, std::memory_order_relaxed);

        load_[node_id].fetch_add(1, std::memory_order_relaxed);
    }

    // 获取当前所有节点窗口负载
    std::vector<int> get_loads() const
    {
        std::vector<int> ret(nodeCount_);
        for (int i = 0; i < nodeCount_; i++) {
            ret[i] = load_[i].load(std::memory_order_relaxed);
        }
        return ret;
    }

private:
    int nodeCount_;
    size_t windowSize_;

    std::atomic<size_t> pos_;               // 全局光标
    std::vector<std::atomic<int>> window_;  // 环形缓冲区
    std::vector<std::atomic<int>> load_;    // 每个节点的计数
};