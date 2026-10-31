#pragma once
#include "account_key_permutation.h"
#include <vector>
#include <thread>
#include <random>
#include <utility>
#include <unordered_set>
#include <algorithm>
#include <numeric>
#include <iostream>
#include <cassert>
#include <fstream>

// Map account IDs to cities using a seeded permutation of logical positions.
// This preserves city sizes without changing actual account IDs or load order.
class SmallBankCityAssignment {
public:
    SmallBankCityAssignment(int num_users, int city_cnt, bool shuffled, uint64_t seed)
        : permutation_(num_users, seed), per_city_(city_cnt > 0 ? std::max(1, num_users / city_cnt) : 1),
          city_count_(city_cnt), shuffled_(shuffled) {
        if (city_cnt <= 0 || city_cnt > num_users)
            throw std::invalid_argument("Invalid SmallBank city count");
    }

    int city(int account_id) const {
        const uint64_t position = shuffled_ ? permutation_(account_id) : account_id;
        return std::min(static_cast<int>((position - 1) / per_city_), city_count_ - 1);
    }
private:
    AccountKeyPermutation permutation_;
    int per_city_, city_count_;
    bool shuffled_;
};

// 生成模拟社交网络图的函数
void generate_friend_city_simulate_graph(std::vector<std::vector<std::pair<int, float>>> &adj_list,
                                        int num_users, int city_cnt,
                                        bool shuffled = false, uint64_t seed = 42);

// 生成模拟社交网络图的函数, 
void generate_friend_simulate_graph(std::vector<std::vector<std::pair<int, float>>> &adj_list, int num_users);

// 动态更改朋友关系图
void change_friends_dynamic(std::vector<std::vector<std::pair<int, float>>> &orig_graph,
                            std::vector<std::vector<std::pair<int, float>>> &new_graph,
                            double change_ratio);

// 导出社交网络图到 CSV: src,dst,prob
bool dump_friend_graph_csv(const std::vector<std::vector<std::pair<int,float>>> &adj_list, const std::string &path, size_t limit_nodes = 0);
