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

// 生成模拟社交网络图的函数
void generate_friend_city_simulate_graph(std::vector<std::vector<std::pair<int, float>>> &adj_list, int num_users, int city_cnt);

// 生成模拟社交网络图的函数, 
void generate_friend_simulate_graph(std::vector<std::vector<std::pair<int, float>>> &adj_list, int num_users);

// 导出社交网络图到 CSV: src,dst,prob
bool dump_friend_graph_csv(const std::vector<std::vector<std::pair<int,float>>> &adj_list, const std::string &path, size_t limit_nodes = 0);