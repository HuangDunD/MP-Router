#include <vector>
#include <thread>
#include <random>
#include <utility>
#include <unordered_set>
#include <algorithm>
#include <numeric>
#include <iostream>

// 生成模拟社交网络图的函数, 
void generate_friend_simulate_graph(std::vector<std::vector<std::pair<int, float>>> &adj_list, int num_users) {
	adj_list.clear();
	if (num_users <= 20) {
        std::cerr << "Number of users must be greater than 20 to generate a meaningful graph." << std::endl;
        return;
    }

	adj_list.resize(num_users);

	int minFriends = std::min(5, std::max(0, num_users - 1));
	int maxFriends = std::min(20, std::max(0, num_users - 1));

	// 随机数引擎
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<int> userDist(0, std::max(0, num_users - 1));
	std::uniform_real_distribution<float> weightDist(0.001f, 1.0f);

	// 度目标：每个用户目标朋友数（5-20，受限于用户数量）
	std::vector<int> target(num_users, 0);
	if (maxFriends >= minFriends && maxFriends > 0) {
		std::uniform_int_distribution<int> degDist(minFriends, maxFriends);
		for (int i = 0; i < num_users; ++i) target[i] = degDist(gen);
	}

	// 使用集合维护无向边，避免重复
	std::vector<std::unordered_set<int>> adj(num_users);
	auto degree = [&](int u) { return static_cast<int>(adj[u].size()); };
	auto add_edge = [&](int u, int v) {
		if (u == v) return; // 防自环
		if (adj[u].insert(v).second) {
			adj[v].insert(u);
		}
	};

    // 快速生成朋友关系
    for( int u = 0; u < num_users; ++u ) {
        while(degree(u) < target[u]) { 
            int v = userDist(gen); // 随机选择一个用户
            add_edge(u, v); // 添加无向边
        }    
    }

	// 生成每个用户对其朋友的正随机概率，并归一化到 1
	for (int i = 0; i < num_users; ++i) {
		const auto &neis = adj[i];
		if (neis.empty()) {
			adj_list[i].clear();
			continue;
		}
		adj_list[i].reserve(neis.size());
		float sum_w = 0.0f;
		for (int v : neis) {
			float w = weightDist(gen);
			sum_w += w;
			adj_list[i].emplace_back(v, w);
		}
		if (sum_w > 0.0f) {
			for (auto &p : adj_list[i]) p.second = p.second / sum_w;
		} else {
			float equal = 1.0f / static_cast<float>(adj_list[i].size());
			for (auto &p : adj_list[i]) p.second = equal;
		}
		// 可选：为了更稳定的输出，对朋友 id 排序（非必须）
		std::sort(adj_list[i].begin(), adj_list[i].end(), [](const auto &a, const auto &b){ return a.first < b.first; });
		// 调整最后一个概率以防止浮点误差导致和不为 1
		float acc = 0.0f;
		for (size_t k = 0; k + 1 < adj_list[i].size(); ++k) acc += adj_list[i][k].second;
		if (!adj_list[i].empty()) {
			adj_list[i].back().second = std::max(0.0f, 1.0f - acc);
		}
	}
}

// 导出社交网络图到 CSV: src,dst,prob
inline bool dump_friend_graph_csv(const std::vector<std::vector<std::pair<int,float>>> &adj_list, const std::string &path, size_t limit_nodes = 0){
	std::ofstream ofs(path, std::ios::out | std::ios::trunc);
	if(!ofs.is_open()) return false;
	ofs << "src,dst,prob\n";
	size_t n = adj_list.size();
	if(limit_nodes>0) n = std::min(limit_nodes, n);
	for(size_t u=0; u<n; ++u){
		for(const auto &p : adj_list[u]){
			ofs << u << ',' << p.first << ',' << p.second << '\n';
		}
	}
	return true;
}

// int main(){
//     std::vector<std::vector<std::pair<int, float>>> adj_list;
//     int num_users = 1000000; // Example number of users
//     generate_friend_simulate_graph(adj_list, num_users);

//     // Print the generated graph for verification
//     for (int i = 0; i < adj_list.size(); ++i) {
//         std::cout << "User " << i << " friends: ";
//         for (const auto &p : adj_list[i]) {
//             std::cout << "(" << p.first << ", " << p.second << ") ";
//         }
//         std::cout << std::endl;
//     }

//     return 0;
// }