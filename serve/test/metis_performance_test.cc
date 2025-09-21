#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <thread>
#include <iomanip>
#include <numeric>
#include <fstream>
#include <algorithm>
#include <map>
#include <cmath>
#include <set>
#include <functional>

// 直接包含METIS库
#include <metis.h>

// METIS性能基准测试类
class MetisPerformanceBenchmark {
private:
    std::random_device rd;
    std::mt19937 gen;
    std::ofstream csv_output;

    struct TestResult {
        int nodes;
        int edges;
        int partitions;
        double metis_time_ms;
        double preprocessing_time_ms;
        double graph_generation_time_ms;  // 新增：图生成时间
        double total_time_ms;
        bool success;
        double graph_density;
        int edge_cut;
        double load_imbalance;
    };

    struct PerformanceStats {
        double min_time;
        double max_time;
        double avg_time;
        double std_dev;
        double median_time;
        int success_count;
        int total_count;
    };

public:
    MetisPerformanceBenchmark() : gen(rd()) {
        csv_output.open("metis_performance_results.csv");
        csv_output << "test_type,nodes,edges,partitions,metis_time_ms,preprocessing_time_ms,total_time_ms,graph_density,edge_cut,load_imbalance,success\n";
    }

    ~MetisPerformanceBenchmark() {
        if (csv_output.is_open()) {
            csv_output.close();
        }
    }

    // 生成随机图的邻接表
    void generate_random_graph(int node_count, double edge_probability,
                              std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
        xadj.clear();
        adjncy.clear();
        xadj.resize(node_count + 1);

        std::uniform_real_distribution<double> prob_dist(0.0, 0.01);
        std::vector<std::vector<int>> adj_list(node_count);

        // 生成随机边
        for (int i = 0; i < node_count; i++) {
            auto friendly_neighbors = prob_dist(gen) * node_count;
            for (int j = 0; j < friendly_neighbors; j++) {
                int neighbor = gen() % node_count;
                if (neighbor != i) {
                    adj_list[i].push_back(neighbor);
                    adj_list[neighbor].push_back(i);
                }
            }
        }

        // 转换为CSR格式
        int edge_idx = 0;
        xadj[0] = 0;
        for (int i = 0; i < node_count; i++) {
            for (int neighbor : adj_list[i]) {
                adjncy.push_back(neighbor);
                edge_idx++;
            }
            xadj[i + 1] = edge_idx;
        }
    }

    // 生成小世界图
    void generate_small_world_graph(int node_count, int k, double p,
                                   std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
        xadj.clear();
        adjncy.clear();
        xadj.resize(node_count + 1);

        std::uniform_real_distribution<double> prob_dist(0.0, 1.0);
        std::uniform_int_distribution<int> node_dist(0, node_count - 1);
        std::vector<std::set<int>> adj_set(node_count);

        // 首先创建环形结构
        for (int i = 0; i < node_count; i++) {
            for (int j = 1; j <= k/2; j++) {
                int neighbor1 = (i + j) % node_count;
                int neighbor2 = (i - j + node_count) % node_count;
                adj_set[i].insert(neighbor1);
                adj_set[neighbor1].insert(i);
                adj_set[i].insert(neighbor2);
                adj_set[neighbor2].insert(i);
            }
        }

        // 随机重连
        for (int i = 0; i < node_count; i++) {
            auto it = adj_set[i].begin();
            while (it != adj_set[i].end()) {
                if (prob_dist(gen) < p) {
                    int old_neighbor = *it;
                    adj_set[i].erase(it++);
                    adj_set[old_neighbor].erase(i);

                    int new_neighbor = node_dist(gen);
                    while (new_neighbor == i || adj_set[i].count(new_neighbor)) {
                        new_neighbor = node_dist(gen);
                    }
                    adj_set[i].insert(new_neighbor);
                    adj_set[new_neighbor].insert(i);
                } else {
                    ++it;
                }
            }
        }

        // 转换为CSR格式
        int edge_idx = 0;
        xadj[0] = 0;
        for (int i = 0; i < node_count; i++) {
            for (int neighbor : adj_set[i]) {
                adjncy.push_back(neighbor);
                edge_idx++;
            }
            xadj[i + 1] = edge_idx;
        }
    }

    // 生成网格图
    void generate_grid_graph(int width, int height,
                            std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
        int node_count = width * height;
        xadj.clear();
        adjncy.clear();
        xadj.resize(node_count + 1);

        std::vector<std::vector<int>> adj_list(node_count);

        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                int node = i * width + j;

                // 连接到右邻居
                if (j < width - 1) {
                    adj_list[node].push_back(node + 1);
                    adj_list[node + 1].push_back(node);
                }

                // 连接到下邻居
                if (i < height - 1) {
                    adj_list[node].push_back(node + width);
                    adj_list[node + width].push_back(node);
                }
            }
        }

        // 转换为CSR格式
        int edge_idx = 0;
        xadj[0] = 0;
        for (int i = 0; i < node_count; i++) {
            for (int neighbor : adj_list[i]) {
                adjncy.push_back(neighbor);
                edge_idx++;
            }
            xadj[i + 1] = edge_idx;
        }
    }

    // 计算统计数据
    PerformanceStats calculate_stats(const std::vector<double>& times, int total_tests) {
        PerformanceStats stats;
        stats.total_count = total_tests;
        stats.success_count = times.size();

        if (times.empty()) {
            stats.min_time = stats.max_time = stats.avg_time = stats.std_dev = stats.median_time = 0.0;
            return stats;
        }

        std::vector<double> sorted_times = times;
        std::sort(sorted_times.begin(), sorted_times.end());

        stats.min_time = sorted_times.front();
        stats.max_time = sorted_times.back();
        stats.avg_time = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
        stats.median_time = sorted_times[sorted_times.size() / 2];

        // 计算标准差
        double variance = 0.0;
        for (double time : times) {
            variance += std::pow(time - stats.avg_time, 2);
        }
        stats.std_dev = std::sqrt(variance / times.size());

        return stats;
    }

    // 执行单个METIS分区测试
    TestResult run_single_metis_test(const std::vector<idx_t>& xadj,
                                    const std::vector<idx_t>& adjncy,
                                    int partition_count) {
        TestResult result;
        result.nodes = xadj.size() - 1;
        result.edges = adjncy.size() / 2;
        result.partitions = partition_count;
        result.success = false;
        result.edge_cut = 0;
        result.load_imbalance = 0.0;
        result.graph_density = static_cast<double>(result.edges) / (result.nodes * (result.nodes - 1) / 2);

        try {
            auto preprocess_start = std::chrono::high_resolution_clock::now();

            // 准备METIS输入数据
            idx_t nvtx = result.nodes;
            idx_t ncon = 1;  // 单约束
            std::vector<idx_t> xadj_copy = xadj;
            std::vector<idx_t> adjncy_copy = adjncy;
            idx_t nparts = partition_count;
            idx_t objval;
            std::vector<idx_t> part(nvtx);

            // METIS选项
            std::vector<idx_t> options(METIS_NOPTIONS);
            METIS_SetDefaultOptions(options.data());
            options[METIS_OPTION_PTYPE] = METIS_PTYPE_KWAY;
            options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT;
            options[METIS_OPTION_CTYPE] = METIS_CTYPE_SHEM;
            options[METIS_OPTION_IPTYPE] = METIS_IPTYPE_GROW;
            options[METIS_OPTION_RTYPE] = METIS_RTYPE_GREEDY;
            options[METIS_OPTION_NUMBERING] = 0; // C-style numbering

            auto preprocess_end = std::chrono::high_resolution_clock::now();
            result.preprocessing_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                preprocess_end - preprocess_start).count() / 1000.0;

            // 调用METIS进行图分区
            auto metis_start = std::chrono::high_resolution_clock::now();

            int ret = METIS_PartGraphKway(&nvtx, &ncon, xadj_copy.data(), adjncy_copy.data(),
                                        nullptr, nullptr, nullptr, &nparts, nullptr, nullptr,
                                        options.data(), &objval, part.data());

            auto metis_end = std::chrono::high_resolution_clock::now();
            result.metis_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                metis_end - metis_start).count() / 1000.0;

            if (ret == METIS_OK) {
                result.success = true;
                result.edge_cut = objval;
                result.total_time_ms = result.preprocessing_time_ms + result.metis_time_ms;

                // 计算负载不平衡度
                std::vector<int> partition_sizes(partition_count, 0);
                for (int i = 0; i < nvtx; i++) {
                    partition_sizes[part[i]]++;
                }

                int max_size = *std::max_element(partition_sizes.begin(), partition_sizes.end());
                int min_size = *std::min_element(partition_sizes.begin(), partition_sizes.end());
                double ideal_size = static_cast<double>(nvtx) / partition_count;
                result.load_imbalance = (max_size - ideal_size) / ideal_size;
            }

        } catch (const std::exception& e) {
            std::cerr << "METIS测试异常: " << e.what() << std::endl;
        }

        return result;
    }

    // 节点规模性能测试
    void test_scalability() {
        std::cout << "\n=== METIS 节点规模性能测试 ===" << std::endl;

        const std::vector<int> node_counts = {1000, 10000, 20000, 50000,100000,300000,500000,1000000};
        const int tests_per_size = 2;
        const int partition_count = 64;
        const double edge_probability = 0.01;

        std::cout << std::setw(8) << "节点数"
                  << std::setw(8) << "边数"
                  << std::setw(12) << "图生成"
                  << std::setw(12) << "预处理"
                  << std::setw(12) << "METIS时间"
                  << std::setw(12) << "总时间"
                  << std::setw(10) << "成功率"
                  << std::setw(8) << "边割" << std::endl;
        std::cout << std::string(96, '-') << std::endl;

        for (int node_count : node_counts) {
            std::vector<double> graph_gen_times, preprocess_times, metis_times, total_times;
            std::vector<int> edge_cuts;
            int total_edges = 0;

            for (int i = 0; i < tests_per_size; i++) {
                // 测量图生成时间
                auto graph_gen_start = std::chrono::high_resolution_clock::now();
                std::vector<idx_t> xadj, adjncy;
                generate_random_graph(node_count, edge_probability, xadj, adjncy);
                auto graph_gen_end = std::chrono::high_resolution_clock::now();

                double graph_gen_time = std::chrono::duration_cast<std::chrono::microseconds>(
                    graph_gen_end - graph_gen_start).count() / 1000.0;

                TestResult result = run_single_metis_test(xadj, adjncy, partition_count);
                result.graph_generation_time_ms = graph_gen_time;

                if (result.success) {
                    graph_gen_times.push_back(result.graph_generation_time_ms);
                    preprocess_times.push_back(result.preprocessing_time_ms);
                    metis_times.push_back(result.metis_time_ms);
                    total_times.push_back(result.graph_generation_time_ms + result.total_time_ms);
                    edge_cuts.push_back(result.edge_cut);
                    total_edges = result.edges;

                    csv_output << "scalability," << result.nodes << "," << result.edges
                              << "," << result.partitions << "," << result.metis_time_ms
                              << "," << result.preprocessing_time_ms << "," << result.graph_generation_time_ms
                              << "," << result.graph_density << "," << result.edge_cut
                              << "," << result.load_imbalance << "," << result.success << "\n";
                }
            }

            auto graph_gen_stats = calculate_stats(graph_gen_times, tests_per_size);
            auto preprocess_stats = calculate_stats(preprocess_times, tests_per_size);
            auto metis_stats = calculate_stats(metis_times, tests_per_size);
            auto total_stats = calculate_stats(total_times, tests_per_size);
            int avg_edge_cut = edge_cuts.empty() ? 0 : std::accumulate(edge_cuts.begin(), edge_cuts.end(), 0) / edge_cuts.size();

            // 自动选择时间单位显示
            auto format_time = [](double time_ms) -> std::string {
                if (time_ms < 1000) {
                    return std::to_string(static_cast<int>(time_ms + 0.5)) + "ms";
                } else {
                    return std::to_string(static_cast<int>(time_ms / 1000 * 10 + 0.5) / 10.0) + "s";
                }
            };

            std::cout << std::setw(8) << node_count
                      << std::setw(8) << total_edges
                      << std::setw(12) << format_time(graph_gen_stats.avg_time)
                      << std::setw(12) << format_time(preprocess_stats.avg_time)
                      << std::setw(12) << format_time(metis_stats.avg_time)
                      << std::setw(12) << format_time(total_stats.avg_time)
                      << std::setw(9) << std::fixed << std::setprecision(1)
                      << (100.0 * total_stats.success_count / tests_per_size) << "%"
                      << std::setw(8) << avg_edge_cut << std::endl;
        }
    }

    // 分区数量影响测试
    void test_partition_count_impact() {
        std::cout << "\n=== METIS 分区数量影响测试 ===" << std::endl;

        const std::vector<int> partition_counts = {2, 4, 8, 16, 32, 64};
        const int node_count = 10000;
        const int tests_per_count = 5;
        const double edge_probability = 0.01;

        std::cout << std::setw(8) << "分区数"
                  << std::setw(15) << "预处理(ms)"
                  << std::setw(15) << "METIS时间"
                  << std::setw(15) << "总时间"
                  << std::setw(10) << "成功率"
                  << std::setw(10) << "边割"
                  << std::setw(12) << "不平衡度" << std::endl;
        std::cout << std::string(92, '-') << std::endl;

        // 自动选择时间单位显示的函数
        auto format_time = [](double time_ms) -> std::string {
            if (time_ms < 1000) {
                return std::to_string(static_cast<int>(time_ms + 0.5)) + "ms";
            } else {
                return std::to_string(static_cast<int>(time_ms / 1000 * 10 + 0.5) / 10.0) + "s";
            }
        };

        for (int part_count : partition_counts) {
            std::vector<double> preprocess_times, metis_times, total_times;
            std::vector<int> edge_cuts;
            std::vector<double> imbalances;

            for (int i = 0; i < tests_per_count; i++) {
                std::vector<idx_t> xadj, adjncy;
                generate_random_graph(node_count, edge_probability, xadj, adjncy);
                TestResult result = run_single_metis_test(xadj, adjncy, part_count);

                if (result.success) {
                    preprocess_times.push_back(result.preprocessing_time_ms);
                    metis_times.push_back(result.metis_time_ms);
                    total_times.push_back(result.total_time_ms);
                    edge_cuts.push_back(result.edge_cut);
                    imbalances.push_back(result.load_imbalance);

                    csv_output << "partition_count," << result.nodes << "," << result.edges
                              << "," << result.partitions << "," << result.metis_time_ms
                              << "," << result.preprocessing_time_ms << "," << result.total_time_ms
                              << "," << result.graph_density << "," << result.edge_cut
                              << "," << result.load_imbalance << "," << result.success << "\n";
                }
            }

            auto total_stats = calculate_stats(total_times, tests_per_count);
            int avg_edge_cut = edge_cuts.empty() ? 0 : std::accumulate(edge_cuts.begin(), edge_cuts.end(), 0) / edge_cuts.size();
            double avg_imbalance = imbalances.empty() ? 0 : std::accumulate(imbalances.begin(), imbalances.end(), 0.0) / imbalances.size();

            std::cout << std::setw(8) << part_count
                      << std::setw(15) << format_time(calculate_stats(preprocess_times, tests_per_count).avg_time)
                      << std::setw(15) << format_time(calculate_stats(metis_times, tests_per_count).avg_time)
                      << std::setw(15) << format_time(total_stats.avg_time)
                      << std::setw(9) << std::fixed << std::setprecision(1)
                      << (100.0 * total_stats.success_count / tests_per_count) << "%"
                      << std::setw(10) << avg_edge_cut
                      << std::setw(12) << std::fixed << std::setprecision(3) << avg_imbalance << std::endl;
        }
    }

    // 图类型性能对比测试
    void test_graph_type_performance() {
        std::cout << "\n=== METIS 不同图类型性能测试 ===" << std::endl;

        const int node_count = 5000;
        const int partition_count = 4;
        const int tests_per_type = 3;

        struct GraphType {
            std::string name;
            std::function<void(std::vector<idx_t>&, std::vector<idx_t>&)> generator;
        };

        std::vector<GraphType> graph_types = {
            {"随机稀疏图", [&](std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
                generate_random_graph(node_count, 0.005, xadj, adjncy); }},
            {"随机密集图", [&](std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
                generate_random_graph(node_count, 0.02, xadj, adjncy); }},
            {"小世界图", [&](std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
                generate_small_world_graph(node_count, 6, 0.1, xadj, adjncy); }},
            {"网格图", [&](std::vector<idx_t>& xadj, std::vector<idx_t>& adjncy) {
                int side = static_cast<int>(std::sqrt(node_count));
                generate_grid_graph(side, side, xadj, adjncy); }}
        };

        std::cout << std::setw(12) << "图类型"
                  << std::setw(10) << "边数"
                  << std::setw(12) << "METIS时间"
                  << std::setw(12) << "总时间"
                  << std::setw(10) << "成功率"
                  << std::setw(10) << "边割" << std::endl;
        std::cout << std::string(68, '-') << std::endl;

        for (const auto& graph_type : graph_types) {
            std::vector<double> metis_times, total_times;
            std::vector<int> edge_cuts;
            int total_edges = 0;

            for (int i = 0; i < tests_per_type; i++) {
                std::vector<idx_t> xadj, adjncy;
                graph_type.generator(xadj, adjncy);
                TestResult result = run_single_metis_test(xadj, adjncy, partition_count);

                if (result.success) {
                    metis_times.push_back(result.metis_time_ms);
                    total_times.push_back(result.total_time_ms);
                    edge_cuts.push_back(result.edge_cut);
                    total_edges = result.edges;

                    csv_output << "graph_type_" << graph_type.name << "," << result.nodes << "," << result.edges
                              << "," << result.partitions << "," << result.metis_time_ms
                              << "," << result.preprocessing_time_ms << "," << result.total_time_ms
                              << "," << result.graph_density << "," << result.edge_cut
                              << "," << result.load_imbalance << "," << result.success << "\n";
                }
            }

            auto metis_stats = calculate_stats(metis_times, tests_per_type);
            auto total_stats = calculate_stats(total_times, tests_per_type);
            int avg_edge_cut = edge_cuts.empty() ? 0 : std::accumulate(edge_cuts.begin(), edge_cuts.end(), 0) / edge_cuts.size();

            std::cout << std::setw(12) << graph_type.name
                      << std::setw(10) << total_edges
                      << std::setw(12) << std::fixed << std::setprecision(2) << metis_stats.avg_time
                      << std::setw(12) << std::fixed << std::setprecision(2) << total_stats.avg_time
                      << std::setw(9) << std::fixed << std::setprecision(1)
                      << (100.0 * total_stats.success_count / tests_per_type) << "%"
                      << std::setw(10) << avg_edge_cut << std::endl;
        }
    }

    // METIS算法参数对比测试
    void test_metis_algorithm_comparison() {
        std::cout << "\n=== METIS 算法参数对比测试 ===" << std::endl;

        const int node_count = 8000;
        const int partition_count = 8;
        const int tests_per_config = 3;

        struct AlgoConfig {
            std::string name;
            int ptype;
            int objtype;
            int ctype;
            int iptype;
            int rtype;
        };

        std::vector<AlgoConfig> configs = {
            {"递归二分", METIS_PTYPE_RB, METIS_OBJTYPE_CUT, METIS_CTYPE_SHEM, METIS_IPTYPE_GROW, METIS_RTYPE_GREEDY},
            {"k-way分区", METIS_PTYPE_KWAY, METIS_OBJTYPE_CUT, METIS_CTYPE_SHEM, METIS_IPTYPE_GROW, METIS_RTYPE_GREEDY},
            {"k-way体积", METIS_PTYPE_KWAY, METIS_OBJTYPE_VOL, METIS_CTYPE_SHEM, METIS_IPTYPE_GROW, METIS_RTYPE_GREEDY},
            {"随机匹配", METIS_PTYPE_KWAY, METIS_OBJTYPE_CUT, METIS_CTYPE_RM, METIS_IPTYPE_RANDOM, METIS_RTYPE_GREEDY}
        };

        std::cout << std::setw(12) << "算法配置"
                  << std::setw(12) << "METIS时间"
                  << std::setw(10) << "成功率"
                  << std::setw(10) << "边割"
                  << std::setw(12) << "不平衡度" << std::endl;
        std::cout << std::string(58, '-') << std::endl;

        for (const auto& config : configs) {
            std::vector<double> metis_times;
            std::vector<int> edge_cuts;
            std::vector<double> imbalances;

            for (int i = 0; i < tests_per_config; i++) {
                std::vector<idx_t> xadj, adjncy;
                generate_random_graph(node_count, 0.01, xadj, adjncy);

                TestResult result;
                result.nodes = xadj.size() - 1;
                result.edges = adjncy.size() / 2;
                result.partitions = partition_count;
                result.success = false;

                try {
                    idx_t nvtx = result.nodes;
                    idx_t ncon = 1;
                    idx_t nparts = partition_count;
                    idx_t objval;
                    std::vector<idx_t> part(nvtx);

                    std::vector<idx_t> options(METIS_NOPTIONS);
                    METIS_SetDefaultOptions(options.data());
                    options[METIS_OPTION_PTYPE] = config.ptype;
                    options[METIS_OPTION_OBJTYPE] = config.objtype;
                    options[METIS_OPTION_CTYPE] = config.ctype;
                    options[METIS_OPTION_IPTYPE] = config.iptype;
                    options[METIS_OPTION_RTYPE] = config.rtype;
                    options[METIS_OPTION_NUMBERING] = 0;

                    auto metis_start = std::chrono::high_resolution_clock::now();
                    int ret = METIS_PartGraphKway(&nvtx, &ncon,
                                                const_cast<idx_t*>(xadj.data()),
                                                const_cast<idx_t*>(adjncy.data()),
                                                nullptr, nullptr, nullptr, &nparts,
                                                nullptr, nullptr, options.data(),
                                                &objval, part.data());
                    auto metis_end = std::chrono::high_resolution_clock::now();

                    result.metis_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                        metis_end - metis_start).count() / 1000.0;

                    if (ret == METIS_OK) {
                        result.success = true;
                        result.edge_cut = objval;

                        std::vector<int> partition_sizes(partition_count, 0);
                        for (int j = 0; j < nvtx; j++) {
                            partition_sizes[part[j]]++;
                        }

                        int max_size = *std::max_element(partition_sizes.begin(), partition_sizes.end());
                        double ideal_size = static_cast<double>(nvtx) / partition_count;
                        result.load_imbalance = (max_size - ideal_size) / ideal_size;

                        metis_times.push_back(result.metis_time_ms);
                        edge_cuts.push_back(result.edge_cut);
                        imbalances.push_back(result.load_imbalance);
                    }
                } catch (...) {
                    // 忽略异常，继续下一次测试
                }
            }

            auto metis_stats = calculate_stats(metis_times, tests_per_config);
            int avg_edge_cut = edge_cuts.empty() ? 0 : std::accumulate(edge_cuts.begin(), edge_cuts.end(), 0) / edge_cuts.size();
            double avg_imbalance = imbalances.empty() ? 0 : std::accumulate(imbalances.begin(), imbalances.end(), 0.0) / imbalances.size();

            std::cout << std::setw(12) << config.name
                      << std::setw(12) << std::fixed << std::setprecision(2) << metis_stats.avg_time
                      << std::setw(9) << std::fixed << std::setprecision(1)
                      << (100.0 * metis_stats.success_count / tests_per_config) << "%"
                      << std::setw(10) << avg_edge_cut
                      << std::setw(12) << std::fixed << std::setprecision(3) << avg_imbalance << std::endl;
        }
    }

    // 运行所有测试
    void run_all_tests() {
        std::cout << "METIS 图分区库原生API性能基准测试" << std::endl;
        std::cout << "=====================================" << std::endl;

        auto start_time = std::chrono::high_resolution_clock::now();

        test_scalability();
        test_partition_count_impact();
        test_graph_type_performance();
        test_metis_algorithm_comparison();

        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

        std::cout << "\n=====================================" << std::endl;
        std::cout << "所有测试完成，总耗时: " << total_duration.count() << " 秒" << std::endl;
        std::cout << "详细结果已保存到: metis_performance_results.csv" << std::endl;
    }
};

int main() {
    try {
        MetisPerformanceBenchmark benchmark;
        benchmark.run_all_tests();
    } catch (const std::exception& e) {
        std::cerr << "测试过程中发生错误: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
