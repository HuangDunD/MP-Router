#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <thread>
#include <iomanip>
#include <numeric>

#include "threadpool.h"
#include "metis_partitioner.h"

Logger logger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::INFO, "server.log", 1024);

// 测试图分区延迟的程序 - 专注于分区性能测试

void test_partition_latency() {
    std::cout << "=== Graph Partitioning Latency Test ===" << std::endl;
    std::cout << "测试不同节点数量下的图分区延迟性能" << std::endl;

    NewMetis metis_partitioner;
    metis_partitioner.init_node_nums(4);

    // 测试不同的节点数量: 1万, 10万, 100万
    const int node_counts[] = {10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000}; // 1w, 10w, 100w 节点
    const int num_node_counts = sizeof(node_counts) / sizeof(node_counts[0]);
    const int num_tests_per_size = 10; // 每个规模测试10次以获得稳定结果

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> region_id_dist(1, 1000000);

    std::cout << std::setw(12) << "节点数量"
              << std::setw(15) << "平均延迟(ms)"
              << std::setw(15) << "最小延迟(ms)"
              << std::setw(15) << "最大延迟(ms)"
              << std::setw(12) << "成功率%"
              << std::setw(15) << "标准差(ms)" << std::endl;
    std::cout << std::string(85, '-') << std::endl;

    for (int n = 0; n < num_node_counts; n++) {
        int node_count = node_counts[n];
        std::vector<double> partition_times;
        partition_times.reserve(num_tests_per_size);

        int success_count = 0;

        std::cout << "正在测试 " << node_count << " 个节点的分区延迟..." << std::flush;

        for (int i = 0; i < num_tests_per_size; i++) {
            // 生成指定数量的region IDs
            std::vector<uint64_t> region_ids(node_count);
            for (int j = 0; j < node_count; j++) {
                region_ids[j] = region_id_dist(gen);
            }

            // 添加一些重复的region IDs来模拟真实场景中的连接性
            if (node_count > 100) {
                int duplicate_count = node_count / 100; // 1% 重复率
                for (int j = 0; j < duplicate_count; j++) {
                    int src_idx = j;
                    int dst_idx = node_count / 2 + j;
                    if (dst_idx < node_count) {
                        region_ids[dst_idx] = region_ids[src_idx];
                    }
                }
            }

            try {
                // 测试完整的分区过程
                auto start_time = std::chrono::high_resolution_clock::now();

                // 1. 构建内部图
                idx_t graph_result = metis_partitioner.build_internal_graph(region_ids);

                // 2. 执行图分区
                if (graph_result >= 0) {
                    // 使用临时文件进行分区
                    std::string graph_file = "/tmp/test_graph_" + std::to_string(node_count) + "_" + std::to_string(i) + ".graph";
                    std::string part_file = "/tmp/test_part_" + std::to_string(node_count) + "_" + std::to_string(i) + ".part";

                    metis_partitioner.partition_internal_graph(graph_file, 4);

                    auto end_time = std::chrono::high_resolution_clock::now();

                    // 只记录延迟，不检查分区结果
                    double duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() / 1000.0;
                    partition_times.push_back(duration_ms);
                    success_count++;

                    // 清理临时文件
                    std::remove(graph_file.c_str());
                    std::remove(part_file.c_str());
                } else {
                    std::cout << "x" << std::flush; // 构图失败标记
                }

            } catch (const std::exception& e) {
                std::cout << "e" << std::flush; // 异常标记
                std::cerr << "\n异常: " << e.what() << std::endl;
            }

            if ((i + 1) % 2 == 0) {
                std::cout << "." << std::flush;
            }
        }

        std::cout << " 完成!" << std::endl;

        if (partition_times.empty()) {
            std::cout << std::setw(12) << node_count
                      << std::setw(15) << "N/A"
                      << std::setw(15) << "N/A"
                      << std::setw(15) << "N/A"
                      << std::setw(11) << "0.0%"
                      << std::setw(15) << "N/A" << std::endl;
            continue;
        }

        // 计算统计数据
        double min_time = *std::min_element(partition_times.begin(), partition_times.end());
        double max_time = *std::max_element(partition_times.begin(), partition_times.end());
        double avg_time = std::accumulate(partition_times.begin(), partition_times.end(), 0.0) / partition_times.size();
        double success_rate = (double)success_count / num_tests_per_size * 100;

        // 计算标准差
        double variance = 0.0;
        for (double time : partition_times) {
            variance += (time - avg_time) * (time - avg_time);
        }
        variance /= partition_times.size();
        double std_dev = std::sqrt(variance);

        std::cout << std::setw(12) << node_count
                  << std::setw(15) << std::fixed << std::setprecision(2) << avg_time
                  << std::setw(15) << std::fixed << std::setprecision(2) << min_time
                  << std::setw(15) << std::fixed << std::setprecision(2) << max_time
                  << std::setw(11) << std::fixed << std::setprecision(1) << success_rate << "%"
                  << std::setw(15) << std::fixed << std::setprecision(2) << std_dev << std::endl;

        // 输出详细时间信息用于分析
        std::cout << "  详细时间: ";
        for (size_t t = 0; t < partition_times.size() && t < 5; t++) {
            std::cout << std::fixed << std::setprecision(2) << partition_times[t] << "ms ";
        }
        if (partition_times.size() > 5) {
            std::cout << "...";
        }
        std::cout << std::endl;
    }

    std::cout << "\n=== 图分区延迟测试完成 ===" << std::endl;
}

// 测试不同分区数量的影响
void test_partition_count_impact() {
    std::cout << "\n=== 不同分区数量对延迟的影响测试 ===" << std::endl;

    NewMetis metis_partitioner;
    metis_partitioner.init_node_nums(4);

    const int node_count = 100000; // 固定10万个节点
    const int partition_counts[] = {2, 4, 8, 16}; // 不同的分区数量
    const int num_partition_counts = sizeof(partition_counts) / sizeof(partition_counts[0]);
    const int num_tests = 5;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> region_id_dist(1, 10000000);

    std::cout << std::setw(12) << "分区数量"
              << std::setw(15) << "平均延迟(ms)"
              << std::setw(15) << "最小延迟(ms)"
              << std::setw(15) << "最大延迟(ms)"
              << std::setw(12) << "成功率%" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    for (int p = 0; p < num_partition_counts; p++) {
        int part_count = partition_counts[p];
        std::vector<double> partition_times;
        int success_count = 0;

        std::cout << "测试 " << part_count << " 个分区..." << std::flush;

        for (int i = 0; i < num_tests; i++) {
            std::vector<uint64_t> region_ids(node_count);
            for (int j = 0; j < node_count; j++) {
                region_ids[j] = region_id_dist(gen);
            }

            try {
                auto start_time = std::chrono::high_resolution_clock::now();

                idx_t graph_result = metis_partitioner.build_internal_graph(region_ids);
                if (graph_result >= 0) {
                    std::string graph_file = "/tmp/test_parts_" + std::to_string(part_count) + "_" + std::to_string(i) + ".graph";
                    std::string part_file = "/tmp/test_parts_" + std::to_string(part_count) + "_" + std::to_string(i) + ".part";

                    metis_partitioner.partition_internal_graph(graph_file, part_count);

                    auto end_time = std::chrono::high_resolution_clock::now();

                    double duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() / 1000.0;
                    partition_times.push_back(duration_ms);
                    success_count++;

                    std::remove(graph_file.c_str());
                    std::remove(part_file.c_str());
                }
            } catch (const std::exception& e) {
                std::cout << "e" << std::flush;
            }

            std::cout << "." << std::flush;
        }

        std::cout << " 完成!" << std::endl;

        if (!partition_times.empty()) {
            double min_time = *std::min_element(partition_times.begin(), partition_times.end());
            double max_time = *std::max_element(partition_times.begin(), partition_times.end());
            double avg_time = std::accumulate(partition_times.begin(), partition_times.end(), 0.0) / partition_times.size();
            double success_rate = (double)success_count / num_tests * 100;

            std::cout << std::setw(12) << part_count
                      << std::setw(15) << std::fixed << std::setprecision(2) << avg_time
                      << std::setw(15) << std::fixed << std::setprecision(2) << min_time
                      << std::setw(15) << std::fixed << std::setprecision(2) << max_time
                      << std::setw(11) << std::fixed << std::setprecision(1) << success_rate << "%" << std::endl;
        } else {
            std::cout << std::setw(12) << part_count
                      << std::setw(15) << "N/A"
                      << std::setw(15) << "N/A"
                      << std::setw(15) << "N/A"
                      << std::setw(11) << "0.0%" << std::endl;
        }
    }

    std::cout << "\n=== 分区数量影响测试完成 ===" << std::endl;
}

int main() {
    try {
        test_partition_latency();
        test_partition_count_impact();
    } catch (const std::exception& e) {
        std::cerr << "测试过程中发生错误: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}