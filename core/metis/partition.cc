#include "config.h"
#include "partition.h"

void Metis::process_transactions_to_graph(const std::string& input_path, const std::string& output_path) {
    std::ifstream infile(input_path);
    if (!infile.is_open()) {
        std::cerr << "Error: Could not open input file " << input_path << std::endl;
        return;
    }

    std::unordered_map<int, std::set<int>> page_graph;  // 邻接表
    std::unordered_map<int, int> page_weight;           // 页面权重
    std::unordered_map<int, int> page_id_map;           // 页面ID重映射
    int next_page_id = 0;

    std::string line;
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        int txn_id, num_pages;
        iss >> txn_id >> num_pages;

        std::vector<int> pages;
        for (int i = 0; i < num_pages; i++) {
            int page;
            iss >> page;

            if (page_id_map.find(page) == page_id_map.end()) {
                page_id_map[page] = next_page_id++;  // 重新编号
            }

            int mapped_page = page_id_map[page];
            page_weight[mapped_page]++;  // 统计每个页面的访问次数
            pages.push_back(mapped_page);
        }

        // 在访问相同事务的页面之间建立边
        for (size_t i = 0; i < pages.size(); i++) {
            for (size_t j = i + 1; j < pages.size(); j++) {
                page_graph[pages[i]].insert(pages[j]);
                page_graph[pages[j]].insert(pages[i]);
            }
        }
    }
    infile.close();

    // 计算总节点数和边数
    int num_nodes = page_graph.size();
    int num_edges = 0;
    for (const auto& entry : page_graph) {
        num_edges += entry.second.size();
    }
    num_edges /= 2;  // 无向图，每条边算两次

    std::ofstream outfile(output_path);
    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open output file " << output_path << std::endl;
        return;
    }

    outfile << num_nodes << " " << num_edges << std::endl;

    for (int i = 0; i < num_nodes; i++) {
        outfile << page_weight[i];  // 写入页面权重
        for (int neighbor : page_graph[i]) {
            outfile << " " << (neighbor + 1) << " 1";  // 邻接点 + 默认权重1
        }
        outfile << " -1" << std::endl;
    }
    
    outfile.close();
    std::cout << "Graph successfully written to " << output_path << std::endl;
}

void Metis::metis_partition_graph(const std::string& input_path, const std::string& output_path) {
    std::ifstream infile(input_path);
    if (!infile.is_open()) {
        std::cerr << "Error: Could not open input file " << input_path << std::endl;
        return;
    }

    idx_t num_nodes, num_edges;
    infile >> num_nodes >> num_edges; // 读取图的基本信息

    std::vector<idx_t> xadj(num_nodes + 1, 0);  // 邻接表索引
    std::vector<idx_t> adjncy;                  // 邻接点
    std::vector<idx_t> adjwgt;                  // 边权重
    std::vector<idx_t> vwgt(num_nodes, 1);      // 节点权重（默认1）

    std::unordered_map<uint64_t, int32_t> key_coordinator_cache;

    idx_t edge_count = 0;  // 记录边数
    for (idx_t i = 0; i < num_nodes; i++) {
        xadj[i] = adjncy.size();  // 记录当前节点的邻接表起始位置
        
        idx_t vertex_weight;
        infile >> vertex_weight;
        vwgt[i] = vertex_weight;  // 读取节点权重
        
        idx_t neighbor, weight;
        while (infile >> neighbor >> weight) {
            if (neighbor == -1) break;  // 约定 -1 结束一行
            adjncy.push_back(neighbor - 1);  // METIS 采用 0-based 索引
            adjwgt.push_back(weight);
            edge_count++;
        }
    }
    xadj[num_nodes] = adjncy.size(); // 记录最后一个节点的边界

    infile.close(); // 关闭文件

    // 设置 METIS 参数
    idx_t nVertices = num_nodes;
    idx_t nWeights = 1;
    idx_t nParts = ComputeNodeCount;
    idx_t objval;
    std::vector<idx_t> parts(nVertices, 0);

    int ret = METIS_PartGraphKway(
        &nVertices, &nWeights, xadj.data(), adjncy.data(),
        vwgt.data(), NULL, adjwgt.data(), &nParts, NULL,
        NULL, NULL, &objval, parts.data());

    if (ret != METIS_OK) {
        std::cerr << "METIS partitioning failed!" << std::endl;
        return;
    }

    std::ofstream outpartition(output_path);
    for (idx_t i = 0; i < nVertices; i++) {
        outpartition << i + 1 << " " << parts[i] << "\n";
    }
    outpartition.close();

    std::cout << "METIS partitioning completed successfully!" << std::endl;
}