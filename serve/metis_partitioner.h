#ifndef NEWMETIS_PARTITIONER_H // Renamed Include Guard
#define NEWMETIS_PARTITIONER_H // Renamed Include Guard

#include <vector>
#include <string>
#include <unordered_map>
#include <set>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <set>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <stdexcept>

// Include necessary headers from your project and METIS
#include "config.h"
#include <metis.h> // Still needs the original METIS library header

class NewMetis {
    // Renamed class
public:
    /**
     * @brief Processes a group of unique, pre-mapped IDs to build an internal graph representation.
     * (Documentation remains the same)
     * @param unique_mapped_ids_in_group Vector containing unique, mapped partition IDs appearing together.
     */
    void build_internal_graph(const std::vector<int> &unique_mapped_ids_in_group);

    /**
     * @brief Writes the internally stored graph to a file in METIS format.
     * (Documentation remains the same)
     * @param output_path Path to write the generated METIS graph file.
     */
    void write_metis_graph(const std::string &output_path) const;

    /**
     * @brief Exports the internally stored graph data into CSV files suitable for Python.
     * (Documentation remains the same)
     * @param output_path_edges Path for the edges CSV file.
     * @param output_path_nodes Path for the nodes CSV file.
     */
    void export_graph_for_python(const std::string &output_path_edges, const std::string &output_path_nodes) const;

    /**
     * @brief Partitions a graph stored in a METIS format file using METIS_PartGraphKway.
     * (Documentation remains the same)
     * @param input_metis_file Path to the METIS graph file.
     * @param output_partition_file Path to write the partition results file.
     * @param ComputeNodeCount The desired number of partitions (nParts for METIS).
     */
    void partition_graph_from_file(const std::string &input_metis_file, const std::string &output_partition_file,
                                   int ComputeNodeCount);

private:
    // Internal graph representation
    std::unordered_map<int, std::set<int> > partition_graph_;
    std::unordered_map<int, int> partition_weight_;
    int num_nodes_ = 0;
    int num_edges_ = 0;
};

// --- Implementation to build the internal graph ---
void NewMetis::build_internal_graph(const std::vector<int> &unique_mapped_ids_in_group) {
    // Renamed Scope
    // --- Clear previous internal state ---
    partition_graph_.clear();
    partition_weight_.clear();
    num_nodes_ = 0;
    num_edges_ = 0;

    // --- Determine Node Count ---
    if (!unique_mapped_ids_in_group.empty()) {
        try {
            for (int id: unique_mapped_ids_in_group) {
                if (id < 0) { throw std::invalid_argument("Input vector contains negative ID(s)."); }
            }
            num_nodes_ = *std::max_element(unique_mapped_ids_in_group.begin(), unique_mapped_ids_in_group.end()) + 1;
        } catch (const std::exception &e) {
            std::cerr << "Error determining node count: " << e.what() << std::endl;
            num_nodes_ = 0;
            return;
        }
    } else {
        std::cout << "Input vector is empty. Internal graph remains empty." << std::endl;
        return;
    }

    // --- Initialize Graph Structures ---
    for (int i = 0; i < num_nodes_; ++i) {
        partition_graph_[i] = std::set<int>();
        partition_weight_[i] = 1;
    }

    // --- Build Edges ---
    if (unique_mapped_ids_in_group.size() >= 2) {
        for (size_t i = 0; i < unique_mapped_ids_in_group.size(); ++i) {
            for (size_t j = i + 1; j < unique_mapped_ids_in_group.size(); ++j) {
                int u = unique_mapped_ids_in_group[i];
                int v = unique_mapped_ids_in_group[j];
                if (u >= 0 && u < num_nodes_ && v >= 0 && v < num_nodes_) {
                    partition_graph_[u].insert(v);
                    partition_graph_[v].insert(u);
                }
            }
        }
    }

    // --- Calculate Edge Count ---
    num_edges_ = 0;
    for (const auto &pair: partition_graph_) { num_edges_ += pair.second.size(); }
    num_edges_ /= 2;

    std::cout << "Internal graph built: " << num_nodes_ << " nodes, " << num_edges_ << " edges." << std::endl;
}

// --- Implementation to write the internal graph to METIS format ---
void NewMetis::write_metis_graph(const std::string &output_path) const {
    // Renamed Scope
    if (num_nodes_ == 0) {
        std::cout << "Internal graph is empty. Writing an empty METIS file." << std::endl;
        std::ofstream outfile(output_path);
        if (!outfile.is_open()) {
            std::cerr << "Error: Could not open output file " << output_path << std::endl;
            return;
        }
        outfile << "0 0" << std::endl;
        outfile.close();
        return;
    }

    std::ofstream outfile(output_path);
    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open output file " << output_path << std::endl;
        return;
    }

    // Header
    outfile << num_nodes_ << " " << num_edges_ << " 011" << std::endl;

    // Node data
    for (int i = 0; i < num_nodes_; ++i) {
        int weight = partition_weight_.count(i) ? partition_weight_.at(i) : 1;
        outfile << weight;
        if (partition_graph_.count(i)) {
            for (int neighbor_mapped_id: partition_graph_.at(i)) {
                outfile << " " << (neighbor_mapped_id + 1) << " 1";
            }
        }
        outfile << std::endl;
    }
    outfile.close();
    std::cout << "Internal graph successfully written to METIS file: " << output_path << std::endl;
}

// --- Implementation to export graph for Python ---
void NewMetis::export_graph_for_python(const std::string &output_path_edges,
                                       const std::string &output_path_nodes) const {
    // Renamed Scope
    // --- Write Nodes File ---
    std::ofstream nodes_outfile(output_path_nodes);
    if (!nodes_outfile.is_open()) {
        std::cerr << "Error: Could not open nodes output file " << output_path_nodes << std::endl;
        return;
    }
    nodes_outfile << "NodeID,Weight\n";
    for (int i = 0; i < num_nodes_; ++i) {
        int weight = partition_weight_.count(i) ? partition_weight_.at(i) : 1;
        nodes_outfile << i << "," << weight << "\n";
    }
    nodes_outfile.close();
    std::cout << "Node data successfully exported for Python: " << output_path_nodes << std::endl;

    // --- Write Edges File ---
    std::ofstream edges_outfile(output_path_edges);
    if (!edges_outfile.is_open()) {
        std::cerr << "Error: Could not open edges output file " << output_path_edges << std::endl;
        return;
    }
    edges_outfile << "Source,Target\n";
    for (int u = 0; u < num_nodes_; ++u) {
        if (partition_graph_.count(u)) {
            for (int v: partition_graph_.at(u)) {
                if (u < v) { edges_outfile << u << "," << v << "\n"; }
            }
        }
    }
    edges_outfile.close();
    std::cout << "Edge data successfully exported for Python: " << output_path_edges << std::endl;
}

// --- Implementation of partition_graph_from_file ---
void NewMetis::partition_graph_from_file(const std::string &input_metis_file, const std::string &output_partition_file,
                                         int ComputeNodeCount) {
    // Renamed Scope
    // This function reads the METIS file directly.
    // The reading and METIS call logic remain the same.

    std::ifstream infile(input_metis_file);
    if (!infile.is_open()) {
        /* Error handling */
        return;
    }

    idx_t num_nodes, num_edges, fmt = 0, ncon = 1;
    // Read header
    std::string header_line;
    std::getline(infile, header_line);
    std::istringstream header_iss(header_line);
    if (!(header_iss >> num_nodes >> num_edges)) {
        /* Error handling */
        infile.close();
        return;
    }
    if (header_iss >> fmt) { if (fmt / 10 % 10 == 1) { if (!(header_iss >> ncon)) ncon = 1; } } else {
        fmt = 11;
        ncon = 1; /* Warning */
    }
    bool has_vertex_weights = (fmt / 10 % 10 == 1);
    bool has_edge_weights = (fmt % 10 == 1);
    std::vector<idx_t> xadj(num_nodes + 1, 0);
    std::vector<idx_t> adjncy;
    std::vector<idx_t> adjwgt;
    std::vector<idx_t> vwgt;
    adjncy.reserve(num_edges * 2);
    if (has_edge_weights) adjwgt.reserve(num_edges * 2);
    if (has_vertex_weights) vwgt.resize(num_nodes * ncon);
    idx_t current_edge_idx = 0;
    for (idx_t i = 0; i < num_nodes; i++) {
        xadj[i] = current_edge_idx;
        std::string line;
        if (!std::getline(infile, line)) {
            /* Error handling */
            infile.close();
            return;
        }
        std::istringstream iss(line);
        if (has_vertex_weights) {
            for (idx_t w = 0; w < ncon; ++w) {
                if (!(iss >> vwgt[i * ncon + w])) {
                    /* Error handling */
                    infile.close();
                    return;
                }
            }
        }
        idx_t neighbor;
        while (iss >> neighbor) {
            adjncy.push_back(neighbor - 1);
            idx_t weight = 1;
            if (has_edge_weights) {
                if (!(iss >> weight)) {
                    /* Error handling */
                    infile.close();
                    return;
                }
                adjwgt.push_back(weight);
            }
            current_edge_idx++;
        }
    }
    xadj[num_nodes] = current_edge_idx;
    infile.close();
    if (current_edge_idx != num_edges * 2) {
        /* Warning */
    }

    // --- Call METIS ---
    idx_t nVertices = num_nodes;
    idx_t nWeights = ncon;
    idx_t nParts = ComputeNodeCount;
    idx_t objval;
    std::vector<idx_t> parts(nVertices);
    idx_t *vwgt_ptr = has_vertex_weights ? vwgt.data() : NULL;
    idx_t *adjwgt_ptr = has_edge_weights ? adjwgt.data() : NULL;
    int ret = METIS_PartGraphKway(&nVertices, &nWeights, xadj.data(), adjncy.data(), vwgt_ptr, NULL, adjwgt_ptr,
                                  &nParts, NULL, NULL, NULL, &objval, parts.data());
    if (ret != METIS_OK) {
        std::cerr << "METIS partitioning failed..." << std::endl;
        return;
    }

    // --- Write Partition Output ---
    std::ofstream outpartition(output_partition_file);
    if (!outpartition.is_open()) {
        /* Error handling */
        return;
    }
    outpartition << "NodeID,PartitionID\n";
    for (idx_t i = 0; i < nVertices; i++) {
        outpartition << i << "," << parts[i] << "\n";
    }
    outpartition.close();

    std::cout << "METIS partitioning completed successfully! Objective value (edge cut): " << objval << std::endl;
}

#endif // NEWMETIS_PARTITIONER_H // Renamed Include Guard
