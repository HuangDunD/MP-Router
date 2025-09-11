#pragma once
#include <iostream>
#include <pqxx/pqxx> // PostgreSQL C++ library
#include <chrono>
#include <vector>
#include <thread>
#include <csignal>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <sstream>
#include <cassert>
#include "common.h"

// 解析ctid字符串，提取page_id
// ctid格式为 "(page_id,tuple_index)"，例如 "(0,1)"
std::pair<int, int> parse_page_id_from_ctid(const std::string& ctid) {
    int block = -1, offset = -1;
    sscanf(ctid.c_str(), "(%d,%d)", &block, &offset);
    return {block, offset};
}

int64_t decode_hex_key(const std::string &hex_str) {
    std::stringstream ss;
    std::string clean_hex;

    for (char c : hex_str) {
        if (std::isxdigit(c)) clean_hex += c;
    }

    // 解析为 64-bit 小端整数
    if (clean_hex.size() < 16) return -1;

    int64_t value = 0;
    for (int i = 0; i < 8; i++) {
        std::string byte_str = clean_hex.substr(i * 2, 2);
        uint8_t byte_val = std::stoi(byte_str, nullptr, 16);
        value |= (static_cast<int64_t>(byte_val) << (8 * i));  // Little-endian shift
    }

    return value;
}

enum class BtreeNodeType {
    LEAF,
    INTERNAL, 
    ROOT
};

class BtreeNode {
public:
    std::mutex node_mutex; // Mutex for thread safety
    std::string index_name; // Name of the B-tree index
    int page_id; // Page ID of the B-tree node
    std::vector<int> keys; // Keys in the B-tree node
    std::vector<int> values; // Values associated with the keys
    BtreeNodeType node_type; // Type of the B-tree node (leaf, internal, root)
    BtreeNode(int id, BtreeNodeType type) : page_id(id), node_type(type) {}
    void add_key(int key, int value) {
        keys.push_back(key);
        values.push_back(value);
    }
    void print_node() {
        std::cout << "Node ID: " << page_id << ", Type: " << (node_type == BtreeNodeType::LEAF ? "Leaf" : (node_type == BtreeNodeType::INTERNAL ? "Internal" : "Root")) << std::endl;
        std::cout << "Keys: ";
        for (size_t i = 0; i < keys.size(); ++i) {
            std::cout << keys[i] << " (Value: " << values[i] << ") ";
        }
        std::cout << std::endl;
    }
}; 

class BtreeIndex {
friend class BtreeIndexService;
private:
    std::mutex index_mutex; // Mutex for thread safety
    std::string index_name; // Name of the B-tree index
    int root_page_id; // Page ID of the root node
    int level = 0; // Level of the B-tree
    std::unordered_map<int, BtreeNode*> nodes; // Map of page IDs to B-tree nodes

public:
    BtreeIndex(const std::string &name) : index_name(name), root_page_id(-1) {}

    page_id_t get_root_page_id() {
        std::lock_guard<std::mutex> lock(index_mutex);
        return root_page_id;
    }
    
    BtreeNode* add_or_update_node(int page_id, BtreeNodeType node_type) {
        std::lock_guard<std::mutex> lock(index_mutex); // Ensure thread safety
        auto it = nodes.find(page_id);
        if (it != nodes.end()) {
            // Node already exists, delete the old node
            delete it->second;
            nodes.erase(it);
        }
        BtreeNode* node = new BtreeNode(page_id, node_type);
        nodes.emplace(page_id, node);
        // If it's the root node, update root_page_id
        if (node_type == BtreeNodeType::ROOT) {
            root_page_id = page_id;
        }
        return node;
    }

    void read_btree_node_from_db(const int page_id, pqxx::connection* conn, BtreeNode** return_node = nullptr) {
        // Placeholder for reading B-tree node from the database
        // This function should query the database to populate keys and values
        std::cout << "Reading B-tree node with page_id: " << page_id << " from database." << std::endl;
        std::string items_query = "SELECT * FROM bt_page_items('" + index_name + "', " + std::to_string(page_id) + ")";
        std::string stats_query = "SELECT * FROM bt_page_stats('" + index_name + "', " + std::to_string(page_id) + ")";

        pqxx::work txn(*conn);

        try {    
            pqxx::result stats_result = txn.exec(stats_query);
            BtreeNode* new_node = nullptr;
            if (!stats_result.empty()) {
                std::string node_type = stats_result[0]["type"].as<std::string>();
                if (node_type == "l") {
                    // for leaf node, always create a new node, but not maintain in memory
                    // ! this node will be deleted after reading all items
                    new_node = new BtreeNode(page_id, BtreeNodeType::LEAF);
                } else if (node_type == "i") {
                    new_node = add_or_update_node(page_id, BtreeNodeType::INTERNAL);
                } else if (node_type == "r") {
                    new_node = add_or_update_node(page_id, BtreeNodeType::ROOT);
                }
                if (return_node) {
                    *return_node = new_node;
                }
            }
            pqxx::result result = txn.exec(items_query);
            for (const auto &row : result) {
                std::string hex_key = row["data"].as<std::string>(); // 'data' 是 hex 字符串
                std::string ctid = row["ctid"].as<std::string>();    // ctid 形如 "(50,1)"

                int64_t key = decode_hex_key(hex_key);
                // if (key == -1) {
                //     std::cout << "Page ID: " << page_id << ", 
                // }
                auto [child_page_id, tuple_idx] = parse_page_id_from_ctid(ctid); // "(50,1)" -> 50, 1

                if (new_node) {
                    new_node->add_key(key, child_page_id);
                }
                // std::cout << "Page ID: " << page_id << ", Key: " << key << ", ctid: " 
                    // << row["ctid"].as<std::string>() << std::endl;
            }
        }
        catch (const std::exception &e) {
            std::cerr << "Error while reading B-tree node: " << e.what() << std::endl;
        }
        txn.commit();
    }

    void read_btree_meta(pqxx::connection* conn) {
        // Placeholder for reading B-tree metadata from the database
        // This function should query the database to get the root page ID and other metadata
        std::cout << "Reading B-tree metadata for index: " << index_name << std::endl;
        std::string meta_query = "SELECT * FROM bt_metap ('" + index_name + "')";
        try {
            pqxx::work txn(*conn);
            pqxx::result result = txn.exec(meta_query);
            if (!result.empty()) {
                std::lock_guard<std::mutex> lock(index_mutex); // Ensure thread safety
                root_page_id = result[0]["root"].as<int>();
                std::cout << "Root page ID: " << root_page_id << std::endl;
                level = result[0]["level"].as<int>();
                std::cout << "B-tree level: " << level << std::endl;
            } else {
                std::cerr << "No metadata found for index: " << index_name << std::endl;
            }
            txn.commit();
        } catch (const std::exception &e) {
            std::cerr << "Error while reading B-tree metadata: " << e.what() << std::endl;
        }
    }
        
    void read_all_internal_nodes(pqxx::connection* conn) { 
        assert(root_page_id != -1 && "Root page ID is not set.");
        BtreeNode* root_node = nodes[root_page_id];
        if (!root_node) {
            std::cerr << "Root node not read for page ID: " << root_page_id << std::endl;
            return;
        }
        std::vector<int> internal_nodes;
        std::vector<int> next_level_nodes;
        internal_nodes.push_back(root_page_id);
        for(int l = 0; l < level-1; l++) {
            std::cout << "Reading internal nodes at level " << l + 1 << std::endl;
            for(const auto& node : internal_nodes) {
                for (const auto& child_page_id : nodes[node]->values) {
                    // 读取子节点
                    // std::cout << "Reading internal node with page_id: " << child_page_id << std::endl;
                    read_btree_node_from_db(child_page_id, conn);
                    next_level_nodes.push_back(child_page_id);
                }
            }
            internal_nodes = next_level_nodes;
            next_level_nodes.clear();
        }
    }

    // 静态查找, 具体来说是指B+树不变化, 即当前数据库不进行UPDATE或INSERT操作, 且没有vacuum操作
    // 这种情况下, B+树的结构是固定的, 可以直接进行查找
    // 注意: 这种查找方式假设B+树的结构不会变化
    page_id_t search_static(int key, pqxx::connection* conn, BtreeNode** return_node = nullptr) {
        assert(root_page_id != -1 && "Root page ID is not set.");
        
        int current_page_id = root_page_id;
        BtreeNode* current_node = nodes[current_page_id];
        
        if (!current_node) {
            std::cerr << "Root node not found for page ID: " << root_page_id << std::endl;
            return -1;
        }

        // 遍历B+树的不同层次，直到找到叶子节点
        while (current_node->node_type != BtreeNodeType::LEAF) {
            // 在当前内部节点中查找合适的子节点
            int child_page_id = find_child_page_id(current_node, key);
            
            if (child_page_id == -1) {
                std::cerr << "Failed to find child page for key: " << key << std::endl;
                return -1;
            }
            
            // 如果子节点还没有被读取，先从数据库读取
            BtreeNode* child_node = nullptr;
            if (nodes.find(child_page_id) == nodes.end()) {
                read_btree_node_from_db(child_page_id, conn, &child_node);
            }
            else {
                child_node = nodes[child_page_id];
            }

            current_node = child_node;
            current_page_id = child_page_id;
            
            if (!current_node) {
                std::cerr << "Child node not found for page ID: " << child_page_id << std::endl;
                return -1;
            }
        }
        
        if(return_node) {
            *return_node = current_node;
        }

        // 现在在叶子节点中查找确切的键值
        for (size_t i = 0; i < current_node->keys.size(); ++i) {
            if (current_node->keys[i] == key) {
                // 找到了键值，返回对应的页面ID（实际数据页面）
                return current_node->values[i];
            }
        }
        
        // 没有找到键值
        std::cout << "Key " << key << " not found in B+ tree" << std::endl;
        return kInvalidPageId;
    }
    
    // 动态查找, 具体来说是指B+树可能会发生变化, 即当前数据库可能会进行UPDATE或INSERT操作,
    // 这种情况下, B+树的结构可能会变化, 需要重新读取
    page_id_t search_dynamic(int key) {
        
    }

private:
    // 在内部节点中查找应该进入哪个子节点
    int find_child_page_id(BtreeNode* internal_node, int key) {
        if (!internal_node || internal_node->keys.empty()) {
            return -1;
        }
        
        // B+树内部节点的搜索逻辑：
        int search_index = 0;
        // 首先先判断一下第一个key是否有值, 如果有值, 他表示该中间节点页的下一个最大值的位置
        if (internal_node->keys[0] != -1) {
            search_index = 1; // 从第二个键开始搜索
        }
        // 如果key小于第一个键，进入最左边的子节点
        assert(internal_node->keys[search_index] == -1);
        if (key < internal_node->keys[search_index + 1]) {
            return internal_node->values[search_index];
        }
        
        search_index++;

        // 查找合适的子节点
        for (; search_index < internal_node->keys.size() - 1; ++search_index) {
            if (key >= internal_node->keys[search_index] && key < internal_node->keys[search_index + 1]) {
                return internal_node->values[search_index];
            }
        }
        
        // 如果key大于等于最后一个键，进入最右边的子节点
        return internal_node->values[internal_node->values.size() - 1];
    }
    
    // 打印B+树的结构（用于调试）
    void print_tree_structure() {
        std::cout << "\n=== B+ Tree Structure ===" << std::endl;
        std::cout << "Index: " << index_name << std::endl;
        std::cout << "Root Page ID: " << root_page_id << std::endl;
        std::cout << "Tree Level: " << level << std::endl;
        
        // 按层次打印节点
        std::vector<int> current_level_nodes;
        current_level_nodes.push_back(root_page_id);
        
        for (int l = 0; l <= level; l++) {
            std::cout << "\n--- Level " << l << " ---" << std::endl;
            std::vector<int> next_level_nodes;
            
            for (int page_id : current_level_nodes) {
                auto it = nodes.find(page_id);
                if (it != nodes.end()) {
                    BtreeNode* node = it->second;
                    std::cout << "Page " << page_id << " (" 
                              << (node->node_type == BtreeNodeType::ROOT ? "ROOT" :
                                  node->node_type == BtreeNodeType::INTERNAL ? "INTERNAL" : "LEAF")
                              << "): ";
                    
                    for (size_t i = 0; i < node->keys.size(); i++) {
                        std::cout << "[" << node->keys[i] << ":" << node->values[i] << "] ";
                    }
                    std::cout << std::endl;
                    
                    // 如果不是叶子节点，将子节点添加到下一层
                    if (node->node_type != BtreeNodeType::LEAF) {
                        for (int child_page : node->values) {
                            next_level_nodes.push_back(child_page);
                        }
                    }
                }
            }
            
            current_level_nodes = next_level_nodes;
            if (current_level_nodes.empty()) break;
        }
        std::cout << "=========================" << std::endl;
    }
};

class BtreeIndexService {
public:
    BtreeIndexService(std::vector<std::string> conn, std::vector<std::string> index_names, int btree_read_mode = 0, int frequency = 100000000) {
        int index_id = 0;
        for (const auto& index_name : index_names) {
            std::vector<pqxx::connection*> connections_;
            // Initialize connections and table name
            for (const auto& conn_str : conn) {
                auto run_conn = new pqxx::connection(conn_str);
                if (!run_conn->is_open()) {
                    std::cerr << "Failed to connect to the database." << std::endl;
                    return;
                }
                connections_.push_back(run_conn);
            }
            auto btree_index = new BtreeIndex(index_name);
            btree_index_vec[index_id++] = btree_index;
            std::cout << "Starting B-tree background thread..." << std::endl;
            std::cout << "Read mode: " << btree_read_mode << ", Frequency: " << frequency << " seconds" << std::endl;

            // Read B-tree metadata
            btree_index->read_btree_meta(connections_[0]);
            // Read B-tree root nodes
            btree_index->read_btree_node_from_db(btree_index->root_page_id, connections_[0]);
            // Read B-tree internal nodes
            btree_index->read_all_internal_nodes(connections_[0]);
            // Start background thread to periodically read B-tree index
            std::thread btree_background_thread([this, btree_read_mode, frequency, btree_index, connections_]() {
                while (true) {
                    std::this_thread::sleep_for(std::chrono::seconds(frequency));
                    std::cout << "Checking B-tree index for checking..." << std::endl;
                    pqxx::connection* conn = nullptr;
                    if(btree_read_mode == 0) {
                        conn = connections_[0]; // Use the first connection for reading
                    } else {
                        conn = rand() % 2 == 0 ? connections_[0] : connections_[1]; // Randomly select a connection
                    }
                    // Read B-tree metadata
                    btree_index->read_btree_meta(conn);
                    // Read B-tree root nodes
                    btree_index->read_btree_node_from_db(btree_index->root_page_id, conn);
                    // Read B-tree internal nodes
                    btree_index->read_all_internal_nodes(conn);

                    std::cout << "B-tree index for checking is up to date." << std::endl;
                } 
            });
            std::string thread_name = "BtreeBG_" + index_name;
            pthread_setname_np(btree_background_thread.native_handle(), thread_name.c_str());
            btree_background_thread.detach();
        }
    }

    page_id_t get_page_id_by_key(table_id_t table_id, itemkey_t key, pqxx::connection* conn, BtreeNode** return_node = nullptr) {
        //! TODO: 这里默认都使用第一个连接
        std::cout << "Looking up key: " << key << " in table_id: " << table_id << std::endl;
        return btree_index_vec[table_id]->search_static(key, conn, return_node);
    }

private:
    std::vector<BtreeIndex*> btree_index_vec{MAX_DB_TABLE_NUM, nullptr};
};