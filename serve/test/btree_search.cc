#include "btree_search.h"

std::vector<std::string> DBConnection;

int main() {    
    // DBConnection.push_back("host=10.12.2.125 port=54322 user=system password=123456 dbname=smallbank");
    // DBConnection.push_back("host=10.12.2.127 port=54322 user=system password=123456 dbname=smallbank");
    
    DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");
    DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");

    pqxx::connection *conn0 = nullptr;
    pqxx::connection *conn1 = nullptr;
    try {
        // Create a connection for each thread
        conn0 = new pqxx::connection(DBConnection[0]);
        if (!conn0->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return -1;
        } else {
            std::cout << "Connected to the database successfully." << std::endl;
        }
        conn1 = new pqxx::connection(DBConnection[1]);
        if (!conn1->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[1] << std::endl;
            return -1;
        } else {
            std::cout << "Connected to the database successfully." << std::endl;
        }
    } catch (const std::exception &e) {
        std::cerr << "Error while connecting to KingBase: " + std::string(e.what()) << std::endl;
        return -1;
    }

    BtreeIndex checking_btree_index("idx_checking_id");
    checking_btree_index.read_btree_meta(conn0);

    checking_btree_index.read_btree_node_from_db(checking_btree_index.root_page_id, conn0);

    checking_btree_index.read_all_internal_nodes(conn0);

    // // 打印B+树结构
    // checking_btree_index.print_tree_structure();

    // 测试搜索功能
    std::cout << "\n=== Testing B+ tree search ===" << std::endl;
    
    // 测试搜索一些键值
    std::vector<int> test_keys = {1, 100, 1000, 50000, 250000, 499999, 909976};
    
    for (int key : test_keys) {
        std::cout << "Searching for key: " << key << std::endl;
        int result_page = checking_btree_index.search_static(key, conn0);
        if (result_page != -1) {
            std::cout << "Found key " << key << " in page: " << result_page << std::endl;
        } else {
            std::cout << "Key " << key << " not found" << std::endl;
        }
        std::cout << "---" << std::endl;
    }

    // 清理资源
    delete conn0;
    delete conn1;
    
    return 0;

}