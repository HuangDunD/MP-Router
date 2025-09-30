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
#include <cmath>
#include <random>

#include "smallbank.h"
#include "common.h"
#include "btree_search.h"
#include "smart_router.h"
#include "friend_simulate.h"
#include "util/zipf.h"

std::vector<std::string> DBConnection;
std::atomic<uint64_t> tx_id_generator;
auto start = std::chrono::high_resolution_clock::now();
int try_count = 10000;
std::atomic<int> exe_count = 0;

int smallbank_account = 300000;

// int system_mode = 0;
int access_pattern = 0; // 0: uniform, 1: zipfian, 2: hotspot
double zipfian_theta = 0.99; // Zipfian distribution parameter
double hotspot_fraction = 0.2; // Fraction of accounts that are hot
double hotspot_access_prob = 0.8; // Probability of accessing hot accounts
int read_btree_mode = 0; // 0: read from conn0, 1: read from random conn
int read_frequency = 5; // seconds
int worker_threads = 16;
std::mutex log_mutex;
std::string access_log_file_name = "access_key.log"; // 日志文件路径
std::string friend_graph_export_file = "friend_graph.csv"; // 导出社交图 CSV 文件
std::ofstream access_key_log_file;

std::unordered_map<int, long long> key_freq;

SmartRouter* smart_router = nullptr;

thread_local std::vector<pqxx::connection*> thread_conns_vec;
thread_local uint64_t thread_gid;
// Global Zipfian generator
thread_local ZipfGen* zipfian_gen = nullptr;

std::vector<std::vector<std::pair<int, float>>> user_friend_graph; // 每个用户的朋友图, 模拟亲和性
std::vector<std::atomic<int>> routed_txn_cnt_per_node(MaxComputeNodeCount); // 每个节点路由的事务数

// Generate account ID based on access pattern
void generate_account_id(itemkey_t &acc1) {
    switch (access_pattern) {
        case 0: // Uniform distribution
            acc1 = rand() % smallbank_account + 1;
            break;
        case 1: // Zipfian distribution
            acc1 = zipfian_gen->next() + 1; // ZipfGen is 0-based, account IDs are 1-based
            break;
        case 2: // Hotspot distribution
        {
            double r = (double)rand() / RAND_MAX;
            int hot_accounts = (int)(smallbank_account * hotspot_fraction);
            
            if (r < hotspot_access_prob) {
                // Access hot accounts (first hotspot_fraction of accounts)
                acc1 = rand() % hot_accounts + 1;
            } else {
                // Access cold accounts (remaining accounts)
                acc1 = hot_accounts + rand() % (smallbank_account - hot_accounts) + 1;
            }
            break;
        }
        default:
            acc1 = rand() % smallbank_account + 1;
            break;
    }
}

void generate_two_account_ids(itemkey_t &acc1, itemkey_t &acc2) {
    // 先生成第一个账号
    generate_account_id(acc1);
    // 再生成第二个亲和性账号
    // 生成一个随机小数，决定是否使用亲和性账号
    double r = (double)rand() / RAND_MAX;
    if (r < AffinityTxnRatio) {
        // 使用亲和性账号
        const auto &friends = user_friend_graph[acc1 - 1]; // acc1
        if (!friends.empty()) {
            // 根据朋友的权重选择一个朋友账号
            double p = (double)rand() / RAND_MAX;
            double cumulative_prob = 0.0;
            for (const auto &[friend_id, prob] : friends) {
                cumulative_prob += prob;
                if (p <= cumulative_prob) {
                    acc2 = friend_id + 1; // friend_id 是从0开始的，账号从1开始
                    return;
                }
            }
            // 如果没有选中任何朋友（理论上不应该发生），则随机选择一个
            acc2 = friends.back().first + 1;
        } else {
            // 如果没有朋友，则随机选择一个账号
            acc2 = rand() % smallbank_account + 1;
        }
    } else {
        // 不使用亲和性账号，随机选择一个账号
        // 确保两个账号不相同
        do {
            generate_account_id(acc2);
        } while (acc2 == acc1);
    }
}

void create_table(pqxx::connection *conn0) {
    std::cout << "Create table..." << std::endl;
    // Load data into the database if needed
    // This is a placeholder for actual data loading logic
    std::string drop_table_sql = "DROP TABLE IF EXISTS checking";
    try {
        pqxx::work txn(*conn0);
        txn.exec(drop_table_sql);
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while dropping table: " << e.what() << std::endl;
    }

    std::string drop_table_sql2 = "DROP TABLE IF EXISTS savings";
    try {
        pqxx::work txn(*conn0);
        txn.exec(drop_table_sql2);
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while dropping table: " << e.what() << std::endl;
    } 

    // Create a new table and insert data
    try {
        pqxx::work txn(*conn0);
        txn.exec("CREATE unlogged TABLE checking (id INT PRIMARY KEY, balance INT, name CHAR(500)) WITH (FILLFACTOR = 70)");
        txn.exec("CREATE unlogged TABLE savings (id INT PRIMARY KEY, balance INT, name CHAR(500)) WITH (FILLFACTOR = 70)");
        std::cout << "Tables created successfully." << std::endl;
        // create index
        txn.exec("CREATE INDEX idx_checking_id ON checking (id)");
        txn.exec("CREATE INDEX idx_savings_id ON savings (id)");
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating table: " << e.what() << std::endl;
    }   
    try{
        // pg not support
        pqxx::work txn(*conn0);
        // pre-extend table to avoid frequent page extend during txn processing
        txn.exec("SELECT sys_extend('checking', 1000000)");
        txn.commit();
    }
    catch (const std::exception &e) {
        std::cerr << "Error while pre-extending table: " << e.what() << std::endl;
    }
    try{
        // pg not support
        pqxx::work txn(*conn0);
        // pre-extend table to avoid frequent page extend during txn processing
        txn.exec("SELECT sys_extend('savings', 1000000)");
        txn.commit();
    }
    catch (const std::exception &e) {
        std::cerr << "Error while pre-extending table: " << e.what() << std::endl;
    }
}

void load_data(pqxx::connection *conn0) {
    std::cout << "Loading data..." << std::endl;
    std::cout << "Will load " << smallbank_account << " accounts into checking and savings tables" << std::endl;
    // Load data into the database if needed
    // Insert data into checking and savings tables
    const int num_threads = 16;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;
    auto worker = [](int start, int end) {
        pqxx::connection conn00(DBConnection[0]);
        if (!conn00.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        for(int i = start; i < end; i++) {
            int id = i + 1;
            int balance = 1000 + (i % 1000); // Random balance
            std::string name = "Account_" + std::to_string(id);
            // 使用RETURNING子句获取插入数据的位置信息
            std::string insert_checking_sql = "INSERT INTO checking (id, balance, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", '" +
                                        name + "') RETURNING ctid, id";
            std::string insert_savings_sql = "INSERT INTO savings (id, balance, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", '" +
                                        name + "') RETURNING ctid, id";

            try {
                pqxx::work txn_create(conn00);
                // 执行checking表插入并获取位置信息
                pqxx::result checking_result = txn_create.exec(insert_checking_sql);                
                if (!checking_result.empty()) {
                    std::string ctid = checking_result[0]["ctid"].as<std::string>();
                    // ctid 为 (page_id, tuple_index) 格式, 这里要把ctid转换为page_id
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = checking_result[0]["id"].as<int>();
                    if(smart_router != nullptr){
                        smart_router->initial_key_page((table_id_t)SmallBankTableType::kCheckingTable, inserted_id, page_id);
                    }
                }
                
                // 执行savings表插入并获取位置信息
                pqxx::result savings_result = txn_create.exec(insert_savings_sql);
                if (!savings_result.empty()) {
                    std::string ctid = savings_result[0]["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = savings_result[0]["id"].as<int>();
                    if(smart_router != nullptr){
                        smart_router->initial_key_page((table_id_t)SmallBankTableType::kSavingsTable, inserted_id, page_id);
                    }
                }
                
                txn_create.commit();
            } catch (const std::exception &e) {
                std::cerr << "Error while inserting data: " << e.what() << std::endl;
            }
        }
    }; 

    auto friend_worker = [](){
        // 生成用户朋友关系
        int num_users = smallbank_account;
        generate_friend_simulate_graph(user_friend_graph, num_users);
        std::cout << "Generated friend graph for " << num_users << " users." << std::endl;
#if LOG_FRIEND_GRAPH
        if(!friend_graph_export_file.empty()){
            if(dump_friend_graph_csv(user_friend_graph, friend_graph_export_file)){
                std::cout << "Friend graph exported to: " << friend_graph_export_file << std::endl;
            } else {
                std::cerr << "Failed to export friend graph to: " << friend_graph_export_file << std::endl;
            }
        }
#else
        // delete existing friend graph file if any
        std::remove(friend_graph_export_file.c_str());
#endif
    };

    // Create and start threads
    for(int i = 0; i < num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? smallbank_account : (i + 1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }
    std::thread friend_thread(friend_worker);
    // Wait for friend thread to complete
    friend_thread.join();
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }
    std::cout << "Data loaded successfully." << std::endl;

    // 输出一些导入数据的统计信息
    try{
        auto txn = pqxx::work(*conn0);
        pqxx::result checking_size = txn.exec("select sys_size_pretty(sys_relation_size('checking')) ");
        pqxx::result savings_size = txn.exec("select sys_size_pretty(sys_relation_size('savings')) ");
        if(!checking_size.empty()){
            std::cout << "Checking table size: " << checking_size[0][0].as<std::string>() << std::endl;
        }
        if(!savings_size.empty()){
            std::cout << "Savings table size: " << savings_size[0][0].as<std::string>() << std::endl;
        }
        txn.commit();
    }catch(const std::exception &e) {
        std::cerr << "Error while getting table size: " << e.what() << std::endl;
    }
}

int create_perf_kwr_snapshot(pqxx::connection *conn0){
    std::cout << "Getting the perf snapshot..." << std::endl;
    int snapshot_id = 0;
    std::string create_snapshot_sql = "SELECT * FROM perf.create_snapshot()";
    try {
        pqxx::work txn(*conn0);
        pqxx::result result0 = txn.exec(create_snapshot_sql);
        if (!result0.empty()) {
            snapshot_id = result0[0]["create_snapshot"].as<int>();
            std::cout << "Snapshot created successfully with ID: " << snapshot_id << std::endl;
        } else {
            std::cerr << "Failed to create snapshot." << std::endl;
        }
        // Commit the transaction
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating snapshot: " << e.what() << std::endl;
    }
    return snapshot_id;
}

void generate_perf_kwr_report(pqxx::connection *conn0, int start_snapshot_id, int end_snapshot_id, std::string file_name) {
    std::cout << "Generating performance report for snapshot IDs: " << start_snapshot_id << " to " << end_snapshot_id << std::endl;
    std::string report_sql = "SELECT * FROM perf.kwr_report_to_file(" + std::to_string(start_snapshot_id) + ", " 
            + std::to_string(end_snapshot_id) + ", 'html', '/home/kingbase/hcy/MP-Router/build/serve/test/" + file_name + "')";
    std::cout << "Report SQL: " << report_sql << std::endl;
    // Execute the SQL to generate the report
    try {
        pqxx::work txn(*conn0);
        pqxx::result result = txn.exec(report_sql);
        if (!result.empty()) {
            std::cout << "Performance report generated successfully." << std::endl;
            // Print the report
            for (const auto& row : result) {
                std::cout << row[0].as<std::string>() << std::endl; // Assuming the first column is the report content
            }
        } else {
            std::cerr << "Failed to generate performance report." << std::endl;
        }
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while generating performance report: " << e.what() << std::endl;
    }
}

static const std::vector<table_id_t> TABLE_IDS_ARR[] = {
    // txn_type == 0
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable, (table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 1
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 2
    {(table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 3
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable},
    // txn_type == 4
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable},
    // txn_type == 5
    {(table_id_t)SmallBankTableType::kSavingsTable}
};

std::vector<table_id_t>& get_table_ids_by_txn_type(int txn_type) {
    assert(txn_type >= 0 && txn_type < sizeof(TABLE_IDS_ARR)/sizeof(TABLE_IDS_ARR[0]));
    return const_cast<std::vector<table_id_t>&>(TABLE_IDS_ARR[txn_type]);
}

void get_keys_by_txn_type(int txn_type, itemkey_t account1, itemkey_t account2, std::vector<itemkey_t> &keys) {
    keys.clear();
    switch(txn_type) {
        case 0:
            keys = {account1, account1, account2};
            break;
        case 1:
            keys = {account1, account2};
            break;
        case 2:
            keys = {account1};
            break;
        case 3:
        case 4:
            keys = {account1, account1};
            break;
        case 5:
            keys = {account1};
            break;
        default:
            break;
    }
    return; 
}

void decide_route_node(itemkey_t account1, itemkey_t account2, int txn_type, int &node_id) {
    if(SYSTEM_MODE == 0) {
        node_id = rand() % 2; // Randomly select node ID for system mode 0
    }
    else if(SYSTEM_MODE == 1){
        if(txn_type == 0 || txn_type == 1) {
            int node1 = account1 / (smallbank_account / ComputeNodeCount); // Range partitioning
            int node2 = account2 / (smallbank_account / ComputeNodeCount); // Range partitioning
            if(node1 == node2) {
                node_id = node1;
            }
            else {
                // randomly pick one
                node_id = (rand() % 2 == 0) ? node1 : node2;
            }
        }
        else {
            node_id = account1 / (smallbank_account / ComputeNodeCount); // Range partitioning
        }
    }
    else if(SYSTEM_MODE == 2) {
        // get page_id from checking_page_map
        node_id = rand() % ComputeNodeCount; // Fallback to random node if not found
    }
    else if(SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 6) {
        assert(smart_router != nullptr);
        // keys 只和 account1/account2有关，不能静态化，但可以用局部变量，每次只构造一份
        std::vector<itemkey_t> keys;
        switch(txn_type) {
            case 0:
                keys = {account1, account1, account2};
                break;
            case 1:
                keys = {account1, account2};
                break;
            case 2:
                keys = {account1};
                break;
            case 3:
            case 4:
                keys = {account1, account1};
                break;
            case 5:
                keys = {account1};
                break;
            default:
                break;
        }
        // table_ids 静态化后只需引用
        const std::vector<table_id_t>& table_ids = TABLE_IDS_ARR[txn_type < 6 ? txn_type : 0];

#if LOG_ACCESS_KEY
        // 写日志记录一下
        std::unique_lock<std::mutex> lock(log_mutex);
        if(access_key_log_file.is_open()) {
            int i=0;
            for(i=0; i<table_ids.size()-1; i++) access_key_log_file << "{" << table_ids[i] << ":" << keys[i] << "},";
            access_key_log_file << "{" << table_ids[i] << ":" << keys[i] << "}" << std::endl;
            access_key_log_file.flush();
        }
        for (auto key: keys) key_freq[key]++;
#endif

        SmartRouter::SmartRouterResult result = smart_router->get_route_primary(const_cast<std::vector<table_id_t>&>(table_ids), keys, thread_conns_vec);
        if(result.success) {
            node_id = result.smart_router_id;
        }
        else {
            // fallback to random
            node_id = rand() % ComputeNodeCount;
            std::cerr << "Warning: SmartRouter get_route_primary failed: " << result.error_message << std::endl;
        }
    }
    else if(SYSTEM_MODE == 4) {
        node_id = 0; // All to node 0 for single
    }
    routed_txn_cnt_per_node[node_id]++;
}

struct thread_params
{
    int thread_id;
    int thread_count;
};

void run_smallbank_txns(thread_params* params) {
    std::cout << "Running smallbank transactions..." << std::endl;
    // This is a placeholder for actual transaction logic
    // You would implement the logic to run transactions against the smallbank database here
    // For example, you could create threads that perform various operations like deposits, withdrawals, etc.
    // Update exe_count as transactions are executed

    // init the thread connections
    thread_conns_vec.clear();
    for(auto con_str: DBConnection) {
        auto con = new pqxx::connection(con_str);
        if(!con->is_open()) {
            std::cerr << "Failed to connect to database: " << con_str << std::endl;
            assert(false);
            exit(-1);
        }
        thread_conns_vec.push_back(con);
    }
    // init the thread id
    thread_gid = params->thread_id;
    if (!zipfian_gen) {
        uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
        uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
        zipfian_gen = new ZipfGen(smallbank_account, zipfian_theta, zipf_seed & zipf_seed_mask);
    }

    // run smallbank transactions
    for (int i = 0; i < try_count; ++i) {
        exe_count++;
        tx_id_t tx_id = tx_id_generator++; // global atomic transaction ID
        // Simulate some work
        // Randomly select a transaction type and accounts
        int txn_type = rand() % 6;  // 6 types of transactions
        
        itemkey_t account1, account2;
        if(txn_type == 0 || txn_type == 1) { // TxAmagamate or TxSendPayment
            generate_two_account_ids(account1, account2);
        } else {
            generate_account_id(account1);
        }
        int routed_node_id = 0; // Default node ID
        
        decide_route_node(account1, account2, txn_type, routed_node_id);

        // Create a new transaction
        pqxx::work* txn = nullptr;
        auto txn_con = thread_conns_vec[routed_node_id];
        while (!txn_con->is_open()){
            std::cerr << "Connection is broken, reconnecting..." << std::endl;
            delete txn_con;
            txn_con = new pqxx::connection(DBConnection[routed_node_id]);
            thread_conns_vec[routed_node_id] = txn_con;
        }
        txn = new pqxx::work(*txn_con);

        try {  
            switch(txn_type) {
                case 0: { // TxAmagamate
                    int checking_balance, savings_balance;
                    pqxx::result result1 = txn->exec("UPDATE checking SET balance = 0 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result1.empty()) {
                        std::string ctid = result1[0]["ctid"].as<std::string>();
                        int id = result1[0]["id"].as<int>();
                        assert(id == account1);
                        checking_balance = result1[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, page_id, routed_node_id);
                        }
                    }
                    pqxx::result result2 = txn->exec("UPDATE savings SET balance = 0 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result2.empty()) {
                        std::string ctid = result2[0]["ctid"].as<std::string>();
                        int id = result2[0]["id"].as<int>();
                        int balance = result2[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        savings_balance = result2[0]["balance"].as<int>();
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, page_id, routed_node_id);
                        }
                    }
                    int total = checking_balance + savings_balance;
                    pqxx::result result3 = txn->exec("UPDATE checking SET balance = " + 
                            std::to_string(total) + " WHERE id = " + std::to_string(account2) + 
                            " RETURNING ctid, id, balance");
                    if (!result3.empty()) {
                        std::string ctid = result3[0]["ctid"].as<std::string>();
                        int id = result3[0]["id"].as<int>();
                        int balance = result3[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, page_id, routed_node_id);
                        }
                    }
                }
                case 1: {  // TxSendPayment
                    // 第一次更新：减少余额并获取位置信息
                    pqxx::result result1 = txn->exec("UPDATE checking SET balance = balance - 10 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result1.empty()) {
                        std::string ctid = result1[0]["ctid"].as<std::string>();
                        int id = result1[0]["id"].as<int>();
                        int balance = result1[0]["balance"].as<int>();
                        // get and update page_id
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, page_id, routed_node_id);
                        }
                    }
                    
                    // 第二次更新：增加余额并获取位置信息
                    pqxx::result result2 = txn->exec("UPDATE checking SET balance = balance + 10 WHERE id = " + 
                            std::to_string(account2) + " RETURNING ctid, id, balance");
                    if (!result2.empty()) {
                        std::string ctid = result2[0]["ctid"].as<std::string>();
                        int id = result2[0]["id"].as<int>();
                        int balance = result2[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, page_id, routed_node_id);
                        }
                    }
                    break;
                }
                case 2: {  // TxDepositChecking
                    pqxx::result result = txn->exec("UPDATE checking SET balance = balance + 100 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result.empty()) {
                        std::string ctid = result[0]["ctid"].as<std::string>();
                        int id = result[0]["id"].as<int>();
                        int balance = result[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, page_id, routed_node_id);
                        }
                    }
                    break;
                }
                case 3: {  // TxWriteCheck
                    pqxx::result result = txn->exec("Select balance, ctid, id FROM savings WHERE id = " + 
                            std::to_string(account1));
                    pqxx::result result2 = txn->exec("Update checking SET balance = balance - 50 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result.empty()) {
                        int balance = result[0]["balance"].as<int>();
                        std::string ctid = result[0]["ctid"].as<std::string>();
                        int id = result[0]["id"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, page_id, routed_node_id);
                        }
                    }
                    if (!result2.empty()) {
                        std::string ctid = result2[0]["ctid"].as<std::string>();
                        int id = result2[0]["id"].as<int>();
                        int balance = result2[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, page_id, routed_node_id);
                        }
                    }
                    break;
                }
                case 4: { // TxBalance 
                    pqxx::result result = txn->exec("SELECT id, balance, ctid FROM checking WHERE id = " + 
                            std::to_string(account1));
                    pqxx::result result2 = txn->exec("SELECT id, balance, ctid FROM savings WHERE id = " + 
                            std::to_string(account1));
                    if (!result.empty()) {
                        std::string ctid = result[0]["ctid"].as<std::string>();
                        int id = result[0]["id"].as<int>();
                        int balance = result[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, page_id, routed_node_id);
                        }
                    } 
                    if (!result2.empty()) {
                        std::string ctid = result2[0]["ctid"].as<std::string>();
                        int id = result2[0]["id"].as<int>();
                        int balance = result2[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, page_id, routed_node_id);
                        }
                    }
                }
                case 5: { // TxTransactSavings
                    pqxx::result result = txn->exec("UPDATE savings SET balance = balance + 20 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result.empty()) {
                        std::string ctid = result[0]["ctid"].as<std::string>();
                        int id = result[0]["id"].as<int>();
                        int balance = result[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        if(smart_router) {
                            smart_router->update_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, page_id, routed_node_id);
                        }
                    }
                    break;
                }
            }
            txn->commit();
        } catch (const std::exception &e) {
            std::cerr << "Transaction failed: " << e.what() << std::endl;
        }
        delete txn;
        // std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::cout << "Finished running smallbank transactions." << std::endl;

}

void dtx_exe(){

}

void signal_handler(int signum) {
    std::cout << "\nCaught signal " << signum << " (SIGINT)" << std::endl;
    std::cout << "Printing final statistics before exit..." << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    // 计算吞吐
    std::cout << "Total transactions executed: " << exe_count << std::endl;
    double throughput = (double) exe_count / (ms / 1000.0);
    std::cout << "Throughput: " << throughput << " transactions per second" << std::endl;
    std::cout << "Exiting..." << std::endl;
    // Clean up and close database connections

    exit(signum);
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --system-mode <mode>        System mode (0=random node, 1=account-based, 2=page-based) [default: 0]" << std::endl;
    std::cout << "  --access-pattern <pattern>  Data access pattern (0=uniform, 1=zipfian, 2=hotspot) [default: 0]" << std::endl;
    std::cout << "  --zipfian-theta <theta>     Zipfian distribution parameter (0.0-1.0) [default: 0.99]" << std::endl;
    std::cout << "  --hotspot-fraction <frac>   Fraction of hot accounts (0.0-1.0) [default: 0.1]" << std::endl;
    std::cout << "  --hotspot-prob <prob>       Probability of accessing hot accounts (0.0-1.0) [default: 0.8]" << std::endl;
    std::cout << "  --btree-read-mode <mode>    B-tree read mode (0=conn0, 1=random) [default: 0]" << std::endl;
    std::cout << "  --btree-frequency <seconds> B-tree refresh frequency in seconds [default: 5]" << std::endl;
    std::cout << "  --account-count <number>    Number of accounts to load [default: 300000]" << std::endl;
    std::cout << "  --help                      Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " --system-mode 1 --access-pattern 1 --zipfian-theta 0.95" << std::endl;
    std::cout << "  " << program_name << " --access-pattern 2 --hotspot-fraction 0.2 --hotspot-prob 0.9" << std::endl;
    std::cout << "  " << program_name << " --system-mode 2 --account-count 100000" << std::endl;
}

int main(int argc, char *argv[]) {
    // Register signal handler for SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    int system_mode = 0; // Default system mode
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
        else if (arg == "--system-mode") {
            if (i + 1 < argc) {
                system_mode = std::stoi(argv[++i]);
                SYSTEM_MODE = system_mode;
                std::cout << "System mode set to: " << system_mode << std::endl;
            } else {
                std::cerr << "Error: --system-mode requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--access-pattern") {
            if (i + 1 < argc) {
                access_pattern = std::stoi(argv[++i]);
                if (access_pattern < 0 || access_pattern > 2) {
                    std::cerr << "Error: Access pattern must be 0, 1, or 2" << std::endl;
                    return -1;
                }
                std::cout << "Access pattern set to: " << access_pattern << std::endl;
            } else {
                std::cerr << "Error: --access-pattern requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--zipfian-theta") {
            if (i + 1 < argc) {
                zipfian_theta = std::stod(argv[++i]);
                if (zipfian_theta < 0.0 || zipfian_theta >= 1.0) {
                    std::cerr << "Error: Zipfian theta must be between 0.0 and 1.0" << std::endl;
                    return -1;
                }
                std::cout << "Zipfian theta set to: " << zipfian_theta << std::endl;
            } else {
                std::cerr << "Error: --zipfian-theta requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--hotspot-fraction") {
            if (i + 1 < argc) {
                hotspot_fraction = std::stod(argv[++i]);
                if (hotspot_fraction <= 0.0 || hotspot_fraction >= 1.0) {
                    std::cerr << "Error: Hotspot fraction must be between 0.0 and 1.0" << std::endl;
                    return -1;
                }
                std::cout << "Hotspot fraction set to: " << hotspot_fraction << std::endl;
            } else {
                std::cerr << "Error: --hotspot-fraction requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--hotspot-prob") {
            if (i + 1 < argc) {
                hotspot_access_prob = std::stod(argv[++i]);
                if (hotspot_access_prob <= 0.0 || hotspot_access_prob >= 1.0) {
                    std::cerr << "Error: Hotspot probability must be between 0.0 and 1.0" << std::endl;
                    return -1;
                }
                std::cout << "Hotspot access probability set to: " << hotspot_access_prob << std::endl;
            } else {
                std::cerr << "Error: --hotspot-prob requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--btree-read-mode") {
            if (i + 1 < argc) {
                read_btree_mode = std::stoi(argv[++i]);
                std::cout << "B-tree read mode set to: " << read_btree_mode << std::endl;
            } else {
                std::cerr << "Error: --btree-read-mode requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--btree-frequency") {
            if (i + 1 < argc) {
                read_frequency = std::stoi(argv[++i]);
                std::cout << "B-tree refresh frequency set to: " << read_frequency << " seconds" << std::endl;
            } else {
                std::cerr << "Error: --btree-frequency requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--account-count") {
            if (i + 1 < argc) {
                smallbank_account = std::stoi(argv[++i]);
                if (smallbank_account <= 0) {
                    std::cerr << "Error: Account count must be greater than 0" << std::endl;
                    return -1;
                }
                if (smallbank_account > 10000000) {
                    std::cerr << "Warning: Account count is very large (" << smallbank_account << "), this may take a long time" << std::endl;
                }
                std::cout << "Account count set to: " << smallbank_account << std::endl;
            } else {
                std::cerr << "Error: --account-count requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--worker-threads") {
            if (i + 1 < argc) {
                worker_threads = std::stoi(argv[++i]);
                if (worker_threads <= 0) {
                    std::cerr << "Error: Worker threads must be greater than 0" << std::endl;
                    return -1;
                }
                std::cout << "Worker threads set to: " << worker_threads << std::endl;
            } else {
                std::cerr << "Error: --worker-threads requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--partition-interval") {
            if (i + 1 < argc) {
                PARTITION_INTERVAL = std::stoi(argv[++i]);
                if (PARTITION_INTERVAL <= 0) {
                    std::cerr << "Error: Partition interval must be greater than 0" << std::endl;
                    return -1;
                }
                std::cout << "Partition interval set to: " << PARTITION_INTERVAL << std::endl;
            } else {
                std::cerr << "Error: --partition-interval requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--try-count") {
            if (i + 1 < argc) {
                try_count = std::stoi(argv[++i]);
                if (try_count <= 0) {
                    std::cerr << "Error: Try count must be greater than 0" << std::endl;
                    return -1;
                }
                std::cout << "Try count set to: " << try_count << std::endl;
            } else {
                std::cerr << "Error: --try-count requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else {
            std::cerr << "Error: Unknown argument " << arg << std::endl;
            print_usage(argv[0]);
            return -1;
        }
    }

    // Display current configuration
    std::cout << "\n=== Configuration ===" << std::endl;
    std::cout << "System mode: " << SYSTEM_MODE << " ----> ";
    switch (SYSTEM_MODE)
    {
    case 0:
        std::cout << "\033[31m  random router \033[0m" << std::endl; // 标注为红色
        break;
    case 1:
        std::cout << "\033[31m  account hashing router \033[0m" << std::endl;
        break;
    case 2:
        std::cout << "\033[31m  page hashing router \033[0m" << std::endl;
        break;
    case 3:
        std::cout << "\033[31m  page affinity router \033[0m" << std::endl;
        break;
    case 4:
        std::cout << "\033[31m  single node \033[0m" << std::endl;
        break; 
    case 5:
        std::cout << "\033[31m  ontime router \033[0m" << std::endl;
        break;
    case 6:
        std::cout << "\033[31m  key affinity router \033[0m" << std::endl;
        break;
    default:
        std::cerr << "\033[31m  <Unknown> \033[0m" << std::endl;
        return -1;
    }
    std::cout << "Account count: " << smallbank_account << std::endl;
    
    std::string access_pattern_name;
    switch (access_pattern) {
        case 0: access_pattern_name = "Uniform"; break;
        case 1: access_pattern_name = "Zipfian"; break;
        case 2: access_pattern_name = "Hotspot"; break;
        default: access_pattern_name = "Unknown"; break;
    }
    std::cout << "Access pattern: " << access_pattern << " (" << access_pattern_name << ")" << std::endl;
    
    if (access_pattern == 1) {
        std::cout << "Zipfian theta: " << zipfian_theta << std::endl;
    } else if (access_pattern == 2) {
        std::cout << "Hotspot fraction: " << hotspot_fraction << std::endl;
        std::cout << "Hotspot access probability: " << hotspot_access_prob << std::endl;
    }
    
    std::cout << "Worker threads: " << worker_threads << std::endl;
    std::cout << "====================" << std::endl;

    // --- Load Database Connection Info ---
    std::cout << "Loading database connection info..." << std::endl;

    // !!! need to update when changing the cluster environment
    DBConnection.push_back("host=10.12.2.125 port=54321 user=system password=123456 dbname=smallbank");
    DBConnection.push_back("host=10.12.2.127 port=54321 user=system password=123456 dbname=smallbank");

    // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");
    // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");
    ComputeNodeCount = DBConnection.size();
    std::cout << "Database connection info loaded. Total nodes: " << ComputeNodeCount << std::endl;

#if LOG_ACCESS_KEY
    // open the access key log file
    access_key_log_file.open(access_log_file_name, std::ios::out | std::ios::trunc);
    if (!access_key_log_file.is_open()) {
        std::cerr << "Failed to open access key log file." << std::endl;
        return -1;
    }
#else 
    // delete existing log file if any
    std::remove(access_log_file_name.c_str());
#endif

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

    // Create table and indexes
    create_table(conn0);

    if(SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 6) {
        std::cout << "Initializing Smart Router..." << std::endl;
        // Create a BtreeService
        BtreeIndexService *index_service = new BtreeIndexService(DBConnection, {"idx_checking_id", "idx_savings_id"}, read_btree_mode, read_frequency);
        SmartRouter::Config cfg{};
        smart_router = new SmartRouter(cfg, index_service);
        std::cout << "Smart Router initialized." << std::endl;
    }
    else {
        std::cout << "Smart Router not used in this system mode." << std::endl;
    }

    // Load data into the database if needed
    load_data(conn0);
    std::cout << "Data loaded successfully." << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Create a performance snapshot
    int start_snapshot_id = create_perf_kwr_snapshot(conn0);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    // --- Start Transaction Threads ---
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Starting transaction threads..." << std::endl;

    std::vector<std::thread> threads;
    // !Start the transaction threads
    for(int i = 0; i < worker_threads; i++) {
        thread_params* params = new thread_params();
        params->thread_id = i;
        params->thread_count = worker_threads;
        threads.emplace_back(run_smallbank_txns, params);
    }
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Create a performance snapshot after running transactions
    std::this_thread::sleep_for(std::chrono::seconds(2)); // sleep for a while to ensure all operations are completed
    int end_snapshot_id = create_perf_kwr_snapshot(conn0);
    std::cout << "Performance snapshots created: Start ID = " << start_snapshot_id 
              << ", End ID = " << end_snapshot_id << std::endl;
    
    if(smart_router){
        std::cout << "********** Smart Router page stats **********" << std::endl;
        int change_page_cnt = smart_router->get_stats().change_page_cnt;
        int page_update_cnt = smart_router->get_stats().page_update_cnt;
        int hit_cnt = smart_router->get_stats().hot_hit;
        int miss_cnt = smart_router->get_stats().hot_miss;
        std::cout << "Hot page hit: " << hit_cnt << ", miss: " << miss_cnt 
                  << ", hit ratio: " << (hit_cnt + miss_cnt > 0 ? (double)hit_cnt / (hit_cnt + miss_cnt) * 100.0 : 0.0) << "%" << std::endl;
        std::cout << "Page ID changes: " << change_page_cnt << std::endl;
        std::cout << "Page updates: " << page_update_cnt << std::endl; 
        const NewMetis::Stats& metis_stats = smart_router->get_metis_stats();
        std::cout << "Metis total nodes in graph: " << metis_stats.total_nodes_in_graph << std::endl;
        std::cout << "Metis total edges in graph: " << metis_stats.total_edges_in_graph << std::endl;
        std::cout << "Metis total edges weight: " << metis_stats.total_edges_weight << std::endl;
        std::cout << "Metis edge weight cut: " << metis_stats.cut_edges_weight << std::endl;
        std::cout << "Metis edge cut ratio: " << 1.0 * metis_stats.cut_edges_weight / metis_stats.total_edges_weight << std::endl;
        std::cout << "Metis cross node decisions: " << metis_stats.total_cross_partition_decisions << std::endl;
        std::cout << "Metis partial affinity decisions: " << metis_stats.partial_affinity_decisions << std::endl;
        std::cout << "Metis full affinity decisions: " << metis_stats.entire_affinity_decisions << std::endl;
        std::cout << "Metis missing decisions: " << metis_stats.missing_node_decisions << std::endl; 
        std::cout << "*********************************************" << std::endl;
    }
    std::cout << "All transaction threads completed." << std::endl;
    for(int i =0; i<DBConnection.size(); i++){
        std::cout << "node " << i << " routed txn count: " << routed_txn_cnt_per_node[i] << std::endl;
    }
    std::cout << "Total accounts loaded: " << smallbank_account << std::endl;
    std::cout << "Access pattern used: " << access_pattern_name << std::endl;
    std::cout << "Total transactions executed: " << exe_count << std::endl;
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    double s = ms / 1000.0; // Convert milliseconds to seconds
    std::cout << "Throughput: " << exe_count / s << " transactions per second" << std::endl;

    // Generate performance report, file name inluding the timestamp
    // get the current time as a string
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm* now_tm = std::localtime(&now_time);
    char buffer[100];
    std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", now_tm);
    std::string timestamp(buffer);  
    std::string report_file = "smallbank_report_" + timestamp + "_mode" + std::to_string(SYSTEM_MODE) + ".html";
    generate_perf_kwr_report(conn0, start_snapshot_id, end_snapshot_id, report_file);

    // 关闭连接
    delete conn0;
    delete conn1;

    // 清理Zipfian生成器
    if (zipfian_gen) {
        delete zipfian_gen;
        zipfian_gen = nullptr;
    }

#if LOG_ACCESS_KEY
    // 转到 vector 便于排序
    std::vector<std::pair<int, long long>> vec(key_freq.begin(), key_freq.end());

    // 按 value 从大到小排序
    std::sort(vec.begin(), vec.end(),
              [](auto &a, auto &b) { return a.second > b.second; });

    // 输出前 50 个
    int topN = 50;
    if (vec.size() < topN) topN = vec.size();
    for (int i = 0; i < topN; i++) {
        std::cout << "Key: " << vec[i].first
                  << "  Count: " << vec[i].second << "\n";
    }
    
    // 计算总访问次数
    long long total = 0;
    for (auto &p : vec) total += p.second;

    auto calc_ratio = [&](double percent) {
        size_t topN = std::max<size_t>(1, size_t(vec.size() * percent));
        long long sum = 0;
        for (size_t i = 0; i < topN && i < vec.size(); i++) sum += vec[i].second;
        return double(sum) / total * 100.0;
    };

    double r1  = calc_ratio(0.01);
    double r10 = calc_ratio(0.10);
    double r50 = calc_ratio(0.50);

    std::cout << "前 1% key 占总访问比例:  " << r1  << "%\n";
    std::cout << "前10% key 占总访问比例:  " << r10 << "%\n";
    std::cout << "前50% key 占总访问比例:  " << r50 << "%\n";
#endif

    return 0;
}