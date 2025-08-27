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

#include "common.h"
#include "btree_search.h"

std::vector<std::string> DBConnection;
auto start = std::chrono::high_resolution_clock::now();
int try_count = 2000;
std::atomic<int> exe_count = 0;

int smallbank_account = 300000;

int system_mode = 0;
int access_pattern = 0; // 0: uniform, 1: zipfian, 2: hotspot
double zipfian_theta = 0.99; // Zipfian distribution parameter
double hotspot_fraction = 0.2; // Fraction of accounts that are hot
double hotspot_access_prob = 0.8; // Probability of accessing hot accounts
int read_btree_mode = 0; // 0: read from conn0, 1: read from random conn
int read_frequency = 5; // seconds
bool enable_btree_thread = false; // Whether to enable B-tree background thread

std::mutex savings_mutex;
std::mutex checking_mutex;
std::unordered_map<itemkey_t, page_id_t> savings_page_map;
std::unordered_map<itemkey_t, page_id_t> checking_page_map;
std::atomic<int> change_page_cnt = 0;
std::atomic<int> page_update_cnt = 0;

// Zipfian distribution generator
class ZipfianGenerator {
private:
    int num_items;
    double theta;
    double alpha;
    double zetan;
    double eta;
    
public:
    ZipfianGenerator(int n, double theta) : num_items(n), theta(theta) {
        alpha = 1.0 / (1.0 - theta);
        zetan = zeta(num_items, theta);
        eta = (1.0 - std::pow(2.0 / num_items, 1.0 - theta)) / (1.0 - zeta(2, theta) / zetan);
    }
    
    int next() {
        double u = (double)rand() / RAND_MAX;
        double uz = u * zetan;
        
        if (uz < 1.0) return 1;
        if (uz < 1.0 + std::pow(0.5, theta)) return 2;
        
        return 1 + (int)(num_items * std::pow(eta * u - eta + 1.0, alpha));
    }
    
private:
    double zeta(int n, double theta) {
        double sum = 0.0;
        for (int i = 1; i <= n; i++) {
            sum += 1.0 / std::pow(i, theta);
        }
        return sum;
    }
};

// Global Zipfian generator
ZipfianGenerator* zipfian_gen = nullptr;

// Generate account ID based on access pattern
int generate_account_id() {
    switch (access_pattern) {
        case 0: // Uniform distribution
            return rand() % smallbank_account + 1;
            
        case 1: // Zipfian distribution
            if (!zipfian_gen) {
                zipfian_gen = new ZipfianGenerator(smallbank_account, zipfian_theta);
            }
            return zipfian_gen->next();
            
        case 2: // Hotspot distribution
        {
            double r = (double)rand() / RAND_MAX;
            int hot_accounts = (int)(smallbank_account * hotspot_fraction);
            
            if (r < hotspot_access_prob) {
                // Access hot accounts (first hotspot_fraction of accounts)
                return rand() % hot_accounts + 1;
            } else {
                // Access cold accounts (remaining accounts)
                return hot_accounts + rand() % (smallbank_account - hot_accounts) + 1;
            }
        }
        
        default:
            return rand() % smallbank_account + 1;
    }
}

void load_data(pqxx::connection *conn0) {
    std::cout << "Loading data..." << std::endl;
    std::cout << "Will load " << smallbank_account << " accounts into checking and savings tables" << std::endl;
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
        txn.exec("CREATE TABLE checking (id INT PRIMARY KEY, balance INT, name CHAR(500)) WITH (FILLFACTOR = 70)");
        txn.exec("CREATE TABLE savings (id INT PRIMARY KEY, balance INT, name CHAR(500)) WITH (FILLFACTOR = 70)");
        std::cout << "Tables created successfully." << std::endl;
        // create index
        txn.exec("CREATE INDEX idx_checking_id ON checking (id)");
        txn.exec("CREATE INDEX idx_savings_id ON savings (id)");
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating table: " << e.what() << std::endl;
    }   
    
    // Insert data into checking and savings tables
    const int num_threads = 16;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;
    auto worker = [](int start, int end) {
        pqxx::connection* conn00 = new pqxx::connection(DBConnection[0]);
        if (!conn00->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return -1;
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
                pqxx::work txn_create(*conn00);
                
                // 执行checking表插入并获取位置信息
                pqxx::result checking_result = txn_create.exec(insert_checking_sql);                
                if (!checking_result.empty()) {
                    std::string ctid = checking_result[0]["ctid"].as<std::string>();
                    // ctid 为 (page_id, tuple_index) 格式, 这里要把ctid转换为page_id
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = checking_result[0]["id"].as<int>();
                    std::unique_lock<std::mutex> lock(checking_mutex);
                    checking_page_map[inserted_id] = page_id; // Store the page ID
                    // std::cout << "Checking table - ID: " << inserted_id 
                    //           << ", Physical location (ctid): " << ctid 
                    //           << ", Page ID: " << page_id << std::endl;
                }
                
                // 执行savings表插入并获取位置信息
                pqxx::result savings_result = txn_create.exec(insert_savings_sql);
                if (!savings_result.empty()) {
                    std::string ctid = savings_result[0]["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = savings_result[0]["id"].as<int>();
                    std::unique_lock<std::mutex> lock(savings_mutex);
                    savings_page_map[inserted_id] = page_id; // Store the page ID
                    // std::cout << "Savings table - ID: " << inserted_id 
                    //           << ", Physical location (ctid): " << ctid 
                    //           << ", Page ID: " << page_id << std::endl;
                }
                
                txn_create.commit();
            } catch (const std::exception &e) {
                std::cerr << "Error while inserting data: " << e.what() << std::endl;
            }
        }
    }; 

    // Create and start threads
    for(int i = 0; i < num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? smallbank_account : (i + 1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }
    std::cout << "Data loaded successfully." << std::endl;
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

void run_smallbank_txns() {
    std::cout << "Running smallbank transactions..." << std::endl;
    // This is a placeholder for actual transaction logic
    // You would implement the logic to run transactions against the smallbank database here
    // For example, you could create threads that perform various operations like deposits, withdrawals, etc.
    // Update exe_count as transactions are executed
    auto run_conn0 = new pqxx::connection(DBConnection[0]);
    auto run_conn1 = new pqxx::connection(DBConnection[1]);
    if (!run_conn0->is_open() || !run_conn1->is_open())
    {
        std::cerr << "Failed to connect to the database." << std::endl;
        return;
    }
    
    for (int i = 0; i < try_count; ++i) {
        exe_count++;
        // Simulate some work
        // Randomly select a transaction type and accounts
        int txn_type = rand() % 3;  // 3 types of transactions
        int account1 = generate_account_id();
        int account2 = generate_account_id();
        int node_id = 0; // Default node ID
        if(system_mode == 0) {
            node_id = rand() % 2; // Randomly select node ID for system mode 0
        }
        else if(system_mode == 1){
            node_id = account1 <= (smallbank_account / 2) ? 0 : 1; // Even accounts go to node 0, odd accounts go to node 1
        }
        else if(system_mode == 2) {
            // get page_id from checking_page_map
            std::unique_lock<std::mutex> lock(checking_mutex);
            auto it = checking_page_map.find(account1);
            if (it != checking_page_map.end()) {
                page_id_t page_id = it->second;
                node_id = (page_id % 2 == 0) ? 0 : 1; // Even page_id goes to node 0, odd page_id goes to node 1
            } 
            else {
                node_id = rand() % 2; // Fallback to random node if not found
            }
        }

        pqxx::work* txn = nullptr;
        // check whether the connection is broken
        while (!run_conn0->is_open() || !run_conn1->is_open())
        {
            std::cerr << "Connection is broken, reconnecting..." << std::endl;
            delete run_conn0;
            delete run_conn1;
            run_conn0 = new pqxx::connection(DBConnection[0]);
            run_conn1 = new pqxx::connection(DBConnection[1]);
        }
        if (node_id == 0) {
            txn = new pqxx::work(*run_conn0);
        } else {
            txn = new pqxx::work(*run_conn1);
        }

        try {  
            switch(txn_type) {
                case 0: {  // Balance transfer
                    // 第一次更新：减少余额并获取位置信息
                    pqxx::result result1 = txn->exec("UPDATE checking SET balance = balance - 10 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result1.empty()) {
                        std::string ctid = result1[0]["ctid"].as<std::string>();
                        int id = result1[0]["id"].as<int>();
                        int balance = result1[0]["balance"].as<int>();
                        // get and update page_id
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        std::unique_lock<std::mutex> lock(checking_mutex);
                        // 检查page_id 是否变化
                        page_id_t old_page_id = checking_page_map[id];
                        if(old_page_id != page_id) {
                            // 如果page_id发生变化，记录变化
                            change_page_cnt++;
                            checking_page_map[id] = page_id; // Update the page ID
                            std::cout << "Page ID changed for ID: " << id 
                                      << ", Old Page ID: " << old_page_id 
                                      << ", New Page ID: " << page_id << std::endl;
                        }
                        page_update_cnt++;
                    }
                    
                    // 第二次更新：增加余额并获取位置信息
                    pqxx::result result2 = txn->exec("UPDATE checking SET balance = balance + 10 WHERE id = " + 
                            std::to_string(account2) + " RETURNING ctid, id, balance");
                    if (!result2.empty()) {
                        std::string ctid = result2[0]["ctid"].as<std::string>();
                        int id = result2[0]["id"].as<int>();
                        int balance = result2[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        std::unique_lock<std::mutex> lock(checking_mutex);
                        // 检查page_id 是否变化
                        page_id_t old_page_id = checking_page_map[id];
                        if(old_page_id != page_id) {
                            // 如果page_id发生变化，记录变化
                            change_page_cnt++;
                            checking_page_map[id] = page_id; // Update the page ID
                            std::cout << "Page ID changed for ID: " << id 
                                      << ", Old Page ID: " << old_page_id 
                                      << ", New Page ID: " << page_id << std::endl;
                        }
                        page_update_cnt++;
                    }
                    break;
                }
                case 1: {  // Deposit check
                    pqxx::result result = txn->exec("UPDATE checking SET balance = balance + 100 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result.empty()) {
                        std::string ctid = result[0]["ctid"].as<std::string>();
                        int id = result[0]["id"].as<int>();
                        int balance = result[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        std::unique_lock<std::mutex> lock(checking_mutex);
                        // 检查page_id 是否变化
                        page_id_t old_page_id = checking_page_map[id];
                        if(old_page_id != page_id) {
                            // 如果page_id发生变化，记录变化
                            change_page_cnt++;
                            checking_page_map[id] = page_id; // Update the page ID
                            std::cout << "Page ID changed for ID: " << id 
                                      << ", Old Page ID: " << old_page_id 
                                      << ", New Page ID: " << page_id << std::endl;
                        }
                        page_update_cnt++;
                    }
                    break;
                }
                case 2: {  // Write check
                    pqxx::result result = txn->exec("UPDATE savings SET balance = balance - 50 WHERE id = " + 
                            std::to_string(account1) + " RETURNING ctid, id, balance");
                    if (!result.empty()) {
                        std::string ctid = result[0]["ctid"].as<std::string>();
                        int id = result[0]["id"].as<int>();
                        int balance = result[0]["balance"].as<int>();
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                        std::unique_lock<std::mutex> lock(savings_mutex);
                        // 检查page_id 是否变化
                        page_id_t old_page_id = savings_page_map[id];
                        if(old_page_id != page_id) {
                            // 如果page_id发生变化，记录变化
                            change_page_cnt++;
                            savings_page_map[id] = page_id; // Update the page ID
                            std::cout << "Page ID changed for ID: " << id 
                                      << ", Old Page ID: " << old_page_id 
                                      << ", New Page ID: " << page_id << std::endl;
                        }
                        page_update_cnt++;
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

void btree_background_thread(int btree_read_mode = 0, int frequency = 5) {
    std::cout << "Starting B-tree background thread..." << std::endl;
    std::cout << "Read mode: " << btree_read_mode << ", Frequency: " << frequency << " seconds" << std::endl;
    auto run_conn0 = new pqxx::connection(DBConnection[0]);
    auto run_conn1 = new pqxx::connection(DBConnection[1]);
    if (!run_conn0->is_open() || !run_conn1->is_open())
    {
        std::cerr << "Failed to connect to the database." << std::endl;
        return;
    }

    BtreeIndex btree_index_checking("idx_checking_id");
    BtreeIndex btree_index_savings("idx_savings_id");

    // Read B-tree metadata
    btree_index_checking.read_btree_meta(run_conn0);
    btree_index_savings.read_btree_meta(run_conn0);

    // Read B-tree root nodes
    btree_index_checking.read_btree_node_from_db(btree_index_checking.root_page_id, run_conn0);
    btree_index_savings.read_btree_node_from_db(btree_index_savings.root_page_id, run_conn0);

    // Read B-tree internal nodes
    btree_index_checking.read_all_internal_nodes(run_conn0);
    btree_index_savings.read_all_internal_nodes(run_conn0);

    // 每隔指定秒数检查一次 B-tree 索引
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(frequency));
        std::cout << "Checking B-tree index for checking..." << std::endl;
        pqxx::connection* conn = nullptr;
        if(btree_read_mode == 0) {
            conn = run_conn0; // Use the first connection for reading
        } else {
            conn = rand() % 2 == 0 ? run_conn0 : run_conn1; // Randomly select a connection
        }
        // Read B-tree metadata
        btree_index_checking.read_btree_meta(conn);
        btree_index_savings.read_btree_meta(conn);

        btree_index_checking.read_btree_node_from_db(btree_index_checking.root_page_id, conn);
        btree_index_savings.read_btree_node_from_db(btree_index_savings.root_page_id, conn);

        // Read B-tree internal nodes
        btree_index_checking.read_all_internal_nodes(conn);
        btree_index_savings.read_all_internal_nodes(conn);
        std::cout << "B-tree index for checking is up to date." << std::endl;
    }
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
    std::cout << "  --enable-btree-thread       Enable B-tree background monitoring thread [default: disabled]" << std::endl;
    std::cout << "  --btree-read-mode <mode>    B-tree read mode (0=conn0, 1=random) [default: 0]" << std::endl;
    std::cout << "  --btree-frequency <seconds> B-tree refresh frequency in seconds [default: 5]" << std::endl;
    std::cout << "  --account-count <number>    Number of accounts to load [default: 300000]" << std::endl;
    std::cout << "  --help                      Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " --system-mode 1 --access-pattern 1 --zipfian-theta 0.95" << std::endl;
    std::cout << "  " << program_name << " --access-pattern 2 --hotspot-fraction 0.2 --hotspot-prob 0.9" << std::endl;
    std::cout << "  " << program_name << " --system-mode 2 --enable-btree-thread --account-count 100000" << std::endl;
}

int main(int argc, char *argv[]) {
    // Register signal handler for SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler);
    signal(SIGPIPE, SIG_IGN);

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
        else if (arg == "--enable-btree-thread") {
            enable_btree_thread = true;
            std::cout << "B-tree background thread enabled" << std::endl;
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
        else {
            std::cerr << "Error: Unknown argument " << arg << std::endl;
            print_usage(argv[0]);
            return -1;
        }
    }

    // Display current configuration
    std::cout << "\n=== Configuration ===" << std::endl;
    std::cout << "System mode: " << system_mode << std::endl;
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
    
    std::cout << "B-tree thread enabled: " << (enable_btree_thread ? "Yes" : "No") << std::endl;
    if (enable_btree_thread) {
        std::cout << "B-tree read mode: " << read_btree_mode << std::endl;
        std::cout << "B-tree refresh frequency: " << read_frequency << " seconds" << std::endl;
    }
    std::cout << "====================" << std::endl;
    
    // --- Load Database Connection Info ---
    std::cout << "Loading database connection info..." << std::endl;

    DBConnection.push_back("host=10.12.2.125 port=54321 user=system password=123456 dbname=smallbank");
    DBConnection.push_back("host=10.12.2.127 port=54321 user=system password=123456 dbname=smallbank");
    
    // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");
    // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");

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

    // Load data into the database if needed
    load_data(conn0);
    std::cout << "Data loaded successfully." << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Create a Background B-tree index reading thread (if enabled)
    std::thread btree_thread;
    if (enable_btree_thread) {
        std::cout << "Creating B-tree background thread..." << std::endl;
        btree_thread = std::thread(btree_background_thread, read_btree_mode, read_frequency);
        btree_thread.detach(); // Detach the thread to run in the background
    } else {
        std::cout << "B-tree background thread is disabled" << std::endl;
    }

    // Create a performance snapshot
    int start_snapshot_id = create_perf_kwr_snapshot(conn0);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    // --- Start Transaction Threads ---
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Starting transaction threads..." << std::endl;

    std::vector<std::thread> threads;
    const int num_threads = 16;  // Number of transaction threads
    // Start the transaction threads
    for(int i = 0; i < num_threads; i++) {
        threads.emplace_back(run_smallbank_txns);
    }
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::cout << "All transaction threads completed." << std::endl;
    std::cout << "Total accounts loaded: " << smallbank_account << std::endl;
    std::cout << "Access pattern used: " << access_pattern_name << std::endl;
    std::cout << "Total transactions executed: " << exe_count << std::endl;
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    double s = ms / 1000.0; // Convert milliseconds to seconds
    std::cout << "Throughput: " << exe_count / s << " transactions per second" << std::endl;
    std::cout << "Page ID changes: " << change_page_cnt << std::endl;
    std::cout << "Page updates: " << page_update_cnt << std::endl;
    
    if (enable_btree_thread) {
        std::cout << "B-tree background thread was running with:" << std::endl;
        std::cout << "  - Read mode: " << read_btree_mode << std::endl;
        std::cout << "  - Refresh frequency: " << read_frequency << " seconds" << std::endl;
    } else {
        std::cout << "B-tree background thread was disabled" << std::endl;
    }

    // Create a performance snapshot after running transactions
    std::this_thread::sleep_for(std::chrono::seconds(2)); // sleep for a while to ensure all operations are completed
    int end_snapshot_id = create_perf_kwr_snapshot(conn0);
    std::cout << "Performance snapshots created: Start ID = " << start_snapshot_id 
              << ", End ID = " << end_snapshot_id << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2)); // sleep for a while to ensure all operations are completed

    // Generate performance report, file name inluding the timestamp
    // get the current time as a string
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm* now_tm = std::localtime(&now_time);
    char buffer[100];
    std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", now_tm);
    std::string timestamp(buffer);  
    std::string report_file = "smallbank_report_" + timestamp + ".html";
    generate_perf_kwr_report(conn0, start_snapshot_id, end_snapshot_id, report_file);

    // 关闭连接
    delete conn0;
    delete conn1;
    
    // 清理Zipfian生成器
    if (zipfian_gen) {
        delete zipfian_gen;
        zipfian_gen = nullptr;
    }

    return 0;
}