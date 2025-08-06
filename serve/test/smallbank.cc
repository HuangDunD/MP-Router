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
#include "util/json_config.h"

std::vector<std::string> DBConnection;
auto start = std::chrono::high_resolution_clock::now();
int try_count = 20000;
std::atomic<int> exe_count = 0;

int smallbank_account = 1000000;

int system_mode = 0;

std::mutex savings_mutex;
std::mutex checking_mutex;
std::unordered_map<itemkey_t, page_id_t> savings_page_map;
std::unordered_map<itemkey_t, page_id_t> checking_page_map;
std::atomic<int> change_page_cnt = 0;
std::atomic<int> page_update_cnt = 0;

// 解析ctid字符串，提取page_id
// ctid格式为 "(page_id,tuple_index)"，例如 "(0,1)"
page_id_t parse_page_id_from_ctid(const std::string& ctid) {
    // 查找第一个逗号的位置
    size_t comma_pos = ctid.find(',');
    if (comma_pos == std::string::npos) {
        std::cerr << "Invalid ctid format: " << ctid << std::endl;
        return -1;
    }
    
    // 提取page_id部分（去掉开头的括号）
    std::string page_id_str = ctid.substr(1, comma_pos - 1);
    
    try {
        return std::stoll(page_id_str);
    } catch (const std::exception& e) {
        std::cerr << "Failed to parse page_id from ctid: " << ctid << ", error: " << e.what() << std::endl;
        return -1;
    }
}

void load_data(pqxx::connection *conn0) {
    std::cout << "Loading data..." << std::endl;
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
                    page_id_t page_id = parse_page_id_from_ctid(ctid);
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
                    page_id_t page_id = parse_page_id_from_ctid(ctid);
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
        int account1 = rand() % smallbank_account + 1;
        int account2 = rand() % smallbank_account + 1;
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
                        page_id_t page_id = parse_page_id_from_ctid(ctid);
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
                        page_id_t page_id = parse_page_id_from_ctid(ctid);
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
                        page_id_t page_id = parse_page_id_from_ctid(ctid);
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
                        page_id_t page_id = parse_page_id_from_ctid(ctid);
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

int main(int argc, char *argv[]) {
    // Register signal handler for SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    if(argc > 1) {
        system_mode = std::stoi(argv[1]);
        std::cout << "System mode set to: " << system_mode << std::endl;
    } else {
        std::cout << "No system mode specified, defaulting to 0." << std::endl;
    }
    
    // --- Load Database Connection Info ---
    std::cout << "Loading database connection info..." << std::endl;

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
    double s = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "All transaction threads completed." << std::endl;
    std::cout << "Total transactions executed: " << exe_count << std::endl;
    std::cout << "Elapsed time: " << s << " seconds" << std::endl;
    std::cout << "Throughput: " << exe_count / s << " transactions per second" << std::endl;
    std::cout << "Page ID changes: " << change_page_cnt << std::endl;
    std::cout << "Page updates: " << page_update_cnt << std::endl;

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

    return 0;
}