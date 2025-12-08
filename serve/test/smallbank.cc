// Copyright 2025
// Author: huangdund
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
#include <fstream>
#include <streambuf>
#include <iomanip>

#include "smallbank.h"
#include "common.h"
#include "btree_search.h"
#include "smart_router.h"
#include "util/zipf.h"
#include "router_stat_snapshot.h"
#include "txn_queue.h"
#include "tit.h"

std::atomic<uint64_t> tx_id_generator;
auto start = std::chrono::high_resolution_clock::now();
int try_count = 10000;
std::atomic<int> exe_count = 0; // 这个是所有线程的总事务数
std::atomic<int> generated_txn_count = 0; // 这个是所有线程生成的总事务数

SmallBank* smallbank = nullptr;

SmartRouter* smart_router = nullptr;
TxnPool* txn_pool = nullptr;
std::vector<TxnQueue*> txn_queues; // one queue per compute node

thread_local uint64_t thread_gid;
// Global Zipfian generator
thread_local ZipfGen* zipfian_gen = nullptr;

std::vector<std::atomic<int>> exec_txn_cnt_per_node(MaxComputeNodeCount); // 每个节点路由的事务数

// 全局事务信息表
SlidingTransactionInforTable* tit;

// A streambuf that tees output to two destinations (console + file)
class TeeBuf : public std::streambuf {
public:
    TeeBuf(std::streambuf* sb1, std::streambuf* sb2) : sb1_(sb1), sb2_(sb2) {}
protected:
    int overflow(int c) override {
        if (c == EOF) return !EOF;
        int const r1 = sb1_ ? sb1_->sputc(c) : c;
        int const r2 = sb2_ ? sb2_->sputc(c) : c;
        return (r1 == EOF || r2 == EOF) ? EOF : c;
    }
    int sync() override {
        int const r1 = sb1_ ? sb1_->pubsync() : 0;
        int const r2 = sb2_ ? sb2_->pubsync() : 0;
        return (r1 == 0 && r2 == 0) ? 0 : -1;
    }
private:
    std::streambuf* sb1_;
    std::streambuf* sb2_;
};

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
        txn.exec("CREATE unlogged TABLE checking (id INT, balance INT, city INT, name CHAR(500)) WITH (FILLFACTOR = 50)");
        txn.exec("CREATE unlogged TABLE savings (id INT, balance INT, city INT, name CHAR(500)) WITH (FILLFACTOR = 50)");
        // create index
        txn.exec("CREATE INDEX idx_checking_id ON checking (id)");
        txn.exec("CREATE INDEX idx_savings_id ON savings (id)");
        std::cout << "Tables created successfully." << std::endl;
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating table: " << e.what() << std::endl;
    }   
    try {
        pqxx::work txn(*conn0);
        txn.exec(R"SQL(
        ALTER TABLE checking SET ( 
            autovacuum_enabled = on,
            autovacuum_vacuum_scale_factor = 0.05,   
            autovacuum_vacuum_threshold = 500,       
            autovacuum_analyze_scale_factor = 0.05,
            autovacuum_analyze_threshold = 500
        );
        )SQL");
        std::cout << "Set checking table autovacuum parameters." << std::endl;
        txn.exec(R"SQL(
        ALTER TABLE savings SET ( 
            autovacuum_enabled = on,
            autovacuum_vacuum_scale_factor = 0.05,   
            autovacuum_vacuum_threshold = 500,       
            autovacuum_analyze_scale_factor = 0.05,
            autovacuum_analyze_threshold = 500
        );
        )SQL");
        std::cout << "Set savings table autovacuum parameters." << std::endl;
        txn.commit();
    }
    catch (const std::exception &e) {
        std::cerr << "Error while setting checking table autovacuum: " << e.what() << std::endl;
    }
    std::thread extend_thread1([](){
        pqxx::connection conn_extend(DBConnection[0]);
        if (!conn_extend.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        try{
            // pg not support
            pqxx::nontransaction txn(conn_extend);
            // pre-extend table to avoid frequent page extend during txn processing
            std::string extend_sql = "SELECT sys_extend('checking', " + std::to_string(PreExtendPageSize) + ")";
            txn.exec(extend_sql);
            std::cout << "Pre-extended checking table." << std::endl;
        }
        catch (const std::exception &e) {
            std::cerr << "Error while pre-extending checking table: " << e.what() << std::endl;
        }
    });

    std::thread extend_thread2([](){
        pqxx::connection conn_extend(DBConnection[0]);
        if (!conn_extend.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        try{
            // pg not support
            pqxx::nontransaction txn(conn_extend);
            // pre-extend table to avoid frequent page extend during txn processing
            std::string extend_sql = "SELECT sys_extend('savings', " + std::to_string(PreExtendPageSize) + ")";
            txn.exec(extend_sql);
            std::cout << "Pre-extended savings table." << std::endl;
        }
        catch (const std::exception &e) {
            std::cerr << "Error while pre-extending savings table: " << e.what() << std::endl;
        }
    });
    extend_thread1.join();
    extend_thread2.join();
    std::cout << "Table creation and pre-extension completed." << std::endl;
}

void load_data(pqxx::connection *conn0) {
    auto smallbank_account = smallbank->get_account_count();
    std::cout << "Loading data..." << std::endl;
    std::cout << "Will load " << smallbank_account << " accounts into checking and savings tables" << std::endl;
    // Load data into the database if needed
    // Insert data into checking and savings tables
    const int num_threads = 16;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;
    // 这里创建一个导入数据的账户id的列表, 随机导入
    std::vector<int> id_list;
    for(int i = 1; i <= smallbank_account; i++) id_list.push_back(i);
    // 随机打乱 id_list
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(id_list.begin(), id_list.end(), g);
    auto worker = [&id_list, smallbank_account](int start, int end) {
        pqxx::connection conn00(DBConnection[0]);
        if (!conn00.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        int city_cnt = static_cast<int>(SmallBankCityType::Count);
        for(int i = start; i < end; i++) {
            int id = id_list[i];
            int balance = 1000 + ((id - 1) % 1000); // Random balance
            std::string name = "Account_" + std::to_string(id);
            int city = ((id - 1) / (smallbank_account / city_cnt) ) % city_cnt;
            assert(city >=0 && city < city_cnt);
            // 使用RETURNING子句获取插入数据的位置信息
            std::string insert_checking_sql = "INSERT INTO checking (id, balance, city, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", " +
                                        std::to_string(city) + ", '" +
                                        name + "') RETURNING ctid, id";
            std::string insert_savings_sql = "INSERT INTO savings (id, balance, city, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", " +
                                        std::to_string(city) + ", '" +
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
                    if(smart_router){
                        smart_router->initial_key_page((table_id_t)SmallBankTableType::kCheckingTable, inserted_id, page_id);
                    }
                }
                
                // 执行savings表插入并获取位置信息
                pqxx::result savings_result = txn_create.exec(insert_savings_sql);
                if (!savings_result.empty()) {
                    std::string ctid = savings_result[0]["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = savings_result[0]["id"].as<int>();
                    if(smart_router){
                        smart_router->initial_key_page((table_id_t)SmallBankTableType::kSavingsTable, inserted_id, page_id);
                    }
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
    std::thread friend_thread([&]() {
        smallbank->generate_friend_graph();
    });

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
            // + std::to_string(end_snapshot_id) + ", 'html', '/home/hcy/MP-Router/build/serve/test/" + file_name + "')";
            + std::to_string(end_snapshot_id) + ", 'html', '/home/kingbase/MP-Router/kwr/" + file_name + "')";
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

void generate_smallbank_txns_worker(thread_params* params) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("txn_gen_t_" + std::to_string(params->thread_id)).c_str());

    std::vector<itemkey_t> accounts_vec(2);
    if (!zipfian_gen) {
        uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
        uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
        zipfian_gen = new ZipfGen(smallbank->get_account_count(), params->zipfian_theta, zipf_seed & zipf_seed_mask);
    }

    // 全局一共进行 MetisWarmupRound * PARTITION_INTERVAL的冷启动事务生成，每个工作节点具有worker_threads个线程，每个线程生成try_count个事务
    int total_txn_to_generate = MetisWarmupRound * PARTITION_INTERVAL + try_count * worker_threads * ComputeNodeCount;
    while(generated_txn_count < total_txn_to_generate) {
        std::vector<TxnQueueEntry*> txn_batch;
        for (int i = 0; i < 100; i++){
            generated_txn_count++;
            tx_id_t tx_id = tx_id_generator++; // global atomic transaction ID
            // Simulate some work
            // Randomly select a transaction type and accounts
            int txn_type = smallbank->generate_txn_type();
            if(txn_type == 0 || txn_type == 1) { // TxAmagamate or TxSendPayment
                smallbank->generate_two_account_ids(accounts_vec[0], accounts_vec[1], zipfian_gen);
            } else {
                smallbank->generate_account_id(accounts_vec[0], zipfian_gen);
            }
            // Create a new transaction object
            TxnQueueEntry* txn_entry = new TxnQueueEntry(tx_id, txn_type, accounts_vec);
            txn_batch.push_back(txn_entry);
        }
        // Enqueue the transaction into the global transaction pool
        // txn_pool->receive_txn_from_client(txn_entry);
        txn_pool->receive_txn_from_client_batch(txn_batch);
    }
    txn_pool->stop_pool();
}

void run_smallbank_empty(thread_params* params, Logger* logger_){
    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

    // init the thread id
    thread_gid = params->thread_id;

    int con_batch_id = 0;
    while (true) {
        std::list<TxnQueueEntry*> txn_entries = txn_queue->pop_txn();
        if (txn_entries.empty()) {
            if(txn_queue->is_finished()) {
                // 说明该计算节点的事务队列已经均处理完成, 可以退出线程了
                break;
            }
            else if(txn_queue->is_batch_finished()) {
                // 说明该计算节点的该批事务已经均处理完成, 告诉smart router 该批次这个节点完成了
                smart_router->notify_batch_finished(compute_node_id, params->thread_id, con_batch_id);
                // 等待下一批事务到来，即 batch_finished 标志被重置
                smart_router->wait_for_next_batch(compute_node_id, params->thread_id, con_batch_id);
                con_batch_id++;
                continue; // 重新进入循环，处理下一批事务
            }
            else assert(false);
        }
        for(auto& txn_entry : txn_entries) {
            // just for statistics
            exe_count++;
            // statistics
            exec_txn_cnt_per_node[compute_node_id]++;
            tit->mark_done(txn_entry); // 一体化：标记完成，删除由 TIT 统一管理
        }
    }
    std::cout << "Empty txn worker thread " << params->thread_id << " on compute node " 
              << compute_node_id << " finished processing." << std::endl;
}

// this function runs smallbank transactions for one compute node
void run_smallbank_txns(thread_params* params, Logger* logger_) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("dbcon_n" + std::to_string(params->compute_node_id_connecter)
                                                + "_t_" + std::to_string(params->thread_id)).c_str());

    std::cout << "Running smallbank transactions..." << std::endl;

    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

    // init the thread id
    thread_gid = params->thread_id;

    // run smallbank transactions
    // for (int i = 0; i < try_count; ++i) {
    int con_batch_id = 0;
    while (true) {
        // ! Fetch a transaction from the global transaction pool
        // ! pay attention here, different system mode may have different txn fetching strategy, now all use pool front
        std::list<TxnQueueEntry*> txn_entries = txn_queue->pop_txn();
        if (txn_entries.empty()) {
            if(txn_queue->is_finished()) {
                // 说明该计算节点的事务队列已经均处理完成, 可以退出线程了
                break;
            }
            else if(txn_queue->is_batch_finished()) {
                // 说明该计算节点的该批事务已经均处理完成, 告诉smart router 该批次这个节点完成了
                smart_router->notify_batch_finished(compute_node_id, params->thread_id, con_batch_id);
                // 等待下一批事务到来，即 batch_finished 标志被重置
                smart_router->wait_for_next_batch(compute_node_id, params->thread_id, con_batch_id);
                con_batch_id++;
                continue; // 重新进入循环，处理下一批事务
            }
            else assert(false);
        }

        // 执行std::list<TxnQueueEntry*> txn_entries中的每个事务
        for (auto& txn_entry : txn_entries) {
            exe_count++;
            // statistics
            exec_txn_cnt_per_node[compute_node_id]++;

            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            itemkey_t account1 = txn_entry->accounts[0];
            itemkey_t account2 = txn_entry->accounts[1];
            int txn_decision_type = txn_entry->txn_decision_type;
            
            // Create a new transaction
            while (con->is_open() == false) {
                std::cerr << "Connection is broken, reconnecting..." << std::endl;
                delete con;
                con = new pqxx::connection(con_str);
            }
            pqxx::work* txn = new pqxx::work(*con);

            // init the table ids and keys
            std::vector<table_id_t>& tables = smallbank->get_table_ids_by_txn_type(txn_type);
            assert(tables.size() > 0);
            std::vector<itemkey_t> keys;
            smallbank->get_keys_by_txn_type(txn_type, account1, account2, keys);
            assert(tables.size() == keys.size());
            std::vector<page_id_t> ctid_ret_page_ids; 
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
                            ctid_ret_page_ids.push_back(page_id);
                        }
                        pqxx::result result2 = txn->exec("UPDATE savings SET balance = 0 WHERE id = " + 
                                std::to_string(account1) + " RETURNING ctid, id, balance");
                        if (!result2.empty()) {
                            std::string ctid = result2[0]["ctid"].as<std::string>();
                            int id = result2[0]["id"].as<int>();
                            int balance = result2[0]["balance"].as<int>();
                            auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                            savings_balance = result2[0]["balance"].as<int>();
                            ctid_ret_page_ids.push_back(page_id);
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
                            ctid_ret_page_ids.push_back(page_id);
                        }
                        break;
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
                            ctid_ret_page_ids.push_back(page_id);
                        }
                        
                        // 第二次更新：增加余额并获取位置信息
                        pqxx::result result2 = txn->exec("UPDATE checking SET balance = balance + 10 WHERE id = " + 
                                std::to_string(account2) + " RETURNING ctid, id, balance");
                        if (!result2.empty()) {
                            std::string ctid = result2[0]["ctid"].as<std::string>();
                            int id = result2[0]["id"].as<int>();
                            int balance = result2[0]["balance"].as<int>();
                            auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                            ctid_ret_page_ids.push_back(page_id);
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
                            ctid_ret_page_ids.push_back(page_id);
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
                            ctid_ret_page_ids.push_back(page_id);
                        }
                        if (!result2.empty()) {
                            std::string ctid = result2[0]["ctid"].as<std::string>();
                            int id = result2[0]["id"].as<int>();
                            int balance = result2[0]["balance"].as<int>();
                            auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                            ctid_ret_page_ids.push_back(page_id);
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
                            ctid_ret_page_ids.push_back(page_id);
                        } 
                        if (!result2.empty()) {
                            std::string ctid = result2[0]["ctid"].as<std::string>();
                            int id = result2[0]["id"].as<int>();
                            int balance = result2[0]["balance"].as<int>();
                            auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                            ctid_ret_page_ids.push_back(page_id);
                        }
                        break;
                    }
                    case 5: { // TxTransactSavings
                        pqxx::result result = txn->exec("UPDATE savings SET balance = balance + 20 WHERE id = " + 
                                std::to_string(account1) + " RETURNING ctid, id, balance");
                        if (!result.empty()) {
                            std::string ctid = result[0]["ctid"].as<std::string>();
                            int id = result[0]["id"].as<int>();
                            int balance = result[0]["balance"].as<int>();
                            auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                            ctid_ret_page_ids.push_back(page_id);
                        }
                        break;
                    }
                }
                txn->commit();
                // update the smart router page map if needed
                if(smart_router) smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables), keys, ctid_ret_page_ids, compute_node_id);
                
            } catch (const std::exception &e) {
                std::cerr << "Transaction failed: " << e.what() << std::endl;
                logger_->info("Transaction failed: " + std::string(e.what()));
            }
            tit->mark_done(txn_entry); // 一体化：标记完成，删除由 TIT 统一管理
            delete txn;
            // std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    std::cout << "Finished running smallbank transactions." << std::endl;
}

// Run transactions using stored procedures
void run_smallbank_txns_sp(thread_params* params, Logger* logger_) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("dbconsp_n" + std::to_string(params->compute_node_id_connecter)
                                                + "_t_" + std::to_string(params->thread_id)).c_str());

    std::cout << "Running smallbank transactions via stored procedures..." << std::endl;

    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

    // init the thread id
    thread_gid = params->thread_id;

    int con_batch_id = 0;
    while (true) {
        // 计时 
        timespec pop_start_time, pop_end_time;
        clock_gettime(CLOCK_MONOTONIC, &pop_start_time);

        std::list<TxnQueueEntry*> txn_entries = txn_queue->pop_txn();
        // if(WarmupEnd)
        //     logger_->info("Compute Node " + std::to_string(compute_node_id) + 
        //                 " Thread " + std::to_string(params->thread_id) + 
        //                 " popped " + std::to_string(txn_entries.size()) + " txns in batch " + 
        //                 std::to_string(con_batch_id));

        clock_gettime(CLOCK_MONOTONIC, &pop_end_time);
        double pop_time = (pop_end_time.tv_sec - pop_start_time.tv_sec) * 1000.0 +
                          (pop_end_time.tv_nsec - pop_start_time.tv_nsec) / 1000000.0;
        smart_router->add_worker_thread_pop_time(params->compute_node_id_connecter, params->thread_id, pop_time);

        if (txn_entries.empty()) {
            if(txn_queue->is_finished()) {
                break;
            } else if(txn_queue->is_batch_finished()) {
                smart_router->notify_batch_finished(compute_node_id, params->thread_id, con_batch_id);
                // 计时
                timespec wait_start_time, wait_end_time;
                clock_gettime(CLOCK_MONOTONIC, &wait_start_time);
                smart_router->wait_for_next_batch(compute_node_id, params->thread_id, con_batch_id);
                clock_gettime(CLOCK_MONOTONIC, &wait_end_time);
                double wait_time = (wait_end_time.tv_sec - wait_start_time.tv_sec) * 1000.0 +
                                   (wait_end_time.tv_nsec - wait_start_time.tv_nsec) / 1000000.0;
                smart_router->add_worker_thread_wait_next_batch_time(params->compute_node_id_connecter, params->thread_id, wait_time);
                con_batch_id++;
                continue;
            } else continue;
        }

        for (auto& txn_entry : txn_entries) {
            exe_count++;
            exec_txn_cnt_per_node[compute_node_id]++;

            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            itemkey_t account1 = txn_entry->accounts[0];
            itemkey_t account2 = txn_entry->accounts[1];

            while (con->is_open() == false) {
                std::cerr << "Connection is broken, reconnecting..." << std::endl;
                delete con;
                con = new pqxx::connection(con_str);
            }

            std::vector<table_id_t>& tables = smallbank->get_table_ids_by_txn_type(txn_type);
            assert(tables.size() > 0);
            std::vector<itemkey_t> keys;
            smallbank->get_keys_by_txn_type(txn_type, account1, account2, keys);
            assert(tables.size() == keys.size());
            std::vector<page_id_t> ctid_ret_page_ids;

            // 计时
            timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            // !不需要外层事务
            pqxx::nontransaction txn(*con);

            try {
                pqxx::result res;
                switch(txn_type) {
                    case 0: { // Amalgamate
                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_amalgamate(" +
                                          std::to_string(account1) + "," + std::to_string(account2) + ")";
                        res = txn.exec(sql);
                        break;
                    }
                    case 1: { // SendPayment
                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_send_payment(" +
                                          std::to_string(account1) + "," + std::to_string(account2) + ")";
                        res = txn.exec(sql);
                        break;
                    }
                    case 2: { // DepositChecking
                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_deposit_checking(" +
                                          std::to_string(account1) + ")";
                        res = txn.exec(sql);
                        break;
                    }
                    case 3: { // WriteCheck
                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_write_check(" +
                                          std::to_string(account1) + ")";
                        res = txn.exec(sql);
                        break;
                    }
                    case 4: { // Balance
                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_balance(" +
                                          std::to_string(account1) + ")";
                        res = txn.exec(sql);
                        break;
                    }
                    case 5: { // TransactSavings
                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_transact_savings(" +
                                          std::to_string(account1) + ")";
                        res = txn.exec(sql);
                        break;
                    }
                }

                for (const auto& row : res) {
                    std::string ctid_str = row["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid_str);
                    ctid_ret_page_ids.push_back(page_id);
                }

                // ! 无需commit 外层事务
                // txn->commit();

                if(smart_router) {
                    smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables),
                                                  keys, ctid_ret_page_ids, compute_node_id);
                }
            } catch (const std::exception &e) {
                std::cerr << "Transaction (SP) failed: " << e.what() << std::endl;
                logger_->info("Transaction (SP) failed: " + std::string(e.what()));
            }

            // 计时结束
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double exec_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_exec_time(params->compute_node_id_connecter, params->thread_id, exec_time);

            tit->mark_done(txn_entry); // 一体化：标记完成，删除由 TIT 统一管理
        }
    }
    std::cout << "Finished running smallbank transactions via stored procedures." << std::endl;
}

void signal_handler(int signum) {
    std::cout << "\nCaught signal " << signum << " (SIGINT)" << std::endl;
    std::cout << "Printing final statistics before exit..." << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    if(smart_router){
        std::cout << "********** Smart Router page stats **********" << std::endl;
        int change_page_cnt = smart_router->get_stats().change_page_cnt;
        int page_update_cnt = smart_router->get_stats().page_update_cnt;
        int hit_cnt = smart_router->get_stats().hot_hit;
        int miss_cnt = smart_router->get_stats().hot_miss;
        std::cout << "Hot page hit: " << hit_cnt << ", miss: " << miss_cnt 
                  << ", hit ratio: " << (hit_cnt + miss_cnt > 0 ? (double)hit_cnt / (hit_cnt + miss_cnt) * 100.0 : 0.0) << "%" << std::endl;
        std::cout << "Page ID changes: " << change_page_cnt << std::endl;
        std::cout << "Ownership changes: " << smart_router->get_ownership_changes() << std::endl;
        std::cout << "Page Operations count: " << page_update_cnt << std::endl; 
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

void print_tps_loop() {
    using namespace std::chrono;
    uint64_t exec_last_count = 0;
    uint64_t route_last_count = 0;
    auto last_time = steady_clock::now();
    std::vector<int> routed_txn_cnt_per_node_last_snapshot, routed_txn_cnt_per_node_snapshot;
    std::vector<int> exec_txn_cnt_per_node_last_snapshot, exec_txn_cnt_per_node_snapshot, txn_queue_size_snapshot;
    routed_txn_cnt_per_node_last_snapshot.resize(ComputeNodeCount, 0);
    routed_txn_cnt_per_node_snapshot.resize(ComputeNodeCount, 0);
    exec_txn_cnt_per_node_last_snapshot.resize(ComputeNodeCount, 0);
    exec_txn_cnt_per_node_snapshot.resize(ComputeNodeCount, 0);
    txn_queue_size_snapshot.resize(ComputeNodeCount, 0);
    uint64_t page_op_last_count = 0;
    uint64_t cache_fusion_last_count = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        auto now = steady_clock::now();
        uint64_t exec_cur_count = exe_count.load(std::memory_order_relaxed);
        uint64_t route_cur_count = 0;
        uint64_t page_op_cur_count = 0;
        uint64_t cache_fusion_cur_count = 0;
        double seconds = duration_cast<duration<double>>(now - last_time).count();
        double exec_tps = (exec_cur_count - exec_last_count) / seconds;
        // print routed txn count per node
        auto routed_txn_cnt_per_node = smart_router->get_routed_txn_cnt_per_node();
        for(int i=0; i<ComputeNodeCount; i++) {
            routed_txn_cnt_per_node_snapshot[i] = routed_txn_cnt_per_node[i];
            route_cur_count += routed_txn_cnt_per_node_snapshot[i];
        }
        double route_tps = (route_cur_count - route_last_count) / seconds;
        std::cout << "[Routed TPS] " << std::fixed << std::setprecision(2) << route_tps
                  << " txn/sec (total: " << exec_cur_count << "). ";
        for(int i=0; i<ComputeNodeCount; i++) {
            int routed_cnt = routed_txn_cnt_per_node_snapshot[i] - routed_txn_cnt_per_node_last_snapshot[i];
            routed_txn_cnt_per_node_last_snapshot[i] = routed_txn_cnt_per_node_snapshot[i];
            std::cout << "Node " << i << ": " << routed_cnt << ' ';
        }
        if(smart_router) {
            page_op_cur_count = smart_router->get_stats().page_update_cnt;
            cache_fusion_cur_count = smart_router->get_ownership_changes();
            uint64_t page_op_cnt = page_op_cur_count - page_op_last_count;
            uint64_t cache_fusion_cnt = cache_fusion_cur_count - cache_fusion_last_count;
            page_op_last_count = page_op_cur_count;
            cache_fusion_last_count = cache_fusion_cur_count;
            std::cout << " [Page Ops]: " << page_op_cnt << " [Cache Fusions]: " << cache_fusion_cnt <<  " ratio: " << (page_op_cnt > 0 ? (double)cache_fusion_cnt / page_op_cnt : 0.0) * 100.0 << "% ";
        }
        std::cout << '\n';
        // print exec txn count per node
        for(int i=0; i<ComputeNodeCount; i++) exec_txn_cnt_per_node_snapshot[i] = exec_txn_cnt_per_node[i].load(std::memory_order_relaxed);
        for(int i=0; i<ComputeNodeCount; i++) txn_queue_size_snapshot[i] = txn_queues[i]->size();
        std::cout << "[Exec TPS] " << std::fixed << std::setprecision(2) << exec_tps
                  << " txn/sec (total: " << exec_cur_count << "). ";
        for(int i=0; i<ComputeNodeCount; i++) {
            int routed_cnt = exec_txn_cnt_per_node_snapshot[i] - exec_txn_cnt_per_node_last_snapshot[i];
            exec_txn_cnt_per_node_last_snapshot[i] = exec_txn_cnt_per_node_snapshot[i];
            std::cout << "Node " << i << ": " << routed_cnt << " queue remain: " << txn_queue_size_snapshot[i] << ' ';
        }
        int pool_size = txn_pool->size();
        std::cout << " Txn pool size: " << pool_size << ' ' << '\n';
        // reset for next interval
        exec_last_count = exec_cur_count;
        route_last_count = route_cur_count;
        last_time = now;
    }
}

int main(int argc, char *argv[]) {
    // Tee std::cout/std::cerr to both console and output.txt for easy collection
    static std::ofstream output_file("result.txt", std::ios::out | std::ios::trunc);
    static TeeBuf cout_tbuf(std::cout.rdbuf(), output_file.rdbuf());
    static TeeBuf cerr_tbuf(std::cerr.rdbuf(), output_file.rdbuf());
    std::streambuf* old_cout = std::cout.rdbuf(&cout_tbuf);
    std::streambuf* old_cerr = std::cerr.rdbuf(&cerr_tbuf);
    // Make cout auto-flush after each insertion to keep file updated
    std::cout.setf(std::ios::unitbuf);

    Logger* logger_ = new Logger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::INFO, partition_log_file_, 4096);
    // Register signal handler for SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    int system_mode = 0; // Default system mode
    int access_pattern = 0; // 0: uniform, 1: zipfian, 2: hotspot
    // default parameters
    double zipfian_theta = 0.99; // Zipfian distribution parameter
    double hotspot_fraction = 0.2; // Fraction of accounts that are hot
    double hotspot_access_prob = 0.8; // Probability of accessing hot accounts
    // execution mode
    bool use_sp = true; // default: use stored procedures
    
    // for smallbank
    int smallbank_account = 300000; // Number of accounts to load

    // btree read parameters
    int read_btree_mode = 0; // 0: read from conn0, 1: read from random conn
    int read_frequency = 5; // seconds

    // kwr report name
    std::string kwr_report_name = "kwr_report";

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
        else if (arg == "--use-sp") {
            if (i + 1 < argc) {
                int v = std::stoi(argv[++i]);
                use_sp = (v != 0);
                std::cout << "Use stored procedures: " << (use_sp ? "yes" : "no") << std::endl;
            } else {
                std::cerr << "Error: --use-sp requires 0 or 1" << std::endl;
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
                std::cout << "input try count is " << try_count << std::endl;
                // try_count 是每个线程的尝试次数，总尝试次数要乘以线程数
                // try_count += MetisWarmupRound * PARTITION_INTERVAL / worker_threads; // add warmup rounds
                // std::cout << "Total try count (including warmup) is " << try_count << " per thread. " << std::endl;
            } else {
                std::cerr << "Error: --try-count requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--kwr-name") {
            if (i + 1 < argc) {
                kwr_report_name = argv[++i];
                std::cout << "KWR report name set to: " << kwr_report_name << std::endl;
            }
            else{
                // get the current time as a string
                auto now = std::chrono::system_clock::now();
                std::time_t now_time = std::chrono::system_clock::to_time_t(now);
                std::tm* now_tm = std::localtime(&now_time);
                char buffer[100];
                std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", now_tm);
                std::string timestamp(buffer);  
                kwr_report_name = "smallbank_report_" + timestamp + "_mode" + std::to_string(SYSTEM_MODE);
                std::cout << "KWR report name set to: " << kwr_report_name << std::endl;
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
    case 7:
        std::cout << "\033[31m  page ownership history \033[0m" << std::endl;
        break;
    case 8: 
        std::cout << "\033[31m  hybrid router (page affinity + ownership history) \033[0m" << std::endl;
        break;
    case 9:
        std::cout << "\033[31m  2 phase switch \033[0m" << std::endl;
        break;
    case 10:
        std::cout << "\033[31m  k-router \033[0m" << std::endl;
        break;
    case 11:
        std::cout << "\033[31m  k-router-pipeline \033[0m" << std::endl;
        break;
    case 13: 
        std::cout << "\033[31m  score-based router \033[0m" << std::endl;
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
    std::cout << "Use stored procedures: " << (use_sp ? "yes" : "no") << std::endl;
    
    if (access_pattern == 1) {
        std::cout << "Zipfian theta: " << zipfian_theta << std::endl;
    } else if (access_pattern == 2) {
        std::cout << "Hotspot fraction: " << hotspot_fraction << std::endl;
        std::cout << "Hotspot access probability: " << hotspot_access_prob << std::endl;
    }
    
    // --- Initialize SmallBank Benchmark ---
    smallbank = new SmallBank(smallbank_account, access_pattern); 
    if(access_pattern == 2) smallbank->set_hotspot_params(hotspot_fraction, hotspot_access_prob); 
    std::cout << "SmallBank benchmark initialized." << std::endl; 

    std::cout << "Worker threads: " << worker_threads << std::endl;
    std::cout << "====================" << std::endl;

    // --- Load Database Connection Info ---
    std::cout << "Loading database connection info..." << std::endl;

    // !!! need to update when changing the cluster environment
    // DBConnection.push_back("host=10.12.2.125 port=54321 user=system password=123456 dbname=smallbank");
    // DBConnection.push_back("host=10.12.2.127 port=54321 user=system password=123456 dbname=smallbank");

    // kes 双机, 旧版本
    // DBConnection.push_back("host=10.10.2.41 port=54321 user=system password=123456 dbname=smallbank");
    // DBConnection.push_back("host=10.10.2.42 port=54321 user=system password=123456 dbname=smallbank");

    // kes 双机, 新版本
    DBConnection.push_back("host=10.10.2.41 port=44321 user=system password=123456 dbname=smallbank");
    DBConnection.push_back("host=10.10.2.42 port=44321 user=system password=123456 dbname=smallbank");

    // kes 单机
    // DBConnection.push_back("host=10.10.2.41 port=64321 user=system password=123456 dbname=smallbank");
    // DBConnection.push_back("host=10.10.2.41 port=64321 user=system password=123456 dbname=smallbank");

    // 147 本机 pg
    // DBConnection.push_back("host=127.0.0.1 port=6432 user=hcy password=123456 dbname=smallbank"); // pg12
    // DBConnection.push_back("host=127.0.0.1 port=6432 user=hcy password=123456 dbname=smallbank"); // pg12 

    // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank"); // pg13
    // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank"); // pg13
    ComputeNodeCount = DBConnection.size();
    std::cout << "Database connection info loaded. Total nodes: " << ComputeNodeCount << std::endl;

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
    // Create stored procedures for SP-based execution (only if enabled)
    if (use_sp) create_smallbank_stored_procedures(conn0);

    // Initialize Smart Router anyway
    std::cout << "Initializing Smart Router..." << std::endl;
    // Create a BtreeService
    BtreeIndexService *index_service = new BtreeIndexService(DBConnection, {"idx_checking_id", "idx_savings_id"}, read_btree_mode, read_frequency);
    // initialize the transaction pool
    tit = new SlidingTransactionInforTable(logger_, 10*BatchRouterProcessSize);
    txn_pool = new TxnPool(TxnPoolMaxSize, tit);
    auto shared_txn_queue = new SharedTxnQueue(tit, logger_, TxnQueueMaxSize);
    // initialize the transaction queues for each compute node connection
    for (int i = 0; i < ComputeNodeCount; i++) { 
        txn_queues.push_back(new TxnQueue(tit, shared_txn_queue, logger_, i, TxnQueueMaxSize));
    }
    // Initialize NewMetis
    NewMetis* metis = new NewMetis(logger_);

    SmartRouter::Config cfg{};
    smart_router = new SmartRouter(cfg, txn_pool, txn_queues, worker_threads, index_service, metis, logger_);
    std::cout << "Smart Router initialized." << std::endl;

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

    std::cout << "Create the smart router snapshot." << std::endl;
    RouterStatSnapshot snapshot0, snapshot1, snapshot2;
    if(smart_router) {
        snapshot0 = take_router_snapshot(smart_router);
        print_snapshot(snapshot0);
    }
    
    // !start the client transaction generation threads
    std::vector<std::thread> client_gen_txn_threads;
    for(int i = 0; i < worker_threads; i++) {
        thread_params* params = new thread_params();
        params->thread_id = i;
        params->thread_count = worker_threads;
        params->zipfian_theta = zipfian_theta;
        client_gen_txn_threads.emplace_back(generate_smallbank_txns_worker, params);
    }

    // !Start the transaction threads
    std::vector<std::thread> db_conn_threads;
    for(int i = 0; i < ComputeNodeCount; i++) {
        for(int j = 0; j < worker_threads; j++) {
            thread_params* params = new thread_params();
            params->compute_node_id_connecter = i;
            params->thread_id = j;
            params->thread_count = worker_threads;
            params->zipfian_theta = zipfian_theta;
            if (use_sp)
                db_conn_threads.emplace_back(run_smallbank_txns_sp, params, logger_);
            else
                db_conn_threads.emplace_back(run_smallbank_txns, params, logger_);
            // 测试路由吞吐量
            // db_conn_threads.emplace_back(run_smallbank_empty, params, logger_);
        }
    }

    // !begin RUN Router
    smart_router->start_router();

    // Start a separate thread to print TPS periodically
    std::thread tps_thread(print_tps_loop);
    tps_thread.detach(); // Detach the thread to run independently

    while(exe_count <= MetisWarmupRound * PARTITION_INTERVAL * 1.0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "\033[31m Warmup rounds completed. Create the smart router snapshot. \033[0m" << std::endl;
    if(smart_router) { 
        snapshot1 = take_router_snapshot(smart_router);
        print_diff_snapshot(snapshot0, snapshot1);
    }
    // Create a performance snapshot after warmup
    int mid_snapshot_id = create_perf_kwr_snapshot(conn0);

    // Wait for all threads to complete
    for(auto& thread : db_conn_threads) {
        thread.join();
    }
    // Stop the client transaction generation threads
    for(auto& thread : client_gen_txn_threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Create a performance snapshot after running transactions
    std::this_thread::sleep_for(std::chrono::seconds(2)); // sleep for a while to ensure all operations are completed
    int end_snapshot_id = create_perf_kwr_snapshot(conn0);
    std::cout << "Performance snapshots created: Start ID = " << start_snapshot_id 
              << ", End ID = " << end_snapshot_id << std::endl;
    
    // Print Report
    std::cout << "\n=== Performance Report ===" << std::endl;
    std::cout << "System mode: " << SYSTEM_MODE << " !!!" << std::endl;
    if(smart_router) {
        snapshot2 = take_router_snapshot(smart_router);
        print_diff_snapshot(snapshot1, snapshot2);
    }
    {
        smart_router->sum_worker_thread_stat_time();
        std::vector<double> exec_txn_sum_ms = smart_router->get_time_breakdown().sum_worker_thread_exec_time_ms_per_node;
        double total_exec_time = 0.0;
        for(int i =0; i<exec_txn_sum_ms.size(); i++){
            total_exec_time += exec_txn_sum_ms[i];
        }
        std::cout << "exec txn sum ms: " << total_exec_time << " ms" << std::endl;
        std::cout << "Average txn exec ms per thread: " << (worker_threads > 0 ? total_exec_time / (worker_threads * ComputeNodeCount) : 0.0) << " ms" << std::endl;
        std::cout << "Average txn exec ms: " << (exe_count > 0 ? total_exec_time / exe_count : 0.0) << " ms" << std::endl;
    }
    std::cout << "All transaction threads completed." << std::endl;
    for(int i =0; i<DBConnection.size(); i++){
        std::cout << "node " << i << " routed txn count: " << exec_txn_cnt_per_node[i] << std::endl;
    }
    std::cout << "Total accounts loaded: " << smallbank_account << std::endl;
    std::cout << "Access pattern used: " << access_pattern_name << std::endl;
    std::cout << "Total transactions executed: " << exe_count << std::endl;
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    double s = ms / 1000.0; // Convert milliseconds to seconds
    std::cout << "Throughput: " << exe_count / s << " transactions per second" << std::endl;

    // Generate performance report, file name inluding the timestamp

    std::string report_file_warm_phase = kwr_report_name +  "_fisrt.html";
    generate_perf_kwr_report(conn0, start_snapshot_id, mid_snapshot_id, report_file_warm_phase);
    std::string report_file_run_phase = kwr_report_name +  "_end.html";
    generate_perf_kwr_report(conn0, start_snapshot_id, end_snapshot_id, report_file_run_phase);

    // 关闭连接
    delete conn0;
    delete conn1;

    // 清理Zipfian生成器
    if (zipfian_gen) {
        delete zipfian_gen;
        zipfian_gen = nullptr;
    }

    // restore streams (best-effort; program may exit via signal handler earlier)
    std::cout.rdbuf(old_cout);
    std::cerr.rdbuf(old_cerr);
    output_file.flush();
    output_file.close();
    return 0;
}