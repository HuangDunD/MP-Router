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
#include "ycsb.h"
#include "tpcc.h"
#include "common.h"
#include "btree_search.h"
#include "smart_router.h"
#include "util/zipf.h"
#include "router_stat_snapshot.h"
#include "txn_queue.h"
#include "tit.h"
#include "config.h"
#include "yacli.h"

std::vector<TxnQueue*> txn_queues; // one queue per compute node
std::vector<std::atomic<int>> exec_txn_cnt_per_node(MaxComputeNodeCount); // 每个节点路由的事务数

// SmallBank 数据加载时产生的键→页映射（按 id 直接索引），用于初始化 Router
static SmallBank::TableKeyPageMap g_smallbank_key_page_map;

// thread parameters structure for DB connector threads
struct thread_params
{
    node_id_t compute_node_id_connecter; // the compute node id this thread connects to
    int thread_id;
    int thread_count;
    std::vector<double> *latency_record = nullptr; // pointer to the latency record vector
    std::vector<double> *fetch_latency_record = nullptr; // pointer to the fetch latency record vector

    SmartRouter* smart_router = nullptr; // pointer to the smart router
    SlidingTransactionInforTable* tit = nullptr;  // pointer to the global transaction information table
    SmallBank* smallbank = nullptr;
    YCSB* ycsb = nullptr;
    TPCC* tpcc = nullptr;
};

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

int create_perf_kwr_snapshot(){
    // new connection
    pqxx::connection* conn0 = new pqxx::connection(DBConnection[0]);
    if (conn0 == nullptr || !conn0->is_open()) {
        std::cerr << "Failed to connect to the database. conninfo: " + DBConnection[0] << std::endl;
        return -1;
    }

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
    delete conn0;
    return snapshot_id;
}

void generate_perf_kwr_report(int start_snapshot_id, int end_snapshot_id, std::string file_name) {
    // new connection
    pqxx::connection* conn0 = new pqxx::connection(DBConnection[0]);
    if (conn0 == nullptr || !conn0->is_open()) {
        std::cerr << "Failed to connect to the database. conninfo: " + DBConnection[0] << std::endl;
        return;
    }
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
    delete conn0;
}

void init_key_page_map(SmartRouter* smart_router, SmallBank* smallbank, YCSB* ycsb, TPCC* tpcc) {
    int keys_num;
    if(Workload_Type == 0){
        keys_num = smallbank->get_account_count();
    } else if (Workload_Type == 1){
        keys_num = ycsb->get_record_count();
    } else if (Workload_Type == 2){
        // For TPC-C, we iterate over warehouses for simplicity in this init function structure,
        // but actually we need to init all tables.
        // Let's just use num_warehouses as the loop count and handle logic inside.
        keys_num = tpcc->get_num_warehouses();
    } else {
        std::cerr << "Unknown Workload_Type: " << Workload_Type << std::endl;
        return;
    }
    
    int num_threads = 100;  // Number of worker threads
    if (keys_num < num_threads) {
        num_threads = keys_num;
    }
    std::vector<std::thread> threads;
    const int chunk_size = std::max(1, keys_num / num_threads);
    auto worker = [smart_router, tpcc](int start, int end) {
        pqxx::connection conn00(DBConnection[0]);
        if (!conn00.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        int city_cnt = static_cast<int>(SmallBankCityType::Count);
        for(int i = start; i < end; i++) {
            // 使用Select 语句获取插入数据的ctid
            int id = i + 1;
            std::string select_sql0;
            std::string select_sql1;
            if(Workload_Type == 0){
                select_sql0 = "SELECT ctid, id FROM checking WHERE id = " + std::to_string(id) + ";";
                select_sql1 = "SELECT ctid, id FROM savings WHERE id = " + std::to_string(id) + ";";
            } else if (Workload_Type == 1){
                select_sql0 = "SELECT ctid, id FROM usertable WHERE id = " + std::to_string(id) + ";";
            } else if (Workload_Type == 2) {
                // TPC-C initialization logic
                // We need to init Warehouse, District, Customer, Stock for w_id = id
                // This is a bit complex to fit into the existing loop structure which assumes 1 key = 1 row.
                // But let's try to do it per warehouse.
                int w_id = id;
                try {
                    pqxx::nontransaction txn_select(conn00);
                    
                    // Warehouse
                    pqxx::result res = txn_select.exec("SELECT ctid, w_id FROM warehouse WHERE w_id = " + std::to_string(w_id));
                    if (!res.empty()) {
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(res[0]["ctid"].as<std::string>());
                        smart_router->initial_key_page((table_id_t)TPCCTableType::kWarehouse, tpcc->make_warehouse_key(w_id), page_id);
                    }

                    // District (10 per warehouse)
                    for (int d = 1; d <= TPCC::DIST_PER_WARE; ++d) {
                        res = txn_select.exec("SELECT ctid, d_w_id, d_id FROM district WHERE d_w_id = " + std::to_string(w_id) + " AND d_id = " + std::to_string(d));
                        if (!res.empty()) {
                            auto [page_id, tuple_index] = parse_page_id_from_ctid(res[0]["ctid"].as<std::string>());
                            smart_router->initial_key_page((table_id_t)TPCCTableType::kDistrict, tpcc->make_district_key(w_id, d), page_id);
                        }
                    }
                    
                    // Customer (3000 per district)
                    res = txn_select.exec("SELECT ctid, c_w_id, c_d_id, c_id FROM customer WHERE c_w_id = " + std::to_string(w_id));
                    for (auto row : res) {
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(row["ctid"].as<std::string>());
                        int d_id = row["c_d_id"].as<int>();
                        int c_id = row["c_id"].as<int>();
                        smart_router->initial_key_page((table_id_t)TPCCTableType::kCustomer, tpcc->make_customer_key(w_id, d_id, c_id), page_id);
                    }

                    // Stock (100000 per warehouse)
                    res = txn_select.exec("SELECT ctid, s_w_id, s_i_id FROM stock WHERE s_w_id = " + std::to_string(w_id));
                    for (auto row : res) {
                        auto [page_id, tuple_index] = parse_page_id_from_ctid(row["ctid"].as<std::string>());
                        int i_id = row["s_i_id"].as<int>();
                        smart_router->initial_key_page((table_id_t)TPCCTableType::kStock, tpcc->make_stock_key(w_id, i_id), page_id);
                    }
                } catch (const std::exception &e) {
                    std::cerr << "Error while selecting TPC-C data: " << e.what() << std::endl;
                }
                continue; // Skip the rest of the loop body
            } else {
                std::cerr << "Unknown Workload_Type: " << Workload_Type << std::endl;
                return;
            }

            try {
                pqxx::nontransaction txn_select(conn00);
                if(Workload_Type == 0){
                    pqxx::result select_result = txn_select.exec(select_sql0);
                    assert(!select_result.empty());
                    std::string ctid = select_result[0]["ctid"].as<std::string>();
                    // ctid 为 (page_id, tuple_index) 格式, 这里要把ctid转换为page_id
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = select_result[0]["id"].as<int>();
                    smart_router->initial_key_page((table_id_t)SmallBankTableType::kCheckingTable, inserted_id, page_id);

                    select_result = txn_select.exec(select_sql1);
                    assert(!select_result.empty());
                    ctid = select_result[0]["ctid"].as<std::string>();
                    // ctid 为 (page_id, tuple_index) 格式, 这里要把ctid转换为page_id
                    std::tie(page_id, tuple_index) = parse_page_id_from_ctid(ctid);
                    inserted_id = select_result[0]["id"].as<int>();
                    smart_router->initial_key_page((table_id_t)SmallBankTableType::kSavingsTable, inserted_id, page_id);
                } else if (Workload_Type == 1){
                    pqxx::result select_result = txn_select.exec(select_sql0);
                    assert(!select_result.empty());
                    std::string ctid = select_result[0]["ctid"].as<std::string>();
                    // ctid 为 (page_id, tuple_index) 格式, 这里要把ctid转换为page_id
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = select_result[0]["id"].as<int>();
                    smart_router->initial_key_page((table_id_t)YCSBTableType::kYCSBTable, inserted_id, page_id);
                }
            } catch (const std::exception &e) {
                std::cerr << "Error while selecting data: " << e.what() << std::endl;
            }
        }
    };

    // Create and start threads
    for(int i = 0; i < num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? keys_num : (i + 1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }

    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }
#if MLP_PREDICTION
    // after initializing the key-page map, train the MLP model
    smart_router->mlp_train_after_init();
#endif
    std::cout << "Data page mapping initialization completed." << std::endl;
}

void run_smallbank_empty(thread_params* params, Logger* logger_){
    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    SmallBank* smallbank = params->smallbank;

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

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

            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            itemkey_t account1 = txn_entry->accounts[0];
            itemkey_t account2 = txn_entry->accounts[1];
            int txn_decision_type = txn_entry->txn_decision_type;
            
            // init the table ids and keys
            std::vector<table_id_t>& tables = smallbank->get_table_ids_by_txn_type(txn_type);
            assert(tables.size() > 0);
            std::vector<itemkey_t> keys;
            smallbank->get_keys_by_txn_type(txn_type, account1, account2, keys);
            assert(tables.size() == keys.size());
            std::vector<bool> rw = smallbank->get_rw_by_txn_type(txn_type);
            std::vector<page_id_t> ctid_ret_page_ids; 

            ctid_ret_page_ids = txn_entry->accessed_page_ids;
            // update the smart router page map if needed
            if(smart_router) smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables), keys, rw, ctid_ret_page_ids, compute_node_id);

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
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    SmallBank* smallbank = params->smallbank;
    assert(txn_queue != nullptr && smart_router != nullptr && smart_router != nullptr && smallbank != nullptr);

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

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
            if(!WarmupEnd && SYSTEM_MODE == 0 && exe_count > MetisWarmupRound * PARTITION_INTERVAL) {
                WarmupEnd = true;
                std::cout << "Warmup Ended for Mode 0, exe_count: " << exe_count << std::endl;
            }
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
            timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);
            pqxx::work* txn = new pqxx::work(*con);

            // init the table ids and keys
            std::vector<table_id_t>& tables = smallbank->get_table_ids_by_txn_type(txn_type);
            assert(tables.size() > 0);
            std::vector<itemkey_t> keys;
            smallbank->get_keys_by_txn_type(txn_type, account1, account2, keys);
            assert(tables.size() == keys.size());
            std::vector<bool> rw = smallbank->get_rw_by_txn_type(txn_type);
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
                if(smart_router) smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables), keys, rw, ctid_ret_page_ids, compute_node_id);
                
            } catch (const std::exception &e) {
                std::cerr << "Transaction failed: " << e.what() << std::endl;
                logger_->info("Transaction failed: " + std::string(e.what()));
            }
            
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double exec_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            double current_time_ms = end_time.tv_sec * 1000.0 + end_time.tv_nsec / 1000000.0;
            if (WarmupEnd) {
                if (params->latency_record) params->latency_record->push_back(exec_time);
                if (params->fetch_latency_record && txn_entry->fetch_time > 0) {
                     params->fetch_latency_record->push_back(current_time_ms - txn_entry->fetch_time);
                }
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
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    SmallBank* smallbank = params->smallbank;
    assert(txn_queue != nullptr && smart_router != nullptr && smart_router != nullptr && smallbank != nullptr);

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

    int con_batch_id = 0;
    while (true) {
        // 计时 
        timespec pop_start_time, pop_end_time;
        clock_gettime(CLOCK_MONOTONIC, &pop_start_time);

        int call_id;
        std::list<TxnQueueEntry*> txn_entries = txn_queue->pop_txn(&call_id);
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
            // 计时
            timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            exe_count++;
            if(!WarmupEnd && (SYSTEM_MODE == 0 || SYSTEM_MODE == 2 || SYSTEM_MODE == 11) && exe_count > MetisWarmupRound * PARTITION_INTERVAL) {
                WarmupEnd = true;
                std::cout << "Warmup Ended for Mode 0, exe_count: " << exe_count << std::endl;
            }
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

            std::vector<table_id_t> tables_store;
            std::vector<table_id_t>* tables_ptr;
            std::vector<itemkey_t> keys;

            if (txn_type == 6) {
                keys = txn_entry->accounts;
                tables_store.assign(keys.size(), (table_id_t)SmallBankTableType::kCheckingTable);
                tables_ptr = &tables_store;
            } else {
                tables_ptr = &smallbank->get_table_ids_by_txn_type(txn_type);
                smallbank->get_keys_by_txn_type(txn_type, account1, account2, keys);
            }
            std::vector<table_id_t>& tables = *tables_ptr;

            assert(tables.size() > 0);
            assert(tables.size() == keys.size());
            std::vector<bool> rw = smallbank->get_rw_by_txn_type(txn_type);
            if(txn_type == 6) rw.assign(keys.size(), true); // Update RW flags for dynamic length

            std::vector<page_id_t> ctid_ret_page_ids;

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
                    case 6: { // MultiUpdate
                        // 构造 SQL 数组字符串 array[1,2,3]
                        std::string ids_str = "array[";
                        for(size_t i = 0; i < keys.size(); ++i) {
                            ids_str += std::to_string(keys[i]);
                            if (i < keys.size() - 1) ids_str += ",";
                        }
                        ids_str += "]";

                        std::string sql = "SELECT rel, id, ctid, balance, txid FROM sp_multi_update(" +
                                          ids_str + ", 1)";
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
                                                  keys, rw, ctid_ret_page_ids, compute_node_id);
                }
            } catch (const std::exception &e) {
                std::cerr << "Transaction (SP) failed: " << e.what() << std::endl;
                logger_->info("Transaction (SP) failed: " + std::string(e.what()));
            }

            // // 模拟执行时间, 再sleep 1 ms
            // std::this_thread::sleep_for(std::chrono::milliseconds(1));
            
            // 计时结束
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double exec_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_exec_time(params->compute_node_id_connecter, params->thread_id, exec_time);

            double current_time_ms = end_time.tv_sec * 1000.0 + end_time.tv_nsec / 1000000.0;
            if (WarmupEnd) {
                if(params->latency_record) params->latency_record->push_back(exec_time);
                if(params->fetch_latency_record && txn_entry->fetch_time > 0) {
                     params->fetch_latency_record->push_back(current_time_ms - txn_entry->fetch_time);
                }
            }

            clock_gettime(CLOCK_MONOTONIC, &start_time);
            tit->mark_done(txn_entry, call_id); // 一体化：标记完成，删除由 TIT 统一管理
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double mark_done_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_mark_done_time(params->compute_node_id_connecter, params->thread_id, mark_done_time);
            
        #if LOG_TXN_EXEC
            clock_gettime(CLOCK_MONOTONIC, &start_time);
            for (auto account : txn_entry->accounts) {
                for(auto key : hottest_keys) {
                    if (account == key) {
                        // log the hotspot exection
                        logger_->info("Batch " + std::to_string(txn_entry->batch_id) + 
                            " Node: " + std::to_string(compute_node_id) + 
                            " txn id: " + std::to_string(txn_entry->tx_id) + 
                            " txn type (smallbank): " + std::to_string(txn_entry->txn_type) + 
                            " connector id: " + std::to_string(params->compute_node_id_connecter) + 
                            " txn type (schedule): " + std::to_string((int)txn_entry->schedule_type) + 
                            " access account " + std::to_string(account) + 
                            " exec time: " + std::to_string(exec_time) + 
                            " group id: " + std::to_string(txn_entry->group_id) + 
                            " dependency group ids: " + [&]() {
                                std::string os;
                                for(auto dep_id : txn_entry->dependency_group_id) {
                                    os += std::to_string(dep_id) + " ";
                                }
                                return os;
                            }() +
                            " batch id: " + std::to_string(txn_entry->batch_id)
                        );
                    }
                }
            }
        #endif
        // #if LOG_TXN_EXEC
            if (exec_time > 1000) 
                logger_->warning("Node: " + std::to_string(compute_node_id) + "Transaction execution time exceeded 10 ms: " + std::to_string(exec_time) + 
                " ms, call id: " + std::to_string(call_id) + " Txn op: " + [&]() {
                    std::string os;
                    os += "txn_type: " + std::to_string(txn_entry->txn_type) + " ";
                    os += "table: ";
                    for(int i= 0; i < tables.size(); i++) {
                        os += std::to_string(tables[i]) + " ";
                    }
                    os += "key: ";
                    for(int i= 0; i < keys.size(); i++) {
                        os += std::to_string(keys[i]) + " ";
                    }
                    os += "original pages: ";
                    for(int i= 0; i < txn_entry->accessed_page_ids.size(); i++) {
                        os += std::to_string(txn_entry->accessed_page_ids[i]) + " ";
                    }
                    os += " return pages: ";
                    for(int i= 0; i < ctid_ret_page_ids.size(); i++) {
                        os += std::to_string(ctid_ret_page_ids[i]) + " ";
                    }
                    os += " group id: " + std::to_string(txn_entry->group_id) + " ";
                    os += " dependency group ids: ";
                    for(auto dep_id : txn_entry->dependency_group_id) {
                        os += std::to_string(dep_id) + " ";
                    }
                    os += " batch id: " + std::to_string(txn_entry->batch_id) + " ";
                    return os;
                }());
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double log_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_log_debug_info_time(params->compute_node_id_connecter, params->thread_id, log_time);
        // #endif
        }
    #if LOG_TXN_EXEC
        struct timespec start_time, end_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        logger_->info("Finished processing batch with call_id: " + std::to_string(call_id));
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        double log_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                           (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
        smart_router->add_worker_thread_log_debug_info_time(params->compute_node_id_connecter, params->thread_id, log_time);
    #endif 
    }
    std::cout << "Finished running smallbank transactions via stored procedures." << std::endl;
}

void run_ycsb_txns_sp(thread_params* params, Logger* logger_) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("dbconsp_n" + std::to_string(params->compute_node_id_connecter)
                                                + "_t_" + std::to_string(params->thread_id)).c_str());

    std::cout << "Running smallbank transactions via stored procedures..." << std::endl;

    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    YCSB* ycsb = params->ycsb;
    assert(txn_queue != nullptr && smart_router != nullptr && smart_router != nullptr && ycsb != nullptr);

    // 构造数组字符串：array['k1','k2',...]
    auto build_array = [](const std::vector<itemkey_t>& v, size_t start, size_t count) {
        std::string s = "array[";
        for(size_t i = 0; i < count; ++i) {
            if(i > 0) s += ",";
            s += std::to_string(v[start + i]);
        }
        s += "]";
        return s;
    };

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

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
            while (con->is_open() == false) {
                std::cerr << "Connection is broken, reconnecting..." << std::endl;
                delete con;
                con = new pqxx::connection(con_str);
            }

            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            std::vector<table_id_t> tables = ycsb->get_table_ids_by_txn_type();
            assert(tables.size() > 0);
            std::vector<itemkey_t> keys = txn_entry->ycsb_keys;
            assert(tables.size() == keys.size());
            assert(txn_entry->ycsb_keys.size() == 10);
            std::vector<bool> rw = ycsb->get_rw_flags();

            std::vector<page_id_t> ctid_ret_page_ids;

            // 计时
            timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            // !不需要外层事务
            pqxx::nontransaction txn(*con);
            try {
                pqxx::result res;
                switch(txn_type) {
                    case 0: { 
                        // 读集合取前 read_cnt 个，写集合取后 write_cnt 个
                        std::string read_arr = build_array(keys, 0, ycsb->get_read_cnt());
                        std::string write_arr = build_array(keys, ycsb->get_read_cnt(), ycsb->get_write_cnt());

                        std::string sql = "SELECT id, ctid, txid FROM ycsb_multi_rw(" + read_arr + ", " + write_arr + ")";
                        res = txn.exec(sql);
                        break;
                    }
                    default: {
                        assert(false);
                    }
                }

                for (const auto& row : res) {
                    std::string ctid_str = row["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid_str);
                    ctid_ret_page_ids.push_back(page_id);
                }

                if(smart_router) {
                    smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables),
                                                  keys, rw, ctid_ret_page_ids, compute_node_id);
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

            double current_time_ms = end_time.tv_sec * 1000.0 + end_time.tv_nsec / 1000000.0;
            if (WarmupEnd) {
                if(params->latency_record) params->latency_record->push_back(exec_time);
                if(params->fetch_latency_record && txn_entry->fetch_time > 0) {
                     params->fetch_latency_record->push_back(current_time_ms - txn_entry->fetch_time);
                }
            }

            tit->mark_done(txn_entry); // 一体化：标记完成，删除由 TIT 统一管理
        }
    }
    std::cout << "Finished running smallbank transactions via stored procedures." << std::endl;
}

void run_tpcc_txns_sp(thread_params* params, Logger* logger_) {
    pthread_setname_np(pthread_self(), ("dbconsp_n" + std::to_string(params->compute_node_id_connecter)
                                                + "_t_" + std::to_string(params->thread_id)).c_str());

    std::cout << "Running TPC-C transactions via stored procedures..." << std::endl;

    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    TPCC* tpcc = params->tpcc;
    assert(txn_queue != nullptr && smart_router != nullptr && tpcc != nullptr);

    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

    int con_batch_id = 0;
    while (true) {
        timespec pop_start_time, pop_end_time;
        clock_gettime(CLOCK_MONOTONIC, &pop_start_time);

        int call_id;
        std::list<TxnQueueEntry*> txn_entries = txn_queue->pop_txn(&call_id);

        clock_gettime(CLOCK_MONOTONIC, &pop_end_time);
        double pop_time = (pop_end_time.tv_sec - pop_start_time.tv_sec) * 1000.0 +
                          (pop_end_time.tv_nsec - pop_start_time.tv_nsec) / 1000000.0;
        smart_router->add_worker_thread_pop_time(params->compute_node_id_connecter, params->thread_id, pop_time);

        if (txn_entries.empty()) {
            if(txn_queue->is_finished()) {
                break;
            } else if(txn_queue->is_batch_finished()) {
                smart_router->notify_batch_finished(compute_node_id, params->thread_id, con_batch_id);
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
            // 计时
            timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            exe_count++;
            exec_txn_cnt_per_node[compute_node_id]++;

            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            std::vector<itemkey_t> tpcc_params = txn_entry->tpcc_params;
            TPCCTxType type = static_cast<TPCCTxType>(txn_type);

            while (con->is_open() == false) {
                std::cerr << "Connection is broken, reconnecting..." << std::endl;
                delete con;
                con = new pqxx::connection(con_str);
            }

            std::vector<itemkey_t> tpcc_keys = txn_entry->tpcc_keys;
            assert(tpcc_keys.size() > 0);
            std::vector<table_id_t> tables = tpcc->get_table_ids_by_txn_type(txn_type, tpcc_keys.size());
            assert(tables.size() == tpcc_keys.size());
            std::vector<bool> rw = tpcc->get_rw_flags_by_txn_type(txn_type, tpcc_keys.size());
            std::vector<page_id_t> ctid_ret_page_ids; // Dummy for now as we didn't get ctids from SP

            // !不需要外层事务
            pqxx::nontransaction txn(*con);
            
            try{
                pqxx::result res;
                switch(type) {
                    case TPCCTxType::kNewOrder: {
                        if (tpcc_params.size() < 4) {
                            assert(false);
                        }
                        int w_id = tpcc_params[0];
                        int d_id = tpcc_params[1];
                        int c_id = tpcc_params[2];
                        int o_ol_cnt = tpcc_params[3];
                        
                        std::string i_ids_str = "{";
                        std::string supply_w_ids_str = "{";
                        std::string quantities_str = "{";
                        
                        for (int i = 0; i < o_ol_cnt; ++i) {
                            int base = 4 + i * 3;
                            if (base + 2 >= (int)tpcc_params.size()) break;
                            
                            if (i > 0) {
                                i_ids_str += ",";
                                supply_w_ids_str += ",";
                                quantities_str += ",";
                            }
                            i_ids_str += std::to_string(tpcc_params[base]);
                            supply_w_ids_str += std::to_string(tpcc_params[base+1]);
                            quantities_str += std::to_string(tpcc_params[base+2]);
                        }
                        i_ids_str += "}";
                        supply_w_ids_str += "}";
                        quantities_str += "}";

                        std::string sql = "SELECT * FROM tpcc_new_order(" + 
                            std::to_string(w_id) + ", " + 
                            std::to_string(d_id) + ", " + 
                            std::to_string(c_id) + ", " + 
                            std::to_string(o_ol_cnt) + ", '" + 
                            i_ids_str + "', '" + 
                            supply_w_ids_str + "', '" + 
                            quantities_str + "')";
                        
                        res = txn.exec(sql);
                        break;
                    }
                    case TPCCTxType::kPayment: {
                        if (tpcc_params.size() < 4) {
                            assert(false);
                        }
                        int w_id = tpcc_params[0];
                        int d_id = tpcc_params[1];
                        int c_id = tpcc_params[2];
                        int h_amount = tpcc_params[3];

                        std::string sql = "SELECT * FROM tpcc_payment(" + 
                            std::to_string(w_id) + ", " + 
                            std::to_string(d_id) + ", " + 
                            std::to_string(w_id) + ", " + 
                            std::to_string(d_id) + ", " + 
                            std::to_string(c_id) + ", " + 
                            std::to_string(h_amount) + ")";
                        
                        res = txn.exec(sql);
                        break;
                    }
                    default:
                        assert(false);
                }

                for (const auto& row : res) {
                    std::string ctid_str = row["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid_str);
                    ctid_ret_page_ids.push_back(page_id);
                }

                if(smart_router) {
                    smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables),
                                                    tpcc_keys, rw, ctid_ret_page_ids, compute_node_id);
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

            double current_time_ms = end_time.tv_sec * 1000.0 + end_time.tv_nsec / 1000000.0;
            if (WarmupEnd) {
                if(params->latency_record) params->latency_record->push_back(exec_time);
                if(params->fetch_latency_record && txn_entry->fetch_time > 0) {
                     params->fetch_latency_record->push_back(current_time_ms - txn_entry->fetch_time);
                }
            }

            clock_gettime(CLOCK_MONOTONIC, &start_time);
            tit->mark_done(txn_entry, call_id); // 一体化：标记完成，删除由 TIT 统一管理
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double mark_done_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                    (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_mark_done_time(params->compute_node_id_connecter, params->thread_id, mark_done_time);
        }
    }
    
    std::cout << "Finished running TPC-C transactions via stored procedures." << std::endl;
}

void run_ycsb_txns_empty(thread_params* params, Logger* logger_) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("dbconsp_n" + std::to_string(params->compute_node_id_connecter)
                                                + "_t_" + std::to_string(params->thread_id)).c_str());

    std::cout << "Running smallbank transactions via stored procedures..." << std::endl;

    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    YCSB* ycsb = params->ycsb;
    assert(txn_queue != nullptr && smart_router != nullptr && smart_router != nullptr && ycsb != nullptr);

    // 构造数组字符串：array['k1','k2',...]
    auto build_array = [](const std::vector<itemkey_t>& v, size_t start, size_t count) {
        std::string s = "array[";
        for(size_t i = 0; i < count; ++i) {
            if(i > 0) s += ",";
            s += std::to_string(v[start + i]);
        }
        s += "]";
        return s;
    };

    // init the thread connection for this compute node
    auto con_str = DBConnection[compute_node_id];
    auto con = new pqxx::connection(con_str);
    if(!con->is_open()) {
        std::cerr << "Failed to connect to database: " << con_str << std::endl;
        assert(false);
        exit(-1);
    }

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
            std::vector<table_id_t> tables = ycsb->get_table_ids_by_txn_type();
            assert(tables.size() > 0);
            std::vector<itemkey_t> keys = txn_entry->ycsb_keys;
            assert(tables.size() == keys.size());
            assert(txn_entry->ycsb_keys.size() == 10);
            std::vector<bool> rw = ycsb->get_rw_flags();

            std::vector<page_id_t> ctid_ret_page_ids;
            ctid_ret_page_ids = txn_entry->accessed_page_ids;

            if(smart_router) {
                smart_router->update_key_page(txn_entry, const_cast<std::vector<table_id_t>&>(tables),
                                                keys, rw, ctid_ret_page_ids, compute_node_id);
            }

            tit->mark_done(txn_entry); // 一体化：标记完成，删除由 TIT 统一管理
        }
    }
    std::cout << "Finished running smallbank transactions via stored procedures." << std::endl;
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

void print_tps_loop(SmartRouter* smart_router, TxnPool* txn_pool, Logger* logger_) {
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
        logger_->info("[Routed TPS] " + std::to_string(route_tps) +
                      " txn/sec (total: " + std::to_string(exec_cur_count) + "). ");
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
        logger_->info("[Exec TPS] " + std::to_string(exec_tps) +
                      " txn/sec (total: " + std::to_string(exec_cur_count) + "). ");
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

void run_yashan_smallbank_txns_sp(thread_params* params, Logger* logger_) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("dbconya_n" + std::to_string(params->compute_node_id_connecter)
                                                + "_t_" + std::to_string(params->thread_id)).c_str());

    std::cout << "Running smallbank transactions via YashanDB..." << std::endl;

    node_id_t compute_node_id = params->compute_node_id_connecter;
    TxnQueue* txn_queue = txn_queues[compute_node_id];
    SmartRouter* smart_router = params->smart_router;
    SlidingTransactionInforTable *tit = params->tit;
    SmallBank* smallbank = params->smallbank;
    assert(txn_queue != nullptr && smart_router != nullptr && tit != nullptr && smallbank != nullptr);
    
    // Connect to YashanDB
    YashanConnInfo info = YashanDBConnections[compute_node_id];

    YacHandle env = NULL;
    YacHandle conn = NULL;
    
    // For Direct Execute
    YacHandle stmt_direct = NULL;
    
    // For Scalar Binding
    const bool USE_SCALAR_PARAM = true; 
    YacHandle stmts_scalar[6] = {NULL};
    const char* sp_sqls_scalar[] = {
        "BEGIN sp_amalgamate_scalar(?, ?, ?, ?, ?); END;",     // 0: Amalgamate(a1, a2, out b1, out b2, out b3)
        "BEGIN sp_send_payment_scalar(?, ?, ?, ?); END;",      // 1: SendPayment(a1, a2, out b1, out b2)
        "BEGIN sp_deposit_checking_scalar(?, ?, ?); END;",     // 2: DepositChecking(a1, amt, out b1)
        "BEGIN sp_write_check_scalar(?, ?, ?, ?); END;",          // 3: WriteCheck(a1, amt, out b1)
        "BEGIN sp_balance_scalar(?, ?, ?); END;",              // 4: Balance(a1, out b1, out b2)
        "BEGIN sp_transact_savings_scalar(?, ?, ?); END;"      // 5: TransactSavings(a1, amt, out b1)
    };

    yacAllocHandle(YAC_HANDLE_ENV, NULL, &env);
    yacAllocHandle(YAC_HANDLE_DBC, env, &conn);
    if((YacResult)yacConnect(conn, (YacChar*)info.ip_port.c_str(), YAC_NULL_TERM_STR, 
                (YacChar*)info.user.c_str(), YAC_NULL_TERM_STR, 
                (YacChar*)info.password.c_str(), YAC_NULL_TERM_STR) != YAC_SUCCESS) {
        std::cerr << "Initial connection to YashanDB failed." << std::endl;
    }
    else {
        std::cout << "Connected to YashanDB at " << info.ip_port << std::endl;
    }
    
    if (USE_SCALAR_PARAM) {
        for(int i = 0; i < 6; i++) {
            yacAllocHandle(YAC_HANDLE_STMT, conn, &stmts_scalar[i]);
            if((YacResult)yacPrepare(stmts_scalar[i], (YacChar*)sp_sqls_scalar[i], YAC_NULL_TERM_STR) != YAC_SUCCESS) {
                std::cerr << "Pre-Prepare failed for txn type " << i << " at YashanDB." << std::endl;
                YacInt32 errCode; 
                char msg[1024];
                YacTextPos pos;
                yacGetDiagRec(&errCode, msg, sizeof(msg), NULL, NULL, 0, &pos);
                std::cerr << "Error Code: " << errCode << ", Message: " << msg << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::cout << "Prepared Scalar Statements." << std::endl;
    } else {
        // Allocate one statement handle for Direct Execution of V2
        if (yacAllocHandle(YAC_HANDLE_STMT, conn, &stmt_direct) != YAC_SUCCESS) {
             std::cerr << "Failed to allocate statement handle." << std::endl;
        }
        std::cout << "Allocated Direct Execution Handle." << std::endl;
    }

    int con_batch_id = 0;
    while (true) {
        // 1. Fetch Txn
        timespec pop_start_time, pop_end_time;
        clock_gettime(CLOCK_MONOTONIC, &pop_start_time);

        int call_id;
        std::list<TxnQueueEntry*> txn_entries = txn_queue->pop_txn(&call_id);


        clock_gettime(CLOCK_MONOTONIC, &pop_end_time);
        double pop_time = (pop_end_time.tv_sec - pop_start_time.tv_sec) * 1000.0 +
                          (pop_end_time.tv_nsec - pop_start_time.tv_nsec) / 1000000.0;
        smart_router->add_worker_thread_pop_time(params->compute_node_id_connecter, params->thread_id, pop_time);

        // 2. Handle Empty/Batch Finish
        if (txn_entries.empty()) {
            if(txn_queue->is_finished()) {
                break;
            } else if(txn_queue->is_batch_finished()) {
                smart_router->notify_batch_finished(compute_node_id, params->thread_id, con_batch_id);
                
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

        // 3. Execute Txns
        for (auto& txn_entry : txn_entries) {
            // Check connection health
            // YacInt32 dummy_attr;
            // if (yacGetConnAttr(conn, YAC_ATTR_AUTOCOMMIT, &dummy_attr, 0, NULL) != YAC_SUCCESS) {
            //     YacInt32 errCode;
            //     char msg[1024];
            //     YacTextPos pos;
            //     yacGetDiagRec(&errCode, msg, sizeof(msg), NULL, NULL, 0, &pos);
            //     std::cerr << "Error Code: " << errCode << ", Message: " << msg << std::endl;
            //     std::cerr << "Connection appears lost. Reconnecting..." << std::endl; 
            //     while(!try_connect()) {
            //         std::this_thread::sleep_for(std::chrono::seconds(1));
            //         std::cerr << "Retrying connection..." << std::endl;
            //     }
            // }

            timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            // Update stats
            exec_txn_cnt_per_node[compute_node_id]++;
            exe_count++;
            if(!WarmupEnd && (SYSTEM_MODE == 0 || SYSTEM_MODE == 2 || SYSTEM_MODE == 11) && exe_count > MetisWarmupRound * PARTITION_INTERVAL) {
                WarmupEnd = true;
                std::cout << "Warmup Ended for Mode 0, exe_count: " << exe_count << std::endl;
            }

            tx_id_t tx_id = txn_entry->tx_id;
            int txn_type = txn_entry->txn_type;
            
            itemkey_t account1 = txn_entry->accounts[0];
            itemkey_t account2 = txn_entry->accounts[1];

            std::vector<table_id_t>& tables = smallbank->get_table_ids_by_txn_type(txn_type);
            std::vector<itemkey_t> keys;
            smallbank->get_keys_by_txn_type(txn_type, account1, account2, keys);
            std::vector<bool> rw = smallbank->get_rw_by_txn_type(txn_type);
            std::vector<page_id_t> ctid_ret_page_ids(keys.size(), 0);
            
            // Prepare inputs
            YacInt32 acc1_val = (YacInt32)account1;
            YacInt32 acc2_val = (YacInt32)account2;
            YacInt32 amount_val = 0;
            if (txn_type == 2) amount_val = 1; // 1.3 -> 1
            if (txn_type == 3) amount_val = 5;
            if (txn_type == 5) amount_val = 20;

            // Construct SQL for Direct Execution
            if (USE_SCALAR_PARAM) {
                // Scalar Binding Logic
                YacHandle stmt = stmts_scalar[txn_type];
                YacInt32 b1 = 0, b2 = 0, b3 = 0; // Output blocks
                YacInt32 ind = 0;

                // Bind Inputs
                yacBindParameter(stmt, 1, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, &acc1_val, sizeof(acc1_val), sizeof(acc1_val), NULL);
                
                if (txn_type == 0) { // Amalgamate(a1, a2, out b1, out b2, out b3)
                    yacBindParameter(stmt, 2, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, &acc2_val, sizeof(acc2_val), sizeof(acc2_val), NULL);
                    yacBindParameter(stmt, 3, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b1, sizeof(b1), sizeof(b1), &ind);
                    yacBindParameter(stmt, 4, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b2, sizeof(b2), sizeof(b2), &ind);
                    yacBindParameter(stmt, 5, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b3, sizeof(b3), sizeof(b3), &ind);
                } else if (txn_type == 1 || txn_type == 3) { // SendPayment(a1, a2, out b1, out b2), WriteCheck(a1, amt, out b1, out b2)
                    yacBindParameter(stmt, 2, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, &acc2_val, sizeof(acc2_val), sizeof(acc2_val), NULL);
                    yacBindParameter(stmt, 3, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b1, sizeof(b1), sizeof(b1), &ind);
                    yacBindParameter(stmt, 4, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b2, sizeof(b2), sizeof(b2), &ind);
                } else if (txn_type == 2 || txn_type == 5) { 
                    // Deposit(a1, amt, out b1), TransactSav(a1, amt, out b1)
                    yacBindParameter(stmt, 2, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, &amount_val, sizeof(amount_val), sizeof(amount_val), NULL);
                    yacBindParameter(stmt, 3, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b1, sizeof(b1), sizeof(b1), &ind);
                } else if (txn_type == 4) { // Balance(a1, out b1, out b2)
                    yacBindParameter(stmt, 2, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b1, sizeof(b1), sizeof(b1), &ind);
                    yacBindParameter(stmt, 3, YAC_PARAM_OUTPUT, YAC_SQLT_INTEGER, &b2, sizeof(b2), sizeof(b2), &ind);
                }

                try {
                    if (yacExecute(stmt) != YAC_SUCCESS) {
                        std::cerr << "Exec Scalar failed txn=" << txn_type << std::endl;
                        YacInt32 errCode;
                        char msg[1024];
                        YacTextPos pos;
                        yacGetDiagRec(&errCode, msg, sizeof(msg), NULL, NULL, 0, &pos);
                        std::cerr << "Error Code: " << errCode << ", Message: " << msg << std::endl;
                    } else {
                        // yacCommit(conn);
                        // Collect outputs into ctid_ret_page_ids
                        
                        ctid_ret_page_ids.clear();
                        if (txn_type == 0) {
                            ctid_ret_page_ids.push_back(page_id_t(b1));
                            ctid_ret_page_ids.push_back(page_id_t(b2));
                            ctid_ret_page_ids.push_back(page_id_t(b3));
                        } else if (txn_type == 1 || txn_type == 3) {
                            ctid_ret_page_ids.push_back(page_id_t(b1));
                            ctid_ret_page_ids.push_back(page_id_t(b2));
                        } else if (txn_type == 2 || txn_type == 5) {
                            ctid_ret_page_ids.push_back(page_id_t(b1));
                        } else if (txn_type == 4) {
                            ctid_ret_page_ids.push_back(page_id_t(b1)); 
                            ctid_ret_page_ids.push_back(page_id_t(b2));
                        }
                        
                        smart_router->update_key_page(txn_entry, tables, keys, rw, ctid_ret_page_ids, compute_node_id);
                    }
                } catch (const std::exception &e) {
                    std::cerr << "Transaction (YashanDB Scalar) failed: " << e.what() << std::endl;
                    logger_->info("Transaction (YashanDB Scalar) failed: " + std::string(e.what()));
                }

            } else {
                std::string sql_str;
                if(txn_type == 0) { // Amalgamate
                    sql_str = "BEGIN sp_amalgamate_v2(" + std::to_string(acc1_val) + ", " + std::to_string(acc2_val) + "); END;";
                } else if(txn_type == 1) { // SendPayment
                    sql_str = "BEGIN sp_send_payment_v2(" + std::to_string(acc1_val) + ", " + std::to_string(acc2_val) + "); END;";
                } else if(txn_type == 2) { // DepositChecking
                    sql_str = "BEGIN sp_deposit_checking_v2(" + std::to_string(acc1_val) + ", " + std::to_string(amount_val) + "); END;";
                } else if(txn_type == 3) { // WriteCheck
                    sql_str = "BEGIN sp_write_check_v2(" + std::to_string(acc1_val) + ", " + std::to_string(amount_val) + "); END;";
                } else if(txn_type == 4) { // Balance
                    sql_str = "BEGIN sp_balance_v2(" + std::to_string(acc1_val) + "); END;";
                } else if(txn_type == 5) { // TransactSavings
                    sql_str = "BEGIN sp_transact_savings_v2(" + std::to_string(acc1_val) + ", " + std::to_string(amount_val) + "); END;";
                }

                // Execute
                try {
                if (yacDirectExecute(stmt_direct, (YacChar*)sql_str.c_str(), YAC_NULL_TERM_STR) != YAC_SUCCESS) {
                    std::cerr << "DirectExec failed txn=" << txn_type << " SQL: " << sql_str << " at ip " << info.ip_port << std::endl;
                    YacInt32 errCode;
                    char msg[1024];
                    YacTextPos pos;
                    yacGetDiagRec(&errCode, msg, sizeof(msg), NULL, NULL, 0, &pos);
                    std::cerr << "Error Code: " << errCode << ", Message: " << msg << std::endl;
                    } else {
                        // Success
                        // In V2, no cursor output is returned.
                        
                        // commit
                        yacCommit(conn);
                        
                        // Update Router Stats (ctid_ret_page_ids will be empty)
                        // smart_router->update_key_page(txn_entry, tables, keys, rw, ctid_ret_page_ids, compute_node_id); // no need to update pages   
                    }
                } catch (const std::exception &e) {
                    std::cerr << "Transaction (YashanDB) failed: " << e.what() << std::endl;
                    logger_->info("Transaction (YashanDB) failed: " + std::string(e.what()));
                }
            }

            // 计时
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double exec_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 + 
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_exec_time(params->compute_node_id_connecter, params->thread_id, exec_time);

            double current_time_ms = end_time.tv_sec * 1000.0 + end_time.tv_nsec / 1000000.0;
            if (WarmupEnd) {
                if(params->latency_record) params->latency_record->push_back(exec_time);
                if(params->fetch_latency_record && txn_entry->fetch_time > 0) {
                     params->fetch_latency_record->push_back(current_time_ms - txn_entry->fetch_time);
                }
            }

            clock_gettime(CLOCK_MONOTONIC, &start_time);
            tit->mark_done(txn_entry, call_id); // 一体化：标记完成，删除由 TIT 统一管理
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double mark_done_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_mark_done_time(params->compute_node_id_connecter, params->thread_id, mark_done_time);
            
        #if LOG_TXN_EXEC
            clock_gettime(CLOCK_MONOTONIC, &start_time);
            for (auto account : txn_entry->accounts) {
                for(auto key : hottest_keys) {
                    if (account == key) {
                        // log the hotspot exection
                        logger_->info("Batch " + std::to_string(txn_entry->batch_id) + 
                            " Node: " + std::to_string(compute_node_id) + 
                            " txn id: " + std::to_string(txn_entry->tx_id) + 
                            " txn type (smallbank): " + std::to_string(txn_entry->txn_type) + 
                            " connector id: " + std::to_string(params->compute_node_id_connecter) + 
                            " txn type (schedule): " + std::to_string((int)txn_entry->schedule_type) + 
                            " access account " + std::to_string(account) + 
                            " exec time: " + std::to_string(exec_time) + 
                            " group id: " + std::to_string(txn_entry->group_id) + 
                            " dependency group ids: " + [&]() {
                                std::string os;
                                for(auto dep_id : txn_entry->dependency_group_id) {
                                    os += std::to_string(dep_id) + " ";
                                }
                                return os;
                            }() +
                            " batch id: " + std::to_string(txn_entry->batch_id)
                        );
                    }
                }
            }
        #endif
        // #if LOG_TXN_EXEC
            if (exec_time > 1000) 
                logger_->warning("Node: " + std::to_string(compute_node_id) + "Transaction execution time exceeded 10 ms: " + std::to_string(exec_time) + 
                " ms, call id: " + std::to_string(call_id) + " Txn op: " + [&]() {
                    std::string os;
                    os += "txn_type: " + std::to_string(txn_entry->txn_type) + " ";
                    os += "table: ";
                    for(int i= 0; i < tables.size(); i++) {
                        os += std::to_string(tables[i]) + " ";
                    }
                    os += "key: ";
                    for(int i= 0; i < keys.size(); i++) {
                        os += std::to_string(keys[i]) + " ";
                    }
                    os += "original pages: ";
                    for(int i= 0; i < txn_entry->accessed_page_ids.size(); i++) {
                        os += std::to_string(txn_entry->accessed_page_ids[i]) + " ";
                    }
                    os += " return pages: ";
                    for(int i= 0; i < ctid_ret_page_ids.size(); i++) {
                        os += std::to_string(ctid_ret_page_ids[i]) + " ";
                    }
                    os += " group id: " + std::to_string(txn_entry->group_id) + " ";
                    os += " dependency group ids: ";
                    for(auto dep_id : txn_entry->dependency_group_id) {
                        os += std::to_string(dep_id) + " ";
                    }
                    os += " batch id: " + std::to_string(txn_entry->batch_id) + " ";
                    return os;
                }());
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double log_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                               (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
            smart_router->add_worker_thread_log_debug_info_time(params->compute_node_id_connecter, params->thread_id, log_time);
        }
    #if LOG_TXN_EXEC
        struct timespec start_time, end_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        logger_->info("Finished processing batch with call_id: " + std::to_string(call_id));
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        double log_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                        (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
        smart_router->add_worker_thread_log_debug_info_time(params->compute_node_id_connecter, params->thread_id, log_time);
    #endif 
    }
    // Cleanup YashanDB handles
    if (USE_SCALAR_PARAM) {
        for(int i = 0; i < 6; i++) {
             if(stmts_scalar[i]) yacFreeHandle(YAC_HANDLE_STMT, stmts_scalar[i]);
        }
    } else {
        if(stmt_direct) yacFreeHandle(YAC_HANDLE_STMT, stmt_direct);
    }
    yacDisconnect(conn);
    yacFreeHandle(YAC_HANDLE_DBC, conn);
    yacFreeHandle(YAC_HANDLE_ENV, env);
    std::cout << "Finished running Yashan smallbank transactions via stored procedures." << std::endl;
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

    int system_mode = 0; // Default system mode
    int access_pattern = 0; // 0: uniform, 1: zipfian, 2: hotspot
    // default parameters
    double zipfian_theta = 0.99; // Zipfian distribution parameter
    double hotspot_fraction = 0.2; // Fraction of accounts that are hot
    double hotspot_access_prob = 0.8; // Probability of accessing hot accounts
    // execution mode
    bool use_sp = true; // default: use stored procedures
    
    // for smallbank
    int account_num = 300000; // Number of accounts to load
    int warehouse_num = 10; // Number of warehouses for tpcc

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
        else if (arg == "--workload") {
            if (i + 1 < argc) {
                std::string workload_name = argv[++i];
                if (workload_name == "smallbank") {
                    Workload_Type = 0;
                } else if (workload_name == "ycsb") {
                    Workload_Type = 1;
                } else if (workload_name == "tpcc") {
                    Workload_Type = 2;
                }
                else {
                    std::cerr << "Error: Unknown workload type '" << workload_name << "'. Supported types are 'smallbank', 'ycsb', and 'tpcc'." << std::endl;
                    return -1;
                }
                std::cout << "Workload type set to: " << workload_name << std::endl;
            } else {
                std::cerr << "Error: --workload requires a value (smallbank|ycsb|tpcc)" << std::endl;
                return -1;
            }
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
                account_num = std::stoi(argv[++i]);
                if (account_num <= 0) {
                    std::cerr << "Error: Account count must be greater than 0" << std::endl;
                    return -1;
                }
                if (account_num > 10000000) {
                    std::cerr << "Warning: Account count is very large (" << account_num << "), this may take a long time" << std::endl;
                }
                std::cout << "Account count set to: " << account_num << std::endl;
            } else {
                std::cerr << "Error: --account-count requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--warehouse-count") {
            if (i + 1 < argc) {
                warehouse_num = std::stoi(argv[++i]);
                if (warehouse_num <= 0) {
                    std::cerr << "Error: Warehouse count must be greater than 0" << std::endl;
                    return -1;
                }
                if (warehouse_num > 10000) {
                    std::cerr << "Warning: Warehouse count is very large (" << warehouse_num << "), this may take a long time" << std::endl;
                }
                std::cout << "Warehouse count set to: " << warehouse_num << std::endl;
            } else {
                std::cerr << "Error: --warehouse-count requires a value" << std::endl;
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
        else if (arg == "--sys_extend_size") {
            if (i + 1 < argc) {
                PreExtendPageSize = std::stoi(argv[++i]);
                if (PreExtendPageSize < 0) {
                    std::cerr << "Error: sys_extend_size must be non-negative" << std::endl;
                    return -1;
                }
                std::cout << "System extend size set to: " << PreExtendPageSize << std::endl;
            } else {
                std::cerr << "Error: --sys_extend_size requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--sys_index_extend_size") {
            if (i + 1 < argc) {
                PreExtendIndexPageSize = std::stoi(argv[++i]);
                if (PreExtendIndexPageSize < 0) {
                    std::cerr << "Error: sys_index_extend_size must be non-negative" << std::endl;
                    return -1;
                }
                std::cout << "System index extend size set to: " << PreExtendIndexPageSize << std::endl;
            } else {
                std::cerr << "Error: --sys_index_extend_size requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--affinity-txn-ratio") {
            if (i + 1 < argc) {
                AffinityTxnRatio = std::stod(argv[++i]);
                if (AffinityTxnRatio < 0.0 || AffinityTxnRatio > 1.0) {
                    std::cerr << "Error: AffinityTxnRatio must be between 0.0 and 1.0" << std::endl;
                    return -1;
                }
                std::cout << "Affinity transaction ratio set to: " << AffinityTxnRatio << std::endl;
            } else {
                std::cerr << "Error: --AffinityTxnRatio requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--load-data-only") {
            LOAD_DATA_ONLY = true;
            std::cout << "Load data only mode enabled." << std::endl;
        }
        else if (arg == "--skip-load-data") {
            SKIP_LOAD_DATA = true;
            std::cout << "Skip load data mode enabled." << std::endl;
        }
        else if (arg == "--batch-size") {
            if (i + 1 < argc) {
                BatchRouterProcessSize = std::stoi(argv[++i]);
                if (BatchRouterProcessSize <= 0) {
                    std::cerr << "Error: Batch size must be greater than 0" << std::endl;
                    return -1;
                }
                std::cout << "Batch size set to: " << BatchRouterProcessSize << std::endl;
            } else {
                std::cerr << "Error: --batch-size requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--db-type") {
             if (i + 1 < argc) {
                DB_TYPE = std::stoi(argv[++i]);
                if (DB_TYPE != 0 && DB_TYPE != 1) {
                    std::cerr << "Error: db-type must be 0 (PostgreSQL) or 1 (YashanDB)" << std::endl;
                    return -1;
                }
                std::cout << "DB Type set to: " << (DB_TYPE == 0 ? "PostgreSQL" : "YashanDB") << std::endl;
            } else {
                std::cerr << "Error: --db-type requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--num-bucket") {
            if (i + 1 < argc) {
                NumBucket = std::stoi(argv[++i]);
                if (NumBucket <= 0) {
                    std::cerr << "Error: num-buckets must be greater than 0" << std::endl;
                    return -1;
                }
                std::cout << "Number of buckets set to: " << NumBucket << std::endl;
            } else {
                std::cerr << "Error: --num-bucket requires a value" << std::endl;
                print_usage(argv[0]);
                return -1;
            }
        }
        else if (arg == "--enable-long-txn") {
            Enable_Long_Txn = true;
            std::cout << "Enable long transaction mode." << std::endl;
        }
        else if (arg == "--long-txn-length") {
            if (i + 1 < argc) {
                Long_Txn_Length = std::stoi(argv[++i]);
                if (Long_Txn_Length <= 0) {
                    std::cerr << "Error: long-txn-length must be greater than 0" << std::endl;
                    return -1;
                }
                std::cout << "Long transaction length set to: " << Long_Txn_Length << std::endl;
            } else {
                std::cerr << "Error: --long-txn-length requires a value" << std::endl;
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
    std::cout << "Workload: ";
    if (Workload_Type == 0) std::cout << "SmallBank";
    else if (Workload_Type == 1) std::cout << "YCSB";
    else if (Workload_Type == 2) std::cout << "TPC-C";
    else std::cout << "Unknown";
    std::cout << std::endl;
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
        std::cout << "\033[31m  key hashing router \033[0m" << std::endl;
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
    case 23: 
        std::cout << "\033[31m  score-based router with metis and load balancing \033[0m" << std::endl;
        break;
    case 24:
        std::cout << "\033[31m  score-based router with page ownership and load balancing \033[0m" << std::endl;
        break;
    case 25:
        std::cout << "\033[31m  score-based router with load balancing only \033[0m" << std::endl;
        break;
    default:
        std::cerr << "\033[31m  <Unknown> \033[0m" << std::endl;
        return -1;
    }
    std::cout << "Account count: " << account_num << std::endl;
    
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
    
    // --- Initialize Workload ---
    SmallBank* smallbank = nullptr;
    YCSB* ycsb = nullptr;
    TPCC* tpcc = nullptr;
    if (Workload_Type == 0) {
        smallbank = new SmallBank(account_num, access_pattern);
        if(access_pattern == 1) smallbank->set_zipfian_theta(zipfian_theta);
        if(access_pattern == 2) smallbank->set_hotspot_params(hotspot_fraction, hotspot_access_prob);
        std::cout << "SmallBank benchmark initialized." << std::endl;
    } else if (Workload_Type == 1) {
        ycsb = new YCSB(account_num, access_pattern); 
        if(access_pattern == 1) ycsb->set_zipfian_theta(zipfian_theta);
        if(access_pattern == 2) ycsb->set_hotspot_params(hotspot_fraction, hotspot_access_prob);  
        std::cout << "YCSB benchmark initialized." << std::endl;
    } else if (Workload_Type == 2) {
        tpcc = new TPCC(warehouse_num); // Use the specified number of warehouses
        std::cout << "TPC-C benchmark initialized with " << warehouse_num << " warehouses." << std::endl;
    }

    std::cout << "Worker threads: " << worker_threads << std::endl;
    std::cout << "====================" << std::endl;

    // --- Load Database Connection Info ---
    std::cout << "Loading database connection info..." << std::endl;

    if(DB_TYPE == 0) {
        std::cout << "Database Type: PostgreSQL" << std::endl;
    
        // !!! need to update when changing the cluster environment
        // DBConnection.push_back("host=10.12.2.125 port=54321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.12.2.127 port=54321 user=system password=123456 dbname=smallbank");

        // kes 双机, 旧版本
        // DBConnection.push_back("host=10.10.2.41 port=54321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.10.2.42 port=54321 user=system password=123456 dbname=smallbank");

        // kes 双机, 新版本
        // DBConnection.push_back("host=10.10.2.41 port=44321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.10.2.42 port=44321 user=system password=123456 dbname=smallbank");

        // kes 四机, 新版本
        // DBConnection.push_back("host=10.10.2.41 port=44321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.10.2.42 port=44321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.10.2.44 port=44321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.10.2.45 port=44321 user=system password=123456 dbname=smallbank");

        // kes 单机
        // DBConnection.push_back("host=10.10.2.41 port=64321 user=system password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.10.2.41 port=64321 user=system password=123456 dbname=smallbank");

        // 147 本机 pg
        // DBConnection.push_back("host=127.0.0.1 port=6432 user=hcy password=123456 dbname=smallbank"); // pg12
        // DBConnection.push_back("host=127.0.0.1 port=6432 user=hcy password=123456 dbname=smallbank"); // pg12 

        // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank"); // pg13
        // DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank"); // pg13

        DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");
        DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");

        // DBConnection.push_back("host=10.77.110.147 port=5432 user=hcy password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.77.110.147 port=5432 user=hcy password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.77.110.147 port=5432 user=hcy password=123456 dbname=smallbank");
        // DBConnection.push_back("host=10.77.110.147 port=5432 user=hcy password=123456 dbname=smallbank");

        ComputeNodeCount = DBConnection.size();
        std::cout << "Database connection info loaded. Total nodes: " << ComputeNodeCount << std::endl;
    } else if (DB_TYPE == 1) {
        std::cout << "Database Type: YashanDB" << std::endl;
        // 崖山RAC
        YashanDBConnections.clear();
        YashanDBConnections.push_back({"10.10.2.35:1688", "sys", "Rdjc#2025"});
        YashanDBConnections.push_back({"10.10.2.36:1688", "sys", "Rdjc#2025"});
        // YashanDBConnections.push_back({"10.10.2.39:1688", "sys", "Rdjc#2025"});
        // YashanDBConnections.push_back({"10.10.2.40:1688", "sys", "Rdjc#2025"});
        ComputeNodeCount = YashanDBConnections.size();
        std::cout << "YashanDB connection info loaded. Total nodes: " << ComputeNodeCount << std::endl;
    }

    if (DB_TYPE == 0) {
        //! --- PG series Database Connection ---
        std::vector<pqxx::connection*> conns;
        try {
            for(int i = 0; i < ComputeNodeCount; i++) {
                pqxx::connection* conn = new pqxx::connection(DBConnection[i]);
                if (!conn->is_open()) {
                    std::cerr << "Failed to connect to the database. conninfo: " + DBConnection[i] << std::endl;
                    return -1;
                } else {
                    std::cout << "Connected to the database node " << i << " successfully." << std::endl;
                }
                conns.push_back(conn);
            }
        } catch (const std::exception &e) {
            std::cerr << "Error while connecting to KingBase: " + std::string(e.what()) << std::endl;
            return -1;
        }
        assert(conns.size() == ComputeNodeCount);
        pqxx::connection* conn0 = conns[0];

        // --- Load Database Data ---
        // Create table and indexes
        if(!SKIP_LOAD_DATA) {
            std::cout << "Creating tables and indexes..." << std::endl;
            if (Workload_Type == 0) {
                smallbank->create_table(conn0);
                if (use_sp) smallbank->create_smallbank_stored_procedures(conn0);
            } else if (Workload_Type == 1) {
                ycsb->create_table(conn0);
                if (use_sp) ycsb->create_ycsb_stored_procedures(conn0);
            } else if (Workload_Type == 2) {
                tpcc->create_table(conn0);
                if (use_sp) tpcc->create_tpcc_stored_procedures(conn0);
            }
            std::cout << "Tables and indexes created successfully." << std::endl;

            // generate friend graph in a separate thread
            std::cout << "Generating friend graph in a separate thread..." << std::endl;
            auto start_friend_gen = std::chrono::high_resolution_clock::now();
            std::thread friend_thread([&]() {
                if(Workload_Type == 0) smallbank->generate_friend_graph();
            });

            // load data into the database
            std::cout << "Loading data into the database..." << std::endl;
            // 若为 SmallBank，记录键→页映射以便后续初始化 Router 时跳过 DB 扫描
            if (Workload_Type == 0) {
                g_smallbank_key_page_map = smallbank->load_data(conn0);
            } else if (Workload_Type == 1) {
                ycsb->load_data(conn0);
            } else if (Workload_Type == 2) {
                tpcc->load_data();
            }
            std::cout << "Data loaded successfully." << std::endl;
            // Wait for friend thread to complete
            friend_thread.join();
            auto end_friend_gen = std::chrono::high_resolution_clock::now();
            double friend_gen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_friend_gen - start_friend_gen).count();
            std::cout << "Friend graph generation completed in " << friend_gen_ms << " ms." << std::endl;
        } else {
            std::cout << "Skipping data loading. "<< std::endl;
            std::cout << "Checking if tables exist..." << std::endl;
            bool tables_exist = false;
            if (Workload_Type == 0) {
                tables_exist = smallbank->check_table_exists(conn0);
            } else if (Workload_Type == 1) {
                tables_exist = ycsb->check_table_exists(conn0);
            } else if (Workload_Type == 2) {
                tables_exist = tpcc->check_table_exists(conn0);
            }
            std::cout << "Table existence check completed." << std::endl;
            if (!tables_exist) {
                std::cerr << "Error: Required tables do not exist in the database. Please load the data first." << std::endl;
                return -1;
            } else {
                std::cout << "Check OK: Required tables exist in the database." << std::endl;
            }
            // generate friend graph in a separate thread
            std::cout << "Generating friend graph in a separate thread..." << std::endl;
            auto start_friend_gen = std::chrono::high_resolution_clock::now();
            std::thread friend_thread([&]() {
                if(Workload_Type == 0) smallbank->generate_friend_graph();
            });

            bool accounts_num_verify = false;
            if (Workload_Type == 0) {
                accounts_num_verify = smallbank->check_account_count(conn0, account_num);
            } else if (Workload_Type == 1) {
                accounts_num_verify = ycsb->check_record_count(conn0, account_num);
            } else if (Workload_Type == 2) {
                accounts_num_verify = tpcc->check_warehouse_count(conn0, warehouse_num);
            }
            if (!accounts_num_verify) {
                std::cerr << "Error: Account/Record count in the database does not match the expected count. Please verify the data." << std::endl;
                return -1;
            } else {
                std::cout << "Check OK: Account/Record count matches the expected count." << std::endl;
            }
            // Wait for friend thread to complete
            friend_thread.join();
            auto end_friend_gen = std::chrono::high_resolution_clock::now();
            double friend_gen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_friend_gen - start_friend_gen).count();
            std::cout << "Friend graph generation completed in " << friend_gen_ms << " ms." << std::endl;
        }
        if(LOAD_DATA_ONLY) {
            std::cout << "Load data only mode enabled. Exiting after data load." << std::endl;
            // Clean up connections
            for(auto conn : conns) {
                delete conn;
            }
            return 0;
        }
    }
    else if (DB_TYPE == 1) {
        if (!SKIP_LOAD_DATA) {
            std::cout << "Starting YashanDB initialization..." << std::endl;
            assert(YashanDBConnections.size() > 0);
            assert(Workload_Type == 0); // 目前仅支持 SmallBank
            smallbank->create_table_yashan();
            // smallbank->create_smallbank_stored_procedures_yashan(); // !不要在这里jian建 SP，手动建, 否则会影响后续 load data 步骤
            // smallbank->create_smallbank_stored_procedures_yashan_new(); // Create V2 procedures
            smallbank->create_smallbank_stored_procedures_yashan_scalar(); // Create Scalar procedures
            
            // generate friend graph in a separate thread
            std::cout << "Generating friend graph in a separate thread..." << std::endl;
            auto start_friend_gen = std::chrono::high_resolution_clock::now();
            std::thread friend_thread([&]() {
                if(Workload_Type == 0) smallbank->generate_friend_graph();
            });

            // load data into the database
            // 若为 SmallBank，记录键→页映射以便后续初始化 Router 时跳过 DB 扫描
            std::cout << "Loading data into YashanDB..." << std::endl;
            if (Workload_Type == 0) {
                smallbank->load_data_yashan(); // load data
                g_smallbank_key_page_map = smallbank->fetch_row_id_yashan(); // fetch row id mapping
            }
            std::cout << "Data loaded into YashanDB successfully." << std::endl;
            // Wait for friend thread to complete
            friend_thread.join();
            auto end_friend_gen = std::chrono::high_resolution_clock::now();
            double friend_gen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_friend_gen - start_friend_gen).count();
            std::cout << "Friend graph generation completed in " << friend_gen_ms << " ms." << std::endl;
        } else {
            std::cout << "Skipping data loading. "<< std::endl;
            // generate friend graph in a separate thread
            std::cout << "Generating friend graph in a separate thread..." << std::endl;
            auto start_friend_gen = std::chrono::high_resolution_clock::now();
            std::thread friend_thread([&]() {
                if(Workload_Type == 0) smallbank->generate_friend_graph();
            });

            // load data into the database
            std::cout << "Fetching row ID mapping from YashanDB..." << std::endl;
            if (Workload_Type == 0) {
                g_smallbank_key_page_map = smallbank->fetch_row_id_yashan(); // fetch row id mapping
            }
            std::cout << "Row ID mapping fetched successfully." << std::endl;
            // Wait for friend thread to complete
            friend_thread.join();
            auto end_friend_gen = std::chrono::high_resolution_clock::now();
            double friend_gen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_friend_gen - start_friend_gen).count();
            std::cout << "Friend graph generation completed in " << friend_gen_ms << " ms." << std::endl;
        }
        if(LOAD_DATA_ONLY) {
            std::cout << "Load data only mode enabled. Exiting after data load." << std::endl;
            return 0;
        }
    }
    else assert(false);

    // Initialize Smart Router anyway
    std::cout << "Initializing Smart Router..." << std::endl;
    // Create a BtreeService（索引名因 workload 不同而不同）
    std::vector<std::string> index_names;
    if (Workload_Type == 0) index_names = {"idx_checking_id", "idx_savings_id"};
    else if (Workload_Type == 1) index_names = {"idx_usertable_id"};
    // else if (Workload_Type == 2) index_names = {"warehouse_pkey", "district_pkey", "customer_pkey", "stock_pkey"}; // TPC-C indexes
    else if (Workload_Type == 2) index_names = {}; // TPC-C indexes
    
    // BtreeIndexService *index_service = new BtreeIndexService(DBConnection, index_names, read_btree_mode, read_frequency); // !not used

    // initialize the transaction pool
    SlidingTransactionInforTable* tit = new SlidingTransactionInforTable(logger_, 2*ComputeNodeCount*worker_threads*BatchRouterProcessSize);
    TxnPool* txn_pool = new TxnPool(4, TxnPoolMaxSize, tit);
    auto shared_txn_queue = new SharedTxnQueue(tit, logger_, TxnQueueMaxSize);
    auto pending_txn_queue = new PendingTxnSet(tit, logger_);
    // initialize the transaction queues for each compute node connection
    for (int i = 0; i < ComputeNodeCount; i++) { 
        txn_queues.push_back(new TxnQueue(tit, shared_txn_queue, pending_txn_queue, logger_, i, TxnQueueMaxSize));
    }
    // Initialize NewMetis
    NewMetis* metis = new NewMetis(logger_);

    SmartRouter::Config cfg{};
    SmartRouter* smart_router = new SmartRouter(cfg, txn_pool, txn_queues, pending_txn_queue, worker_threads, nullptr, metis, logger_, smallbank, ycsb, tpcc);
    std::cout << "Smart Router initialized." << std::endl;

    // TIT: 当后续事务的入度变为0时，立即调度到目标节点
    tit->set_ready_callback([smart_router](std::vector<TxnQueueEntry*> entry, int finish_call_id){
        smart_router->schedule_ready_txn(std::move(entry), finish_call_id);
    });

    // Initialize the key-page map
    if (!SKIP_LOAD_DATA && Workload_Type == 0) {
        int N = smallbank->get_account_count();
        for (int id = 1; id <= N; ++id) {
            if (id < (int)g_smallbank_key_page_map.checking_page.size()) {
                int cpg = g_smallbank_key_page_map.checking_page[id];
                if (cpg >= 0) {
                    smart_router->initial_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, cpg);
                }
                else std::cout << "Warning: Invalid checking page for account " << id << std::endl;
            }
            if (id < (int)g_smallbank_key_page_map.savings_page.size()) {
                int spg = g_smallbank_key_page_map.savings_page[id];
                if (spg >= 0) {
                    smart_router->initial_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, spg);
                }
                else std::cout << "Warning: Invalid savings page for account " << id << std::endl;
            }
        }
#if MLP_PREDICTION
        smart_router->mlp_train_after_init();
#endif
        std::cout << "Data page mapping initialization from load_data completed." << std::endl;
    } 
    else if (SKIP_LOAD_DATA && Workload_Type == 0 && DB_TYPE == 1) {
        // 若跳过数据加载，且为 SmallBank + YashanDB，则从 g_smallbank_key_page_map 初始化
        int N = smallbank->get_account_count();
        for (int id = 1; id <= N; ++id) {
            if (id < (int)g_smallbank_key_page_map.checking_page.size()) {
                int cpg = g_smallbank_key_page_map.checking_page[id];
                if (cpg >= 0) {
                    smart_router->initial_key_page((table_id_t)SmallBankTableType::kCheckingTable, id, cpg);
                }
                else std::cout << "Warning: Invalid checking page for account " << id << std::endl;
            }
            if (id < (int)g_smallbank_key_page_map.savings_page.size()) {
                int spg = g_smallbank_key_page_map.savings_page[id];
                if (spg >= 0) {
                    smart_router->initial_key_page((table_id_t)SmallBankTableType::kSavingsTable, id, spg);
                }
                else std::cout << "Warning: Invalid savings page for account " << id << std::endl;
            }
        }
    }
    else if (DB_TYPE == 0){
        // 走原有从数据库扫描初始化的路径
        init_key_page_map(smart_router, smallbank, ycsb, tpcc);
    }
    else assert(false);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Create a performance snapshot
    int start_snapshot_id = -1;
    if(DB_TYPE == 0) start_snapshot_id = create_perf_kwr_snapshot();

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
    for(int i = 0; i < worker_threads * 2; i++) {
        if(Workload_Type == 0) {
            client_gen_txn_threads.emplace_back([i, txn_pool, smallbank]() {
                smallbank->generate_smallbank_txns_worker(i, txn_pool);
            });
        } else if (Workload_Type == 1) {
            client_gen_txn_threads.emplace_back([i, txn_pool, ycsb]() {
                ycsb->generate_ycsb_txns_worker(i, txn_pool);
            });
        } else if (Workload_Type == 2) {
            client_gen_txn_threads.emplace_back([i, txn_pool, tpcc]() {
                tpcc->generate_tpcc_txns_worker(i, txn_pool);
            });
        }
    }

    // !Start the transaction threads
    std::vector<std::vector<double>> worker_latencies(ComputeNodeCount * worker_threads);
    std::vector<std::vector<double>> worker_fetch_latencies(ComputeNodeCount * worker_threads);
    // pre-reserve some space to avoid frequent reallocations, e.g. 1M per thread
    for(auto& v : worker_latencies) v.reserve(1000000); 
    for(auto& v : worker_fetch_latencies) v.reserve(1000000);

    std::vector<std::thread> db_conn_threads;
    for(int i = 0; i < ComputeNodeCount; i++) {
        for(int j = 0; j < worker_threads; j++) {
            thread_params* params = new thread_params();
            params->compute_node_id_connecter = i;
            params->thread_id = j;
            params->thread_count = worker_threads;
            params->latency_record = &worker_latencies[i * worker_threads + j];
            params->fetch_latency_record = &worker_fetch_latencies[i * worker_threads + j];
            params->smallbank = smallbank;
            params->ycsb = ycsb;
            params->tpcc = tpcc;
            params->smart_router = smart_router;
            params->tit = tit;

            if(Workload_Type == 0) {
                if (use_sp) {
                    if (DB_TYPE == 1) { // YashanDB
                        db_conn_threads.emplace_back(run_yashan_smallbank_txns_sp, params, logger_);
                    } else { // PostgreSQL
                        db_conn_threads.emplace_back(run_smallbank_txns_sp, params, logger_);
                    }
                } else {
                    if (DB_TYPE == 1) {
                        std::cerr << "Error: YashanDB currently only supports SP mode (Client-side logic) for SmallBank." << std::endl;
                        exit(-1);
                    }
                    db_conn_threads.emplace_back(run_smallbank_txns, params, logger_);
                }
            }
            else if (Workload_Type == 1) {
                if (use_sp) 
                    db_conn_threads.emplace_back(run_ycsb_txns_sp, params, logger_);
                else assert(false && "Only support YCSB with stored procedures!");
            }
            else if (Workload_Type == 2) {
                if (use_sp)
                    db_conn_threads.emplace_back(run_tpcc_txns_sp, params, logger_);
                else assert(false && "Only support TPC-C with stored procedures!");
            }
            // 测试路由吞吐量
            // db_conn_threads.emplace_back(run_smallbank_empty, params, logger_);
        }
    }

    // !begin RUN Router
    smart_router->start_router();

    // Start a separate thread to print TPS periodically
    std::thread tps_thread(print_tps_loop, smart_router, txn_pool, logger_);
    tps_thread.detach(); // Detach the thread to run independently

    while(exe_count <= MetisWarmupRound * PARTITION_INTERVAL * 1.0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "\033[31m Warmup rounds completed. Create the smart router snapshot. \033[0m" << std::endl;
    auto warmup_end_time = std::chrono::high_resolution_clock::now();
    long long warmup_exe_count = exe_count;
    if(smart_router) { 
        snapshot1 = take_router_snapshot(smart_router);
        print_diff_snapshot(snapshot0, snapshot1);
    }
    // Create a performance snapshot after warmup
    // int mid_snapshot_id = create_perf_kwr_snapshot();

    // Wait for all threads to complete
    for(auto& thread : db_conn_threads) {
        thread.join();
    }
    snapshot2 = take_router_snapshot(smart_router);

    // Stop the client transaction generation threads
    for(auto& thread : client_gen_txn_threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Create a performance snapshot after running transactions
    std::this_thread::sleep_for(std::chrono::seconds(2)); // sleep for a while to ensure all operations are completed
    int end_snapshot_id = -1;
    if(DB_TYPE == 0) end_snapshot_id = create_perf_kwr_snapshot();
    std::cout << "Performance snapshots created: Start ID = " << start_snapshot_id 
              << ", End ID = " << end_snapshot_id << std::endl;
    
    // Print Report
    std::cout << "\n=== Performance Report ===" << std::endl;
    std::cout << "System mode: " << SYSTEM_MODE << " !!!" << std::endl;
    if(smart_router) {
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
    for(int i =0; i<ComputeNodeCount; i++){
        std::cout << "node " << i << " routed txn count: " << exec_txn_cnt_per_node[i] << std::endl;
    }
    if (Workload_Type == 0 || Workload_Type == 1) {
        std::cout << "Total accounts loaded: " << account_num << std::endl;
        std::cout << "Access pattern used: " << access_pattern_name << std::endl;
    }
    else if (Workload_Type == 2) {
        std::cout << "Warehouse count: " << warehouse_num << std::endl;
        std::cout << "Access pattern used: " << access_pattern_name << std::endl;
    }

    std::cout << "Total transactions executed: " << exe_count << std::endl;
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    double s = ms / 1000.0; // Convert milliseconds to seconds
    std::cout << "Throughput: " << exe_count / s << " transactions per second" << std::endl;

    double ms_after_warmup = std::chrono::duration_cast<std::chrono::milliseconds>(end - warmup_end_time).count();
    double s_after_warmup = ms_after_warmup / 1000.0;
    long long txn_after_warmup = exe_count - warmup_exe_count;
    std::cout << "Throughput (after warmup): " << txn_after_warmup / s_after_warmup << " transactions per second" << std::endl;

    // --- Latency Statistics (After Warmup) ---
    std::vector<double> all_latencies;
    // reserve total size roughly
    size_t total_size = 0;
    for (const auto& v : worker_latencies) total_size += v.size();
    all_latencies.reserve(total_size);
    for (const auto& v : worker_latencies) {
        all_latencies.insert(all_latencies.end(), v.begin(), v.end());
    }
    if (!all_latencies.empty()) {
        std::sort(all_latencies.begin(), all_latencies.end());
        double sum = 0;
        for (double lat : all_latencies) sum += lat;
        double avg = sum / all_latencies.size();
        double p50 = all_latencies[static_cast<size_t>(all_latencies.size() * 0.50)];
        double p95 = all_latencies[static_cast<size_t>(all_latencies.size() * 0.95)];
        double p99 = all_latencies[static_cast<size_t>(all_latencies.size() * 0.99)];
        
        std::cout << "Latency Statistics (After Warmup):" << std::endl;
        std::cout << "  Average: " << avg << " ms" << std::endl;
        std::cout << "  P50: " << p50 << " ms" << std::endl;
        std::cout << "  P95: " << p95 << " ms" << std::endl;
        std::cout << "  P99: " << p99 << " ms" << std::endl;
    } else {
        std::cout << "No transactions recorded after warmup for latency stats." << std::endl;
    }

    // --- Fetch to Complete Latency Statistics (After Warmup) ---
    std::vector<double> all_fetch_latencies;
    total_size = 0;
    for (const auto& v : worker_fetch_latencies) total_size += v.size();
    all_fetch_latencies.reserve(total_size);
    for (const auto& v : worker_fetch_latencies) {
        all_fetch_latencies.insert(all_fetch_latencies.end(), v.begin(), v.end());
    }
    if (!all_fetch_latencies.empty()) {
        std::sort(all_fetch_latencies.begin(), all_fetch_latencies.end());
        double sum = 0;
        for (double lat : all_fetch_latencies) sum += lat;
        double avg = sum / all_fetch_latencies.size();
        double p50 = all_fetch_latencies[static_cast<size_t>(all_fetch_latencies.size() * 0.50)];
        double p95 = all_fetch_latencies[static_cast<size_t>(all_fetch_latencies.size() * 0.95)];
        double p99 = all_fetch_latencies[static_cast<size_t>(all_fetch_latencies.size() * 0.99)];
        
        std::cout << "Fetch-to-Complete Latency Statistics (After Warmup):" << std::endl;
        std::cout << "  Average: " << avg << " ms" << std::endl;
        std::cout << "  P50: " << p50 << " ms" << std::endl;
        std::cout << "  P95: " << p95 << " ms" << std::endl;
        std::cout << "  P99: " << p99 << " ms" << std::endl;
    } else {
        std::cout << "No transactions recorded after warmup for fetch latency stats." << std::endl;
    }

    // Generate performance report, file name inluding the timestamp

    // std::string report_file_warm_phase = kwr_report_name +  "_fisrt.html";
    // generate_perf_kwr_report(conn0, start_snapshot_id, mid_snapshot_id, report_file_warm_phase);
    std::string report_file_run_phase = kwr_report_name +  "_end.html";
    // generate_perf_kwr_report(conn0, mid_snapshot_id, end_snapshot_id, report_file_run_phase);
    if(DB_TYPE == 0) generate_perf_kwr_report(start_snapshot_id, end_snapshot_id, report_file_run_phase);

    // restore streams (best-effort; program may exit via signal handler earlier)
    std::cout.rdbuf(old_cout);
    std::cerr.rdbuf(old_cerr);
    output_file.flush();
    output_file.close();
    return 0;
}