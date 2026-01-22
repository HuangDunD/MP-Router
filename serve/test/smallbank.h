// Author: Chunyue Huang
// Copyright (c) 2024
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>
#include <pqxx/pqxx>

#include "friend_simulate.h"
#include "util/zipf.h"
#include "common.h"
#include "config.h"
#include "txn_queue.h"
#include "parse.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_WRITE_CHECK 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_TRANSACT_SAVINGS 15

// #define FREQUENCY_AMALGAMATE 0
// #define FREQUENCY_BALANCE 0
// #define FREQUENCY_DEPOSIT_CHECKING 50
// #define FREQUENCY_SEND_PAYMENT 0
// #define FREQUENCY_TRANSACT_SAVINGS 0
// #define FREQUENCY_WRITE_CHECK 50

#define TX_HOT 80 /* Percentage of txns that use accounts from hotspot */

// Helpers for generating workload
#define SmallBank_TX_TYPES 7
enum class SmallBankTxType : int {
  kAmalgamate,
  kSendPayment,
  kDepositChecking,
  kWriteCheck,
  kBalance,
  kTransactSavings,
  kMultiUpdate
};

const std::string SmallBank_TX_NAME[SmallBank_TX_TYPES] = {
    "Amalgamate",
    "SendPayment",
    "DepositChecking",
    "WriteCheck",
    "Balance",
    "TransactSavings",
    "MultiUpdate"
};

// Table id
enum class SmallBankTableType : uint64_t {
  kSavingsTable = 0,
  kCheckingTable,
};

// Table id
enum class SmallBankCityType : uint64_t {
  kBeijing = 0,
  kShanghai,
  kGuangzhou,
  kShenzhen,
  kChengdu,
  kWuhan,
  kNanjing,
  kHangzhou,
  kChongqing,
  kTianjin,
  kXiAn,
  // 在最后添加一个计数成员
  Count 
};

class SmallBank {
public: 
    SmallBank(int account_count, int access_pattern_type) 
        : smallbank_account(account_count), access_pattern(access_pattern_type) {
            TABLE_IDS_ARR.resize(SmallBank_TX_TYPES);
            TABLE_IDS_ARR[0] = {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable, (table_id_t)SmallBankTableType::kCheckingTable};
            TABLE_IDS_ARR[1] = {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kCheckingTable};
            TABLE_IDS_ARR[2] = {(table_id_t)SmallBankTableType::kCheckingTable};
            TABLE_IDS_ARR[3] = {(table_id_t)SmallBankTableType::kSavingsTable, (table_id_t)SmallBankTableType::kCheckingTable};
            TABLE_IDS_ARR[4] = {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable};
            TABLE_IDS_ARR[5] = {(table_id_t)SmallBankTableType::kSavingsTable};
            TABLE_IDS_ARR[6] = std::vector<table_id_t>(Long_Txn_Length, (table_id_t)SmallBankTableType::kCheckingTable);

            RW_FLAGS_ARR.resize(SmallBank_TX_TYPES);
            RW_FLAGS_ARR[0] = {true, true, true};
            RW_FLAGS_ARR[1] = {true, true};
            RW_FLAGS_ARR[2] = {true};
            RW_FLAGS_ARR[3] = {false, true};
            RW_FLAGS_ARR[4] = {false, false};
            RW_FLAGS_ARR[5] = {true};
            RW_FLAGS_ARR[6] = std::vector<bool>(Long_Txn_Length, true);
        }

    std::vector<std::vector<table_id_t>> TABLE_IDS_ARR;

    std::vector<std::vector<bool>> RW_FLAGS_ARR;
    
    int get_account_count() const {
        return smallbank_account;
    }

    void set_access_pattern(int pattern_type) {
        access_pattern = pattern_type;
    }

    void set_hotspot_params(double fraction, double access_prob) {
        hotspot_fraction = fraction;
        hotspot_access_prob = access_prob;
    }

    void set_zipfian_theta(double theta) {
        zipfian_theta = theta;
    }

    // Generate transaction type based on defined frequencies
    int generate_txn_type() {
        int r = rand() % 100;
        if (r < FREQUENCY_AMALGAMATE) {
            return 0; // Amalgamate
        } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_SEND_PAYMENT) {
            return 1; // SendPayment
        } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_SEND_PAYMENT + FREQUENCY_DEPOSIT_CHECKING) {
            return 2; // DepositChecking
        } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_SEND_PAYMENT + FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_WRITE_CHECK) {
            return 3; // WriteCheck
        } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_SEND_PAYMENT + FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_WRITE_CHECK + FREQUENCY_BALANCE) {
            return 4; // Balance
        } else {
            return 5; // TransactSaving
        }
    }

    // Generate account ID based on access pattern
    void generate_account_id(itemkey_t &acc1, ZipfGen* zipfian_gen) {
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

    void generate_two_account_ids(itemkey_t &acc1, itemkey_t &acc2, ZipfGen* zipfian_gen) {
        // 先生成第一个账号
        generate_account_id(acc1, zipfian_gen);
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
                generate_account_id(acc2, zipfian_gen);
            } while (acc2 == acc1);
        }
        // 添加一个排序，确保acc1 < acc2
        if (acc1 > acc2) {
            std::swap(acc1, acc2);
        }
            
    }

    std::vector<table_id_t>& get_table_ids_by_txn_type(int txn_type) {
        assert(txn_type >= 0 && txn_type < SmallBank_TX_TYPES);
        return TABLE_IDS_ARR[txn_type];
    }

    // 获取读写标志, 1表示写，0表示读
    std::vector<bool>& get_rw_by_txn_type(int txn_type) {
        assert(txn_type >= 0 && txn_type < SmallBank_TX_TYPES);
        return RW_FLAGS_ARR[txn_type];
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

    void generate_friend_graph() {
        // 生成用户朋友关系
        int num_users = smallbank_account;
    #if WORKLOAD_AFFINITY_MODE == 0
        generate_friend_simulate_graph(user_friend_graph, num_users);
    #elif WORKLOAD_AFFINITY_MODE == 1
        generate_friend_city_simulate_graph(user_friend_graph, num_users, (int)SmallBankCityType::Count);
    #endif
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
            txn.exec("CREATE unlogged TABLE checking (id INT, balance INT, city INT, name CHAR(200)) WITH (FILLFACTOR = 50)");
            txn.exec("CREATE unlogged TABLE savings (id INT, balance INT, city INT, name CHAR(200)) WITH (FILLFACTOR = 50)");
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
                autovacuum_enabled = off,
                autovacuum_vacuum_scale_factor = 0.05,   
                autovacuum_vacuum_threshold = 500,       
                autovacuum_analyze_scale_factor = 0.05,
                autovacuum_analyze_threshold = 500
            );
            )SQL");
            std::cout << "Set checking table autovacuum parameters." << std::endl;
            txn.exec(R"SQL(
            ALTER TABLE savings SET ( 
                autovacuum_enabled = off,
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
    }
    
    // 表键→页映射，使用按 id 直接索引的向量以避免拷贝/搜索开销
    struct TableKeyPageMap {
        // 下标为账户 id(1..N)，值为对应数据页 page_id；0 表示未设置/异常
        std::vector<int> checking_page;
        std::vector<int> savings_page;
    };

    TableKeyPageMap load_data(pqxx::connection *conn0);
    
    // ------ YashanDB compatible methods ------
    void create_table_yashan();
    TableKeyPageMap load_data_yashan();
    void create_smallbank_stored_procedures_yashan();
    // ------ End of YashanDB compatible methods ------

    void generate_smallbank_txns_worker(int thread_id, TxnPool* txn_pool);

    void create_smallbank_stored_procedures(pqxx::connection* conn);

    // 检查表是否存在（checking/savings）
    bool check_table_exists(pqxx::connection* conn) {
        try {
            pqxx::work txn(*conn);
            auto r1 = txn.exec("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='checking'");
            auto r2 = txn.exec("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='savings'");
            long long c1 = !r1.empty() ? r1[0][0].as<long long>(0) : 0;
            long long c2 = !r2.empty() ? r2[0][0].as<long long>(0) : 0;
            txn.commit();
            bool ok = (c1 > 0) && (c2 > 0);
            if(!ok) {
                std::cerr << "Table check failed: checking=" << c1 << ", savings=" << c2 << std::endl;
            }
            return ok;
        } catch (const std::exception &e) {
            std::cerr << "Error in check_table_exists: " << e.what() << std::endl;
            return false;
        }
    }

    // 校验账户数量是否匹配（两个表都应为 expected_count）
    bool check_account_count(pqxx::connection* conn, int expected_count) {
        try {
            pqxx::work txn(*conn);
            auto r1 = txn.exec("SELECT COUNT(*) FROM checking");
            auto r2 = txn.exec("SELECT COUNT(*) FROM savings");
            long long c1 = !r1.empty() ? r1[0][0].as<long long>(-1) : -1;
            long long c2 = !r2.empty() ? r2[0][0].as<long long>(-1) : -1;
            txn.commit();
            bool ok = (c1 == expected_count) && (c2 == expected_count);
            if(!ok) {
                std::cerr << "Account count mismatch: checking=" << c1 << ", savings=" << c2
                          << ", expected=" << expected_count << std::endl;
            }
            return ok;
        } catch (const std::exception &e) {
            std::cerr << "Error in check_account_count: " << e.what() << std::endl;
            return false;
        }
    }

private: 
    int smallbank_account; 
    int access_pattern; 
    double hotspot_fraction; 
    double hotspot_access_prob; 
    double zipfian_theta;
    std::vector<std::vector<std::pair<int, float>>> user_friend_graph; // 每个用户的朋友图, 模拟亲和性
    std::string friend_graph_export_file = "friend_graph.csv"; // 导出社交图 CSV 文件
};