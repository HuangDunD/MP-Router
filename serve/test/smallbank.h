// Author: Chunyue Huang
// Copyright (c) 2024
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>

#include "friend_simulate.h"
#include "util/zipf.h"
#include "common.h"
#include "config.h"
#include <pqxx/pqxx>

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

// #define FREQUENCY_AMALGAMATE 0
// #define FREQUENCY_BALANCE 0
// #define FREQUENCY_DEPOSIT_CHECKING 50
// #define FREQUENCY_SEND_PAYMENT 0
// #define FREQUENCY_TRANSACT_SAVINGS 0
// #define FREQUENCY_WRITE_CHECK 50

#define TX_HOT 80 /* Percentage of txns that use accounts from hotspot */

// Helpers for generating workload
#define SmallBank_TX_TYPES 6
enum class SmallBankTxType : int {
  kAmalgamate,
  kBalance,
  kDepositChecking,
  kSendPayment,
  kTransactSaving,
  kWriteCheck,
};

const std::string SmallBank_TX_NAME[SmallBank_TX_TYPES] = {"Amalgamate", "Balance", "DepositChecking", \
"SendPayment", "TransactSaving", "WriteCheck"};

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

// Thread parameters structure
struct thread_params
{
    node_id_t compute_node_id_connecter; // the compute node id this thread connects to
    int thread_id;
    int thread_count;
    double zipfian_theta;
};

static const std::vector<table_id_t> TABLE_IDS_ARR[] = {
    // txn_type == 0
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable, (table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 1
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 2
    {(table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 3
    {(table_id_t)SmallBankTableType::kSavingsTable, (table_id_t)SmallBankTableType::kCheckingTable},
    // txn_type == 4
    {(table_id_t)SmallBankTableType::kCheckingTable, (table_id_t)SmallBankTableType::kSavingsTable},
    // txn_type == 5
    {(table_id_t)SmallBankTableType::kSavingsTable}
};

class SmallBank {
public: 
  SmallBank(int account_count, int access_pattern_type) 
      : smallbank_account(account_count), access_pattern(access_pattern_type) {} 

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

  // Generate transaction type based on defined frequencies
  int generate_txn_type() {
      int r = rand() % 100;
      if (r < FREQUENCY_AMALGAMATE) {
          return 0; // Amalgamate
      } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE) {
          return 1; // Balance
      } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE + FREQUENCY_DEPOSIT_CHECKING) {
          return 2; // DepositChecking
      } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE + FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_SEND_PAYMENT) {
          return 3; // SendPayment
      } else if (r < FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE + FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_SEND_PAYMENT + FREQUENCY_TRANSACT_SAVINGS) {
          return 4; // TransactSaving
      } else {
          return 5; // WriteCheck
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
  }

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

private: 
  int smallbank_account; 
  int access_pattern; 
  double hotspot_fraction; 
  double hotspot_access_prob; 
  std::vector<std::vector<std::pair<int, float>>> user_friend_graph; // 每个用户的朋友图, 模拟亲和性
  std::string friend_graph_export_file = "friend_graph.csv"; // 导出社交图 CSV 文件
};

// Create or replace SmallBank stored procedures
static void create_smallbank_stored_procedures(pqxx::connection* conn) {
        std::cout << "Creating stored procedures..." << std::endl;
        try {
                pqxx::work txn(*conn);

                // Amalgamate: zero checking/savings of a1, deposit total into a2.checking
                txn.exec(R"SQL(
                CREATE OR REPLACE FUNCTION sp_amalgamate(a1 INT, a2 INT)
                RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
                LANGUAGE plpgsql AS $$
                DECLARE
                    c1_ctid TID;
                    s1_ctid TID;
                    c2_ctid TID;
                    c1_bal INT;
                    s1_bal INT;
                    total_bal INT;
                BEGIN
                -- ① update checking(a1)
                UPDATE checking c
                SET balance = 0
                WHERE c.id = a1
                RETURNING c.ctid, c.balance INTO c1_ctid, c1_bal;

                -- ② update savings(a1)
                UPDATE savings s
                SET balance = 0
                WHERE s.id = a1
                RETURNING s.ctid, s.balance INTO s1_ctid, s1_bal;

                -- ③ 合并
                total_bal := COALESCE(c1_bal,0) + COALESCE(s1_bal,0);

                -- ④ deposit into checking(a2)
                UPDATE checking c2
                SET balance = total_bal
                WHERE c2.id = a2
                RETURNING c2.ctid, c2.balance INTO c2_ctid, balance;

                --------------------------------------------------------------------
                --  最终一次性返回三个修改结果（每个 tuple 的最终 ctid) 
                --------------------------------------------------------------------
                RETURN QUERY 
                    SELECT 'checking'::text, a1, c1_ctid, 0, txid_current();      -- 更新后的 checking(a1)
                RETURN QUERY 
                    SELECT 'savings'::text, a1, s1_ctid, 0, txid_current();       -- 更新后的 savings(a1)
                RETURN QUERY 
                    SELECT 'checking'::text, a2, c2_ctid, balance, txid_current();-- checking(a2) 的最终余额
                END;
                $$;
                )SQL");

                // SendPayment: a1.checking -= 10, a2.checking += 10
                txn.exec(R"SQL(
                CREATE OR REPLACE FUNCTION sp_send_payment(a1 INT, a2 INT)
                RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
                LANGUAGE plpgsql AS $$
                DECLARE
                    c1_ctid TID;
                    c2_ctid TID;
                    c1_bal INT;
                    c2_bal INT;
                BEGIN
                    UPDATE checking c1
                    SET balance = c1.balance - 10
                    WHERE c1.id = a1
                    RETURNING c1.ctid, c1.balance INTO c1_ctid, c1_bal;

                    UPDATE checking c2
                    SET balance = c2.balance + 10
                    WHERE c2.id = a2
                    RETURNING c2.ctid, c2.balance INTO c2_ctid, c2_bal;

                    RETURN QUERY SELECT 'checking'::text, a1, c1_ctid, c1_bal, txid_current();
                    RETURN QUERY SELECT 'checking'::text, a2, c2_ctid, c2_bal, txid_current();
                END; $$;
                )SQL");

                // DepositChecking: a1.checking += 100
                txn.exec(R"SQL(
                CREATE OR REPLACE FUNCTION sp_deposit_checking(a1 INT)
                RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
                LANGUAGE plpgsql AS $$
                DECLARE
                    c_ctid TID;
                    c_bal INT;
                BEGIN
                    UPDATE checking c
                    SET balance = c.balance + 100
                    WHERE c.id = a1
                    RETURNING c.ctid, c.balance INTO c_ctid, c_bal;

                    RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
                END; $$;
                )SQL");

                // WriteCheck: read savings(a1), update checking(a1) -= 50
                txn.exec(R"SQL(
                CREATE OR REPLACE FUNCTION sp_write_check(a1 INT)
                RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
                LANGUAGE plpgsql AS $$
                DECLARE
                    s_ctid TID;
                    s_bal  INT;
                    c_ctid TID;
                    c_bal  INT;
                BEGIN
                    SELECT s.ctid, s.balance INTO s_ctid, s_bal
                    FROM savings s WHERE s.id = a1;

                    UPDATE checking c
                    SET balance = c.balance - 50
                    WHERE c.id = a1
                    RETURNING c.ctid, c.balance INTO c_ctid, c_bal;

                    RETURN QUERY SELECT 'savings'::text,  a1, s_ctid, s_bal, txid_current();
                    RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
                END; $$;
                )SQL");

                // Balance: read checking(a1), savings(a1)
                txn.exec(R"SQL(
                CREATE OR REPLACE FUNCTION sp_balance(a1 INT)
                RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
                LANGUAGE plpgsql AS $$
                DECLARE
                    c_ctid TID;
                    c_bal  INT;
                    s_ctid TID;
                    s_bal  INT;
                BEGIN
                    SELECT c.ctid, c.balance INTO c_ctid, c_bal
                    FROM checking c WHERE c.id = a1;

                    SELECT s.ctid, s.balance INTO s_ctid, s_bal
                    FROM savings s WHERE s.id = a1;

                    RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
                    RETURN QUERY SELECT 'savings'::text,  a1, s_ctid, s_bal, txid_current();
                END; $$;
                )SQL");

                // TransactSavings: a1.savings += 20
                txn.exec(R"SQL(
                CREATE OR REPLACE FUNCTION sp_transact_savings(a1 INT)
                RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
                LANGUAGE plpgsql AS $$
                DECLARE
                    s_ctid TID;
                    s_bal  INT;
                BEGIN
                    UPDATE savings s
                    SET balance = s.balance + 20
                    WHERE s.id = a1
                    RETURNING s.ctid, s.balance INTO s_ctid, s_bal;

                    RETURN QUERY SELECT 'savings'::text, a1, s_ctid, s_bal, txid_current();
                END; $$;
                )SQL");

                txn.commit();
                std::cout << "Stored procedures created." << std::endl;
        } catch (const std::exception &e) {
                std::cerr << "Error creating stored procedures: " << e.what() << std::endl;
        }
}

// Run SmallBank transactions using stored procedures (similar to run_smallbank_txns)
void run_smallbank_txns_sp(thread_params* params, class Logger* logger_);