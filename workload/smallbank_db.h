// Author: Chunyue Huang
// Copyright (c) 2024
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>

#include "util/fast_random.h"
#include "util/json_config.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

#define TX_HOT 80 /* Percentage of txns that use accounts from hotspot */

// Smallbank table keys and values
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/*
 * SAVINGS table.
 */
union smallbank_savings_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_savings_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_savings_key_t) == sizeof(uint64_t), "");

struct smallbank_savings_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_savings_val_t) == sizeof(uint64_t), "");

/*
 * CHECKING table
 */
union smallbank_checking_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_checking_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_checking_key_t) == sizeof(uint64_t), "");

struct smallbank_checking_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_checking_val_t) == sizeof(uint64_t), "");

// Magic numbers for debugging. These are unused in the spec.
#define SmallBank_MAGIC 97 /* Some magic number <= 255 */
#define smallbank_savings_magic (SmallBank_MAGIC)
#define smallbank_checking_magic (SmallBank_MAGIC + 1)

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

class SmallBank {
public:
    std::string bench_name;

    uint32_t total_thread_num;

    uint32_t tatal_page_num;
    uint32_t num_accounts_global, num_hot_global;
    double hot_rate;

    // For server usage: Provide interfaces to servers for loading tables
    // Also for client usage: Provide interfaces to clients for generating ids during tests
    SmallBank(int page_num) : tatal_page_num(page_num) {
        bench_name = "smallbank";
        // Used for populate table (line num) and get account
        std::string config_filepath = "../../config/smallbank_config.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto conf = json_config.get("smallbank");
        num_accounts_global = conf.get("num_accounts").get_uint64();
        num_hot_global = conf.get("num_hot_accounts").get_uint64();
        hot_rate = (double)num_hot_global / (double)num_accounts_global;

        /* Up to 2 billion accounts */
        assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);
    }

    ~SmallBank() {}

    SmallBankTxType* CreateWorkgenArray(double readonly_txn_rate) {
        SmallBankTxType* workgen_arr = new SmallBankTxType[100];

        // SmallBankTxType为kBalance，是只读事务
        int rw = 100 - 100 * readonly_txn_rate;
        int i = 0;
        int j = 100 * readonly_txn_rate;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kBalance;
        // printf("j = %d\n", j);

        int remain = 100 - FREQUENCY_BALANCE;

        j = (j + rw * FREQUENCY_AMALGAMATE / remain) > 100 ? 100 : (j + rw * FREQUENCY_AMALGAMATE / remain);
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kAmalgamate;
        // printf("j = %d\n", j);

        j = (j + rw * FREQUENCY_DEPOSIT_CHECKING / remain) > 100 ? 100 : (j + rw * FREQUENCY_DEPOSIT_CHECKING / remain);
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kDepositChecking;
        // printf("j = %d\n", j);

        j = (j + rw * FREQUENCY_SEND_PAYMENT / remain) > 100 ? 100 : (j + rw * FREQUENCY_SEND_PAYMENT / remain);
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kSendPayment;
        // printf("j = %d\n", j);

        j = (j + rw * FREQUENCY_TRANSACT_SAVINGS / remain) > 100 ? 100 : (j + rw * FREQUENCY_TRANSACT_SAVINGS / remain);
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kTransactSaving;
        // printf("j = %d\n", j);

        j = 100;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kWriteCheck;
        // printf("j = %d\n", j);

        assert(i == 100 && j == 100);

        return workgen_arr;
    }

    /*
    * Generators for new account IDs. Called once per transaction because
    * we need to decide hot-or-not per transaction, not per account.
    */
    inline void get_account(uint64_t* seed, uint64_t* page_id_0, bool is_partitioned, node_id_t gen_node_id, table_id_t table_id = 0) const {
        double global_conflict = 100;
        if(is_partitioned) { //执行本地事务
            // 每个page_id 后面+1是因为page_id从1开始
            int node_id = gen_node_id;
            int page_num = tatal_page_num;
            page_id_t page_id;
            if(FastRand(seed) % 100 < TX_HOT){ // 如果是热点事务
                page_id = FastRand(seed) % (int) ((page_num / ComputeNodeCount) * hot_rate);
                if(FastRand(seed) % 100 < global_conflict) {
                    page_id = page_id  + 1 + node_id * (page_num / ComputeNodeCount);
                } else {
                    page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount) + 1;
                }
            } else { //如果是非热点事务
                page_id = FastRand(seed) % (page_num / ComputeNodeCount);
                if(FastRand(seed) % 100 < global_conflict) {
                    page_id = page_id + 1 + node_id * (page_num / ComputeNodeCount);
                } else {
                    page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount) + 1;
                    if(page_id >= page_num ){
                        page_id--;
                    }
                }
            }
            *page_id_0 = page_id;
        } else { // 执行跨分区事务
            int node_id = gen_node_id;
            int page_num = tatal_page_num;
            page_id_t page_id;
            if(FastRand(seed) % 100 < TX_HOT) { // 如果是热点事务
                int random = FastRand(seed) % (ComputeNodeCount - 1);
                page_id = FastRand(seed) % (int)((page_num / ComputeNodeCount) * hot_rate);
                if(FastRand(seed) % 100 < global_conflict) {
                    page_id = page_id + 1 + (random < node_id ? random : random + 1) * (page_num / ComputeNodeCount) ;
                } else {
                    page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) +
                            node_id * (page_num / ComputeNodeCount / ComputeNodeCount) +
                            (random < node_id ? random : random + 1)  * (page_num / ComputeNodeCount) + 1;
                }
            } else { //如果是非热点事务
            int random = FastRand(seed) % (ComputeNodeCount - 1);
                page_id = FastRand(seed) % (page_num / ComputeNodeCount);
                if(FastRand(seed) % 100 < global_conflict) {
                    page_id = page_id + 1 + (random < node_id ? random : random + 1) * (page_num / ComputeNodeCount) ;
                } else {
                    page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) +
                            node_id * (page_num / ComputeNodeCount / ComputeNodeCount) +
                            (random < node_id ? random : random + 1)  * (page_num / ComputeNodeCount) + 1;
                }
            }
            *page_id_0 = page_id;
        }
    }
   
    inline void get_two_accounts(uint64_t* seed, uint64_t* page_id_0, uint64_t* page_id_1, node_id_t gen_node_id, bool is_partitioned, table_id_t table_id = 0) const {
        if(is_partitioned) {
            get_account(seed, page_id_0, is_partitioned, gen_node_id, table_id);
            get_account(seed, page_id_1, is_partitioned, gen_node_id, table_id);
        } else {
            int node_id = gen_node_id;
            get_account(seed, page_id_0, true, node_id, table_id);
            get_account(seed, page_id_1, is_partitioned, node_id, table_id);
        }
    }
   
};
   