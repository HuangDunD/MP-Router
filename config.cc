#include "config.h"

int SYSTEM_MODE = 0;
std::vector<std::string> DBConnection;
int worker_threads = 16; 
int ComputeNodeCount = 2;
uint64_t ATTEMPTED_NUM = 10000000;
int REGION_SIZE = 1000;
double AffinitySampleRate = 1;
int TPCC_WAREHOUSE_NUM = -1;
double AffinityTxnRatio = 0.2;
uint64_t PARTITION_INTERVAL = 500000;
std::string partition_log_file_ = "partitioning_log.log";
int MetisWarmupRound = 1;
bool WarmupEnd = false;
int TxnPoolMaxSize = 500000;
int TxnQueueMaxSize = 10000;
int BatchRouterProcessSize = 10000;
int BatchExecutorPOPTxnSize = 20;
int PreExtendPageSize = 300000; // 预分配页面大小
int PreExtendIndexPageSize = 50000; // 预分配索引页面大小
bool LOAD_DATA_ONLY = false;
bool SKIP_LOAD_DATA = false;
std::vector<uint64_t> hottest_keys; // for debug

// global variables
int try_count = 10000;
std::atomic<int> exe_count{0}; // 这个是所有线程的总事务数
std::atomic<int> generated_txn_count{0}; // 这个是所有线程生成的总事务数
std::atomic<uint64_t> tx_id_generator{0}; // 全局事务ID生成器
int Workload_Type = 0; // 0: smallbank, 1: ycsb