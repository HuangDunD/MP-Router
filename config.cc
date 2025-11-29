#include "config.h"

int SYSTEM_MODE = 0;
std::vector<std::string> DBConnection;
int worker_threads = 16; 
int ComputeNodeCount = 2;
uint64_t ATTEMPTED_NUM = 10000000;
int REGION_SIZE = 1000;
double AffinitySampleRate = 1;
int TPCC_WAREHOUSE_NUM = -1;
double AffinityTxnRatio = 0.8;
uint64_t PARTITION_INTERVAL = 100000;
std::string partition_log_file_ = "partitioning_log.log";
int MetisWarmupRound = 10;
bool WarmupEnd = false;
int TxnPoolMaxSize = 100000;
int TxnQueueMaxSize = 10000;
int BatchRouterProcessSize = 10000;
int BatchExecutorPOPTxnSize = 20;
int PreExtendPageSize = 1000000; // 预分配页面大小
