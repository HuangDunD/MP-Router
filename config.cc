#include "config.h"

int SYSTEM_MODE = 0;
std::vector<std::string> DBConnection;
int ComputeNodeCount = 2;
uint64_t ATTEMPTED_NUM = 10000000;
double CrossNodeAccessRatio = 0.2;
int REGION_SIZE = 1000;
double AffinitySampleRate = 1;
int TPCC_WAREHOUSE_NUM = -1;
double AffinityTxnRatio = 1;
uint64_t PARTITION_INTERVAL = 300000;
std::string partition_log_file_ = "partitioning_log.log";
int MetisWarmupRound = 20;
bool WarmupEnd = false;
int TxnPoolMaxSize = 10000;