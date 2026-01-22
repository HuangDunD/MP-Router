#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <atomic>
#include "common.h"
/*********************** For common **********************/

// ! pay attention: need modify this when use different workload
// Max data item size.
// 8: smallbank
// 40: tatp
// 664: tpcc
// 40: micro-benchmark
#define MAX_ITEM_SIZE 8

#define ComputeNodeBufferPageSize 262144 // 262144*4KB = 1GB

#define BufferFusionSize ComputeNodeBufferPageSize
#define PartitionDataSize (ComputeNodeBufferPageSize / ComputeNodeCount)
#define MaxComputeNodeCount 128
#define GroupPageAffinitySize 100

// 定义算法版本 0: random 1: affinity 2: single 3: perfect
extern int SYSTEM_MODE; 

// 定义数据库类型 0: PostgreSQL 1: YashanDB
extern int DB_TYPE;

// 定义所跑的workload 0:smallbank 1:tpcc
#define WORKLOAD_MODE 1 // 0: ycsb 1: tpcc
#define LOG_ACCESS_KEY 0 // 0: no log 1: log
#define LOG_FRIEND_GRAPH 0 // 0: no log 1: log
#define LOG_METIS_DECISION 0 // 0: no log 1: log
#define LOG_OWNERSHIP_CHANGE 0 // 0: no log 1: log
#define LOG_PAGE_UPDATE 0 // 0: no log 1: log
#define LOG_KROUTER_SCHEDULING_DEBUG 0 // 0: no log 1: log
#define LOG_METIS_OWNERSHIP_DECISION 0 // 0: no log 1: log
#define WORKLOAD_AFFINITY_MODE 1 // 0: key affinity 1: city-key affinity
#define SYS_8_DECISION_TYPE_COUNT 19 // for SYSTEM_MODE 8, 17 types of ownership changes
#define MLP_PREDICTION 0 // 0: no mlp prediction, just use key-value, 1: use mlp prediction
#define LOG_BATCH_ROUTER 0 // 0: no log 1: log
#define LOG_QUEUE_STATUS 0 // 0: no log 1: log
#define LOG_TXN_EXEC 0 // 0: no log 1: log
// for log
extern std::string partition_log_file_; 

extern int worker_threads; // 工作线程数量, 路由和RAC节点建立的连接数
extern std::vector<std::string> DBConnection; // for PostgreSQL
extern std::vector<YashanConnInfo> YashanDBConnections; // for YashanDB
extern int ComputeNodeCount; // 计算节点数量
extern uint64_t ATTEMPTED_NUM;
extern int REGION_SIZE;
extern double AffinitySampleRate;
extern double AffinityTxnRatio;
extern uint64_t PARTITION_INTERVAL;
extern int MetisWarmupRound; // 这里表示初始情况下首先要先经过多少轮的metis分区，之后分区就不动了
extern bool WarmupEnd;  // 标记是否完成了warmup阶段
extern int TxnPoolMaxSize; // 事务池的最大大小
extern int TxnQueueMaxSize; // 事务队列的最大大小
extern int BatchRouterProcessSize; // 每次批量路由处理的事务数量
extern int BatchExecutorPOPTxnSize; // 每次批量执行pop的事务数量
extern int PreExtendPageSize; // 预分配页面大小
extern int PreExtendIndexPageSize; // 预分配索引页面大小
extern bool LOAD_DATA_ONLY; // 仅加载数据模式
extern bool SKIP_LOAD_DATA; // 跳过加载数据模式
extern std::vector<uint64_t> hottest_keys; // for debug
extern int NumBucket;
extern bool Enable_Long_Txn; // 是否启用长事务
extern int Long_Txn_Length; // 长事务的长度

// for TPC-C
extern int TPCC_WAREHOUSE_NUM;

// global variables
extern int try_count;
extern std::atomic<int> exe_count;
extern std::atomic<int> generated_txn_count;
extern std::atomic<uint64_t> tx_id_generator;
extern int Workload_Type;