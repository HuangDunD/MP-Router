#pragma once
#include <cstdint>
#include <string>
#include <vector>
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

// 定义所跑的workload 0:smallbank 1:tpcc
#define WORKLOAD_MODE 1 // 0: ycsb 1: tpcc
#define LOG_ACCESS_KEY 0 // 0: no log 1: log
#define LOG_FRIEND_GRAPH 0 // 0: no log 1: log
#define LOG_METIS_DECISION 0 // 0: no log 1: log
#define LOG_OWNERSHIP_CHANGE 0 // 0: no log 1: log
#define LOG_PAGE_UPDATE 0 // 0: no log 1: log
#define LOG_KROUTER_SCHEDULING_DEBUG 1 // 0: no log 1: log
#define WORKLOAD_AFFINITY_MODE 1 // 0: key affinity 1: city-key affinity
#define SYS_8_DECISION_TYPE_COUNT 19 // for SYSTEM_MODE 8, 17 types of ownership changes
// for log
extern std::string partition_log_file_; 

extern std::vector<std::string> DBConnection;
extern int ComputeNodeCount; // 计算节点数量
extern uint64_t ATTEMPTED_NUM;
extern int REGION_SIZE;
extern double AffinitySampleRate;
extern double AffinityTxnRatio;
extern uint64_t PARTITION_INTERVAL;
extern int MetisWarmupRound; // 这里表示初始情况下首先要先经过多少轮的metis分区，之后分区就不动了
extern bool WarmupEnd;  // 标记是否完成了warmup阶段
extern int TxnPoolMaxSize; // 事务池的最大大小
extern int BatchRouterProcessSize; // 每次批量路由处理的事务数量

// for TPC-C
extern int TPCC_WAREHOUSE_NUM;