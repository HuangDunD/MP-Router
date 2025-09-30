#pragma once
#include <cstdint>
#include <string>
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

// 定义算法版本 0: random 1: affinity 2: single 3: perfect
extern int SYSTEM_MODE; 

// 定义所跑的workload 0:smallbank 1:tpcc
#define WORKLOAD_MODE 1 // 0: ycsb 1: tpcc
#define LOG_ACCESS_KEY 1 // 0: no log 1: log
#define LOG_FRIEND_GRAPH 1 // 0: no log 1: log

extern int ComputeNodeCount; // 计算节点数量
extern uint64_t ATTEMPTED_NUM;
extern double CrossNodeAccessRatio;
extern int REGION_SIZE;
extern double AffinitySampleRate;
extern double AffinityTxnRatio;
extern uint64_t PARTITION_INTERVAL;

// for TPC-C
extern int TPCC_WAREHOUSE_NUM;