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

// 定义所跑的workload 0:smallbank 1:tpcc
#define WORKLOAD_MODE 1 // 0: ycsb 1: tpch

extern std::string conninfo;
extern int ComputeNodeCount;
extern uint64_t ATTEMPTED_NUM;
extern double CrossNodeAccessRatio;
extern int REGION_SIZE;