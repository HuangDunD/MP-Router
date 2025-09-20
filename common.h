#pragma once
#include <cstdint> 
#include <time.h>

#define ALWAYS_INLINE inline __attribute__((always_inline))

#define PAGE_SIZE 4096 // 16KB

extern int ComputeNodeCount;
extern uint64_t ATTEMPTED_NUM;
extern double CrossNodeAccessRatio;
using region_id_t=int32_t;
using page_id_t = int32_t;    // page id type
using frame_id_t = int32_t;  // frame id type
using table_id_t = int32_t;         // table id type
using column_id_t = int32_t;       // column id type
using itemkey_t = uint64_t;         // Data item key type, used in DB tables
using partition_id_t = int32_t;     // partition id type
using lock_t = uint64_t;      // Lock type
using node_id_t = int32_t;    // Node id type
using batch_id_t = uint64_t;  // batch id type
using lsn_t = uint64_t;       // log sequence number, used for storage_node log storage
using tx_id_t = uint64_t;     // Transaction id type
using t_id_t = uint32_t;      // Thread id type
using coro_id_t = int;        // Coroutine id type
using offset_t = int64_t;     // Offset type. 
using timestamp_t = uint64_t; // Timestamp type

#define MAX_DB_TABLE_NUM 15      // Max DB tables

#define LOG_FILE_NAME "LOG_FILE"   

#define kInvalidPageId -1