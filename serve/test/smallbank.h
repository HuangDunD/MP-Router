// Author: Chunyue Huang
// Copyright (c) 2024
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

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