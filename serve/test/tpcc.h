#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <string>
#include <pqxx/pqxx>
#include <random>
#include <iostream>
#include <thread>
#include <algorithm>
#include <set>

#include "common.h"
#include "config.h"
#include "txn_queue.h"
#include "util/zipf.h"

// TPC-C Transaction Types
enum class TPCCTxType : int {
    kNewOrder = 0,
    kPayment,
    kOrderStatus,
    kDelivery,
    kStockLevel,
    kCount
};

// TPC-C Table IDs
enum class TPCCTableType : uint64_t {
    kWarehouse = 0,
    kDistrict,
    kCustomer,
    kHistory,
    kNewOrder,
    kOrders,
    kOrderLine,
    kItem,
    kStock,
    kCount
};

class TPCC {
public:
    TPCC(int num_warehouses);

    void create_table(pqxx::connection *conn);
    void load_data();
    void create_tpcc_stored_procedures(pqxx::connection *conn);
    void generate_tpcc_txns_worker(int thread_id, TxnPool* txn_pool);
    
    // Helper to get table IDs involved in a transaction type
    std::vector<table_id_t> get_table_ids_by_txn_type(int txn_type, int key_size);
    
    // Helper to get read/write flags for keys
    std::vector<bool> get_rw_flags_by_txn_type(int txn_type, int key_size);

    // Check if tables exist
    bool check_table_exists(pqxx::connection* conn);
    
    bool check_warehouse_count(pqxx::connection* conn, int expected_warehouse_count){
        try {
            pqxx::work txn(*conn);
            auto r = txn.exec("SELECT COUNT(*) FROM warehouse");
            long long c = !r.empty() ? r[0][0].as<long long>(-1) : -1;
            txn.commit();
            if(c != expected_warehouse_count) {
                std::cerr << "Record count mismatch: warehouse=" << c << ", expected=" << expected_warehouse_count << std::endl;
            }
            return c == expected_warehouse_count;
        } catch (const std::exception &e) {
            std::cerr << "Error in TPCC::check_warehouse_count: " << e.what() << std::endl;
            return false;
        }
    }

    int get_num_warehouses() const { return num_warehouses_; }

    // Constants for TPC-C
    // static const int DIST_PER_WARE = 10;
    // static const int CUST_PER_DIST = 3000;
    // static const int ITEM_COUNT = 100000;
    static const int DIST_PER_WARE = 10;
    static const int CUST_PER_DIST = 300;
    static const int ITEM_COUNT = 1000;

    // Key encoding helpers
    static itemkey_t make_warehouse_key(int w_id);
    static itemkey_t make_district_key(int w_id, int d_id);
    static itemkey_t make_customer_key(int w_id, int d_id, int c_id);
    static itemkey_t make_stock_key(int w_id, int i_id);
    // For simplicity, we might not track all tables in the router, 
    // but if we do, we need unique keys. 
    // Since table_id is separate in SmartRouter, we can reuse IDs if they are unique per table.
    // Warehouse: 1..W
    // District: 1..10 (per warehouse? No, usually (W, D))
    // If SmartRouter takes (table_id, key), then key only needs to be unique within table.
    // Warehouse Key: w_id
    // District Key: (w_id-1)*10 + d_id
    // Customer Key: (w_id-1)*10*3000 + (d_id-1)*3000 + c_id
    // Stock Key: (w_id-1)*100000 + i_id
    
    // Helper to get total keys for a table (for init_key_page_map)
    int get_total_keys(TPCCTableType table_type) const;

private:
    int num_warehouses_;
    
    // Helper functions for data loading
    void load_item(pqxx::nontransaction &txn, int start_id, int end_id);
    void load_warehouse(pqxx::nontransaction &txn, int w_id);
    void load_stock(pqxx::nontransaction &txn, int w_id);
    void load_district(pqxx::nontransaction &txn, int w_id);
    void load_customer(pqxx::nontransaction &txn, int w_id, int d_id);
    void load_orders(pqxx::nontransaction &txn, int w_id, int d_id);

    // Helper functions for transaction generation
    int generate_txn_type();
    
    // Random helpers
    static std::string random_string(int min_len, int max_len);
    static std::string random_nstring(int min_len, int max_len);
    static int random_int(int min, int max);
    static int nurand(int A, int x, int y);
    
    // Static data for routing/locking info
    static const std::vector<table_id_t> TABLE_IDS_ARR[5];
    static const std::vector<bool> RW_FLAGS_ARR[5];
};
