#include "tpcc.h"
#include <iostream>
#include <algorithm>
#include <set>

// Initialize static members
const std::vector<table_id_t> TPCC::TABLE_IDS_ARR[5] = {
    // NewOrder: Warehouse, District, Customer, NewOrder, Orders, Stock(x10), OrderLine(x10), Item(x10)
    // Simplified for routing: District, Stock
    {(table_id_t)TPCCTableType::kDistrict, (table_id_t)TPCCTableType::kStock},
    // Payment: Warehouse, District, Customer, History
    {(table_id_t)TPCCTableType::kWarehouse, (table_id_t)TPCCTableType::kDistrict, (table_id_t)TPCCTableType::kCustomer},
    // OrderStatus: Customer, Orders, OrderLine
    {(table_id_t)TPCCTableType::kCustomer, (table_id_t)TPCCTableType::kOrders},
    // Delivery: NewOrder, Orders, OrderLine, Customer
    {(table_id_t)TPCCTableType::kNewOrder, (table_id_t)TPCCTableType::kOrders, (table_id_t)TPCCTableType::kCustomer},
    // StockLevel: District, OrderLine, Stock
    {(table_id_t)TPCCTableType::kDistrict, (table_id_t)TPCCTableType::kStock}
};

// RW Flags (Simplified)
const std::vector<bool> TPCC::RW_FLAGS_ARR[5] = {
    // NewOrder: D(W), S(W)
    {true, true},
    // Payment: W(W), D(W), C(W)
    {true, true, true},
    // OrderStatus: C(R), O(R)
    {false, false},
    // Delivery: NO(R/W), O(W), C(W)
    {true, true, true},
    // StockLevel: D(R), S(R)
    {false, false}
};

TPCC::TPCC(int num_warehouses) : num_warehouses_(num_warehouses) {}

std::vector<table_id_t> TPCC::get_table_ids_by_txn_type(int txn_type, int key_size) {
    if (txn_type == (int)TPCCTxType::kNewOrder) {
        std::vector<table_id_t> new_order_table_ids;
        new_order_table_ids = TABLE_IDS_ARR[txn_type];
        // The first 2 tables are District, Stock (first item)
        for (int i = 2; i < key_size; ++i) {
            new_order_table_ids.push_back((table_id_t)TPCCTableType::kStock);
        }
        return new_order_table_ids;
    }
    return TABLE_IDS_ARR[txn_type];
}

std::vector<bool> TPCC::get_rw_flags_by_txn_type(int txn_type, int key_size) {
    if (txn_type == (int)TPCCTxType::kNewOrder) {
        std::vector<bool> new_order_rw_flags;
        new_order_rw_flags = RW_FLAGS_ARR[txn_type];
        // The first 2 flags are District(W), Stock(W)
        for (int i = 2; i < key_size; ++i) {
            new_order_rw_flags.push_back(true); // Stock is write
        }
        return new_order_rw_flags;
    }
    return RW_FLAGS_ARR[txn_type];
}

itemkey_t TPCC::make_warehouse_key(int w_id) {
    return w_id;
}

itemkey_t TPCC::make_district_key(int w_id, int d_id) {
    return (w_id - 1) * DIST_PER_WARE + d_id;
}

itemkey_t TPCC::make_customer_key(int w_id, int d_id, int c_id) {
    return (itemkey_t)(w_id - 1) * DIST_PER_WARE * CUST_PER_DIST + (d_id - 1) * CUST_PER_DIST + c_id;
}

itemkey_t TPCC::make_stock_key(int w_id, int i_id) {
    return (itemkey_t)(w_id - 1) * ITEM_COUNT + i_id;
}

int TPCC::get_total_keys(TPCCTableType table_type) const {
    switch (table_type) {
        case TPCCTableType::kWarehouse: return num_warehouses_;
        case TPCCTableType::kDistrict: return num_warehouses_ * DIST_PER_WARE;
        case TPCCTableType::kCustomer: return num_warehouses_ * DIST_PER_WARE * CUST_PER_DIST;
        case TPCCTableType::kStock: return num_warehouses_ * ITEM_COUNT;
        default: return 0;
    }
}

void TPCC::create_table(pqxx::connection *conn) {
    std::cout << "Creating TPC-C tables..." << std::endl;
    try {
        pqxx::work txn(*conn);

        // Drop tables if exist
        txn.exec("DROP TABLE IF EXISTS order_line CASCADE");
        txn.exec("DROP TABLE IF EXISTS new_order CASCADE");
        txn.exec("DROP TABLE IF EXISTS orders CASCADE");
        txn.exec("DROP TABLE IF EXISTS history CASCADE");
        txn.exec("DROP TABLE IF EXISTS customer CASCADE");
        txn.exec("DROP TABLE IF EXISTS district CASCADE");
        txn.exec("DROP TABLE IF EXISTS stock CASCADE");
        txn.exec("DROP TABLE IF EXISTS item CASCADE");
        txn.exec("DROP TABLE IF EXISTS warehouse CASCADE");
        
        // Warehouse
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE warehouse (
                w_id INT PRIMARY KEY,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state CHAR(2),
                w_zip CHAR(9),
                w_tax DECIMAL(4,4),
                w_ytd DECIMAL(12,2)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // District
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE district (
                d_id INT,
                d_w_id INT,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state CHAR(2),
                d_zip CHAR(9),
                d_tax DECIMAL(4,4),
                d_ytd DECIMAL(12,2),
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // Customer
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE customer (
                c_id INT,
                c_d_id INT,
                c_w_id INT,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state CHAR(2),
                c_zip CHAR(9),
                c_phone CHAR(16),
                c_since TIMESTAMP,
                c_credit CHAR(2),
                c_credit_lim DECIMAL(12,2),
                c_discount DECIMAL(4,4),
                c_balance DECIMAL(12,2),
                c_ytd_payment DECIMAL(12,2),
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500),
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // History
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE history (
                h_c_id INT,
                h_c_d_id INT,
                h_c_w_id INT,
                h_d_id INT,
                h_w_id INT,
                h_date TIMESTAMP,
                h_amount DECIMAL(6,2),
                h_data VARCHAR(24)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // NewOrder
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE new_order (
                no_o_id INT,
                no_d_id INT,
                no_w_id INT,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // Orders
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE orders (
                o_id INT,
                o_d_id INT,
                o_w_id INT,
                o_c_id INT,
                o_entry_d TIMESTAMP,
                o_carrier_id INT,
                o_ol_cnt INT,
                o_all_local INT,
                PRIMARY KEY (o_w_id, o_d_id, o_id)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // OrderLine
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE order_line (
                ol_o_id INT,
                ol_d_id INT,
                ol_w_id INT,
                ol_number INT,
                ol_i_id INT,
                ol_supply_w_id INT,
                ol_delivery_d TIMESTAMP,
                ol_quantity INT,
                ol_amount DECIMAL(6,2),
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // Item
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE item (
                i_id INT PRIMARY KEY,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price DECIMAL(5,2),
                i_data VARCHAR(50)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // Stock
        txn.exec(R"SQL(
            CREATE UNLOGGED TABLE stock (
                s_i_id INT,
                s_w_id INT,
                s_quantity INT,
                s_dist_01 CHAR(24),
                s_dist_02 CHAR(24),
                s_dist_03 CHAR(24),
                s_dist_04 CHAR(24),
                s_dist_05 CHAR(24),
                s_dist_06 CHAR(24),
                s_dist_07 CHAR(24),
                s_dist_08 CHAR(24),
                s_dist_09 CHAR(24),
                s_dist_10 CHAR(24),
                s_ytd DECIMAL(8,0),
                s_order_cnt INT,
                s_remote_cnt INT,
                s_data VARCHAR(50),
                PRIMARY KEY (s_w_id, s_i_id)
            ) WITH (FILLFACTOR = 50);
        )SQL");

        // Set autovacuum parameters
        std::vector<std::string> tables = {"warehouse", "district", "customer", "history", "new_order", "orders", "order_line", "item", "stock"};
        for (const auto& table : tables) {
            txn.exec("ALTER TABLE " + table + " SET ("
                "autovacuum_enabled = off, "
                "autovacuum_vacuum_scale_factor = 0.05, "
                "autovacuum_vacuum_threshold = 500, "
                "autovacuum_analyze_scale_factor = 0.05, "
                "autovacuum_analyze_threshold = 500"
            ")");
        }

        txn.commit();
        std::cout << "TPC-C tables created." << std::endl;

        // Pre-extend tables
        std::cout << "Pre-extending TPC-C tables..." << std::endl;
        std::vector<std::string> extend_tables = {"warehouse", "district", "customer", "history", "new_order", "orders", "order_line", "item", "stock"};
        // Custom sizes for each table (number of pages) - adjust as needed
        std::vector<int> extend_sizes = {1000, 30000, 500000, 100000, 100000, 50000, 500000, 5000, 1000000}; 
        
        std::vector<std::thread> extend_threads;
        for (size_t i = 0; i < extend_tables.size(); ++i) {
            extend_threads.emplace_back([i, extend_tables, extend_sizes](){
                pqxx::connection conn_extend(DBConnection[0]);
                if (!conn_extend.is_open()) {
                    std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
                    return;
                }
                try {
                    pqxx::nontransaction txn(conn_extend);
                    std::string extend_sql = "SELECT sys_extend('" + extend_tables[i] + "', " + std::to_string(extend_sizes[i]) + ")";
                    txn.exec(extend_sql);
                    std::cout << "Pre-extended " << extend_tables[i] << " table." << std::endl;
                } catch (const std::exception &e) {
                    std::cerr << "Error while pre-extending " << extend_tables[i] << " table: " << e.what() << std::endl;
                }
            });
        }

        for (auto& t : extend_threads) {
            t.join();
        }
        std::cout << "TPC-C tables pre-extended." << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Error creating TPC-C tables: " << e.what() << std::endl;
    }
}

void TPCC::load_data() {
    std::cout << "Loading TPC-C data..." << std::endl;
    try {
        // 1. Load Items in parallel
        std::cout << "Loading Items in parallel..." << std::endl;
        int item_threads_count = 10; 
        std::vector<std::thread> item_threads;
        int items_per_thread = ITEM_COUNT / item_threads_count;
        
        for (int i = 0; i < item_threads_count; ++i) {
            item_threads.emplace_back([i, items_per_thread, item_threads_count, this]() {
                pqxx::connection c(DBConnection[0]);
                if (!c.is_open()) {
                    std::cerr << "Failed to connect to database for item loading." << std::endl;
                    return;
                }
                try {
                    pqxx::nontransaction txn(c);
                    int start = i * items_per_thread + 1;
                    int end = (i == item_threads_count - 1) ? ITEM_COUNT : (i + 1) * items_per_thread;
                    load_item(txn, start, end);
                } catch (const std::exception &e) {
                    std::cerr << "Error loading items: " << e.what() << std::endl;
                }
            });
        }
        for (auto& t : item_threads) t.join();
        std::cout << "Items loaded." << std::endl;

        // 2. Load Warehouses in parallel
        std::cout << "Loading Warehouses in parallel..." << std::endl;
        int wh_threads_count = std::min(num_warehouses_, 10); 
        if (wh_threads_count < 1) wh_threads_count = 1;
        
        std::vector<std::thread> wh_threads;
        
        for (int i = 0; i < wh_threads_count; ++i) {
            wh_threads.emplace_back([i, wh_threads_count, this]() {
                pqxx::connection c(DBConnection[0]);
                if (!c.is_open()) {
                    std::cerr << "Failed to connect to database for warehouse loading." << std::endl;
                    return;
                }
                try {
                    pqxx::nontransaction txn(c);
                    for (int w = i + 1; w <= num_warehouses_; w += wh_threads_count) {
                        std::cout << "Loading Warehouse " << w << "..." << std::endl;
                        load_warehouse(txn, w);
                        load_stock(txn, w);
                        load_district(txn, w);
                        for (int d = 1; d <= DIST_PER_WARE; ++d) {
                            load_customer(txn, w, d);
                            load_orders(txn, w, d);
                        }
                    }
                } catch (const std::exception &e) {
                    std::cerr << "Error loading warehouse data: " << e.what() << std::endl;
                }
            });
        }
        for (auto& t : wh_threads) t.join();
        
        std::cout << "TPC-C data loaded." << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Error loading TPC-C data: " << e.what() << std::endl;
    }
}

void TPCC::load_item(pqxx::nontransaction &txn, int start_id, int end_id) {
    // Simplified item loading
    // std::cout << "Loading Items " << start_id << " to " << end_id << "..." << std::endl;
    for (int i = start_id; i <= end_id; ++i) {
        std::string sql = "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES (" +
            std::to_string(i) + ", " +
            std::to_string(random_int(1, 10000)) + ", '" +
            random_string(14, 24) + "', " +
            std::to_string(random_int(100, 10000) / 100.0) + ", '" +
            random_string(26, 50) + "')";
        txn.exec(sql);
    }
}

void TPCC::load_warehouse(pqxx::nontransaction &txn, int w_id) {
    std::string sql = "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (" +
        std::to_string(w_id) + ", '" +
        random_string(6, 10) + "', '" +
        random_string(10, 20) + "', '" +
        random_string(10, 20) + "', '" +
        random_string(10, 20) + "', '" +
        random_string(2, 2) + "', '" +
        random_nstring(9, 9) + "', " +
        std::to_string(random_int(0, 2000) / 10000.0) + ", 300000.00)";
    txn.exec(sql);
}

void TPCC::load_stock(pqxx::nontransaction &txn, int w_id) {
    for (int i = 1; i <= ITEM_COUNT; ++i) {
        std::string sql = "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES (" +
            std::to_string(i) + ", " +
            std::to_string(w_id) + ", " +
            std::to_string(random_int(10, 100)) + ", '" +
            random_string(24, 24) + "', '" + random_string(24, 24) + "', '" + random_string(24, 24) + "', '" +
            random_string(24, 24) + "', '" + random_string(24, 24) + "', '" + random_string(24, 24) + "', '" +
            random_string(24, 24) + "', '" + random_string(24, 24) + "', '" + random_string(24, 24) + "', '" +
            random_string(24, 24) + "', 0, 0, 0, '" +
            random_string(26, 50) + "')";
        txn.exec(sql);
    }
}

void TPCC::load_district(pqxx::nontransaction &txn, int w_id) {
    for (int d = 1; d <= DIST_PER_WARE; ++d) {
        std::string sql = "INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES (" +
            std::to_string(d) + ", " +
            std::to_string(w_id) + ", '" +
            random_string(6, 10) + "', '" +
            random_string(10, 20) + "', '" +
            random_string(10, 20) + "', '" +
            random_string(10, 20) + "', '" +
            random_string(2, 2) + "', '" +
            random_nstring(9, 9) + "', " +
            std::to_string(random_int(0, 2000) / 10000.0) + ", 30000.00, " + std::to_string(CUST_PER_DIST + 1) + ")";
        txn.exec(sql);
    }
}

void TPCC::load_customer(pqxx::nontransaction &txn, int w_id, int d_id) {
    for (int c = 1; c <= CUST_PER_DIST; ++c) {
        std::string sql = "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES (" +
            std::to_string(c) + ", " +
            std::to_string(d_id) + ", " +
            std::to_string(w_id) + ", '" +
            random_string(8, 16) + "', 'OE', '" +
            random_string(8, 16) + "', '" +
            random_string(10, 20) + "', '" +
            random_string(10, 20) + "', '" +
            random_string(10, 20) + "', '" +
            random_string(2, 2) + "', '" +
            random_nstring(9, 9) + "', '" +
            random_nstring(16, 16) + "', NOW(), '" +
            (random_int(0, 1) == 0 ? "GC" : "BC") + "', 50000.00, " +
            std::to_string(random_int(0, 5000) / 10000.0) + ", -10.00, 10.00, 1, 0, '" +
            random_string(300, 500) + "')";
        txn.exec(sql);
        
        // History
        std::string h_sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (" +
            std::to_string(c) + ", " +
            std::to_string(d_id) + ", " +
            std::to_string(w_id) + ", " +
            std::to_string(d_id) + ", " +
            std::to_string(w_id) + ", NOW(), 10.00, '" +
            random_string(12, 24) + "')";
        txn.exec(h_sql);
    }
}

void TPCC::load_orders(pqxx::nontransaction &txn, int w_id, int d_id) {
    for (int o = 1; o <= CUST_PER_DIST; ++o) {
        std::string sql = "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (" +
            std::to_string(o) + ", " +
            std::to_string(d_id) + ", " +
            std::to_string(w_id) + ", " +
            std::to_string(random_int(1, CUST_PER_DIST)) + ", NOW(), " +
            (o < CUST_PER_DIST - 900 + 1 ? std::to_string(random_int(1, DIST_PER_WARE)) : "NULL") + ", " +
            std::to_string(random_int(5, 15)) + ", 1)";
        txn.exec(sql);
        
        // OrderLine
        int ol_cnt = random_int(5, 15);
        for (int ol = 1; ol <= ol_cnt; ++ol) {
            std::string ol_sql = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (" +
                std::to_string(o) + ", " +
                std::to_string(d_id) + ", " +
                std::to_string(w_id) + ", " +
                std::to_string(ol) + ", " +
                std::to_string(random_int(1, ITEM_COUNT)) + ", " +
                std::to_string(w_id) + ", " +
                (o < CUST_PER_DIST - 900 + 1 ? "NOW()" : "NULL") + ", 5, " +
                (o < CUST_PER_DIST - 900 + 1 ? "0.00" : std::to_string(random_int(10, 10000) / 100.0)) + ", '" +
                random_string(24, 24) + "')";
            txn.exec(ol_sql);
        }
        
        // NewOrder (last 900 orders)
        if (o >= CUST_PER_DIST - 900 + 1) {
            std::string no_sql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (" +
                std::to_string(o) + ", " +
                std::to_string(d_id) + ", " +
                std::to_string(w_id) + ")";
            txn.exec(no_sql);
        }
    }
}

void TPCC::create_tpcc_stored_procedures(pqxx::connection *conn) {
    std::cout << "Creating TPC-C stored procedures..." << std::endl;
    try {
        pqxx::work txn(*conn);
        
        // New Order
        txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION tpcc_new_order(
                p_w_id INT, p_d_id INT, p_c_id INT, p_o_ol_cnt INT, 
                p_i_ids INT[], p_i_w_ids INT[], p_quantities INT[]
            )
            RETURNS TABLE(rel TEXT, ret_w_id INT, ret_d_id INT, ret_i_id INT, ctid TID)
            LANGUAGE plpgsql AS $$
            DECLARE
                o_id INT;
                i INT;
                d_ctid TID;
                s_ctid TID;
            BEGIN
                UPDATE district AS d
                SET d_next_o_id = d.d_next_o_id + 1
                WHERE d.d_w_id = p_w_id AND d.d_id = p_d_id
                RETURNING (d.d_next_o_id - 1), d.ctid INTO o_id, d_ctid;

                -- 返回 district 的“key -> ctid”
                RETURN QUERY SELECT 'district'::text, p_w_id, p_d_id, NULL::INT, d_ctid;
                -- INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
                -- VALUES (o_id, p_d_id, p_w_id, p_c_id, NOW(), p_o_ol_cnt, 1);
                
                -- INSERT INTO new_order (no_o_id, no_d_id, no_w_id) 
                -- VALUES (o_id, p_d_id, p_w_id);
                
                FOR i IN 1..p_o_ol_cnt LOOP
                    UPDATE stock AS s
                    SET s_quantity = s.s_quantity - p_quantities[i]
                    WHERE s.s_w_id = p_i_w_ids[i] AND s.s_i_id = p_i_ids[i]
                    RETURNING s.ctid INTO s_ctid;

                    -- 返回 stock 的“key -> ctid”
                    RETURN QUERY SELECT 'stock'::text, p_i_w_ids[i], NULL::INT, p_i_ids[i], s_ctid;
                    
                    -- INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                    -- VALUES (o_id, p_d_id, p_w_id, i, p_i_ids[i], p_i_w_ids[i], p_quantities[i], 0, 'dist_info');
                END LOOP;
            END;
            $$;
        )SQL");

        // Payment
        txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION tpcc_payment(
                p_w_id   INT,
                p_d_id   INT,
                p_c_w_id INT,
                p_c_d_id INT,
                p_c_id   INT,
                p_h_amount DECIMAL(6,2)
            )
            RETURNS TABLE(rel TEXT, ret_w_id INT, ret_d_id INT, ctid TID)
            LANGUAGE plpgsql AS $$
            DECLARE
                w_ctid TID;
                d_ctid TID;
                c_ctid TID;
            BEGIN
                UPDATE warehouse AS w
                SET w_ytd = w.w_ytd + p_h_amount
                WHERE w.w_id = p_w_id
                RETURNING w.ctid INTO w_ctid;

                RETURN QUERY SELECT 'warehouse'::text, p_w_id, NULL::INT, w_ctid;

                UPDATE district AS d
                SET d_ytd = d.d_ytd + p_h_amount
                WHERE d.d_w_id = p_w_id AND d.d_id = p_d_id
                RETURNING d.ctid INTO d_ctid;

                RETURN QUERY SELECT 'district'::text, p_w_id, p_d_id, d_ctid;

                UPDATE customer AS c
                SET c_balance     = c.c_balance - p_h_amount,
                    c_ytd_payment = c.c_ytd_payment + p_h_amount,
                    c_payment_cnt = c.c_payment_cnt + 1
                WHERE c.c_w_id = p_c_w_id AND c.c_d_id = p_c_d_id AND c.c_id = p_c_id
                RETURNING c.ctid INTO c_ctid;

                RETURN QUERY SELECT 'customer'::text, p_c_w_id, p_c_id, c_ctid;

                -- INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
                -- VALUES (p_c_d_id, p_c_w_id, p_c_id, p_d_id, p_w_id, NOW(), p_h_amount, 'payment');
            END;
            $$;
        )SQL");

        txn.commit();
        std::cout << "TPC-C stored procedures created." << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Error creating TPC-C stored procedures: " << e.what() << std::endl;
    }
}

struct OrderLine {
    int i_id;
    int supply_w_id;
    int quantity;
    itemkey_t stock_key;
    bool operator<(const OrderLine& other) const {
        return stock_key < other.stock_key;
    }
};

void TPCC::generate_tpcc_txns_worker(int thread_id, TxnPool* txn_pool) {
    pthread_setname_np(pthread_self(), ("txn_gen_t_" + std::to_string(thread_id)).c_str());
    
    // Generate transactions
    int total_txn_to_generate = MetisWarmupRound * PARTITION_INTERVAL + try_count * worker_threads * ComputeNodeCount;
    
    while(generated_txn_count < total_txn_to_generate) {
        std::vector<TxnQueueEntry*> txn_batch;
        for (int i = 0; i < 100; i++) {
            generated_txn_count++;
            tx_id_t tx_id = tx_id_generator++;
            
            int txn_type_int = generate_txn_type();
            TPCCTxType txn_type = static_cast<TPCCTxType>(txn_type_int);
            
            std::vector<itemkey_t> routing_keys;
            std::vector<itemkey_t> tpcc_params;
            
            // Generate keys based on txn type
            int w_id = random_int(1, num_warehouses_);
            int d_id = random_int(1, DIST_PER_WARE);
            int c_id = nurand(1023, 1, CUST_PER_DIST);
            
            switch(txn_type) {
                case TPCCTxType::kNewOrder: {
                    // Warehouse, District, Customer
                    routing_keys.push_back(make_district_key(w_id, d_id));
                    
                    tpcc_params.push_back(w_id);
                    tpcc_params.push_back(d_id);
                    tpcc_params.push_back(c_id);

                    int o_ol_cnt = random_int(5, 15);
                    tpcc_params.push_back(o_ol_cnt);

                    std::vector<OrderLine> order_lines;
                    order_lines.reserve(o_ol_cnt);

                    for (int i = 0; i < o_ol_cnt; ++i) {
                        int i_id = nurand(8191, 1, ITEM_COUNT);
                        // todo 添加远程仓库逻辑
                        int supply_w_id = w_id; // !Simplified: home warehouse
                        if (num_warehouses_ > 1 && random_int(1, 100) == 1) {
                            supply_w_id = random_int(1, num_warehouses_);
                            while (supply_w_id == w_id) supply_w_id = random_int(1, num_warehouses_);
                        }
                        int quantity = random_int(1, 10);
                        itemkey_t s_key = make_stock_key(supply_w_id, i_id);

                        order_lines.push_back({i_id, supply_w_id, quantity, s_key});
                    }

                    // Sort to avoid deadlock
                    std::sort(order_lines.begin(), order_lines.end());

                    for (const auto& ol : order_lines) {
                        // Only push the first stock key to routing_keys to match TABLE_IDS_ARR
                        routing_keys.push_back(ol.stock_key);
                        
                        tpcc_params.push_back(ol.i_id);
                        tpcc_params.push_back(ol.supply_w_id);
                        tpcc_params.push_back(ol.quantity);
                    }
                    break;
                }
                case TPCCTxType::kPayment: {
                    routing_keys.push_back(make_warehouse_key(w_id));
                    routing_keys.push_back(make_district_key(w_id, d_id));
                    routing_keys.push_back(make_customer_key(w_id, d_id, c_id));
                    
                    tpcc_params.push_back(w_id);
                    tpcc_params.push_back(d_id);
                    tpcc_params.push_back(c_id);
                    int h_amount = random_int(1, 5000);
                    tpcc_params.push_back(h_amount);
                    break;
                }
                default:
                    // Simplified: just warehouse for others
                    routing_keys.push_back(make_warehouse_key(w_id));
                    break;
            }
            
            TxnQueueEntry* txn_entry = new TxnQueueEntry(tx_id, txn_type_int, routing_keys, {}, tpcc_params, routing_keys);
            txn_batch.push_back(txn_entry);
        }
        
        if(SYSTEM_MODE != 11){
            txn_pool->receive_txn_from_client_batch(txn_batch, thread_id);
        } else {
            txn_pool->receive_txn_from_client_batch(txn_batch, 0);
        }
    }
    txn_pool->stop_pool();
}

int TPCC::generate_txn_type() {
    while(true) {
        int x = random_int(1, 100);
        if (x <= 50) return (int)TPCCTxType::kNewOrder;
        else if (x <= 50) return (int)TPCCTxType::kPayment;
        else {};
    }
}

// Random helpers implementation
std::string TPCC::random_string(int min_len, int max_len) {
    int len = random_int(min_len, max_len);
    static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string s;
    s.reserve(len);
    for (int i = 0; i < len; ++i) s.push_back(alphanum[random_int(0, sizeof(alphanum) - 2)]);
    return s;
}

std::string TPCC::random_nstring(int min_len, int max_len) {
    int len = random_int(min_len, max_len);
    static const char nums[] = "0123456789";
    std::string s;
    s.reserve(len);
    for (int i = 0; i < len; ++i) s.push_back(nums[random_int(0, sizeof(nums) - 2)]);
    return s;
}

int TPCC::random_int(int min, int max) {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(min, max);
    return distribution(generator);
}

int TPCC::nurand(int A, int x, int y) {
    static int C = random_int(0, A);
    return (((random_int(0, A) | random_int(x, y)) + C) % (y - x + 1)) + x;
}

bool TPCC::check_table_exists(pqxx::connection* conn) {
    try {
        pqxx::work txn(*conn);
        auto r = txn.exec("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='warehouse'");
        long long c = !r.empty() ? r[0][0].as<long long>(0) : 0;
        txn.commit();
        return c > 0;
    } catch (const std::exception &e) {
        return false;
    }
}
