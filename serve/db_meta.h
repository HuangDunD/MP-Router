#ifndef DB_META_H
#define DB_META_H

#include "config.h" // Assuming this exists and might be needed
#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm> // Needed for std::transform in TPCHMeta columnNameToID lambda
#include <stdexcept> // For exception handling during JSON parsing
#include <map>       // Keep for existing TPCHMeta usage

// #include "nlohmannjson/single_include/nlohmann/json.hpp"

// Use the nlohmann::json namespace for convenience
// using json = nlohmann::json;


// =========================================================================
// ==                       TPC-H Metadata Class                          ==
// =========================================================================
// (Existing TPCHMeta class - kept mostly as is)
class TPCHMeta {
public:
    // --- MODIFICATION: Added 'inline' before 'static const' ---
    inline static const std::unordered_map<int, std::string> tableIDToName = {
        {0, "NATION"}, {1, "REGION"}, {2, "PART"}, {3, "SUPPLIER"},
        {4, "PARTSUPP"}, {5, "CUSTOMER"}, {6, "ORDERS"}, {7, "LINEITEM"}
    };

    // --- MODIFICATION: Added 'inline' before 'static const' ---
    inline static const std::unordered_map<std::string, int> tablenameToID = {
        {"NATION", 0}, {"REGION", 1}, {"PART", 2}, {"SUPPLIER", 3},
        {"PARTSUPP", 4}, {"CUSTOMER", 5}, {"ORDERS", 6}, {"LINEITEM", 7}
    };

    // --- MODIFICATION: Added 'inline' before 'static const' ---
    inline static const std::vector<std::unordered_map<int, std::string> > columnIDToName = {
        // NATION
        {{0, "n_nationkey"}, {1, "n_name"}, {2, "n_regionkey"}, {3, "n_comment"}},
        // REGION
        {{0, "r_regionkey"}, {1, "r_name"}, {2, "r_comment"}},
        // PART
        {
            {0, "p_partkey"}, {1, "p_name"}, {2, "p_mfgr"}, {3, "p_brand"}, {4, "p_type"},
            {5, "p_size"}, {6, "p_container"}, {7, "p_retailprice"}, {8, "p_comment"}
        },
        // SUPPLIER
        {
            {0, "s_suppkey"}, {1, "s_name"}, {2, "s_address"}, {3, "s_nationkey"},
            {4, "s_phone"}, {5, "s_acctbal"}, {6, "s_comment"}
        },
        // PARTSUPP
        {
            {0, "ps_partkey"}, {1, "ps_suppkey"}, {2, "ps_availqty"},
            {3, "ps_supplycost"}, {4, "ps_comment"}
        },
        // CUSTOMER
        {
            {0, "c_custkey"}, {1, "c_name"}, {2, "c_address"}, {3, "c_nationkey"},
            {4, "c_phone"}, {5, "c_acctbal"}, {6, "c_mktsegment"}, {7, "c_comment"}
        },
        // ORDERS
        {
            {0, "o_orderkey"}, {1, "o_custkey"}, {2, "o_orderstatus"}, {3, "o_totalprice"},
            {4, "o_orderdate"}, {5, "o_orderpriority"}, {6, "o_clerk"}, {7, "o_shippriority"},
            {8, "o_comment"}
        },
        // LINEITEM
        {
            {0, "l_orderkey"}, {1, "l_partkey"}, {2, "l_suppkey"}, {3, "l_linenumber"},
            {4, "l_quantity"}, {5, "l_extendedprice"}, {6, "l_discount"}, {7, "l_tax"},
            {8, "l_returnflag"}, {9, "l_linestatus"}, {10, "l_shipdate"}, {11, "l_commitdate"},
            {12, "l_receiptdate"}, {13, "l_shipinstruct"}, {14, "l_shipmode"}, {15, "l_comment"}
        }
    };

    // Original ID2NAME map - Seems like a leftover or for a different purpose? Kept as is.
    inline static const std::map<int, std::string> ID2NAME = {
        {0, "bmsql_customer"}, {1, "bmsql_district"}, {2, "bmsql_history"},
        {3, "bmsql_item"}, {4, "bmsql_new_order"}, {5, "bmsql_oorder"},
        {6, "bmsql_order_line"}, {7, "bmsql_stock"}, {8, "bmsql_warehouse"},
        {9, "w_id"}, {10, "c_w_id"}, {11, "c_d_id"},
        {12, "c_id"}, {13, "d_w_id"}, {14, "d_id"},
        {15, "i_id"}, {16, "s_w_id"}, {17, "s_i_id"},
        {18, "no_w_id"}, {19, "no_d_id"}, {20, "no_o_id"},
        {21, "o_id"}, {22, "o_w_id"}, {23, "o_d_id"},
        {24, "o_c_id"}, {25, "ol_w_id"}, {26, "ol_d_id"},
        {27, "ol_o_id"}
    };


    // --- MODIFICATION: Added 'inline' before 'static const' ---
    // Also included <algorithm> for std::transform
    inline static const std::vector<std::unordered_map<std::string, int> > columnNameToID = []() {
        std::vector<std::unordered_map<std::string, int> > result;
        for (const auto &table: TPCHMeta::columnIDToName) {
            std::unordered_map<std::string, int> reverseMap;
            for (const auto &[id, name]: table) {
                reverseMap[name] = id;
            }
            result.push_back(reverseMap);
        }
        return result;
    }();
    // ---- End modifications for static members ----

    std::vector<std::vector<long long> > table_column_cardinality;
    std::vector<int> partition_column_ids = std::vector<int>(8, 0); // 8 tables, initialized to 0
    std::mutex mutex_partition_column_ids; // Mutex for thread-safe access

    TPCHMeta() {
        table_column_cardinality.resize(8);
        for (int i = 0; i < 8; ++i) {
            if (i < columnIDToName.size()) {
                table_column_cardinality[i].resize(columnIDToName[i].size(), 0);
            } else {
                // This error suggests columnIDToName might not always have 8 entries
                // Or the loop condition should be based on columnIDToName.size()
                 std::cerr << "Warning: TPCHMeta constructor index " << i <<
                      " out of bounds for columnIDToName (size=" << columnIDToName.size() << ")." << std::endl;
                 // Handle appropriately, maybe resize table_column_cardinality differently?
                 // For now, just skip resizing the inner vector if index is out of bounds.
            }
        }
    };

    // Reads partition column IDs for TPC-H tables from a simple format file
    void ReadColumnIDFromFile(const std::string& fname) {
        std::ifstream infile(fname);
        if (!infile.is_open()) {
            std::cerr << "Error opening TPC-H partition file: " << fname << std::endl;
            return;
        }
        std::string line;
        int table_index = 0;
        // Read up to partition_column_ids.size() lines
        while (table_index < partition_column_ids.size() && std::getline(infile, line)) {
             std::istringstream iss(line);
             int column_id;
             if (!(iss >> column_id)) {
                 std::cerr << "Error parsing column ID for TPC-H table " << table_index << " from line: " << line << std::endl;
                 // Decide how to handle parse errors: skip, set default, stop?
                 // Continuing to next line for now.
                 table_index++;
                 continue;
             }
             partition_column_ids[table_index] = column_id;
             std::cout << "TPC-H Table " << table_index << " partition column id set to: " << column_id << std::endl;
             table_index++;
        }
         if (table_index < partition_column_ids.size() && !infile.eof()) {
              std::cerr << "Warning: TPC-H partition file " << fname << " did not contain enough lines for all tables." << std::endl;
         }
        infile.close();
    }
};

// Global variable declaration (if used across multiple files)
// extern TPCHMeta* TPCH_META; // Declaration should be in one place, maybe a central header or config.h

#endif // DB_META_H