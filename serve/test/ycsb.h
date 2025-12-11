// Author: huangdund
// Year: 2025

#pragma once

#include <cassert>
#include <cstdint>
#include <string>
#include <vector>
#include <thread>
#include <random>
#include <pqxx/pqxx>

#include "common.h"
#include "config.h"
#include "util/zipf.h"
#include "txn_entry.h"
#include "txn_queue.h"
#include "parse.h"

// Table id
enum class YCSBTableType : uint64_t {
  kYCSBTable = 0,
};

class YCSB {
public:
    // access_pattern: 0=uniform, 1=zipfian
    YCSB(int record_count, int access_pattern, int read_pct = 90, int update_pct = 10, int field_len = 100)
        : record_count_(record_count), access_pattern_(access_pattern), read_pct_(read_pct), update_pct_(update_pct), field_len_(field_len) {}

    int get_record_count() const { return record_count_; }
    int get_access_pattern() const { return access_pattern_; }
    int get_read_pct() const { return read_pct_; }
    int get_update_pct() const { return update_pct_; }

    inline static const std::vector<table_id_t> TABLE_IDS_ARR[] = {
        // txn_type == 0 -> 10 zeros
        std::vector<table_id_t>(10, static_cast<table_id_t>(0))
    };

    // 表：usertable(id INT PRIMARY KEY, field0 TEXT)
    void create_table(pqxx::connection* conn) {
        std::cout << "Create YCSB table..." << std::endl;
        try {
            pqxx::work txn(*conn);
            txn.exec("DROP TABLE IF EXISTS usertable");
            txn.exec(R"SQL(
            CREATE UNLOGGED TABLE usertable (
                id INT, 
                FIELD0   VARCHAR(100),
                FIELD1   VARCHAR(100),
                FIELD2   VARCHAR(100),
                FIELD3   VARCHAR(100),
                FIELD4   VARCHAR(100),
                FIELD5   VARCHAR(100),
                FIELD6   VARCHAR(100),
                FIELD7   VARCHAR(100),
                FIELD8   VARCHAR(100),
                FIELD9   VARCHAR(100)
            ) WITH (FILLFACTOR = 50);
            )SQL");
            txn.exec("CREATE INDEX idx_usertable_id ON usertable(id)");
            txn.commit();
            std::cout << "YCSB table created." << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error creating YCSB table: " << e.what() << std::endl;
        }
        try {
            pqxx::work txn(*conn);
            txn.exec(R"SQL(
            ALTER TABLE usertable SET ( 
                autovacuum_enabled = on,
                autovacuum_vacuum_scale_factor = 0.05,   
                autovacuum_vacuum_threshold = 500,       
                autovacuum_analyze_scale_factor = 0.05,
                autovacuum_analyze_threshold = 500
            );
            )SQL");
            std::cout << "Set usertable table autovacuum parameters." << std::endl;
            txn.commit();
        }
        catch (const std::exception &e) {
            std::cerr << "Error while setting usertable table autovacuum: " << e.what() << std::endl;
        }
        std::thread extend_thread1([](){
            pqxx::connection conn_extend(DBConnection[0]);
            if (!conn_extend.is_open()) {
                std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
                return;
            }
            try{
                // pg not support
                pqxx::nontransaction txn(conn_extend);
                // pre-extend table to avoid frequent page extend during txn processing
                std::string extend_sql = "SELECT sys_extend('usertable', " + std::to_string(PreExtendPageSize) + ")";
                txn.exec(extend_sql);
                std::cout << "Pre-extended usertable table." << std::endl;
            }
            catch (const std::exception &e) {
                std::cerr << "Error while pre-extending checking table: " << e.what() << std::endl;
            }
        });

        extend_thread1.join();
        std::cout << "Table creation and pre-extension completed." << std::endl;

    }

    // 装载数据
    void load_data(pqxx::connection* conn0);

    void generate_ten_keys(std::vector<itemkey_t>& keys_vec, ZipfGen* zipfian_gen) {
        for (int i = 0; i < 10; i++) {
            itemkey_t key;
            if (access_pattern_ == 0) { // uniform
                key = rand() % record_count_ + 1;
            } else if (access_pattern_ == 1){ // zipfian
                key = zipfian_gen->next() + 1;
            } else assert(false);
            keys_vec[i] = key;
        }
    }

    int generate_txn_type() const {
        return 0;
    }
    
    void create_ycsb_stored_procedures(pqxx::connection* conn);

    void generate_ycsb_txns_worker(int thread_id, TxnPool* txn_pool);

    std::vector<table_id_t>& get_table_ids_by_txn_type() {
        return const_cast<std::vector<table_id_t>&>(TABLE_IDS_ARR[0]);
    }

    void set_zipfian_theta(double theta) {
        zipfian_theta_ = theta;
    }

    void set_hotspot_params(double fraction, double access_prob) {
        hotspot_fraction_ = fraction;
        hotspot_access_prob_ = access_prob;
    }

    // 检查 YCSB 表是否存在（usertable）
    bool check_table_exists(pqxx::connection* conn) {
        try {
            pqxx::work txn(*conn);
            auto r = txn.exec("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='usertable'");
            long long c = !r.empty() ? r[0][0].as<long long>(0) : 0;
            txn.commit();
            if(c <= 0) {
                std::cerr << "Table check failed: usertable does not exist" << std::endl;
            }
            return c > 0;
        } catch (const std::exception &e) {
            std::cerr << "Error in YCSB::check_table_exists: " << e.what() << std::endl;
            return false;
        }
    }

    // 校验记录数量是否匹配
    bool check_record_count(pqxx::connection* conn, int expected_count) {
        try {
            pqxx::work txn(*conn);
            auto r = txn.exec("SELECT COUNT(*) FROM usertable");
            long long c = !r.empty() ? r[0][0].as<long long>(-1) : -1;
            txn.commit();
            if(c != expected_count) {
                std::cerr << "Record count mismatch: usertable=" << c << ", expected=" << expected_count << std::endl;
            }
            return c == expected_count;
        } catch (const std::exception &e) {
            std::cerr << "Error in YCSB::check_record_count: " << e.what() << std::endl;
            return false;
        }
    }

private:
    int record_count_;
    int access_pattern_;
    int read_pct_;
    int update_pct_;
    int field_len_;
    double zipfian_theta_;
    double hotspot_fraction_;
    double hotspot_access_prob_;

    static std::string random_string(int len) {
        static thread_local std::mt19937 rng{std::random_device{}()};
        static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, (int)sizeof(alphanum) - 2);
        std::string s;
        s.reserve(len);
        for (int i = 0; i < len; ++i) s.push_back(alphanum[dist(rng)]);
        return s;
    }
};