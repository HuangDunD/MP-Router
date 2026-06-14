// Author: huangdund
// Year: 2025

#pragma once

#include <cassert>
#include <cstdint>
#include <string>
#include <vector>
#include <thread>
#include <random>
#include <cmath>
#include <algorithm>
#include <functional>
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
    enum class RwMode {
        FIXED,
        RANDOM
    };

    // access_pattern: 0=uniform, 1=zipfian
    YCSB(int record_count, int access_pattern, int read_pct = 90, int update_pct = 10, int field_len = 100)
        : record_count_(record_count), access_pattern_(access_pattern), read_pct_(read_pct), update_pct_(update_pct),
          field_len_(field_len), zipfian_theta_(0.99), use_finite_zipfian_(false), hotspot_fraction_(0.2),
          hotspot_access_prob_(0.8), rw_mode_(RwMode::FIXED) {
            int total_keys = 10; // 固定每次10个键
            read_ops_per_txn_ = std::max(0, std::min(total_keys, (int)std::round(total_keys * (read_pct / 100.0))));
            write_ops_per_txn_ = total_keys - read_ops_per_txn_;
            // 预构造静态 rw_flags_：0 表示读，1 表示写
            rw_flags_.assign(total_keys, false);
            for (int i = read_ops_per_txn_; i < total_keys; ++i) rw_flags_[i] = true;
        }

    int get_record_count() const { return record_count_; }
    int get_access_pattern() const { return access_pattern_; }
    int get_read_cnt() const { return read_ops_per_txn_; }
    int get_write_cnt() const { return write_ops_per_txn_; }
    int get_field_len() const { return field_len_; }

    static std::string random_field_string(int len) {
        static thread_local std::mt19937 rng{std::random_device{}()};
        static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, (int)sizeof(alphanum) - 2);
        std::string s;
        s.reserve(len);
        for (int i = 0; i < len; ++i) s.push_back(alphanum[dist(rng)]);
        return s;
    }

    inline static const std::vector<table_id_t> TABLE_IDS_ARR[] = {
        // txn_type == 0 -> 10 zeros
        std::vector<table_id_t>(10, static_cast<table_id_t>(0))
    };

    // 表：usertable(id INT PRIMARY KEY, field0 TEXT)
    void create_table(pqxx::connection* conn) {
        std::cout << "Create YCSB table..." << (USE_UNLOGGED_TABLES ? " (UNLOGGED)" : "") << std::endl;
        auto print_usertable_size = [](pqxx::connection* size_conn, const std::string& label) {
            try {
                pqxx::work txn(*size_conn);
                pqxx::result usertable_size =
                    txn.exec("select sys_size_pretty(sys_relation_size('usertable'))");
                std::cout << "YCSB table sizes " << label << ":" << std::endl;
                if (!usertable_size.empty()) {
                    std::cout << "  Usertable table size: "
                              << usertable_size[0][0].as<std::string>() << std::endl;
                }
                txn.commit();
            } catch (const std::exception& e) {
                std::cerr << "Error while getting YCSB table size " << label
                          << ": " << e.what() << std::endl;
            }
        };
        try {
            pqxx::work txn(*conn);
            txn.exec("DROP TABLE IF EXISTS usertable");
            const std::string table_keyword = USE_UNLOGGED_TABLES ? "CREATE UNLOGGED TABLE " : "CREATE TABLE ";
            txn.exec(table_keyword + R"SQL(usertable (
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
            txn.commit();
            std::cout << "YCSB table created." << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error creating YCSB table: " << e.what() << std::endl;
        }
        print_usertable_size(conn, "before pre-extension");
        if (DISABLE_TABLE_AUTOVACUUM) {
            try {
                pqxx::work txn(*conn);
                txn.exec("ALTER TABLE usertable SET (autovacuum_enabled = off)");
                txn.commit();
                std::cout << "Disabled autovacuum for usertable." << std::endl;
            } catch (const std::exception &e) {
                std::cerr << "Error while setting usertable autovacuum: " << e.what() << std::endl;
            }
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
        print_usertable_size(conn, "after pre-extension");

    }

    // 装载数据
    void load_data(pqxx::connection* conn0);

    void generate_ten_keys(std::vector<itemkey_t>& keys_vec,
                           std::vector<bool>& rw_flags,
                           ZipfGen* zipfian_gen,
                           FiniteZipfGen* finite_zipfian_gen,
                           std::mt19937& rng) {
        const bool enforce_unique_keys = record_count_ >= static_cast<int>(keys_vec.size());
        for (int i = 0; i < 10; i++) {
            itemkey_t key;
            do {
                if (access_pattern_ == 0) { // uniform
                    key = rand() % record_count_ + 1;
                } else if (access_pattern_ == 1){ // zipfian
                    key = (use_finite_zipfian_ ? finite_zipfian_gen->next() : zipfian_gen->next()) + 1;
                } else assert(false);
            } while (enforce_unique_keys && std::find(keys_vec.begin(), keys_vec.begin() + i, key) != keys_vec.begin() + i);
            keys_vec[i] = key;
        }

        if (rw_mode_ == RwMode::FIXED) {
            rw_flags = rw_flags_;
        } else {
            std::bernoulli_distribution write_dist(update_pct_ / 100.0);
            rw_flags.assign(keys_vec.size(), false);
            for (size_t i = 0; i < rw_flags.size(); i++) {
                rw_flags[i] = write_dist(rng);
            }
        }

        std::vector<itemkey_t> read_keys;
        std::vector<itemkey_t> write_keys;
        read_keys.reserve(keys_vec.size());
        write_keys.reserve(keys_vec.size());
        for (size_t i = 0; i < keys_vec.size(); i++) {
            if (rw_flags[i]) {
                write_keys.push_back(keys_vec[i]);
            } else {
                read_keys.push_back(keys_vec[i]);
            }
        }

        std::sort(read_keys.begin(), read_keys.end(), std::greater<itemkey_t>());
        std::sort(write_keys.begin(), write_keys.end(), std::greater<itemkey_t>());

        size_t pos = 0;
        for (itemkey_t key : read_keys) {
            keys_vec[pos] = key;
            rw_flags[pos] = false;
            pos++;
        }
        for (itemkey_t key : write_keys) {
            keys_vec[pos] = key;
            rw_flags[pos] = true;
            pos++;
        }
    }

    int generate_txn_type() const {
        return 0;
    }

    // 获取读写标志（零拷贝），1表示写，0表示读
    const std::vector<bool>& get_rw_flags() const { return rw_flags_; }

    void set_rw_mode(RwMode mode) { rw_mode_ = mode; }

    bool random_rw_mode_enabled() const { return rw_mode_ == RwMode::RANDOM; }

    void create_ycsb_stored_procedures(pqxx::connection* conn);
    void create_table_mysql(const MySQLConnInfo& info);
    void load_data_mysql(const MySQLConnInfo& info);
    void create_ycsb_stored_procedures_mysql(const MySQLConnInfo& info);
    bool check_table_exists_mysql(const MySQLConnInfo& info);
    bool check_record_count_mysql(const MySQLConnInfo& info, int expected_count);

    void generate_ycsb_txns_worker(int thread_id, TxnPool* txn_pool);

    std::vector<table_id_t>& get_table_ids_by_txn_type() {
        return const_cast<std::vector<table_id_t>&>(TABLE_IDS_ARR[0]);
    }

    void set_zipfian_theta(double theta) {
        zipfian_theta_ = theta;
    }

    void set_zipfian_generator(bool use_finite_zipfian) {
        use_finite_zipfian_ = use_finite_zipfian;
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
    bool use_finite_zipfian_;
    double hotspot_fraction_;
    double hotspot_access_prob_;
    RwMode rw_mode_;

    int read_ops_per_txn_;
    int write_ops_per_txn_;
    std::vector<bool> rw_flags_; // 大小固定为10：前 read_ops_per_txn_ 为0(读)，其余为1(写)

    static std::string random_string(int len) {
        return random_field_string(len);
    }
};
