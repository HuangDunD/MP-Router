#include "smallbank.h"
#include "ycsb.h"

#include <algorithm>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

#include "mysql_client.h"

namespace {

void mysql_exec_ignore(MySQLClient& client, const std::string& sql) {
    try {
        client.exec(sql);
    } catch (const std::exception& e) {
        std::cerr << "Ignoring MySQL error for SQL: " << sql << " error=" << e.what() << std::endl;
    }
}

std::string mysql_values_for_ycsb(int id, int field_len) {
    std::ostringstream os;
    os << "(" << id;
    for (int i = 0; i < 10; i++) {
        os << ",'" << YCSB::random_field_string(field_len) << "'";
    }
    os << ")";
    return os.str();
}

} // namespace

void YCSB::create_table_mysql(const MySQLConnInfo& info) {
    std::cout << "Create YCSB table in MySQL..." << std::endl;
    MySQLClient client(info);
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS ell_ycsb");
    mysql_exec_ignore(client, "DROP TABLE IF EXISTS usertable");
    client.exec(R"SQL(
        CREATE TABLE usertable (
            id INT PRIMARY KEY,
            field0 VARCHAR(100),
            field1 VARCHAR(100),
            field2 VARCHAR(100),
            field3 VARCHAR(100),
            field4 VARCHAR(100),
            field5 VARCHAR(100),
            field6 VARCHAR(100),
            field7 VARCHAR(100),
            field8 VARCHAR(100),
            field9 VARCHAR(100)
        ) ENGINE=InnoDB
    )SQL");
    std::cout << "YCSB table created in MySQL." << std::endl;
}

void YCSB::load_data_mysql(const MySQLConnInfo& info) {
    std::cout << "Loading YCSB data into MySQL... count=" << record_count_ << std::endl;
    const int num_threads = 16;
    const int batch_size = 200;
    std::vector<std::thread> threads;
    int chunk = (record_count_ + num_threads - 1) / num_threads;
    auto worker = [&](int start_id, int end_id) {
        try {
            MySQLClient client(info);
            for (int id = start_id; id < end_id; id += batch_size) {
                int last = std::min(end_id, id + batch_size);
                std::ostringstream sql;
                sql << "INSERT INTO usertable "
                    << "(id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES ";
                for (int cur = id; cur < last; cur++) {
                    if (cur != id) sql << ",";
                    sql << mysql_values_for_ycsb(cur, get_field_len());
                }
                client.exec(sql.str());
            }
        } catch (const std::exception& e) {
            std::cerr << "YCSB MySQL load worker error: " << e.what() << std::endl;
        }
    };

    for (int t = 0; t < num_threads; t++) {
        int s = t * chunk + 1;
        int e = std::min(record_count_ + 1, (t + 1) * chunk + 1);
        if (s < e) threads.emplace_back(worker, s, e);
    }
    for (auto& th : threads) th.join();
    std::cout << "YCSB data loaded into MySQL." << std::endl;
}

void YCSB::create_ycsb_stored_procedures_mysql(const MySQLConnInfo& info) {
    std::cout << "Creating YCSB stored procedure in MySQL..." << std::endl;
    MySQLClient client(info);
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS ell_ycsb");
    std::ostringstream sql;
    sql << "CREATE PROCEDURE ell_ycsb(";
    for (int i = 0; i < txn_length_; ++i) {
        if (i > 0) sql << ", ";
        sql << "IN k" << i << " INT";
    }
    for (int i = 0; i < txn_length_; ++i) {
        sql << ", IN rw" << i << " BOOLEAN";
    }
    sql << ") BEGIN DECLARE v_dummy VARCHAR(100);";
    for (int i = 0; i < txn_length_; ++i) {
        sql << " IF NOT rw" << i
            << " THEN SELECT field0 INTO v_dummy FROM usertable WHERE id = k" << i << "; END IF;";
    }
    for (int i = 0; i < txn_length_; ++i) {
        sql << " IF rw" << i
            << " THEN UPDATE usertable SET field1 = MD5(RAND()) WHERE id = k" << i << "; END IF;";
    }
    sql << " END";
    client.exec(sql.str());
    std::cout << "YCSB stored procedure created in MySQL." << std::endl;
}

bool YCSB::check_table_exists_mysql(const MySQLConnInfo& info) {
    try {
        MySQLClient client(info);
        return client.scalar_uint64(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'usertable'") > 0;
    } catch (const std::exception& e) {
        std::cerr << "YCSB MySQL table check failed: " << e.what() << std::endl;
        return false;
    }
}

bool YCSB::check_record_count_mysql(const MySQLConnInfo& info, int expected_count) {
    try {
        MySQLClient client(info);
        uint64_t count = client.scalar_uint64("SELECT COUNT(*) FROM usertable");
        if (count != static_cast<uint64_t>(expected_count)) {
            std::cerr << "YCSB MySQL count mismatch: count=" << count
                      << ", expected=" << expected_count << std::endl;
        }
        return count == static_cast<uint64_t>(expected_count);
    } catch (const std::exception& e) {
        std::cerr << "YCSB MySQL count check failed: " << e.what() << std::endl;
        return false;
    }
}

void SmallBank::create_table_mysql(const MySQLConnInfo& info) {
    std::cout << "Create SmallBank tables in MySQL..." << std::endl;
    MySQLClient client(info);
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_amalgamate");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_send_payment");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_deposit_checking");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_write_check");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_balance");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_transact_savings");
    mysql_exec_ignore(client, "DROP TABLE IF EXISTS checking");
    mysql_exec_ignore(client, "DROP TABLE IF EXISTS savings");
    client.exec("CREATE TABLE checking (id INT PRIMARY KEY, balance INT, city INT, name CHAR(200)) ENGINE=InnoDB");
    client.exec("CREATE TABLE savings (id INT PRIMARY KEY, balance INT, city INT, name CHAR(200)) ENGINE=InnoDB");
    std::cout << "SmallBank tables created in MySQL." << std::endl;
}

void SmallBank::load_data_mysql(const MySQLConnInfo& info) {
    std::cout << "Loading SmallBank data into MySQL..." << std::endl;
    const int account_count = get_account_count();
    const int num_threads = 16;
    const int batch_size = 500;
    std::vector<int> id_list;
    id_list.reserve(account_count);
    for (int i = 1; i <= account_count; i++) id_list.push_back(i);
    std::mt19937 g(0);
    std::shuffle(id_list.begin(), id_list.end(), g);

    int chunk = (account_count + num_threads - 1) / num_threads;
    std::vector<std::thread> threads;
    auto worker = [&](int start_index, int end_index) {
        try {
            MySQLClient client(info);
            for (int idx = start_index; idx < end_index; idx += batch_size) {
                int last = std::min(end_index, idx + batch_size);
                std::ostringstream checking_sql;
                std::ostringstream savings_sql;
                checking_sql << "INSERT INTO checking (id, balance, city, name) VALUES ";
                savings_sql << "INSERT INTO savings (id, balance, city, name) VALUES ";
                for (int cur = idx; cur < last; cur++) {
                    int id = id_list[cur];
                    int city = id % static_cast<int>(SmallBankCityType::Count);
                    if (cur != idx) {
                        checking_sql << ",";
                        savings_sql << ",";
                    }
                    checking_sql << "(" << id << ",1000," << city << ",'name" << id << "')";
                    savings_sql << "(" << id << ",1000," << city << ",'name" << id << "')";
                }
                client.exec(checking_sql.str());
                client.exec(savings_sql.str());
            }
        } catch (const std::exception& e) {
            std::cerr << "SmallBank MySQL load worker error: " << e.what() << std::endl;
        }
    };

    for (int t = 0; t < num_threads; t++) {
        int s = t * chunk;
        int e = std::min(account_count, (t + 1) * chunk);
        if (s < e) threads.emplace_back(worker, s, e);
    }
    for (auto& th : threads) th.join();
    std::cout << "SmallBank data loaded into MySQL." << std::endl;
}

void SmallBank::create_smallbank_stored_procedures_mysql(const MySQLConnInfo& info) {
    std::cout << "Creating SmallBank stored procedures in MySQL..." << std::endl;
    MySQLClient client(info);
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_amalgamate");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_send_payment");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_deposit_checking");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_write_check");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_balance");
    mysql_exec_ignore(client, "DROP PROCEDURE IF EXISTS sp_transact_savings");

    client.exec(R"SQL(
        CREATE PROCEDURE sp_amalgamate(IN acc1 INT, IN acc2 INT)
        BEGIN
            DECLARE b_check INT DEFAULT 0;
            DECLARE b_save INT DEFAULT 0;
            SELECT balance INTO b_check FROM checking WHERE id = acc1;
            SELECT balance INTO b_save FROM savings WHERE id = acc1;
            UPDATE checking SET balance = 0 WHERE id = acc1;
            UPDATE savings SET balance = 0 WHERE id = acc1;
            UPDATE checking SET balance = balance + b_check + b_save WHERE id = acc2;
        END
    )SQL");
    client.exec(R"SQL(
        CREATE PROCEDURE sp_send_payment(IN acc1 INT, IN acc2 INT)
        BEGIN
            UPDATE checking SET balance = balance - 10 WHERE id = acc1;
            UPDATE checking SET balance = balance + 10 WHERE id = acc2;
        END
    )SQL");
    client.exec(R"SQL(
        CREATE PROCEDURE sp_deposit_checking(IN acc1 INT)
        BEGIN
            UPDATE checking SET balance = balance + 1 WHERE id = acc1;
        END
    )SQL");
    client.exec(R"SQL(
        CREATE PROCEDURE sp_write_check(IN acc1 INT)
        BEGIN
            DECLARE total_balance INT DEFAULT 0;
            SELECT c.balance + s.balance INTO total_balance
            FROM checking c JOIN savings s ON c.id = s.id
            WHERE c.id = acc1;
            IF total_balance < 5 THEN
                UPDATE checking SET balance = balance - 6 WHERE id = acc1;
            ELSE
                UPDATE checking SET balance = balance - 5 WHERE id = acc1;
            END IF;
        END
    )SQL");
    client.exec(R"SQL(
        CREATE PROCEDURE sp_balance(IN acc1 INT)
        BEGIN
            DECLARE total_balance INT DEFAULT 0;
            SELECT c.balance + s.balance INTO total_balance
            FROM checking c JOIN savings s ON c.id = s.id
            WHERE c.id = acc1;
        END
    )SQL");
    client.exec(R"SQL(
        CREATE PROCEDURE sp_transact_savings(IN acc1 INT)
        BEGIN
            UPDATE savings SET balance = balance + 20 WHERE id = acc1;
        END
    )SQL");
    std::cout << "SmallBank stored procedures created in MySQL." << std::endl;
}

bool SmallBank::check_table_exists_mysql(const MySQLConnInfo& info) {
    try {
        MySQLClient client(info);
        uint64_t count = client.scalar_uint64(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name IN ('checking', 'savings')");
        return count == 2;
    } catch (const std::exception& e) {
        std::cerr << "SmallBank MySQL table check failed: " << e.what() << std::endl;
        return false;
    }
}

bool SmallBank::check_account_count_mysql(const MySQLConnInfo& info, int expected_count) {
    try {
        MySQLClient client(info);
        uint64_t checking_count = client.scalar_uint64("SELECT COUNT(*) FROM checking");
        uint64_t savings_count = client.scalar_uint64("SELECT COUNT(*) FROM savings");
        bool ok = checking_count == static_cast<uint64_t>(expected_count) &&
                  savings_count == static_cast<uint64_t>(expected_count);
        if (!ok) {
            std::cerr << "SmallBank MySQL count mismatch: checking=" << checking_count
                      << ", savings=" << savings_count << ", expected=" << expected_count << std::endl;
        }
        return ok;
    } catch (const std::exception& e) {
        std::cerr << "SmallBank MySQL count check failed: " << e.what() << std::endl;
        return false;
    }
}
