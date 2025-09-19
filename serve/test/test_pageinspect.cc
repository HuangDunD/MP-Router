#include <iostream>
#include <pqxx/pqxx> // PostgreSQL C++ library
#include <chrono>
#include <vector>
#include <thread>
#include <csignal>
#include <atomic>
#include "util/json_config.h"

std::vector<std::string> DBConnection;
auto start = std::chrono::high_resolution_clock::now();
int try_count = 50000;
std::atomic<int> exe_count = 0;

int data_load_num = 500000;
void load_data(pqxx::connection *conn0) {
    std::cout << "Loading data..." << std::endl;
    // Load data into the database if needed
    // This is a placeholder for actual data loading logic
    std::string drop_table_sql = "DROP TABLE IF EXISTS mvcc_test";
    try {
        pqxx::work txn(*conn0);
        txn.exec(drop_table_sql);
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while dropping table: " << e.what() << std::endl;
    }

    // Create a new table and insert data
    try {
        pqxx::work txn(*conn0);
        txn.exec("CREATE TABLE mvcc_test (id INT PRIMARY KEY, score INT, name VARCHAR(500))");
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating table: " << e.what() << std::endl;
    }


    const int num_threads = 16;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = data_load_num / num_threads;

    auto worker = [](int start, int end) {
        pqxx::connection* conn00 = new pqxx::connection(DBConnection[0]);
        if (!conn00->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return -1;
        }
        for(int i = start; i < end; i++) {
            int id = i + 1;
            int score = i % 100;
            std::string name = "Name_" + std::to_string(id);
            std::string insert_data_sql = "INSERT INTO mvcc_test (id, score, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(score) + ", '" +
                                        name + "')";
            try {
                pqxx::work txn_create(*conn00);
                txn_create.exec(insert_data_sql);
                txn_create.commit();
            } catch (const std::exception &e) {
                std::cerr << "Error while inserting data: " << e.what() << std::endl;
            }
        }
    };

    // Create and start threads
    for(int i = 0; i < num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? data_load_num : (i + 1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }

    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }

    std::string create_index_sql = "CREATE INDEX idx_id ON mvcc_test (id)"; 
    try {
        pqxx::work txn(*conn0);
        txn.exec(create_index_sql);
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating index: " << e.what() << std::endl;
    }

}

void signal_handler(int signum) {
    std::cout << "\nCaught signal " << signum << " (SIGINT)" << std::endl;
    std::cout << "Printing final statistics before exit..." << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    // 计算吞吐
    std::cout << "Total transactions executed: " << exe_count << std::endl;
    double throughput = (double) exe_count / (ms / 1000.0);
    std::cout << "Throughput: " << throughput << " transactions per second" << std::endl;
    std::cout << "Exiting..." << std::endl;
    // Clean up and close database connections

    exit(signum);
}

int main(int argc, char *argv[]) {
    // Register signal handler for SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    // --- Load Database Connection Info ---
    std::cout << "Loading database connection info..." << std::endl;

    // DBConnection.push_back("host=10.12.2.125 port=54322 user=system password=123456 dbname=test_mvcc");
    // DBConnection.push_back("host=10.12.2.127 port=54322 user=system password=123456 dbname=test_mvcc");
    
    DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");
    DBConnection.push_back("host=127.0.0.1 port=5432 user=hcy password=123456 dbname=smallbank");

    pqxx::connection *conn0 = nullptr;
    pqxx::connection *conn1 = nullptr;
    try {
        // Create a connection for each thread
        conn0 = new pqxx::connection(DBConnection[0]);
        if (!conn0->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return -1;
        } else {
            std::cout << "Connected to the database successfully." << std::endl;
        }
        conn1 = new pqxx::connection(DBConnection[1]);
        if (!conn1->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[1] << std::endl;
            return -1;
        } else {
            std::cout << "Connected to the database successfully." << std::endl;
        }
    } catch (const std::exception &e) {
        std::cerr << "Error while connecting to KingBase: " + std::string(e.what()) << std::endl;
        return -1;
    }

    // Load data into the database if needed
    // load_data(conn0);
    std::cout << "Data loaded successfully." << std::endl;

    // --- Start Workers Threads ---
    // test page inspection speed
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto start = std::chrono::high_resolution_clock::now();
    int ii = 0;
    // while (ii < try_count) {
    //     ii++;
    //     int page_id = rand() % 1000 + 1;
    //     std::string sql = " " ;
    //     std::string items_query = "SELECT * FROM bt_page_items('idx_id', " + std::to_string(page_id) + ")";
    //     std::string stats_query = "SELECT * FROM bt_page_stats('idx_id', " + std::to_string(page_id) + ")";

    //     try {
    //         pqxx::work txn(*conn0);
    //         pqxx::result r1 = txn.exec(items_query);
    //         pqxx::result r2 = txn.exec(stats_query);
    //         txn.commit();
    //         exe_count++;
    //     } catch (const std::exception &e) {
    //         std::cerr << "Error executing query: " << e.what() << std::endl;
    //     }
    // }
    auto end = std::chrono::high_resolution_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Elapsed time for " << try_count << " page inspections: " << ms << " milliseconds" << std::endl;
    double throughput = (double) try_count / (ms / 1000.0);
    std::cout << "Throughput: " << throughput << " page inspections per second" << std::endl; 
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto start2 = std::chrono::high_resolution_clock::now();
    ii = 0;
    while (ii < try_count) {
        ii++;
        int page_id = rand() % 30000 + 1;
        std::string sql = " " ;
        // std::string items_query = "SELECT * FROM heap_page_items(get_raw_page('mvcc_test', " + std::to_string(page_id) + "))";
        std::string items_query = "SELECT * FROM mvcc_test where id = " + std::to_string(page_id) + " ;" ;
        // std::string items_query = "SELECT ctid FROM mvcc_test where id = " + std::to_string(page_id) + " ;" ;
        // std::string items_query = "SELECT * FROM bt_page_items('idx_id', 10) WHERE data = '72 0b 00 00 00 00 00 00' ";

        try {
            pqxx::work txn(*conn0);
            pqxx::result r1 = txn.exec(items_query);
            txn.commit();
            exe_count++;
        } catch (const std::exception &e) {
            std::cerr << "Error executing query: " << e.what() << std::endl;
        }
    }
    auto end2 = std::chrono::high_resolution_clock::now();
    int ms2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2).count();
    std::cout << "Elapsed time for " << try_count << " page inspections: " << ms2 << " milliseconds" << std::endl;
    double throughput2 = (double) try_count / (ms2 / 1000.0);
    std::cout << "Throughput: " << throughput2 << " page inspections per second" << std::endl; 
    // 计算平均每次事务耗时
    double avg_time = (double) ms2 / try_count;
    std::cout << "Average time per page inspection: " << avg_time << " milliseconds" << std::endl;

    // 关闭连接
    delete conn0;
    delete conn1;

    return 0;
}