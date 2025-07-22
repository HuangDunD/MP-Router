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
int try_count = 100000;
std::atomic<int> exe_count = 0;

int smallbank_account = 500000;

void load_data(pqxx::connection *conn0) {
    std::cout << "Loading data..." << std::endl;
    // Load data into the database if needed
    // This is a placeholder for actual data loading logic
    std::string drop_table_sql = "DROP TABLE IF EXISTS checking";
    try {
        pqxx::work txn(*conn0);
        txn.exec(drop_table_sql);
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while dropping table: " << e.what() << std::endl;
    }

    std::string drop_table_sql2 = "DROP TABLE IF EXISTS savings";
    try {
        pqxx::work txn(*conn0);
        txn.exec(drop_table_sql2);
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while dropping table: " << e.what() << std::endl;
    } 

    // Create a new table and insert data
    try {
        pqxx::work txn(*conn0);
        txn.exec("CREATE TABLE checking (id INT PRIMARY KEY, balance INT, name CHAR(500))");
        txn.exec("CREATE TABLE savings (id INT PRIMARY KEY, balance INT, name CHAR(500))");
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while creating table: " << e.what() << std::endl;
    }   
    
    // Insert data into checking and savings tables
    const int num_threads = 16;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;
    auto worker = [](int start, int end) {
        pqxx::connection* conn00 = new pqxx::connection(DBConnection[0]);
        if (!conn00->is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return -1;
        }
        for(int i = start; i < end; i++) {
            int id = i + 1;
            int balance = 1000 + (i % 1000); // Random balance
            std::string name = "Account_" + std::to_string(id);
            std::string insert_checking_sql = "INSERT INTO checking (id, balance, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", '" +
                                        name + "')";
            std::string insert_savings_sql = "INSERT INTO savings (id, balance, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", '" +
                                        name + "')";

            try {
                pqxx::work txn_create(*conn00);
                txn_create.exec(insert_checking_sql);
                txn_create.exec(insert_savings_sql);
                txn_create.commit();
            } catch (const std::exception &e) {
                std::cerr << "Error while inserting data: " << e.what() << std::endl;
            }
        }
    }; 

    // Create and start threads
    for(int i = 0; i < num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? smallbank_account : (i + 1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }
    std::cout << "Data loaded successfully." << std::endl;
}

void run_smallbank_txns() {
    std::cout << "Running smallbank transactions..." << std::endl;
    // This is a placeholder for actual transaction logic
    // You would implement the logic to run transactions against the smallbank database here
    // For example, you could create threads that perform various operations like deposits, withdrawals, etc.
    // Update exe_count as transactions are executed
    auto run_conn0 = new pqxx::connection(DBConnection[0]);
    auto run_conn1 = new pqxx::connection(DBConnection[1]);
    if (!run_conn0->is_open() || !run_conn1->is_open())
    {
        std::cerr << "Failed to connect to the database." << std::endl;
        return;
    }
    
    for (int i = 0; i < try_count; ++i) {
        exe_count++;
        // Simulate some work
        // Randomly select a transaction type and accounts
        int txn_type = rand() % 3;  // 3 types of transactions
        int account1 = rand() % smallbank_account + 1;
        int account2 = rand() % smallbank_account + 1;
        int node_id = rand() % 2;  // Randomly select a node (0 or 1)
        pqxx::work* txn = nullptr;
        if (node_id == 0) {
            txn = new pqxx::work(*run_conn0);
        } else {
            txn = new pqxx::work(*run_conn0);
        }

        try {  
            switch(txn_type) {
                case 0: {  // Balance transfer
                    txn->exec("UPDATE checking SET balance = balance - 10 WHERE id = " + 
                            std::to_string(account1));
                    txn->exec("UPDATE checking SET balance = balance + 10 WHERE id = " + 
                            std::to_string(account2));
                    break;
                }
                case 1: {  // Deposit check
                    txn->exec("UPDATE checking SET balance = balance + 100 WHERE id = " + 
                            std::to_string(account1));
                    break;
                }
                case 2: {  // Write check
                    txn->exec("UPDATE savings SET balance = balance - 50 WHERE id = " + 
                            std::to_string(account1));
                    break;
                }
            }
            txn->commit();
        } catch (const std::exception &e) {
            std::cerr << "Transaction failed: " << e.what() << std::endl;
        }
        delete txn;
        // std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::cout << "Finished running smallbank transactions." << std::endl;

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

    DBConnection.push_back("host=10.12.2.125 port=54322 user=system password=123456 dbname=smallbank");
    DBConnection.push_back("host=10.12.2.127 port=54322 user=system password=123456 dbname=smallbank");
    
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
    load_data(conn0);
    std::cout << "Data loaded successfully." << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2));
    // --- Start Transaction Threads ---
    
    std::cout << "Starting transaction threads..." << std::endl;
    std::vector<std::thread> threads;
    const int num_threads = 16;  // Number of transaction threads
    // Start the transaction threads
    for(int i = 0; i < num_threads; i++) {
        threads.emplace_back(run_smallbank_txns);
    }
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }

    // 关闭连接
    delete conn0;
    delete conn1;

    return 0;
}