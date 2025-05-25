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
    std::string compute_node_config_path = "../../../config/compute_node_config.json";
    auto compute_node_config = JsonConfig::load_file(compute_node_config_path);
    auto compute_node_list = compute_node_config.get("remote_compute_nodes");
    auto compute_node_count = (int) compute_node_list.get("remote_compute_node_count").get_int64();
    for (int i = 0; i < compute_node_count; i++) {
        auto ip = compute_node_list.get("remote_compute_node_ips").get(i).get_str();
        auto port = (int) compute_node_list.get("remote_compute_node_ports").get(i).get_int64();
        auto username = compute_node_list.get("remote_compute_node_usernames").get(i).get_str();
        auto password = compute_node_list.get("remote_compute_node_passwords").get(i).get_str();
        auto dbname = compute_node_list.get("remote_compute_node_dbnames").get(i).get_str();
        DBConnection.push_back(
            "host=" + ip +
            " port=" + std::to_string(port) +
            " user=" + username +
            " password=" + password +
            " dbname=" + dbname);
    }
    
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

    assert(argc >= 2);
    int system_value = std::stoi(argv[1]);
    int sleep_ms = 0;
    if(argc == 3) {
        sleep_ms = std::stoi(argv[2]);
    }

    int t_c = try_count;
    // 计时 
    start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < 2; i++) {
        threads.emplace_back([i, t_c, system_value, conn0, conn1, sleep_ms]() {
            for (int j = 0; j < t_c; j++) {
                exe_count ++;
                int score = j % 100;
                int id;
                if(system_value == 0) {
                    if(i == 0) {
                        id = 1;
                    }
                    else {
                        id = 4;
                    }
                }
                if(system_value == 1) {
                    if(i == 0) { 
                        id = (j % 2 == 0) ? 1 : 4;
                    }
                    else {
                        id = (j % 2 == 0) ? 4 : 1;
                    }
                }
                std::string sql = "Update hcy_par_test2 set score = " + std::to_string(score) + " where id = " + std::to_string(id) + ";";
                // std::cout << "Thread " << i << " executing transaction " << j << ": " << sql << std::endl;
                // Send SQL to the database
                try{
                    if (i == 0) {
                        pqxx::work txn(*conn0);
                        txn.exec(sql);
                        txn.commit();
                    } else {
                        pqxx::work txn(*conn1);
                        txn.exec(sql);
                        txn.commit();
                    }
                } catch (const std::exception &e) {
                    std::cerr << "Error while executing SQL: " << sql << std::string(e.what()) << std::endl;
                }
                // 统计
                if (j % 10000 == 0) {
                    std::cout << "Thread " << i << " executed transaction " << j << std::endl;
                }
                // sleep
                if(sleep_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                }
            }
            std::cout << "Thread " << i << " finished executing transactions." << std::endl;
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Elapsed time: " << ms << " milliseconds" << std::endl;
    // 计算吞吐
    double throughput = (double) try_count * 2 / (ms / 1000.0);
    std::cout << "Throughput: " << throughput << " transactions per second" << std::endl;
    // 关闭连接
    delete conn0;
    delete conn1;

    return 0;
}