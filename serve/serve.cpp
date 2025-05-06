#include <iostream>
#include <string>
#include <cstring>      // For memset, strerror
#include <unistd.h>     // For close, read
#include <arpa/inet.h>  // For socket-related functions, inet_ntop
#include <sys/socket.h> // For socket API
#include <csignal>
#include <random>

#include "util/json_config.h"
#include "threadpool.h" // Include your thread pool header
#include "queryplan_cardinality.h" // Assuming this is needed
// #include "db_meta.h"
#include "bmsql_meta.h" // Include the header file
#include "metis_partitioner.h" // Assuming this is needed, replace NewMetis if necessary
#include "region/region_generator.h"
#include "log/Logger.h"     // Include the new Logger header
#include <pqxx/pqxx> // PostgreSQL C++ library

#define PORT 8500
#define THREAD_POOL_SIZE 16
#define BUFFER_SIZE 8129000 // max 8192kb=8MB 4096000=4MB

// --- Global Variables ---
Logger logger("server.log");
NewMetis metis; // Assuming NewMetis is the correct type
TPCHMeta *TPCH_META = nullptr; // Initialize global pointer
std::string DBConnection[MaxComputeNodeCount];
std::atomic<long long> send_times = 0; // Use long long for potentially large counts
uint64_t seed = 0xdeadbeef;

// Instantiate RegionProcessor within the scope where needed
RegionProcessor regionProcessor(logger); // Pass the logger
BmSql::Meta bmsqlMetadata; // Create an instance

struct RouterEndpoint {
    std::string ip;
    uint16_t port;
    std::string username;
    std::string password;
    std::string dbname;
};

static bool send_sql_to_router(const std::string &sqls,
                               const std::string &conninfo,
                               Logger &lg) {
    try {
        pqxx::connection conn(conninfo);
        if (!conn.is_open()) {
            lg.log("ERROR: Failed to connect to the database.");
            return -1;
        } else {
            lg.log("INFO: Connected to the database.");
        }

        pqxx::work txn(conn);

        pqxx::result result = txn.exec(sqls + "\n");

        txn.commit();

        return true;
    } catch (const std::exception& e) {
        lg.log("ERROR: Error while connecting to PostgreSQL or getting query plan: ", e.what());
        return false;
    }
}

void process_client_data(std::string_view data, int socket_fd, Logger &log) {
    using namespace std::literals;

    // Local helper to send data and report errors
    const auto safe_send = [&](std::string_view msg) -> bool {
        ssize_t n = send(socket_fd, msg.data(), msg.size(), MSG_NOSIGNAL); // 或无 flag
        if (n < 0) {
            if (errno == EPIPE) {
                log.log("Peer closed connection (EPIPE). Closing socket ", socket_fd);
                std::cerr << "socket is closed , Closing socket : " << socket_fd << std::endl;
                close(socket_fd);
            } else {
                std::cerr<<"send failed: " << strerror(errno) << std::endl;
            }
            return false;
        }
        return true;
    };

    // Immediate FINISH handshake
    if (data == "HELLO\n"sv || data == "BYE\n"sv) {
        safe_send("FINISH\n");
        return;
    }

    // ---- Main request handling ----
    log.log("Socket ", socket_fd, " processing: ",
            data.substr(0, 150), data.size() > 150 ? "..." : "");

    std::vector<uint64_t> region_ids;
    std::string row_sql;
    bool ok = false;
    uint64_t router_node = UINT64_MAX;
    
    // Parse the socket data to SQLs and region IDs
    try {
        ok = regionProcessor.generateRegionIDs(std::string(data), region_ids, bmsqlMetadata, row_sql);
    } catch (const std::exception &e) {
        log.log("CRITICAL: RegionProcessor exception for socket ", socket_fd, ": ", e.what());
    }
    if (!ok) {
        // Hard failure
        safe_send("ERR\n");
        return;
    }
    
    // ---- Successful generation ----
    if(SYSTEM_MODE == 0) {
        // random based algorithm
        // Randomly select a router node
        std::random_device rd;  // 获取随机种子
        std::mt19937 gen(rd()); // 使用Mersenne Twister引擎
        std::uniform_int_distribution<> distrib(0, ComputeNodeCount - 1); // 生成随机数
        router_node = distrib(gen); // 生成随机数
    } else if(SYSTEM_MODE == 1) {
        // affinity based algorithm
        if (!region_ids.empty()) {
            log.log("Socket ", socket_fd, ": generated ", region_ids.size(), " region IDs");
            router_node = metis.build_internal_graph(region_ids);
        }
    }

    if (!row_sql.empty() && router_node != UINT64_MAX) {
        // Send SQL to the router node
        const auto &con = DBConnection[router_node % ComputeNodeCount];
        log.log("Sending SQL to router node ", router_node, ": ", con);
        send_sql_to_router(row_sql, con, log);
    } else {
        log.log("ERROR: No SQL to send or invalid router node.");
        safe_send("ERR\n");
        return;
    }

    // ---- Final client acknowledgment ----
    if (safe_send("OK\n")) {
        if (++send_times % 1000 == 0) {
            std::cout <<"send OK times: " << send_times.load() << std::endl;
        }
    }
}


// --- Main Function ---
int main() {
    signal(SIGPIPE, SIG_IGN);

    logger.set_log_to_console(true);
    logger.log("Server starting...");

    // --- Load Database Connection Info ---
    logger.log("Loading database connection info...");
    std::string compute_node_config_path = "../../config/compute_node_config.json";
    auto compute_node_config = JsonConfig::load_file(compute_node_config_path);
    auto compute_node_list = compute_node_config.get("remote_compute_nodes");
    auto compute_node_count = (int)compute_node_list.get("remote_compute_node_count").get_int64();
    ComputeNodeCount = compute_node_count;
    std::cout << "ComputeNodeCount: " << ComputeNodeCount << std::endl;
    for(int i=0; i < compute_node_count; i++) {
        auto ip = compute_node_list.get("remote_compute_node_ips").get(i).get_str(); 
        auto port = (int)compute_node_list.get("remote_compute_node_ports").get(i).get_int64();
        auto username = compute_node_list.get("remote_compute_node_usernames").get(i).get_str();
        auto password = compute_node_list.get("remote_compute_node_passwords").get(i).get_str();
        auto dbname = compute_node_list.get("remote_compute_node_dbnames").get(i).get_str();
        DBConnection[i] = "host=" + ip +
                          " port=" + std::to_string(port) +
                          " user=" + username +
                          " password=" + password +
                          " dbname=" + dbname;
        logger.log("DBConnection[", i, "] = ", DBConnection[i]);
    }


    // --- Load DB Meta (Conditional) ---
#if WORKLOAD_MODE == 0
    // YCSB workload
    logger.log("WORKLOAD_MODE is YCSB (0). Initializing YCSB Metadata...");
    try {
        std::string metaFilePath = "../../config/ycsb_meta.json";
        std::cout << "Loading YCSB metadata from: " << metaFilePath << std::endl;
        // Load the metadata
        auto json_config = JsonConfig::load_file(metaFilePath);
        auto conf = json_config.get("ycsb"); // Or appropriate config section
        REGION_SIZE = conf.get("key_cnt_per_partition").get_int64();
        logger.log("REGION_SIZE set to: ", REGION_SIZE);
        if (REGION_SIZE <= 0) {
            logger.log("CRITICAL ERROR: REGION_SIZE loaded from config is not positive. Value: ", REGION_SIZE);
            return -1; // Cannot proceed with invalid REGION_SIZE
        }
        std::cout << "Metadata loaded successfully." << std::endl;
        std::cout << "-----------------------------" << std::endl;
    } catch (const std::exception &e) {
        logger.log("CRITICAL ERROR initializing YCSB Metadata: ", e.what());
        return -1;
    }
#elif WORKLOAD_MODE == 1
    // TPC-C workload
    logger.log("WORKLOAD_MODE is TPCC (1). Initializing TPCC Metadata...");
    try {
        std::string metaFilePath = "../../config/tpcc_meta.json";
        std::cout << "Loading TPCC metadata from: " << metaFilePath << std::endl;
        // Load the metadata
        if (!bmsqlMetadata.loadFromJsonFile(metaFilePath)) {
            std::cerr << "Fatal Error: Failed to load TPCC metadata. Exiting." << std::endl;
            return EXIT_FAILURE;
        }

        std::cout << "Metadata loaded successfully." << std::endl;
        std::cout << "-----------------------------" << std::endl;
    } catch (const std::exception &e) {
        logger.log("CRITICAL ERROR initializing TPCC Metadata: ", e.what());
        return -1;
    }
#else
    logger.log("WORKLOAD_MODE is YCSB (0) or undefined. Skipping TPCH Metadata initialization.");
#endif

    // --- Thread Pool Setup ---
    logger.log("Initializing thread pool with ", THREAD_POOL_SIZE, " threads.");
    ThreadPool pool(THREAD_POOL_SIZE);
    metis.set_thread_pool(&pool);

    // --- Server Socket Setup ---
    int server_fd;
    struct sockaddr_in address{};
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        logger.log("CRITICAL ERROR: Failed to create server socket. Error: ", strerror(errno));
        return -1;
    }
    logger.log("Server socket created (FD: ", server_fd, ").");

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        logger.log("ERROR: Failed to set SO_REUSEADDR option. Error: ", strerror(errno));
        // Continue if non-critical, but log the error
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0) {
        logger.log("CRITICAL ERROR: Bind failed on port ", PORT, ". Error: ", strerror(errno));
        close(server_fd);
        return -1;
    }
    logger.log("Server socket bound to port ", PORT, ".");

    if (listen(server_fd, 10) < 0) {
        // Increased backlog slightly
        logger.log("CRITICAL ERROR: Listen failed. Error: ", strerror(errno));
        close(server_fd);
        return -1;
    }
    logger.log("Server is running and listening on port ", PORT, "...");

    logger.set_log_to_console(false);
    // --- Accept Client Connections Loop ---
    while (true) {
        int new_socket;
        struct sockaddr_in client_address{}; // Use a separate struct for client addr
        socklen_t client_addrlen = sizeof(client_address); // Use socklen_t

        if ((new_socket = accept(server_fd, (struct sockaddr *) &client_address, &client_addrlen)) < 0) {
            logger.log("ERROR: Failed to accept connection. Error: ", strerror(errno));
            // Consider adding a small sleep or other logic if accept fails repeatedly
            continue; // Continue to the next iteration to try accepting again
        }

        // Get client address info safely
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_address.sin_port);

        logger.log("New client connected, IP: ", client_ip, ", Port: ", client_port, ", Socket FD: ", new_socket);

        // --- Client Handler Thread ---
        // Create a separate thread to handle this client's communication.
        // This prevents the main accept loop from blocking on reads for one client.
        std::thread client_handler_thread([new_socket, &pool, client_ip, client_port]() {
            // Pass pool by reference, other variables captured by value implicitly
            // Note: The global 'logger' is accessible here directly.

            logger.log("Handler thread started for client ", client_ip, ":", client_port, " (Socket: ", new_socket,
                       ")");
            char *buffer = new char[BUFFER_SIZE]; // Each client handler thread has its own buffer

            // Read incoming data from this specific client
            while (true) {
                memset(buffer, 0, BUFFER_SIZE); // Clear the buffer
                ssize_t valread = read(new_socket, buffer, BUFFER_SIZE - 1);
                // Use ssize_t, leave space for null terminator

                if (valread < 0) {
                    // Error occurred
                    logger.log("ERROR reading from socket ", new_socket, " (Client: ", client_ip, ":", client_port,
                               "). Error: ", strerror(errno));
                    break; // Exit the read loop for this client
                } else if (valread == 0) {
                    // Client disconnected gracefully
                    logger.log("Client ", client_ip, ":", client_port, " (Socket: ", new_socket,
                               ") disconnected gracefully.");
                    break; // Exit the read loop for this client
                }

                // Process received data using the thread pool
                std::string received_data(buffer, valread); // Use valread for accurate length

                // Enqueue the processing task, passing the logger by reference wrapper
                pool.enqueue(process_client_data, received_data, new_socket, std::ref(logger));
            }

            // Close the client socket when the read loop finishes (error or disconnect)
            close(new_socket);
            delete [] buffer; // Free the buffer memory
            logger.log("Closed socket ", new_socket, " for client ", client_ip, ":", client_port);
        }); // End of lambda for client handler thread

        client_handler_thread.detach(); // Detach the thread to run independently
    } // End of accept loop

    // --- Server Shutdown (Code may not be reached in simple loop) ---
    logger.log("Server shutting down...");
    close(server_fd); // Close the listening server socket


    return 0;
}

