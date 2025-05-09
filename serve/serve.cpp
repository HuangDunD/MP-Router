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
#include "region/region_generator.h"
#include "metis_partitioner.h" // Assuming this is needed, replace NewMetis if necessary
// #include "log/Logger.h"     // Include the new Logger header
#include <pqxx/pqxx> // PostgreSQL C++ library

int PORT;
int THREAD_POOL_SIZE;
int SOCKET_BUFFER_SIZE;

constexpr std::string_view HEADER_MARKER = "***Header_Start***";
constexpr std::string_view END_MARKER = "***Txn_End***";


// --- Global Variables ---
Logger logger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::INFO, "server.log", 1024);
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
            lg.error(" Failed to connect to the database. conninfo" + conninfo);
            return -1;
        } else {
            lg.info("Connected to the database successful.");
        }

        pqxx::work txn(conn);

        pqxx::result result = txn.exec(sqls + "\n");

        txn.commit();

        return true;
    } catch (const std::exception &e) {
        lg.error("Error while connecting to KingBase or getting query plan: " + std::string(e.what()));
        return false;
    }
}


// Local helper to send data and report errors
const bool safe_send(std::string_view msg, int socket_fd, Logger &log) {
    ssize_t n = send(socket_fd, msg.data(), msg.size(), MSG_NOSIGNAL); // 或无 flag
    if (n < 0) {
        if (errno == EPIPE) {
            log.error("Peer closed connection (EPIPE). Closing socket " + std::to_string(socket_fd));
            std::cerr << "socket is closed , Closing socket : " << socket_fd << std::endl;
            close(socket_fd);
        } else {
            std::cerr << "send failed: " << strerror(errno) << std::endl;
        }
        return false;
    }
    return true;
};

void process_client_data(std::string_view data, int socket_fd, Logger &log) {
    using namespace std::literals;

    std::vector<uint64_t> region_ids;
    std::string row_sql;
    bool ok = false;
    uint64_t router_node = UINT64_MAX;

    // Parse the socket data to SQLs and region IDs
    try {
        ok = regionProcessor.generateRegionIDs(std::string(data), region_ids, bmsqlMetadata, row_sql);
    } catch (const std::exception &e) {
        log.error(
            "Generate region IDs failed: " + std::string(e.what()) + " send ERR to client " + " Row SQL: " + row_sql);
    }
    if (!ok) {
        // Hard failure
        safe_send("Generate region IDs failed\n", socket_fd, log);
        return;
    }

    // ---- Successful generation ----
    if (SYSTEM_MODE == 0) {
        // random based algorithm
        // Randomly select a router node
        std::random_device rd; // 获取随机种子
        std::mt19937 gen(rd()); // 使用Mersenne Twister引擎
        std::uniform_int_distribution<> distrib(0, ComputeNodeCount - 1); // 生成随机数
        router_node = distrib(gen); // 生成随机数
    } else if (SYSTEM_MODE == 1) {
        // affinity based algorithm
        if (!region_ids.empty()) {
            log.info(
                "Socket " + std::to_string(socket_fd) + ": generated " + std::to_string(region_ids.size()) +
                " region IDs");
            router_node = metis.build_internal_graph(region_ids);
        }
    }

    if (!row_sql.empty() && router_node < 100) {
        // Send SQL to the router node
        const auto &con = DBConnection[router_node % ComputeNodeCount];
        log.info("Sending SQL to router node " + std::to_string(router_node) + ": " + con);
        send_sql_to_router(row_sql, con, log);
    } else {
        if (row_sql.empty()) {
            log.error(" No SQL to send , router node is " + std::to_string(router_node));
            safe_send("No SQL to send\n", socket_fd, log);
        } else {
            log.error("Invalid router node, router node is " + std::to_string(router_node));
            safe_send("Invalid router node, router node is " + std::to_string(router_node) + "\n", socket_fd, log);
        }

        return;
    }

    // ---- Final client acknowledgment ----
    if (safe_send("OK\n", socket_fd, log)) {
        if (++send_times % 1000 == 0) {
            std::cout << "send OK times: " << send_times.load() << std::endl;
        }
    }
}


// --- Main Function ---
int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);

    if (argc != 2) {
        std::cerr << "./run <system_name>. E.g., ./run affinity" << std::endl;
        return 0;
    }
    std::string system_name = argv[1];
    int system_value = 0;
    if (system_name.find("random") != std::string::npos) {
        system_value = 0;
    } else if (system_name.find("affinity") != std::string::npos) {
        system_value = 1;
    } else {
        std::cerr << "Invalid system name." << std::endl;
        return 0;
    }
    SYSTEM_MODE = system_value;

    logger.info("Server is starting...");
    logger.info("SYSTEM_MODE: " + std::to_string(SYSTEM_MODE));
    logger.info("WORKLOAD_MODE: " + std::to_string(WORKLOAD_MODE));

    // --- Load Database Connection Info ---
    logger.info("Loading database connection info...");
    std::string compute_node_config_path = "../../config/compute_node_config.json";
    auto compute_node_config = JsonConfig::load_file(compute_node_config_path);
    auto compute_node_list = compute_node_config.get("remote_compute_nodes");
    auto compute_node_count = (int) compute_node_list.get("remote_compute_node_count").get_int64();
    metis.init_node_nums(compute_node_count);
    ComputeNodeCount = compute_node_count;
    logger.info("ComputeNodeCount: " + std::to_string(ComputeNodeCount));
    for (int i = 0; i < compute_node_count; i++) {
        auto ip = compute_node_list.get("remote_compute_node_ips").get(i).get_str();
        auto port = (int) compute_node_list.get("remote_compute_node_ports").get(i).get_int64();
        auto username = compute_node_list.get("remote_compute_node_usernames").get(i).get_str();
        auto password = compute_node_list.get("remote_compute_node_passwords").get(i).get_str();
        auto dbname = compute_node_list.get("remote_compute_node_dbnames").get(i).get_str();
        DBConnection[i] = "host=" + ip +
                          " port=" + std::to_string(port) +
                          " user=" + username +
                          " password=" + password +
                          " dbname=" + dbname;
        logger.info(
            "Successfully loading connection info, DBConnection[" + std::to_string(i) + "] = " + DBConnection[i]);
    }

    // --- Load Router Config ---
    logger.info("Loading router config info...");
    std::string router_config_path = "../../config/router_config.json";
    auto router_config_file = JsonConfig::load_file(router_config_path);
    auto router_config = router_config_file.get("router");
    PORT = (int) router_config.get("port").get_int64();
    SOCKET_BUFFER_SIZE = (int) router_config.get("socket_buffer_size").get_int64();
    THREAD_POOL_SIZE = (int) router_config.get("thread_pool_size").get_int64();


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
    logger.info("WORKLOAD_MODE is TPCC (1). Initializing TPCC Metadata...");
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
        logger.error("CRITICAL ERROR initializing TPCC Metadata: " + std::string(e.what()));
        return -1;
    }
#else
    logger.log("WORKLOAD_MODE is YCSB (0) or undefined. Skipping TPCH Metadata initialization.");
#endif

    // --- Thread Pool Setup ---
    logger.info("Initializing thread pool with " + std::to_string(THREAD_POOL_SIZE) + " threads.");
    ThreadPool pool(THREAD_POOL_SIZE);
    metis.set_thread_pool(&pool);

    // --- Server Socket Setup ---
    int server_fd;
    struct sockaddr_in address{};
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        logger.error(" Failed to create server socket. Error: " + std::string(strerror(errno)));
        return -1;
    }
    logger.info("Server socket created (FD: " + std::to_string(server_fd) + ").");

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        logger.error(" Failed to set SO_REUSEADDR option. Error: " + std::string(strerror(errno)));
        // Continue if non-critical, but log the error
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0) {
        logger.error("Bind failed on port " + std::to_string(PORT) + ". Error: " + std::string(strerror(errno)));
        close(server_fd);
        return -1;
    }
    logger.info("Server socket bound to port " + std::to_string(PORT) + ".");

    if (listen(server_fd, 10) < 0) {
        // Increased backlog slightly
        logger.error("Listen failed. Error: " + std::string(strerror(errno)));
        close(server_fd);
        return -1;
    }
    logger.info("Server is running and listening on port " + std::to_string(PORT) + "...");


    std::cout << R"(
~~~~~~~~~~~~~~~~~~~~~~~~~~~WELCOME TO THE~~~~~~~~~~~~~~~~~~~~~~~~~~~
  __  __   ____            ____                    _
 |  \/  | |  _ \          |  _ \    ___    _   _  | |_    ___   _ __
 | |\/| | | |_) |  _____  | |_) |  / _ \  | | | | | __|  / _ \ | '__|
 | |  | | |  __/  |_____| |  _ <  | (_) | | |_| | | |_  |  __/ | |
 |_|  |_| |_|             |_| \_\  \___/   \__,_|  \__|  \___| |_|
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
)" << std::endl;
    // --- Accept Client Connections Loop ---
    while (true) {
        int new_socket;
        struct sockaddr_in client_address{}; // Use a separate struct for client addr
        socklen_t client_addrlen = sizeof(client_address); // Use socklen_t

        if ((new_socket = accept(server_fd, (struct sockaddr *) &client_address, &client_addrlen)) < 0) {
            logger.error("Failed to accept connection. Error: " + std::string(strerror(errno)));
            // Consider adding a small sleep or other logic if accept fails repeatedly
            continue; // Continue to the next iteration to try accepting again
        }

        // Get client address info safely
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_address.sin_port);

        logger.info(
            "New client connected, IP: " + std::string(client_ip) + ", Port: " + std::to_string(client_port) +
            ", Socket FD: " + std::to_string(new_socket));

        std::thread client_handler_thread([new_socket, &pool, client_ip, client_port]() {
            logger.info("Handler thread started for client " + std::string(client_ip) + ":" +
                        std::to_string(client_port) + " (Socket: " + std::to_string(new_socket) + ")");

            std::string buffer;
            buffer.reserve(SOCKET_BUFFER_SIZE);

            char tmp[SOCKET_BUFFER_SIZE];

           while (true) {
               ssize_t valread = read(new_socket, tmp, SOCKET_BUFFER_SIZE);
               if (valread < 0) {
                   logger.error("Read error on socket " + std::to_string(new_socket) + ": " + strerror(errno));
                   break;
               } else if (valread == 0) {
                   logger.info("Client disconnected: " + std::string(client_ip) + ":" + std::to_string(client_port));
                   break;
               }

               std::string_view chunk(tmp, valread);

               if (chunk == "HELLO\n" || chunk == "BYE\n") {
                   safe_send("FINISH\n", new_socket, logger);
                   continue;
               }

               // 🔹 拼接并解析完整事务
               buffer.append(chunk);
               if (buffer.starts_with("***Header_Start***") && buffer.ends_with("***Txn_End***")) {
                   pool.enqueue(process_client_data, buffer, new_socket, std::ref(logger));
               }else {
                   while (true) {
                       size_t h_start = buffer.find("***Header_Start***");
                       size_t t_end = buffer.find("***Txn_End***");

                       if (h_start != std::string::npos && t_end != std::string::npos && h_start < t_end) {
                           size_t txn_end_pos = t_end + strlen("***Txn_End***");
                           std::string txn = buffer.substr(h_start, txn_end_pos - h_start);
                           pool.enqueue(process_client_data, std::move(txn), new_socket, std::ref(logger));
                           buffer.erase(0, txn_end_pos);
                       } else {
                           break;
                       }
                   }
               }
           }

            close(new_socket);
            logger.info("Closed socket " + std::to_string(new_socket) + " for client " + std::string(client_ip) + ":" +
                        std::to_string(client_port));
        });

        client_handler_thread.detach(); // Detach the thread to run independently
    } // End of accept loop

    // --- Server Shutdown (Code may not be reached in simple loop) ---
    logger.info("Server shutting down...");
    close(server_fd); // Close the listening server socket


    return 0;
}

