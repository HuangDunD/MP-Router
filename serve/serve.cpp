#include <iostream>
#include <string>
#include <cstring>      // For memset, strerror
#include <unistd.h>     // For close, read
#include <arpa/inet.h>  // For socket-related functions, inet_ntop
#include <sys/socket.h> // For socket API
#include <csignal>

#include "util/json_config.h"
#include "threadpool.h" // Include your thread pool header
#include "queryplan_cardinality.h" // Assuming this is needed
// #include "db_meta.h"
#include "bmsql_meta.h" // Include the header file
#include "metis_partitioner.h" // Assuming this is needed, replace NewMetis if necessary
#include "region/region_generator.h"
#include "log/Logger.h"     // Include the new Logger header

#define PORT 8500
#define THREAD_POOL_SIZE 16
#define BUFFER_SIZE 8129000 // max 8192kb=8MB 4096000=4MB


// --- Global Variables ---
Logger logger("server.log");
NewMetis metis; // Assuming NewMetis is the correct type
TPCHMeta *TPCH_META = nullptr; // Initialize global pointer
std::atomic<long long> send_times = 0; // Use long long for potentially large counts

// Instantiate RegionProcessor within the scope where needed
RegionProcessor regionProcessor(logger); // Pass the logger
BmSql::Meta bmsqlMetadata; // Create an instance


struct RouterEndpoint {
    const char *ip;
    uint16_t port;
};

static const std::array<RouterEndpoint, 8> kRouterTable{
    {
        /* idx:0 */ {"127.0.0.1", 9100},
        /* idx:1 */ {"127.0.0.1", 9101},
        /* idx:2 */ {"127.0.0.1", 9102},
        /* idx:3 */ {"127.0.0.1", 9103},
        /* idx:4 */ {"127.0.0.1", 9104},
        /* idx:5 */ {"127.0.0.1", 9105},
        /* idx:6 */ {"127.0.0.1", 9106},
        /* idx:7 */ {"127.0.0.1", 9107}
    }
};

// -----------------------------------------------------------------------------
// 2. 简单的发送辅助函数
// -----------------------------------------------------------------------------
static bool send_sql_to_router(const std::string &sql,
                               const RouterEndpoint &ep,
                               Logger &lg) {
    int rsock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (rsock < 0) {
        lg.log("ERROR: create router socket failed: ", strerror(errno));
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ep.port);
    if (inet_pton(AF_INET, ep.ip, &addr.sin_addr) != 1) {
        lg.log("ERROR: invalid router IP ", ep.ip);
        ::close(rsock);
        return false;
    }

    if (::connect(rsock, reinterpret_cast<sockaddr *>(&addr),
                  sizeof(addr)) < 0) {
        lg.log("ERROR: connect router ", ep.ip, ":", ep.port,
               " failed: ", strerror(errno));
        ::close(rsock);
        return false;
    }

    std::string payload = sql + "\n";
    ssize_t n = ::send(rsock, payload.data(), payload.size(), 0);
    if (n < 0) {
        lg.log("ERROR: send sql to router ", ep.ip, ":", ep.port,
               " failed: ", strerror(errno));
    } else if (static_cast<size_t>(n) != payload.size()) {
        lg.log("WARN : partial send to router ", ep.ip, ":", ep.port,
               " (", n, "/", payload.size(), " bytes)");
    }
    ::close(rsock);
    return n == static_cast<ssize_t>(payload.size());
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
    if (!region_ids.empty()) {
        log.log("Socket ", socket_fd, ": generated ", region_ids.size(), " region IDs");
        uint64_t router_node = metis.build_internal_graph(region_ids);

        if (!row_sql.empty() && router_node != UINT64_MAX) {
            const auto &ep = kRouterTable[router_node % kRouterTable.size()];
            log.log("Routing row_sql to ", ep.ip, ":", ep.port);
            //send_sql_to_router(row_sql, ep, log);
        }
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

    // --- Configuration Loading ---
    try {
        std::string config_filepath = "../../config/ycsb_config.json"; // Adjust path if necessary
        logger.log("Loading configuration from: ", config_filepath);
        auto json_config = JsonConfig::load_file(config_filepath);
        auto conf = json_config.get("ycsb"); // Or appropriate config section
        REGION_SIZE = conf.get("key_cnt_per_partition").get_int64();
        logger.log("REGION_SIZE set to: ", REGION_SIZE);
        if (REGION_SIZE <= 0) {
            logger.log("CRITICAL ERROR: REGION_SIZE loaded from config is not positive. Value: ", REGION_SIZE);
            return -1; // Cannot proceed with invalid REGION_SIZE
        }
    } catch (const std::exception &e) {
        logger.log("CRITICAL ERROR loading configuration: ", e.what());
        return -1;
    }

    // --- Load DB Meta (Conditional) ---
#if WORKLOAD_MODE == 1
    // TPCH workload
    logger.log("WORKLOAD_MODE is TPCH (1). Initializing TPCH Metadata...");
    try {
        std::string metaFilePath = "/home/mt/MP-Router/serve/db_mate.json";
        std::cout << "Loading BMSQL metadata from: " << metaFilePath << std::endl;
        // Load the metadata
        if (!bmsqlMetadata.loadFromJsonFile(metaFilePath)) {
            std::cerr << "Fatal Error: Failed to load BMSQL metadata. Exiting." << std::endl;
            return EXIT_FAILURE;
        }

        std::cout << "Metadata loaded successfully." << std::endl;
        std::cout << "-----------------------------" << std::endl;
    } catch (const std::exception &e) {
        logger.log("CRITICAL ERROR initializing TPCH Metadata: ", e.what());
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

