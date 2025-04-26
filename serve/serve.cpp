#include <iostream>
#include <string>
#include <cstring>      // For memset, strerror
#include <unistd.h>     // For close, read
#include <arpa/inet.h>  // For socket-related functions, inet_ntop
#include <sys/socket.h> // For socket API

#include "util/json_config.h"
#include "threadpool.h" // Include your thread pool header
#include "queryplan_cardinality.h" // Assuming this is needed
#include "db_meta.h"
#include "metis_partitioner.h" // Assuming this is needed, replace NewMetis if necessary
#include "region/region_generator.h"
#include "log/Logger.h"     // Include the new Logger header

#define PORT 8500       // Port number to listen on
#define BUFFER_SIZE 4096000 // Buffer size for receiving data

// --- Global Variables ---
Logger logger("server.log");
NewMetis metis; // Assuming NewMetis is the correct type
TPCHMeta *TPCH_META = nullptr; // Initialize global pointer
std::atomic<long long> send_times = 0; // Use long long for potentially large counts

// Instantiate RegionProcessor within the scope where needed
RegionProcessor regionProcessor(logger); // Pass the logger

// --- Client Data Processing Function ---
// This function is intended to be run by the thread pool threads.
void process_client_data(const std::string &data, int socket_fd, Logger &logger_ref) {
    // Log processing start using the passed logger reference
    std::string truncated_data = data.substr(0, 150) + (data.length() > 150 ? "..." : "");
    logger_ref.log("Socket ", socket_fd, " processing: ", truncated_data);

    // Vector to hold the final combined IDs
    std::vector<uint64_t> combined_region_ids;

    bool processing_successful = false;
    try {
        // ---- MODIFICATION START ----

        // Call the generateRegionIDs method of the RegionProcessor instance
        processing_successful = regionProcessor.generateRegionIDs(
            data,
            combined_region_ids
        );

        // If region IDs were generated successfully and the vector is not empty,
        // update the Metis graph.
        if (processing_successful && !combined_region_ids.empty()) {
            logger_ref.log("Socket ", socket_fd, ": Successfully generated ", combined_region_ids.size(),
                           " region IDs. Adding to Metis graph.");
            metis.build_internal_graph(combined_region_ids); // Add the IDs to the graph
        } else if (processing_successful) {
            logger_ref.log("Socket ", socket_fd,
                           ": Region ID generation successful, but no IDs were produced (e.g., non-partition key query).");
            // Still considered a success in terms of processing the request
        }
        // If processing_successful is false, the error is logged within generateRegionIDs
        // ---- MODIFICATION END ----
    } catch (const std::exception &e) {
        // Catch potential exceptions from RegionProcessor constructor or methods
        logger_ref.log("CRITICAL ERROR during RegionProcessor usage for socket ", socket_fd, ": ", e.what());
        processing_successful = false; // Ensure failure state
    }


    // Send response back to the client based on processing result
    if (!processing_successful) {
        logger_ref.log("ERROR: Failed to process data for socket ", socket_fd, ". Sending ERR response.");
        // Consider logging more details about why processing failed if available
        send(socket_fd, "ERR", 3, 0); // Send error response
        return; // Exit if processing failed critically
    }

    // Processing was successful (or non-critically failed), send OK
    ssize_t bytes_sent = send(socket_fd, "OK", 2, 0);
    long long current_send_times = send_times.fetch_add(1) + 1; // Atomically increment and get new value

    // Log send status periodically
    if (current_send_times % 5000 == 0) {
        logger_ref.set_log_to_console(true);
        logger_ref.log("Sent response to socket ", socket_fd, " (", current_send_times, " times).");
        logger_ref.set_log_to_console(false);
    }

    // Log send errors
    if (bytes_sent < 0) {
        logger_ref.log("ERROR: Failed to send OK response to socket ", socket_fd, ". Error: ", strerror(errno));
    } else {
        // Optional: Log successful send details
        // logger_ref.log("OK Response sent (", bytes_sent, " bytes) to socket ", socket_fd);
    }
}

// --- Main Function ---
int main() {
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
        TPCH_META = new TPCHMeta();
        // 从文件中初始化partition column_ids (Initialize partition column_ids from file)
        std::string fname = "./partition_column_ids.txt"; // Adjust path if necessary
        logger.log("Reading partition column IDs from: ", fname);
        TPCH_META->ReadColumnIDFromFile(fname); // Assuming this method exists and handles errors/logging
        logger.log("TPCH Metadata initialized successfully.");
    } catch (const std::exception &e) {
        logger.log("CRITICAL ERROR initializing TPCH Metadata: ", e.what());
        delete TPCH_META; // Clean up allocated memory
        TPCH_META = nullptr;
        return -1;
    }
#else
    logger.log("WORKLOAD_MODE is YCSB (0) or undefined. Skipping TPCH Metadata initialization.");
#endif

    // --- Thread Pool Setup ---
    unsigned int num_threads = 12; // Consider making this configurable
    logger.log("Initializing thread pool with ", num_threads, " threads.");
    ThreadPool pool(num_threads);
    metis.set_thread_pool(&pool); // Set thread pool for metis AFTER pool is created

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
            char buffer[BUFFER_SIZE]; // Each client handler thread has its own buffer

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
            logger.log("Closed socket ", new_socket, " for client ", client_ip, ":", client_port);
        }); // End of lambda for client handler thread

        client_handler_thread.detach(); // Detach the thread to run independently
    } // End of accept loop

    // --- Server Shutdown (Code may not be reached in simple loop) ---
    logger.log("Server shutting down...");
    close(server_fd); // Close the listening server socket

#if WORKLOAD_MODE == 1
    delete TPCH_META; // Clean up TPCH meta if allocated
    TPCH_META = nullptr;
#endif

    return 0;
}

