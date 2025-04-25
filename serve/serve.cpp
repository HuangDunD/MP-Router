#include <iostream>
#include <string>
#include <cstring>      // For memset
#include <unistd.h>     // For close
#include <arpa/inet.h>  // For socket-related functions
#include <sys/socket.h> // For socket API

#include "util/json_config.h"
#include "threadpool.h" // Include your thread pool header

#include "parse.h"
#include "queryplan_cardinality.h"
#include "db_meta.h"
#include "region.h"
#include "metis_partitioner.h"

#define PORT 8500       // Port number to listen on
#define BUFFER_SIZE 4096000 // Buffer size for receiving data

NewMetis metis;
TPCHMeta *TPCH_META;
std::atomic<int> send_times = 0;

// Function to handle processing in a separate thread
std::ofstream log_stream("server_log.txt", std::ios::app);

void process_client_data(const std::string &data, int socket_fd) {
    std::thread::id this_id = std::this_thread::get_id();
    // Use a shorter representation for potentially long SQL in logs
    std::string truncated_data = data.substr(0, 150) + (data.length() > 150 ? "..." : "");
    log_message("Socket " + std::to_string(socket_fd) + " processing: " + truncated_data, log_stream);

    // Vector to hold the final combined IDs to pass to metis
    std::vector<uint64_t> combined_region_ids;

#if WORKLOAD_MODE == 0
    // YCSB workload
    // We need the raw keys first to calculate inner_region_id.
    // Extract YCSB keys directly here instead of relying on ParseYcsbKey which calculated region_id.
    std::vector<int> ycsb_keys;
    try {
        std::regex key_pattern(R"(YCSB_KEY\s*=\s*(\d+))"); // Matches YCSB_KEY = <number>
        std::smatch matches;
        std::string::const_iterator search_start(data.cbegin());
        while (std::regex_search(search_start, data.cend(), matches, key_pattern)) {
            ycsb_keys.push_back(std::stoi(matches[1].str()));
            search_start = matches.suffix().first;
        }
        // Optional: Remove duplicates if needed
        std::sort(ycsb_keys.begin(), ycsb_keys.end());
        ycsb_keys.erase(std::unique(ycsb_keys.begin(), ycsb_keys.end()), ycsb_keys.end());

    } catch (const std::exception& e) {
        log_message("[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) + "] Error parsing YCSB keys: " + e.what(), log_stream);
        goto send_response; // Skip graph build on parsing error
    }

    // Assume a default table ID for YCSB keys if not specified otherwise
    const unsigned int ycsb_table_id = 0; // Example: table 0 for YCSB

    for (const auto &key: ycsb_keys) {
         if (key < 0) {
              log_message("Warning: Skipping negative YCSB key " + std::to_string(key), log_stream);
              continue;
         }
         if (REGION_SIZE <= 0) {
              log_message("Error: REGION_SIZE (" + std::to_string(REGION_SIZE) + ") is not positive. Cannot calculate region ID.", log_stream);
              // Skip further processing for this workload if REGION_SIZE is invalid
              combined_region_ids.clear(); // Ensure we don't pass partial data
              break;
         }
         // Calculate inner region ID (ensure unsigned)
         unsigned int _inner_region_id = static_cast<unsigned int>(key / REGION_SIZE);

         // Create Region object
         Region current_region(ycsb_table_id, _inner_region_id);

         // Serialize to uint64_t
         uint64_t combined_id = current_region.serializeToUint64();
         combined_region_ids.push_back(combined_id);
    }

    if (!combined_region_ids.empty()) {
        // Log the combined IDs being sent
        std::stringstream ss_ids;
        ss_ids << "[Thread " << std::to_string(std::hash<std::thread::id>{}(this_id)) << "] YCSB Combined Region IDs (" << combined_region_ids.size() << "): ";
        for(size_t i=0; i< std::min(combined_region_ids.size(), (size_t)5); ++i) ss_ids << combined_region_ids[i] << " "; // Log first few
        if(combined_region_ids.size() > 5) ss_ids << "...";
        log_message(ss_ids.str(), log_stream);

        // Call graph building function
        metis.build_internal_graph(combined_region_ids);
    } else {
         log_message("[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) + "] No valid YCSB keys found or REGION_SIZE invalid.", log_stream);
    }


#elif WORKLOAD_MODE == 1
    // TPCH workload
    try {
        SQLInfo sql_info = parseTPCHSQL(data); // Assume parseTPCHSQL is available

        if (sql_info.type == SQLType::SELECT || sql_info.type == SQLType::UPDATE) {
            std::string type_str = (sql_info.type == SQLType::SELECT) ? "SELECT" : "UPDATE";
            // log_message("[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) + "] Received " + type_str + " statement.", log_stream);

            if (sql_info.tableNames.empty() || sql_info.columnNames.empty()) {
                log_message("Warning: Missing table or column name in " + type_str + ". Skipping graph build.",
                            log_stream);
            } else if (sql_info.keyVector.empty()) {
                // No keys provided in the WHERE clause (e.g., SELECT * FROM table;)
                // log_message("[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) + "] No keys found in " + type_str + " WHERE clause. Skipping graph build.", log_stream);
            } else {
                std::string table_name = sql_info.tableNames[0];
                std::string column_name = sql_info.columnNames[0];

                unsigned int table_id;
                unsigned int column_id;
                // Safely get table and column IDs
                try {
                    table_id = TPCHMeta::tablenameToID.at(table_name); // Use .at() for bounds checking
                    // Ensure table_id is valid index before accessing nested maps/vectors
                    if (table_id >= TPCHMeta::columnNameToID.size() || table_id >= TPCH_META->partition_column_ids.
                        size()) {
                        throw std::out_of_range(
                            "Table ID (" + std::to_string(table_id) + ") out of range for metadata access.");
                    }
                    column_id = TPCHMeta::columnNameToID[table_id].at(column_name); // Use .at()
                } catch (const std::out_of_range &oor) {
                    log_message(
                        "Error: Invalid table/column name ('" + table_name + "'/'" + column_name +
                        "') or ID lookup failed. " + oor.what(), log_stream);
                    goto send_response; // Skip processing if names/IDs are invalid
                }

                // Check if the queried column is the partition key for that table
                if (TPCH_META->partition_column_ids[table_id] != column_id) {
                    log_message(
                        "[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) + "] Query key (" +
                        table_name + "." + column_name + ") is not the partition key for table " +
                        std::to_string(table_id) + ". Skipping graph build.", log_stream);
                } else {
                    // Partition key matches, proceed to calculate combined IDs
                    log_message(
                        "[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) +
                        "] Query key matches partition key (" + table_name + "." + column_name +
                        "). Processing keys...", log_stream);

                    for (const auto &key: sql_info.keyVector) {
                        if (key < 0) {
                            log_message(
                                "Warning: Skipping negative key " + std::to_string(key) + " for table " + table_name,
                                log_stream);
                            continue; // Skip negative keys
                        }
                        if (REGION_SIZE <= 0) {
                            log_message(
                                "Error: REGION_SIZE (" + std::to_string(REGION_SIZE) +
                                ") is not positive. Cannot calculate region ID.", log_stream);
                            combined_region_ids.clear(); // Invalidate calculation for this query
                            break; // Stop processing keys for this query
                        }

                        // Calculate inner region ID (ensure unsigned)
                        auto _inner_region_id = static_cast<unsigned int>(key / REGION_SIZE);

                        // Create Region object using the looked-up table_id
                        Region current_region(table_id, _inner_region_id);

                        // Serialize to uint64_t
                        uint64_t combined_id = current_region.serializeToUint64();
                        combined_region_ids.push_back(combined_id);
                    } // End key loop


                    if (!combined_region_ids.empty()) {
                        // Log the combined IDs being sent
                        std::stringstream ss_ids;
                        ss_ids << "[Thread " << std::to_string(std::hash<std::thread::id>{}(this_id)) <<
                                "] TPCH Combined Region IDs (" << combined_region_ids.size() << "): ";
                        for (size_t i = 0; i < std::min(combined_region_ids.size(), (size_t) 5); ++i)
                            ss_ids << combined_region_ids[i] << " "; // Log first few
                        if (combined_region_ids.size() > 5) ss_ids << "...";
                        log_message(ss_ids.str(), log_stream);

                        // Call graph building function with the vector of combined IDs
                        metis.build_internal_graph(combined_region_ids);
                    } else {
                        log_message(
                            "[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) +
                            "] No valid keys found matching partition key or REGION_SIZE invalid.", log_stream);
                    }
                } // End partition key match check
            } // End check for table/column names and keys present
        } else if (sql_info.type == SQLType::JOIN) {
            log_message(
                "[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) +
                "] Received JOIN statement. Graph building not implemented for JOINs.", log_stream);
            // Existing JOIN logic (e.g., cardinality update) can remain here if needed
        } else {
            log_message(
                "[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) +
                "] Unknown or unhandled SQL type received. SQL: " + truncated_data, log_stream);
        }
    } catch (const std::exception &e) {
        // Catch errors during parsing or metadata lookup
        log_message(
            "[Thread " + std::to_string(std::hash<std::thread::id>{}(this_id)) + "] Error processing SQL: " + e.what(),
            log_stream);
        // Depending on severity, you might want to stop or just log and continue
    }

#endif // End WORKLOAD_MODE check

send_response: // Label to jump to for sending response, especially after errors
    // Send response back to the client
    ssize_t bytes_sent = send(socket_fd, "OK", 2, 0);
    int current_send_count = send_times.fetch_add(1) + 1; // Get updated count before logging

    if (send_times % 5000 == 0) {
        std::cout << "[Thread " << this_id << "] Sent response to socket " << socket_fd
                << " (" << send_times << " times)." << std::endl;
    }
    if (bytes_sent < 0) {
        std::cerr << "[Thread " << this_id << "] Failed to send response to socket " << socket_fd << std::endl;
    } else {
        //std::cout << "[Thread " << this_id << "] Response sent (" << bytes_sent << " bytes)." << std::endl;
    }
}


int main() {
    std::cout << "Server starting..." << std::endl;

    std::string config_filepath = "../../config/ycsb_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("ycsb");
    REGION_SIZE = conf.get("key_cnt_per_partition").get_int64();

    // load db meta
#if WORKLOAD_MODE == 1
    // TPCH workload
    TPCH_META = new TPCHMeta();
    // 从文件中初始化partition column_ids
    std::string fname = "./partition_column_ids.txt";
    TPCH_META->ReadColumnIDFromFile(fname);
#endif

    // Determine number of threads (e.g., based on hardware)
    unsigned int num_threads = 12; // Leave one thread for the main thread
    if (num_threads <= 0) {
        num_threads = 4; // Default to 4 if hardware_concurrency is not available
    }
    std::cout << "Initializing thread pool with " << num_threads << " threads." << std::endl;
    ThreadPool pool(num_threads); // Create the thread pool

    int server_fd, new_socket;
    struct sockaddr_in address{};
    int addrlen = sizeof(address);
    // char buffer[BUFFER_SIZE] = {0}; // Buffer will be created inside the loop now

    // --- Server Socket Setup (socket, setsockopt, bind, listen) ---
    // (Your existing code for this part is fine)
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return -1;
    }
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "Failed to set socket options" << std::endl;
        close(server_fd); // Close socket on failure
        return -1;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        close(server_fd); // Close socket on failure
        return -1;
    }
    if (listen(server_fd, 5) < 0) {
        // Increased backlog queue slightly
        std::cerr << "Listen failed" << std::endl;
        close(server_fd); // Close socket on failure
        return -1;
    }
    // --- End Server Socket Setup ---

    std::cout << "Server is running and listening on port " << PORT << "..." << std::endl;

    // Accept client connections and process data
    while (true) {
        if ((new_socket = accept(server_fd, (struct sockaddr *) &address, (socklen_t *) &addrlen)) < 0) {
            // Handle accept error (e.g., log it), but maybe continue running
            std::cerr << "Failed to accept connection. Error: " << strerror(errno) << std::endl;
            // Consider adding a small sleep or other logic if accept fails repeatedly
            continue; // Continue to the next iteration to try accepting again
        }

        // Get client address info safely
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &address.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(address.sin_port);

        std::cout << "New client connected, IP: " << client_ip
                << ", Port: " << client_port << ", Socket FD: " << new_socket << std::endl;

        // Create a separate thread to handle this client's communication.
        // This prevents the main accept loop from blocking on reads for one client.
        std::thread client_handler_thread([new_socket, &pool, client_ip, client_port]() {
            // Pass pool by reference
            std::cout << "Handler thread started for client " << client_ip << ":" << client_port << " (Socket: " <<
                    new_socket << ")" << std::endl;
            char buffer[BUFFER_SIZE]; // Each client handler thread has its own buffer

            metis.set_thread_pool(&pool);
            // Read incoming data from this specific client
            while (true) {
                memset(buffer, 0, BUFFER_SIZE); // Clear the buffer
                int valread = read(new_socket, buffer, BUFFER_SIZE - 1); // Leave space for null terminator

                if (valread < 0) {
                    // Error occurred
                    std::cerr << "Error reading from socket " << new_socket
                            << " (Client: " << client_ip << ":" << client_port
                            << "). Error: " << strerror(errno) << std::endl;
                    break; // Exit the read loop for this client
                } else if (valread == 0) {
                    // Client disconnected gracefully
                    std::cout << "Client " << client_ip << ":" << client_port
                            << " (Socket: " << new_socket << ") disconnected." << std::endl;
                    break; // Exit the read loop for this client
                }

                // Process received data using the thread pool
                std::string received_data(buffer, valread); // Use valread for accurate length

                pool.enqueue(process_client_data, received_data, new_socket);
            }

            // Close the client socket when the read loop finishes (error or disconnect)
            close(new_socket);
            std::cout << "Closed socket " << new_socket << " for client " << client_ip << ":" << client_port <<
                    std::endl;
        });

        client_handler_thread.detach();
    }

    std::cout << "Server shutting down..." << std::endl;
    close(server_fd); // Close the listening server socket
    return 0;
}
