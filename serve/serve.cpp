#include <iostream>
#include <string>
#include <cstring>      // For memset
#include <unistd.h>     // For close
#include <arpa/inet.h>  // For socket-related functions
#include <sys/socket.h> // For socket API

#include "util/json_config.h"
#include "threadpool.h" // Include your thread pool header

#include "./parse.cpp"   // Include parsing functionality
#include "metis_partitioner.h"

#define PORT 8500       // Port number to listen on
#define BUFFER_SIZE 1024 // Buffer size for receiving data

NewMetis metis;


// Function to handle processing in a separate thread
void process_client_data(const std::string &data, int socket_fd) {
    std::vector<int> region_ids;

    // Get thread ID for logging (optional)

    std::thread::id this_id = std::this_thread::get_id();


    ParseYcsbKey(data, region_ids); // Assuming ParseYcsbKey is thread-safe or only uses local/stack variables

    // Print Region IDs (Consider adding a mutex for std::cout if output gets interleaved)
    std::cout << "[Thread " << this_id << "] Recived Region IDs:" << std::endl;
    std::string ids_;
    for (const auto &id: region_ids) {
        if (id < 0) {
            // Include the invalid ID in the error message
            throw std::invalid_argument("invalid ID " + std::to_string(id));
        }
        ids_ += std::to_string(id) + " ";
    }
    std::cout << ids_ << std::endl;


    metis.build_internal_graph(region_ids); // Call the graph building function

    std::cout << "[Thread " << this_id << "] Graph built for data." << std::endl;


    ssize_t bytes_sent = send(socket_fd, ids_.c_str(), ids_.length(), 0);
    if (bytes_sent < 0) {
        std::cerr << "[Thread " << this_id << "] Failed to send response to socket " << socket_fd << std::endl;
    } else {
        std::cout << "[Thread " << this_id << "] Response sent (" << bytes_sent << " bytes)." << std::endl;
    }
}


int main() {
    std::cout << "Server starting..." << std::endl;

    std::string config_filepath = "../../config/ycsb_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("ycsb");
    REGION_SIZE = conf.get("key_cnt_per_partition").get_int64();

    // Determine number of threads (e.g., based on hardware)
    unsigned int num_threads = std::thread::hardware_concurrency() - 1; // Leave one thread for the main thread
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
