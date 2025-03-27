#include <iostream>
#include <string>
#include <cstring>       // For memset
#include <unistd.h>      // For close
#include <arpa/inet.h>   // For socket-related functions
#include <sys/socket.h>  // For socket API

#include "./parse.cpp"    // Include parsing functionality

#define PORT 8500        // Port number to listen on
#define BUFFER_SIZE 1024 // Buffer size for receiving data

int main() {
    std::cout << "Server starting..." << std::endl;

    int server_fd, new_socket;              // File descriptors for server and client sockets
    struct sockaddr_in address{};           // Server address structure
    int addrlen = sizeof(address);          // Length of address structure
    char buffer[BUFFER_SIZE] = {0};         // Buffer for incoming data

    // Create socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return -1;
    }

    // Set socket options to allow port reuse
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "Failed to set socket options" << std::endl;
        return -1;
    }

    // Configure server address
    address.sin_family = AF_INET;           // Use IPv4
    address.sin_addr.s_addr = INADDR_ANY;   // Listen on all interfaces
    address.sin_port = htons(PORT);         // Convert port to network byte order

    // Bind socket to the specified port
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        return -1;
    }

    // Start listening for incoming connections (max queue length: 3)
    if (listen(server_fd, 3) < 0) {
        std::cerr << "Listen failed" << std::endl;
        return -1;
    }

    std::cout << "Server is running and listening on port " << PORT << "..." << std::endl;

    // Accept client connections and process data
    while (true) {
        if ((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0) {
            std::cerr << "Failed to accept connection" << std::endl;
            return -1;
        }

        std::cout << "New client connected, IP: " << inet_ntoa(address.sin_addr)
                  << ", Port: " << ntohs(address.sin_port) << std::endl;

        // Read incoming data
        while (true) {
            memset(buffer, 0, BUFFER_SIZE); // Clear the buffer
            int valread = read(new_socket, buffer, BUFFER_SIZE);
            if (valread <= 0) { // Client disconnected or error occurred
                std::cout << "Client disconnected" << std::endl;
                break;
            }

            // Process received data
            std::string received_data(buffer);
            std::vector<int> region_ids;
            std::cout << "Received data: " << received_data << std::endl;

            // Parse the received data
            ParseYcsbKey(received_data, region_ids);
            std::cout << "Region IDs (every 1000 keys represent a region):" << std::endl;
            for (const auto& id : region_ids) {
                std::cout << id << " ";
            }
            std::cout << std::endl;

            // Send processed data back to the client
            send(new_socket, received_data.c_str(), received_data.length(), 0);
        }

        close(new_socket); // Close the client socket
    }

    close(server_fd); // Close the server socket
    return 0;
}