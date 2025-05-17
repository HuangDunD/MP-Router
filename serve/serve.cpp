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
#include "RingBuffer/ringbuffer.h"
#include "bmsql_meta.h" // Include the header file
#include "region/region_generator.h"
#include "metis_partitioner.h" // Assuming this is needed, replace NewMetis if necessary
// #include "log/Logger.h"     // Include the new Logger header
#include <pqxx/pqxx> // PostgreSQL C++ library

// --- Latency Statistics Variables ---
std::mutex latency_mutex;
std::vector<double> latencies_overall_us;
std::vector<double> latencies_generate_ids_us;
std::vector<double> latencies_build_graph_us;
std::vector<double> latencies_exec_sql_us;
std::vector<double> latencies_tcp_us;
// --- End Latency Statistics Variables ---


int PORT;
int THREAD_POOL_SIZE;
int SOCKET_BUFFER_SIZE;

constexpr std::string_view HEADER_MARKER = "***Header_Start***";
constexpr std::string_view END_MARKER = "***Txn_End***";


// --- Global Variables ---
Logger logger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::INFO, "server.log", 1024);
NewMetis metis; // Assuming NewMetis is the correct type
TPCHMeta *TPCH_META = nullptr; // Initialize global pointer
std::vector<std::string> DBConnection;
std::atomic<long long> send_times = 0; // Use long long for potentially large counts
uint64_t seed = 0xdeadbeef;

// Instantiate RegionProcessor within the scope where needed
RegionProcessor regionProcessor(logger); // Pass the logger
BmSql::Meta bmsqlMetadata; // Create an instance

void print_latency_stats(long long send_count,
                         const std::vector<std::pair<std::string, std::vector<double> &> > &metrics,
                         Logger &lg_ref,
                         bool clear_after_print = false) {
    std::lock_guard<std::mutex> lock(latency_mutex);

    // 构建整张表的输出流
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "==== Latency Report @ " << send_count << " Txns ====" << "\n";

    // 表头
    oss << std::left << std::setw(20) << "Metric"
            << std::setw(10) << "Count"
            << std::setw(10) << "Min"
            << std::setw(10) << "Mean"
            << std::setw(10) << "Max"
            << std::setw(10) << "StdDev"
            << std::setw(10) << "Median"
            << std::setw(10) << "P90"
            << std::setw(10) << "P95"
            << std::setw(10) << "P99"
            << "\n";

    // 分隔线
    oss << std::string(100, '-') << "\n";

    // 逐行计算并添加数据
    for (auto &m: metrics) {
        const auto &name = m.first;
        auto &latencies = m.second;
        size_t count = latencies.size();

        double min_val = 0, mean = 0, max_val = 0, stddev = 0, median = 0, p90 = 0, p95 = 0, p99 = 0;
        if (!latencies.empty()) {
            std::vector<double> sorted = latencies;
            std::sort(sorted.begin(), sorted.end());
            count = sorted.size();
            double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
            mean = sum / count;
            min_val = sorted.front();
            max_val = sorted.back();
            double sq_sum = std::accumulate(sorted.begin(), sorted.end(), 0.0,
                                            [mean](double acc, double v) { return acc + (v - mean) * (v - mean); });
            stddev = std::sqrt(sq_sum / count);
            median = (count % 2 == 0)
                         ? (sorted[count / 2 - 1] + sorted[count / 2]) / 2
                         : sorted[count / 2];
            auto percentile = [&](double p) {
                double idx = p * (count - 1);
                size_t lo = static_cast<size_t>(std::floor(idx));
                size_t hi = static_cast<size_t>(std::ceil(idx));
                return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
            };
            p90 = percentile(0.90);
            p95 = percentile(0.95);
            p99 = percentile(0.99);
        }

        // 输出一行
        oss << std::left << std::setw(20) << name
                << std::setw(10) << count
                << std::setw(10) << min_val
                << std::setw(10) << mean
                << std::setw(10) << max_val
                << std::setw(10) << stddev
                << std::setw(10) << median
                << std::setw(10) << p90
                << std::setw(10) << p95
                << std::setw(10) << p99
                << "\n";

        if (clear_after_print) {
            latencies.clear();
        }
    }

    // 末尾分隔
    oss << std::string(100, '=') << "\n";

    // 输出到日志与控制台
    lg_ref.info(oss.str());
    std::cout << oss.str();
}

// --- End Latency Statistics Helper Function ---

struct RouterEndpoint {
    std::string ip;
    uint16_t port;
    std::string username;
    std::string password;
    std::string dbname;
};

static bool send_sql_to_router(const std::string &sqls,
                               node_id_t router_node,
                               Logger &lg) {
    pqxx::connection *conn = connections_thread_local[router_node];
    if (conn == nullptr || !conn->is_open()) {
        lg.error("Failed to connect to the database. conninfo" + DBConnection[router_node]);
        return false;
    }
    try {
        pqxx::work txn(*conn);
        pqxx::result result = txn.exec(sqls + "\n");
        txn.commit();
    } catch (const std::exception &e) {
        lg.error("Error while connecting to KingBase or getting query plan: " + std::string(e.what()));
        return false;
    }
    return true;
}

// Local helper to send data and report errors
const bool safe_send(std::string_view msg, int socket_fd) {
    ssize_t n = send(socket_fd, msg.data(), msg.size(), MSG_NOSIGNAL); // 或无 flag
    if (n < 0) {
        if (errno == EPIPE) {
            std::cerr << "socket is closed , Closing socket : " << socket_fd << std::endl;
            close(socket_fd);
        } else {
            std::cerr << "send failed: " << strerror(errno) << std::endl;
        }
        return false;
    }
    return true;
};

void process_client_data(std::string_view data, int socket_fd, Logger &log_ref) {
    // Renamed log to log_ref to avoid conflict
    using namespace std::literals;
    auto pcd_start_time = std::chrono::high_resolution_clock::now(); // Timer for overall function

    std::vector<uint64_t> region_ids;
    std::string row_sql;
    bool ok = false;
    uint64_t router_node = UINT64_MAX;

    // Parse the socket data to SQLs and region IDs
    std::chrono::time_point<std::chrono::high_resolution_clock> gen_ids_start_time, gen_ids_end_time;
    double gen_ids_duration_us = 0;

    try {
        gen_ids_start_time = std::chrono::high_resolution_clock::now();
        ok = regionProcessor.generateRegionIDs(std::string(data), region_ids, bmsqlMetadata, row_sql);
        gen_ids_end_time = std::chrono::high_resolution_clock::now();
        gen_ids_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
            gen_ids_end_time - gen_ids_start_time).count();
    } catch (const std::exception &e) {
        gen_ids_end_time = std::chrono::high_resolution_clock::now(); // Also record time on exception
        gen_ids_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
            gen_ids_end_time - gen_ids_start_time).count();
        log_ref.error(
            "Generate region IDs failed: " + std::string(e.what()) + " send ERR to client " + " Row SQL: " + row_sql);
    }
    // Store generateRegionIDs latency
    {
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies_generate_ids_us.push_back(gen_ids_duration_us);
    }

    if (!ok) {
        // Hard failure
        safe_send("Generate region IDs failed\n", socket_fd);
        // Overall time measurement before early return
        auto pcd_end_time = std::chrono::high_resolution_clock::now();
        double pcd_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(pcd_end_time - pcd_start_time).
                count(); {
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies_overall_us.push_back(pcd_duration_us);
        }
        return;
    }

    // ---- Successful generation ----
    std::chrono::time_point<std::chrono::high_resolution_clock> build_graph_start_time, build_graph_end_time;
    double build_graph_duration_us = 0;
    bool build_graph_called = false;

    if (SYSTEM_MODE == 0) {
        // random based algorithm
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(0, ComputeNodeCount - 1);
        router_node = distrib(gen);
    } else if (SYSTEM_MODE == 1) {
        // affinity based algorithm
        if (!region_ids.empty()) {
            build_graph_start_time = std::chrono::high_resolution_clock::now();
            router_node = metis.build_internal_graph(region_ids);
            build_graph_end_time = std::chrono::high_resolution_clock::now();
            build_graph_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                build_graph_end_time - build_graph_start_time).count();
            build_graph_called = true;
        }
    } else if (SYSTEM_MODE == 2) {
        // single node
        router_node = 0;
    } else {
        log_ref.error("Invalid SYSTEM_MODE: " + std::to_string(SYSTEM_MODE));
        safe_send("Invalid SYSTEM_MODE\n", socket_fd);
        // Overall time measurement before early return
        auto pcd_end_time = std::chrono::high_resolution_clock::now();
        double pcd_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(pcd_end_time - pcd_start_time).
                count(); {
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies_overall_us.push_back(pcd_duration_us);
        }
        return;
    }

    // Store metis.build_internal_graph latency if called
    if (build_graph_called) {
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies_build_graph_us.push_back(build_graph_duration_us);
    }

    if (!row_sql.empty() && router_node < 5) {
        auto sql_start_time = std::chrono::high_resolution_clock::now();
        // Send SQL to the router node
        send_sql_to_router(row_sql, router_node % ComputeNodeCount, log_ref);
        auto sql_end_time = std::chrono::high_resolution_clock::now();
        double sql_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
            sql_end_time - sql_start_time).count();
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies_exec_sql_us.push_back(sql_duration_us);
    } else {
        if (row_sql.empty()) {
            log_ref.error(" No SQL to send , router node is " + std::to_string(router_node));
            safe_send("No SQL to send\n", socket_fd);
        } else {
            log_ref.error("Invalid router node, router node is " + std::to_string(router_node));
            safe_send("Invalid router node, router node is " + std::to_string(router_node) + "\n", socket_fd);
        }
        // Overall time measurement before early return
        auto pcd_end_time = std::chrono::high_resolution_clock::now();
        double pcd_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(pcd_end_time - pcd_start_time).
                count(); {
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies_overall_us.push_back(pcd_duration_us);
        }
        return;
    }

    // ---- Final client acknowledgment ----
    if (safe_send("OK\n", socket_fd)) {
        if (++send_times % 1000 == 0) {
            std::cout << "send OK times: " << send_times.load() << std::endl;
        }
    }

    // Store overall latency at the very end of successful processing
    auto pcd_end_time = std::chrono::high_resolution_clock::now();
    double pcd_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(pcd_end_time - pcd_start_time).
            count(); {
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies_overall_us.push_back(pcd_duration_us);
    }
}


// --- Signal Handler Function ---
void signal_handler(int signum) {
    std::cout << "\nCaught signal " << signum << " (SIGINT)" << std::endl;
    std::cout << "Printing final statistics before exit..." << std::endl;

    print_latency_stats(send_times.load(), {
        {"Overall Txn", latencies_overall_us},
        {"RegionGenerateIDs", latencies_generate_ids_us},
        {"MetisBuildGraph", latencies_build_graph_us},
        {"SQL Execution", latencies_exec_sql_us},
        {"TCP Latency", latencies_tcp_us}
    }, logger, false);

    exit(signum);
}

// --- Main Function ---
int main(int argc, char *argv[]) {
    // Register signal handler for SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler);
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
    } else if (system_name.find("single") != std::string::npos) {
        system_value = 2;
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
        DBConnection.push_back(
            "host=" + ip +
            " port=" + std::to_string(port) +
            " user=" + username +
            " password=" + password +
            " dbname=" + dbname);
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
    AffinitySampleRate = router_config.get("sample_rate").get_double();

    std::cout << "Loaded router config: " << std::endl;
    std::cout << "Port: " << PORT << std::endl;
    std::cout << "Socket Buffer Size: " << SOCKET_BUFFER_SIZE << std::endl;
    std::cout << "Thread Pool Size: " << THREAD_POOL_SIZE << std::endl;
    std::cout << "Affinity Sample Rate: " << AffinitySampleRate << std::endl;
    std::cout << "-----------------------------" << std::endl;

    // --- Load DB Meta (Conditional) ---
#if WORKLOAD_MODE == 0
    // YCSB workload
    logger.log("WORKLOAD_MODE is YCSB (0). Initializing YCSB Metadata...");
    try {
        std::string metaFilePath = "../../config/ycsb_meta.json";
        std::cout << "Loading YCSB metadata from: " << metaFilePath << std::endl;
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
    logger.log("WORKLOAD_MODE is not YCSB (0) or TPCC (1). Skipping specific Metadata initialization.");
#endif

    // --- Thread Pool Setup ---
    logger.info("Initializing thread pool with " + std::to_string(THREAD_POOL_SIZE) + " threads.");
    ThreadPool pool(THREAD_POOL_SIZE, DBConnection, logger); // Pass the global logger
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
        struct sockaddr_in client_address{};
        socklen_t client_addrlen = sizeof(client_address);

        if ((new_socket = accept(server_fd, (struct sockaddr *) &client_address, &client_addrlen)) < 0) {
            logger.error("Failed to accept connection. Error: " + std::string(strerror(errno)));
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_address.sin_port);

        logger.info(
            "New client connected, IP: " + std::string(client_ip) + ", Port: " + std::to_string(client_port) +
            ", Socket FD: " + std::to_string(new_socket));

        std::thread client_handler_thread([new_socket, &pool, client_ip, client_port]() {
            logger.info("Handler thread started for client " + std::string(client_ip) + ":" +
                        std::to_string(client_port) + " (Socket: " + std::to_string(new_socket) + ")");

            const size_t RING_BUFFER_CAP = SOCKET_BUFFER_SIZE * 2; // 足够大

            RingBuffer ring_buffer(RING_BUFFER_CAP);
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

                // 写入环形缓冲区
                if (valread > 0) {
                    if (ring_buffer.size() + valread > RING_BUFFER_CAP) {
                        logger.error("Ring buffer overflow!");
                        break;
                    }
                    ring_buffer.write(tmp, valread);
                }

                // 环形缓冲区内解析所有可处理的包
                while (ring_buffer.size() >= 4) {
                    // 1. 先读4字节包头（可能被环绕了）
                    char header[4];
                    ring_buffer.peek(0, header, 4);
                    uint32_t net_payload_size;
                    std::memcpy(&net_payload_size, header, 4);
                    uint32_t payload_size = ntohl(net_payload_size);

                    if (ring_buffer.size() < 4 + payload_size)
                        break; // 还没收全

                    // 2. 读取完整包内容（可能被环绕）
                    std::string msg(payload_size, '\0');
                    ring_buffer.peek(4, msg.data(), payload_size);

                    // 3. 处理特殊包/普通包
                    if (!msg.empty() && (msg[0] == 'H' || msg[0] == 'B')) {
                        safe_send("FINISH\n", new_socket);
                    } else {
                        auto tcp_start_time = std::chrono::high_resolution_clock::now();

                        pool.enqueue([msg, new_socket, tcp_start_time]() mutable {
                            auto tcp_end_time = std::chrono::high_resolution_clock::now();
                            double tcp_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                tcp_end_time - tcp_start_time).count(); {
                                std::lock_guard<std::mutex> lock(latency_mutex);
                                latencies_tcp_us.push_back(tcp_duration_us);
                            }
                            process_client_data(msg, new_socket, logger);
                        });
                    }

                    // 4. 消费掉缓冲区数据
                    ring_buffer.pop(4 + payload_size);
                }
            }

            close(new_socket);
            logger.info("Closed socket " + std::to_string(new_socket) + " for client " + client_ip + ":" +
                        std::to_string(client_port));
        });

        client_handler_thread.detach();
    }

    // Print final statistics if normal exit
    print_latency_stats(send_times.load(), {
        {"Overall Txn", latencies_overall_us},
        {"RegionGenerateIDs", latencies_generate_ids_us},
        {"MetisBuildGraph", latencies_build_graph_us},
        {"SQL Execution", latencies_exec_sql_us},
        {"TCP Latency", latencies_tcp_us}
    }, logger, false);

    logger.info("Server shutting down...");
    close(server_fd);
    return 0;
}

