#include <iostream>
#include "bmsql_meta.h"
#include <fstream>
#include <pqxx/pqxx>
#include <rapidjson/document.h>
#include <sstream>
#include "../core/util/json_config.h"

BmSql::Meta bmsqlMetadata;
thread_local std::vector<pqxx::connection *> connections_thread_local;

struct RouterEndpoint {
    std::string ip;
    uint16_t port;
    std::string username;
    std::string password;
    std::string dbname;
};

static bool send_sql_to_router(const std::string &sqls, size_t router_node) {
    pqxx::connection *conn = connections_thread_local[router_node];
    if (conn == nullptr || !conn->is_open()) {
        std::cerr << "Failed to connect to the database." << std::endl;
        return false;
    }
    try {
        pqxx::work txn(*conn);
        pqxx::result result = txn.exec(sqls + "\n");
        txn.commit();
    } catch (const std::exception &e) {
        std::cerr << "Error while connecting to KingBase or getting query plan: " + std::string(e.what());
        return false;
    }
    return true;
}

int main(int argc, char *argv[]) {
    bool send_pg = false;
    if (argc == 2) {
        std::string parm = argv[1];
        if (parm == "send") {
            send_pg = true;
        }
    }
    std::vector<std::string> DBConnection;

    std::string compute_node_config_path = "../../config/compute_node_config.json";
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


    std::string metaFilePath = "../../config/tpcc_meta.json";
    if (!bmsqlMetadata.loadFromJsonFile(metaFilePath)) {
        std::cerr << "Fatal Error: Failed to load TPCC metadata. Exiting." << std::endl;
        return EXIT_FAILURE;
    }

    std::ostringstream sql_stream;

    for (auto &i: bmsqlMetadata.tables_) {
        int regionsize = 0;
        for (auto &j: i.columns) {
            if (j.is_affinity) {
                regionsize = j.region_size;
            }
        }
        sql_stream << "-- Table: " << i.name << "\n";
        for (int index = 1; index <= regionsize; ++index) {
            std::string sql = "CREATE TABLE " + i.name + "_" + std::to_string(index) +
                              " PARTITION OF " + i.name +
                              " FOR VALUES FROM (" + std::to_string(index) +
                              ") TO (" + std::to_string(index + 1) + ");\n";
            sql_stream << sql;
        }
        sql_stream << "\n";
    }

    // 写文件
    std::ofstream outputFile("../../config/tpcc_create_table.sql");
    if (!outputFile.is_open()) {
        std::cerr << "Error: Cannot open output file for writing." << std::endl;
        return 0;
    }
    outputFile << sql_stream.str();
    outputFile.close();

    // 发送SQL
    if (send_pg) {
        // 这里填0，实际代码应根据你实际的节点数量判断
        send_sql_to_router(sql_stream.str(), 0);
    }

    return 0;
}
