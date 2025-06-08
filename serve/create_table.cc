#include <iostream>
#include "bmsql_meta.h"
#include <fstream>
#include <pqxx/pqxx>
#include <rapidjson/document.h>
#include <sstream>
#include "../core/util/json_config.h"

BmSql::Meta bmsqlMetadata;
thread_local std::vector<pqxx::connection *> connections_thread_local;

int main(int argc, char *argv[]) {
    int warehouse_num = 1;
    if (argc == 2) {
        warehouse_num = std::stoi(argv[1]);
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
        if(regionsize == 0) continue;
        sql_stream << "-- Table: " << i.name << "\n";
        for (int index = 1; index <= warehouse_num; index += regionsize) {
            std::string sql = "CREATE TABLE " + i.name + "_" + std::to_string(index) +
                              " PARTITION OF " + i.name +
                              " FOR VALUES FROM (" + std::to_string(index) +
                              ") TO (" + std::to_string(index + regionsize) + ");\n";
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

    return 0;
}
