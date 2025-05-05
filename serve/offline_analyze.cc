#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <thread>

#include "queryplan_cardinality.h"
#include "db_meta.h"
#include "util/json_config.h"
#include "parse.h"

TPCHMeta* TPCH_META;

// 去除字符串首尾空白
inline std::string trim(const std::string &s) {
    auto start = s.begin();
    while (start != s.end() && std::isspace(*start)) ++start;

    auto end = s.end();
    do {
        --end;
    } while (std::distance(start, end) > 0 && std::isspace(*end));

    return std::string(start, end + 1);
}

// 从文本文件中读取 SQL 语句
std::vector<std::string> load_sql_statements(const std::string& filename) {
    std::ifstream infile(filename);
    std::vector<std::string> sql_statements;

    if (!infile) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return sql_statements;
    }

    std::string line;
    std::ostringstream current_stmt;

    while (std::getline(infile, line)) {
        std::string trimmed = trim(line);
        if (trimmed.empty()) continue;

        current_stmt << line << "\n";

        if (trimmed.back() == ';') {
            sql_statements.push_back(current_stmt.str());
            current_stmt.str("");
            current_stmt.clear();
        }
    }

    // 若最后一条语句没有分号结尾，也加进去
    std::string leftover = current_stmt.str();
    if (!trim(leftover).empty()) {
        sql_statements.push_back(leftover);
    }

    return sql_statements;
}

// Function to handle processing in a separate thread
void process_offline_data(const std::string &data) {
    // TPCH workload
    SQLInfo sql_info ;
    parseTPCHSQL(data,sql_info);
    if(sql_info.type == SQLType::SELECT) {
        std::cout << "Handling SELECT statement." << std::endl;
        assert(sql_info.tableNames.size() == 1);
        assert(sql_info.columnNames.size() == 1);
        std::string table_name = sql_info.tableNames[0];
        std::string column_name = sql_info.columnNames[0];
        table_id_t table_id = TPCH_META->tablenameToID.at(table_name);
        column_id_t column_id = TPCH_META->columnNameToID[table_id].at(column_name);
        if(TPCH_META->partition_column_ids[table_id] != column_id){ 
            // query key is not the partition key
            std::cout << "Query key is not the partition key." << std::endl;
        }
        else{
            std::vector<int> region_ids;
            for(const auto &key: sql_info.keyVector) {
                assert(key >= 0);
                int region_id = key / REGION_SIZE; // Calculate region_id
                region_ids.push_back(region_id);
            }
            // metis.build_internal_graph(region_ids); // Call the graph building function
        }
        
    } else if(sql_info.type == SQLType::UPDATE) {
        std::cout << "Handling UPDATE statement." << std::endl;
        assert(sql_info.tableNames.size() == 1);
        assert(sql_info.columnNames.size() == 1);
        std::string table_name = sql_info.tableNames[0];
        std::string column_name = sql_info.columnNames[0];
        table_id_t table_id = TPCH_META->tablenameToID.at(table_name);
        column_id_t column_id = TPCH_META->columnNameToID[table_id].at(column_name);
        if(TPCH_META->partition_column_ids[table_id] != column_id){ 
            // query key is not the partition key
            std::cout << "Query key is not the partition key." << std::endl;
        }
        else{
            std::vector<int> region_ids;
            for(const auto &key: sql_info.keyVector) {
                assert(key >= 0);
                int region_id = key / REGION_SIZE; // Calculate region_id
            }
            // metis.build_internal_graph(region_ids); // Call the graph building function
        }
    } else if(sql_info.type == SQLType::JOIN) {
        std::cout << "Handling JOIN statement." << std::endl;
        assert(sql_info.tableNames.size() == 2);
        assert(sql_info.columnNames.size() == 2);
        std::string table1_name = sql_info.tableNames[0];
        std::string table2_name = sql_info.tableNames[1];
        std::string column1_name = sql_info.columnNames[0];
        std::string column2_name = sql_info.columnNames[1];
        table_id_t table1_id = TPCH_META->tablenameToID.at(table1_name);
        table_id_t table2_id = TPCH_META->tablenameToID.at(table2_name);
        int cardinality = get_query_plan_cardinality(data, conninfo);
        if (cardinality != -1) {
            std::cout << "Estimated cardinality: " << cardinality << std::endl;
            TPCH_META->mutex_partition_column_ids.lock();
            // Update partition_column_cardinality based on the cardinality
            TPCH_META->table_column_cardinality[table1_id][TPCH_META->columnNameToID[table1_id].at(column1_name)] += cardinality;
            TPCH_META->table_column_cardinality[table2_id][TPCH_META->columnNameToID[table2_id].at(column2_name)] += cardinality; 
            TPCH_META->mutex_partition_column_ids.unlock();
        }
    } else {
        std::cerr << "Unknown SQL type." << std::endl;
    }

}
int main() {
    std::string config_filepath = "../../config/ycsb_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("ycsb");
    REGION_SIZE = conf.get("key_cnt_per_partition").get_int64();

    // load db meta
    #if WORKLOAD_MODE == 1 
        // TPCH workload
        TPCH_META = new TPCHMeta();
    #endif

    std::string file_path = "../../offline_workload.txt";
    std::vector<std::string> sqls = load_sql_statements(file_path);

    // 处理每个 SQL 语句, 多线程处理
    std::vector<std::thread> threads;
    int thread_count = std::thread::hardware_concurrency() / 2; // 获取可用线程数
    if (thread_count == 0) {
        thread_count = 1; // 至少使用一个线程
    }
    std::cout << "Using " << thread_count << " threads." << std::endl;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&, i]() {
            std::cout << "Thread " << i << " started." << std::endl;
            int sql_start = i * (sqls.size() / thread_count);
            int sql_end = (i == thread_count - 1) ? sqls.size() : (i + 1) * (sqls.size() / thread_count);
            for (int j = sql_start; j < sql_end; ++j) {
                process_offline_data(sqls[j]);
            }
            std::cout << "Thread " << i << " finished." << std::endl;
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    std::cout << "All threads finished." << std::endl;
    // 处理完所有 SQL 语句
    std::cout << "All SQL statements processed." << std::endl;
    // 输出每个表的列的基数, 并选出最大的列作为分区列
    for (int i = 0; i < TPCH_META->table_column_cardinality.size(); ++i) {
        std::cout << "Table " << i << " column cardinality: ";
        for (int j = 0; j < TPCH_META->table_column_cardinality[i].size(); ++j) {
            long long cardinality = TPCH_META->table_column_cardinality[i][j];
            if (cardinality > TPCH_META->table_column_cardinality[i][TPCH_META->partition_column_ids[i]]) {
                TPCH_META->partition_column_ids[i] = j; // 更新分区列
            }
            std::cout << cardinality << " ";
        }
        std::cout << std::endl;
    }
    std::cout << "Partition column ids: ";
    std::ofstream outfile("partition_column_ids.txt");
    for (int i = 0; i < TPCH_META->partition_column_ids.size(); ++i) {
        int column_id = TPCH_META->partition_column_ids[i];
        // 这里的 column_id 是分区列的id
        std::cout << column_id << " " << TPCH_META->table_column_cardinality[i][column_id] << " ";
        outfile << column_id << std::endl;
    }

    return 0;
}
