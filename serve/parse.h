#include "config.h"
#include <vector>
#include <string>
#include <regex>
#include <algorithm>
#include <assert.h>

enum class SQLType {
    SELECT,
    UPDATE,
    JOIN,
    UNKNOWN
};

struct SQLInfo {
    SQLType type = SQLType::UNKNOWN;
    std::vector<std::string> tableNames;
    std::vector<std::string> columnNames;
    std::vector<int> keyVector;
};

// 解析 SQL 语句中的 YCSB_KEY，返回涉及的 region_id 数组
void ParseYcsbKey(const std::string& sql, std::vector<int>& region_ids) {
    std::regex key_pattern(R"(YCSB_KEY\s*=\s*(\d+))"); // 匹配 YCSB_KEY = <数字>
    std::smatch matches;

    // 查找所有匹配的 YCSB_KEY
    std::string::const_iterator search_start(sql.cbegin());
    //std::cout<<"row keys: "<<std::endl;
    while (std::regex_search(search_start, sql.cend(), matches, key_pattern)) {
        int key = std::stoi(matches[1].str());
        //std::cout<<key<<"  ";
        int region_id = key / REGION_SIZE; // 计算 region_id
        region_ids.push_back(region_id);
        search_start = matches.suffix().first;
    }
    //std::cout<<std::endl;
    // 去重
    std::sort(region_ids.begin(), region_ids.end());
    region_ids.erase(std::unique(region_ids.begin(), region_ids.end()), region_ids.end());

}

SQLInfo parseTPCHSQL(const std::string& sql) {
    SQLInfo info;
    std::regex selectPattern(R"(.*SELECT\s+.*\s+FROM\s+(\w+)\.(\w+)\s*(?:WHERE\s+(?:\w+\.)?(\w+)\s+IN\s+\(([^)]+)\))?)");
    std::regex joinPattern(R"(.*JOIN\s+((?:\w+\.)?\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+))");
    std::regex updatePattern(R"(.*UPDATE\s+(\w+)\.(\w+)(?:\s*\*+\s*)*\s+WHERE\s+(?:\w+\.)?(\w+)\s+IN\s+\(([^)]+)\))");

    std::smatch match;

    if (std::regex_search(sql, match, joinPattern)) {
        // 处理 JOIN 子句的表名和列名
        info.tableNames.push_back(match[2]);  // 第一个表名
        info.tableNames.push_back(match[4]);  // 第二个表名
        info.columnNames.push_back(match[3]);  // 第一个表的列名
        info.columnNames.push_back(match[5]);  // 第二个表的列名
        info.type = SQLType::JOIN;
    } else if (std::regex_search(sql, match, selectPattern)) {
        // 处理 FROM 子句的表名
        info.tableNames.push_back(match[2]);
        if (match.length(3) > 0) {
            // 处理 WHERE 子句的列名
            info.columnNames.push_back(match[3]);
            std::string keys = match[4];
            std::stringstream ss(keys);
            std::string key;
            while (std::getline(ss, key, ',')) {
                info.keyVector.push_back(std::stoi(key));
            }
        }
        info.type = SQLType::SELECT;
    } else if (std::regex_search(sql, match, updatePattern)) {
        info.tableNames.push_back(match[2]);
        if (match.length(3) > 0) {
            info.columnNames.push_back(match[3]);
            std::string keys = match[4];
            std::stringstream ss(keys);
            std::string key;
            while (std::getline(ss, key, ',')) {
                info.keyVector.push_back(std::stoi(key));
            }
        }
        info.type = SQLType::UPDATE;
    }

    return info;
}