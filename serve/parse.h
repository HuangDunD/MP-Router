// Header guard (Make sure you have one, e.g.):
#ifndef PARSE_H
#define PARSE_H

#include "config.h" // Keep this if config.h defines REGION_SIZE needed by ParseYcsbKey
#include <vector>
#include <string>
#include <regex>
#include <algorithm>
#include <assert.h>
#include <sstream>

#include <iostream>
#include <map>
#include <stdexcept>
#include <charconv>   // C++17: std::from_chars
#include <cctype>     // C++17: ::isspace


enum class SQLType {
    SELECT,
    UPDATE,
    JOIN,
    UNKNOWN
};

struct SQLInfo {
    SQLType type = SQLType::UNKNOWN;
    std::vector<std::string> tableNames;
    std::vector<int> tableIDs;
    std::vector<std::string> columnNames;
    std::vector<int> columnIDs;

    std::vector<int> keyVector;
};

// --- MODIFICATION: Add 'inline' ---
// 解析 SQL 语句中的 YCSB_KEY，返回涉及的 region_id 数组
inline void ParseYcsbKey(const std::string &sql, std::vector<int> &region_ids) {
    region_ids.clear(); // Good practice to clear output vector
    std::regex key_pattern(R"(YCSB_KEY\s*=\s*(\d+))"); // 匹配 YCSB_KEY = <数字>
    std::smatch matches;

    // 查找所有匹配的 YCSB_KEY
    std::string::const_iterator search_start(sql.cbegin());
    //std::cout<<"row keys: "<<std::endl;
    while (std::regex_search(search_start, sql.cend(), matches, key_pattern)) {
        int key = std::stoi(matches[1].str());
        //std::cout<<key<<"  ";
        // Check REGION_SIZE before division
        if (REGION_SIZE <= 0) {
            // Handle error: Log or throw? For now, skip.
            // Consider adding logging here if possible, or throwing an exception.
            search_start = matches.suffix().first; // Ensure progress
            continue;
        }
        int region_id = key / REGION_SIZE; // 计算 region_id
        region_ids.push_back(region_id);
        search_start = matches.suffix().first;
    }
    //std::cout<<std::endl;
    // 去重
    std::sort(region_ids.begin(), region_ids.end());
    region_ids.erase(std::unique(region_ids.begin(), region_ids.end()), region_ids.end());
}

// --- MODIFICATION: Add 'inline' ---
inline SQLInfo parseTPCHSQL_old(const std::string &sql) {
    SQLInfo info;
    // Consider making regex patterns static const members or defining them outside the function
    // if this function is called frequently, to avoid recompilation overhead.
    std::regex selectPattern(R"(SELECT\s+.*?FROM\s+(\w+)(?:\s+WHERE\s+(?:\w+\.)?(\w+)\s+IN\s*\(([^)]+)\))?)",
                             std::regex::icase);
    std::regex joinPattern(
        R"(FROM\s+(?:(\w+)\.)?(\w+)\s+(?:AS\s+\w+\s+)?JOIN\s+(?:(\w+)\.)?(\w+)\s+(?:AS\s+\w+\s+)?ON\s+(?:\w+\.)?(\w+)\s*=\s*(?:\w+\.)?(\w+))",
        std::regex::icase | std::regex::optimize);
    // Improved JOIN regex: FROM [schema.]table1 [AS alias] JOIN [schema.]table2 [AS alias] ON [alias.]col1 = [alias.]col2

    std::regex updatePattern(R"(UPDATE\s+(\w+)\s+SET\s+.*?WHERE\s+(?:\w+\.)?(\w+)\s+IN\s*\(([^)]+)\))",
                             std::regex::icase | std::regex::optimize);
    // Slightly improved UPDATE regex

    std::smatch match;

    // Match order can matter. JOIN often includes SELECT keywords. Check JOIN first?
    // Or rely on more specific patterns. Current patterns might misclassify complex queries.

    if (std::regex_search(sql, match, joinPattern)) {
        info.type = SQLType::JOIN;
        // Extracting table and column names from the improved regex
        // Match indices depend on optional groups (schema). Need careful extraction.
        // Example assuming no schema names captured:
        info.tableNames.push_back(match[2].str()); // First table name
        info.tableNames.push_back(match[4].str()); // Second table name
        std::string column1 = match[5].str(); // First column for ON
        std::string column2 = match[6].str(); // Second column for ON
        std::transform(column1.begin(), column1.end(), column1.begin(), ::tolower);
        std::transform(column2.begin(), column2.end(), column2.begin(), ::tolower);
        info.columnNames.push_back(column1);
        info.columnNames.push_back(column2);
        // Key vector is typically empty for JOIN based on this simple parsing
    } else if (std::regex_search(sql, match, updatePattern)) {
        info.type = SQLType::UPDATE;
        info.tableNames.push_back(match[1]); // Table being updated
        if (match.length(2) > 0 && match.length(3) > 0) {
            // Check if WHERE clause parts were matched
            info.columnNames.push_back(match[2]); // Column in WHERE IN
            std::string keys = match[3];
            std::stringstream ss(keys);
            std::string key;
            while (std::getline(ss, key, ',')) {
                try {
                    // Add error handling for stoi
                    // Trim whitespace from key
                    key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
                    key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
                    if (!key.empty()) {
                        info.keyVector.push_back(std::stoi(key));
                    }
                } catch (const std::invalid_argument &ia) {
                    // Handle error: Log invalid key format? Skip?
                } catch (const std::out_of_range &oor) {
                    // Handle error: Log key out of range? Skip?
                }
            }
        } else {
            // Handle case: UPDATE without WHERE IN clause (or pattern didn't match it)
        }
    } else if (std::regex_search(sql, match, selectPattern)) {
        info.type = SQLType::SELECT;
        info.tableNames.push_back(match[1]); // Table in FROM
        if (match.length(2) > 0 && match.length(3) > 0) {
            // Check if WHERE clause parts were matched
            info.columnNames.push_back(match[2]); // Column in WHERE IN
            std::string keys = match[3];
            std::stringstream ss(keys);
            std::string key;
            while (std::getline(ss, key, ',')) {
                try {
                    // Add error handling for stoi
                    // Trim whitespace from key
                    key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
                    key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
                    if (!key.empty()) {
                        info.keyVector.push_back(std::stoi(key));
                    }
                } catch (const std::invalid_argument &ia) {
                    // Handle error
                } catch (const std::out_of_range &oor) {
                    // Handle error
                }
            }
        } else {
            // Handle case: SELECT without WHERE IN clause (or pattern didn't match it)
        }
    } else {
        info.type = SQLType::UNKNOWN;
        // Optionally parse other SQL types or log unknown patterns
    }

    return info;
}

// inline: 去除字符串首尾空白字符
inline std::string trim_internal(const std::string &str) {
    auto first_not_space = std::find_if_not(str.begin(), str.end(), ::isspace);
    if (first_not_space == str.end()) {
        return "";
    }
    auto last_not_space = std::find_if_not(str.rbegin(), str.rend(), ::isspace).base();
    return std::string(first_not_space, last_not_space);
}

// inline: 解析逗号分隔的数字字符串
inline std::vector<int> parseNumbers_internal(const std::string &s) {
    std::vector<int> numbers;
    std::stringstream ss(s);
    std::string segment;
    while (std::getline(ss, segment, ',')) {
        std::string trimmed_segment = trim_internal(segment);
        if (!trimmed_segment.empty()) {
            int num;
            auto [ptr, ec] = std::from_chars(trimmed_segment.data(),
                                             trimmed_segment.data() + trimmed_segment.size(),
                                             num);
            if (ec == std::errc() && ptr == trimmed_segment.data() + trimmed_segment.size()) {
                numbers.push_back(num);
            } else {
                std::cerr << "警告 (内部解析): 跳过无法解析为整数的段落: '" << trimmed_segment << "'" << std::endl;
            }
        }
    }
    return numbers;
}


/**
 * @brief 解析包含 TPC-H 类头部信息的文本，提取结构化的 SQL 信息。
 *
 * 该函数读取由 ***Header_Start*** 和 ***Header_End*** 包裹的文本块。
 * 头部内包含多个由 Table, Column, Key 行组成的逻辑单元。
 * 每个逻辑单元被解析成一个 SQLInfo 对象。
 * 每个 SQLInfo 对象的类型根据其关联的 Table 行中的表数量决定：
 * - 1 个表: SELECT
 * - 多个表: JOIN
 *
 * @param sql_like_text 包含头部信息的类 SQL 文本。
 * @param ID2NAME 一个从整数 ID 映射到表名或列名的 map。
 * @param row_txn
 * @return std::vector<SQLInfo> 包含所有解析出的 SQLInfo 对象的向量。
 */
inline std::vector<SQLInfo> parseTPCHSQL(const std::string &sql_like_text, const std::map<int, std::string> &ID2NAME,
                                         std::string &row_txn) {
    std::vector<SQLInfo> results;
    std::stringstream inputStream(sql_like_text);
    std::string line;
    bool inHeader = false;
    SQLInfo currentInfo;
    bool buildingInfo = false;

    bool inTxnHeader = false;
    std::string txn;

    while (std::getline(inputStream, line)) {
        std::string trimmedLine = trim_internal(line);

        if (trimmedLine == "***Header_Start***") {
            inHeader = true;
            buildingInfo = false;
            continue;
        }
        if (trimmedLine == "***Header_End***") {
            inHeader = false;
            if (buildingInfo) {
                std::cerr << "警告: 在 ***Header_End*** 发现未完成的 SQLInfo 块。" << std::endl;
                buildingInfo = false;
            }
            continue;
        }
        if (trimmedLine == "***Txn_Start***") {
            // 开始 Txn
            inTxnHeader = true;
            txn.clear(); // 重置 txn
            continue;
        }
        if (trimmedLine == "***Txn_End***") {
            // 结束 Txn
            inTxnHeader = false;
            row_txn = txn; // 传递 txn
            break;
        }
        if (inTxnHeader) {
            // 累积正文
            txn += line;
            txn.push_back('\n');
            continue; // 不做其他解析
        }

        if (inHeader && !trimmedLine.empty()) {
            size_t colonPos = trimmedLine.find(':');
            size_t bracketOpenPos = trimmedLine.find('[');

            if (colonPos != std::string::npos && bracketOpenPos != std::string::npos && bracketOpenPos < colonPos) {
                std::string typeStr = trimmedLine.substr(0, bracketOpenPos);
                // Renamed 'type' to 'typeStr' to avoid conflict
                std::string numbersStr = trimmedLine.substr(colonPos + 1);
                std::vector<int> numbers = parseNumbers_internal(numbersStr);

                if (typeStr == "Table") {
                    if (buildingInfo) {
                        std::cerr << "警告: 检测到新的 'Table' 行，但前一个 SQLInfo 块似乎未完成。" << std::endl;
                    }
                    currentInfo = SQLInfo(); // 创建/重置当前 SQLInfo 对象
                    buildingInfo = true;

                    // 解析 Table IDs 并查找对应的名称
                    for (int id: numbers) {
                        auto it = ID2NAME.find(id);
                        currentInfo.tableNames.push_back(it != ID2NAME.end()
                                                             ? it->second
                                                             : "未找到表名(" + std::to_string(id) + ")");
                        currentInfo.tableIDs.push_back(id);
                    }

                    // *** 新逻辑：根据 Table 行中的 ID 数量设置类型 ***
                    if (numbers.empty()) {
                        // 如果解析出的数字为空（可能输入有误），保持 UNKNOWN 或设为错误状态
                        currentInfo.type = SQLType::UNKNOWN;
                        std::cerr << "警告: 'Table' 行解析出的 ID 列表为空: " << trimmedLine << std::endl;
                    } else if (numbers.size() == 1) {
                        currentInfo.type = SQLType::SELECT; // 单个表 ID -> SELECT
                    } else {
                        // numbers.size() > 1
                        currentInfo.type = SQLType::JOIN; // 多个表 ID -> JOIN
                    }
                } else if (typeStr == "Column") {
                    if (!buildingInfo) {
                        std::cerr << "警告: 发现 'Column' 行，但当前没有正在构建的 SQLInfo 对象。忽略此行。" << std::endl;
                        continue;
                    }
                    for (int id: numbers) {
                        auto it = ID2NAME.find(id);
                        currentInfo.columnNames.push_back(it != ID2NAME.end()
                                                              ? it->second
                                                              : "未找到列名(" + std::to_string(id) + ")");
                        currentInfo.columnIDs.push_back(id);
                    }
                } else if (typeStr == "Key") {
                    if (!buildingInfo) {
                        std::cerr << "警告: 发现 'Key' 行，但当前没有正在构建的 SQLInfo 对象。忽略此行。" << std::endl;
                        continue;
                    }
                    currentInfo.keyVector.insert(currentInfo.keyVector.end(), numbers.begin(), numbers.end());
                    results.push_back(currentInfo);
                    buildingInfo = false;
                } else {
                    std::cerr << "警告: 忽略未知的头部类型: " << typeStr << std::endl;
                }
            } else {
                std::cerr << "警告: 忽略格式错误的行: " << trimmedLine << std::endl;
            }
        }
    }

    // *** 移除旧的类型设置逻辑 ***
    // 不再需要根据 results.size() 来统一设置类型

    return results;
}

#endif // PARSE_H
