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

#include "common.h"

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
inline static std::vector<SQLInfo> parseTPCHSQL(std::string_view sql, std::string &row_txn, partition_id_t* partition_id = nullptr) {
    std::vector<SQLInfo> results;
    results.reserve(64);

    size_t pos = 0;
    size_t next = 0;

    auto trim_view = [&](std::string_view sv) {
        size_t b = 0, e = sv.size();
        while (b < e && std::isspace((unsigned char)sv[b])) ++b;
        while (e > b && std::isspace((unsigned char)sv[e-1])) --e;
        return sv.substr(b, e - b);
    };

    bool inHeader = false;
    bool building = false;
    bool inTxn = false;
    SQLInfo current;
    std::string txn;

    while (pos < sql.size()) {
        next = sql.find('\n', pos);
        std::string_view line_sv = (next == std::string_view::npos)
            ? trim_view(sql.substr(pos))
            : trim_view(sql.substr(pos, next - pos));

        if (line_sv == "***Header_Start***") {
            inHeader = true;
            building = false;
        }
        else if (line_sv == "***Header_End***") {
            inHeader = false;
            building = false;
        }
        else if (line_sv == "***Txn_Start***") {
            inTxn = true;
            txn.clear();
        }
        else if (line_sv == "***Txn_End***") {
            inTxn = false;
            row_txn = txn;
            break;
        }
        else if (inTxn) {
            if (next == std::string_view::npos) {
                txn.append(sql.substr(pos));
            } else {
                txn.append(sql.substr(pos, next - pos + 1));
            }
        }
        else if (inHeader && !line_sv.empty()) {
            auto colon = line_sv.find(':');
            auto brack = line_sv.find('[');
            if (brack != std::string_view::npos && colon != std::string_view::npos && brack < colon) {
                auto type_sv = trim_view(line_sv.substr(0, brack));
                auto nums_sv = line_sv.substr(colon + 1);
                static thread_local std::vector<int> nums_buf;
                nums_buf.clear();
                nums_buf.reserve(8);
                size_t i = 0, n = nums_sv.size();
                while (i < n) {
                    while (i < n && !std::isdigit((unsigned char)nums_sv[i])) ++i;
                    int v = 0;
                    bool seen = false;
                    while (i < n && std::isdigit((unsigned char)nums_sv[i])) {
                        seen = true;
                        v = v * 10 + (nums_sv[i++] - '0');
                    }
                    if (seen) nums_buf.push_back(v);
                }

                if(type_sv == "WareHouse" && partition_id != nullptr) {
                    assert(nums_buf.size() == 1);
                    *partition_id = nums_buf[0];
                }
                if (type_sv == "Table") {
                    current = SQLInfo();
                    current.tableIDs.reserve(nums_buf.size());
                    for (int id : nums_buf) current.tableIDs.push_back(id);
                    current.type = nums_buf.size() == 1 ? SQLType::SELECT
                                     : nums_buf.empty() ? SQLType::UNKNOWN
                                     : SQLType::JOIN;
                    building = true;
                }
                else if (type_sv == "Column" && building) {
                    current.columnIDs.reserve(nums_buf.size());
                    for (int id : nums_buf) current.columnIDs.push_back(id);
                }
                else if (type_sv == "Key" && building) {
                    current.keyVector.reserve(nums_buf.size());
                    for (int id : nums_buf) current.keyVector.push_back(id);
                    results.push_back(current);
                    building = false;
                }
            }
        }

        if (next == std::string_view::npos) break;
        pos = next + 1;
    }

    return results;
}


#endif // PARSE_H
