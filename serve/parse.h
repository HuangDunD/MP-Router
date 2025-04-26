#include "config.h" // Keep this if config.h defines REGION_SIZE needed by ParseYcsbKey
#include <vector>
#include <string>
#include <regex>
#include <algorithm>
#include <assert.h>
#include <sstream> // Include for std::stringstream used in parseTPCHSQL

// Header guard (Make sure you have one, e.g.):
#ifndef PARSE_H
#define PARSE_H


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

// --- MODIFICATION: Add 'inline' ---
// 解析 SQL 语句中的 YCSB_KEY，返回涉及的 region_id 数组
inline void ParseYcsbKey(const std::string& sql, std::vector<int>& region_ids) {
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
inline SQLInfo parseTPCHSQL(const std::string& sql) {
    SQLInfo info;
    // Consider making regex patterns static const members or defining them outside the function
    // if this function is called frequently, to avoid recompilation overhead.
    std::regex selectPattern(R"(SELECT\s+.*?FROM\s+(\w+)(?:\s+WHERE\s+(?:\w+\.)?(\w+)\s+IN\s*\(([^)]+)\))?)", std::regex::icase);
    std::regex joinPattern(R"(FROM\s+(?:(\w+)\.)?(\w+)\s+(?:AS\s+\w+\s+)?JOIN\s+(?:(\w+)\.)?(\w+)\s+(?:AS\s+\w+\s+)?ON\s+(?:\w+\.)?(\w+)\s*=\s*(?:\w+\.)?(\w+))", std::regex::icase | std::regex::optimize);
    // Improved JOIN regex: FROM [schema.]table1 [AS alias] JOIN [schema.]table2 [AS alias] ON [alias.]col1 = [alias.]col2

    std::regex updatePattern(R"(UPDATE\s+(\w+)\s+SET\s+.*?WHERE\s+(?:\w+\.)?(\w+)\s+IN\s*\(([^)]+)\))", std::regex::icase | std::regex::optimize);
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
        std::string column1 = match[5].str();     // First column for ON
        std::string column2 = match[6].str();     // Second column for ON
        std::transform(column1.begin(), column1.end(), column1.begin(), ::tolower);
        std::transform(column2.begin(), column2.end(), column2.begin(), ::tolower);
        info.columnNames.push_back(column1);
        info.columnNames.push_back(column2);
        // Key vector is typically empty for JOIN based on this simple parsing
    } else if (std::regex_search(sql, match, updatePattern)) {
        info.type = SQLType::UPDATE;
        info.tableNames.push_back(match[1]); // Table being updated
         if (match.length(2) > 0 && match.length(3) > 0) { // Check if WHERE clause parts were matched
             info.columnNames.push_back(match[2]); // Column in WHERE IN
             std::string keys = match[3];
             std::stringstream ss(keys);
             std::string key;
             while (std::getline(ss, key, ',')) {
                try { // Add error handling for stoi
                     // Trim whitespace from key
                     key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
                     key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
                     if (!key.empty()) {
                        info.keyVector.push_back(std::stoi(key));
                     }
                } catch (const std::invalid_argument& ia) {
                    // Handle error: Log invalid key format? Skip?
                } catch (const std::out_of_range& oor) {
                    // Handle error: Log key out of range? Skip?
                }
             }
         } else {
             // Handle case: UPDATE without WHERE IN clause (or pattern didn't match it)
         }
    } else if (std::regex_search(sql, match, selectPattern)) {
        info.type = SQLType::SELECT;
        info.tableNames.push_back(match[1]); // Table in FROM
        if (match.length(2) > 0 && match.length(3) > 0) { // Check if WHERE clause parts were matched
            info.columnNames.push_back(match[2]); // Column in WHERE IN
            std::string keys = match[3];
            std::stringstream ss(keys);
            std::string key;
             while (std::getline(ss, key, ',')) {
                try { // Add error handling for stoi
                     // Trim whitespace from key
                     key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
                     key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
                      if (!key.empty()) {
                        info.keyVector.push_back(std::stoi(key));
                     }
                } catch (const std::invalid_argument& ia) {
                    // Handle error
                } catch (const std::out_of_range& oor) {
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

#endif // PARSE_H