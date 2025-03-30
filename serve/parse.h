#include "config.h"
#include <vector>
#include <string>
#include <regex>
#include <algorithm>

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