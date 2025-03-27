#include "../include/lionrouter.h"
#include "curl/curl.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <cstdio>
#include <regex>
#include <algorithm>
#include <random>

// 单例实例
LionRouter& LionRouter::getInstance() {
    static LionRouter instance;
    return instance;
}

// 私有构造函数
LionRouter::LionRouter() : running_(true) {
    update_thread_ = std::thread(&LionRouter::UpdateThreadFunction, this);
}

LionRouter::~LionRouter() {
    running_ = false;
    if (update_thread_.joinable()) {
        update_thread_.join();
    }
}

void LionRouter::RecordTransactionDetails(const std::vector<int>& keys, int dst_store_id) {
    std::lock_guard<std::mutex> lock(cross_partition_mutex);
    total_transactions++;

    // 记录事务涉及的分区
    std::unordered_set<int> partitions;
    std::unordered_set<int> stores;
    for (int key : keys) {
        int region_id = key / REGION_SIZE;
        partitions.insert(region_id);
        stores.insert(GetRegionPrimaryStoreId(region_id));
    }

    // 如果是跨分区事务
    if (stores.size() > 1) {
        cross_partition_transactions++;
        store_cross_partition_count[dst_store_id]++;  // 统计每个 store_id 的跨分区事务数
        // 记录事务的 keys 和分区
        transaction_details.emplace_back(TxnLog(keys, partitions, dst_store_id));
    }

}

// 更新线程函数
void LionRouter::UpdateThreadFunction() {
    auto last_update_time = std::chrono::steady_clock::now();  // 记录上次更新时间
    auto last_stat_time = std::chrono::steady_clock::now();   // 记录上次统计时间

    while (running_) {
        try {
            // 检查当前时间是否已经超过上次更新时间 + UPDATE_INTERVAL
            auto now = std::chrono::steady_clock::now();
            // 检查是否需要更新路由信息
            auto elapsed_update_time = std::chrono::duration_cast<std::chrono::seconds>(now - last_update_time).count();
            if (elapsed_update_time >= UPDATE_INTERVAL) {
                printf("Starting to update region to store mapping.\n");
                InitRegion2Store("http://10.77.70.212:10080/tables/benchbase/usertable/regions");
                last_update_time = now;  // 更新上次更新时间
            }

            // 检查是否需要统计 SQL 路由情况和跨分区事务比例
            auto elapsed_stat_time = std::chrono::duration_cast<std::chrono::seconds>(now - last_stat_time).count();
            if (elapsed_stat_time >= SHOW_STATS_INTERVAL) {  // 每 10 秒统计一次
                // 加锁保护 store_sql_count 和跨分区事务统计的访问
                std::lock_guard<std::mutex> lock(store_sql_mutex);
                std::lock_guard<std::mutex> cross_lock(cross_partition_mutex);

                // 打印 SQL 路由统计结果
                printf("SQL routing statistics (last %d seconds):\n", SHOW_STATS_INTERVAL);
                for (const auto& [store_id, count] : store_sql_count) {
                    printf("Store %d: %d SQLs\n", store_id, count);
                }

                // 打印跨分区事务统计结果
                if (total_transactions > 0) {
                    double cross_partition_ratio = static_cast<double>(cross_partition_transactions) / total_transactions * 100;
                    printf("Cross-partition transaction ratio: %.2f%% (%d/%d)\n", cross_partition_ratio, cross_partition_transactions, total_transactions);

                    // 打印每个 store_id 的跨分区事务比例
                    for (const auto& [store_id, transaction_count] : store_cross_partition_count) {
                        printf("Cross Store %d: %d\n", store_id, transaction_count);
                    }

                    // 打印分布式事务的 key 与涉及的分区及节点
                    // printf("Distributed transaction details:\n");
                    // for (const auto& log : transaction_details) {
                    //     // printf("  Keys: ");
                    //     // for (int key : keys) {
                    //     //     printf("%d ", key);
                    //     // }
                        
                    //     printf("Store %d |   ", log.store_id);
                    //     for (int region_id : log.region_ids) {
                    //         printf("%d ", region_id);
                    //     }
                    //     printf("\n");
                    // }
                } else {
                    // printf("No transactions recorded.\n");
                }



                // 重置统计
                store_sql_count.clear();
                total_transactions = 0;
                cross_partition_transactions = 0;
                store_cross_partition_count.clear();
                transaction_details.clear();
                last_stat_time = now;  // 更新上次统计时间
            }


            // 如果未达到更新时间间隔，则短暂休眠（例如 100ms），避免忙等待
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        } catch (const std::exception& e) {
            // proxy_info("Failed to update region to store mapping: %s\n", e.what());
        }
    }
}

// 从 JSON 文件读取数据
nlohmann::json LionRouter::ReadJsonFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }

    nlohmann::json root;
    try {
        file >> root;
    } catch (const nlohmann::json::parse_error& e) {
        throw std::runtime_error("Failed to parse JSON: " + std::string(e.what()));
    }

    return root;
}

std::string LionRouter::FetchRemoteData(const std::string& url) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        throw std::runtime_error("Failed to initialize CURL");
    }

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    // 设置 SSL 验证选项（禁用验证）
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    // 设置超时选项
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10);  // 请求超时时间为 10 秒
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10);  // 连接超时时间为 10 秒

    // 执行请求
    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        std::string error_msg = "CURL request failed: ";
        error_msg += curl_easy_strerror(res);
        curl_easy_cleanup(curl);
        throw std::runtime_error(error_msg);
    }

    // 清理 CURL 句柄
    curl_easy_cleanup(curl);

    // 返回响应数据
    return response;
}

size_t LionRouter::WriteCallback(void* ptr, size_t size, size_t nmemb, std::string* data) {
    data->append((char*)ptr, size * nmemb);
    return size * nmemb;
}

// 初始化 TiDB 和 Store 的映射关系
void LionRouter::InitTidb2Store(const std::string& path) {
    nlohmann::json root = ReadJsonFile(path);

    for (const auto& [key, value] : root.items()) {
        tidb2store[key] = value["tikv_ip"].get<std::string>();
        store2tidb[value["tikv_ip"].get<std::string>()] = key;
        tidb2hostgroup[key] = value["hostgroup"].get<int>();
    }
}

// 初始化 Region 和 Store 的映射关系
void LionRouter::InitRegion2Store(const std::string& pd_url) {
    std::string response = FetchRemoteData(pd_url);
    UpdateRegion2Store(response);
}

// 根据 TiDB 名称获取对应的 Store
std::string LionRouter::GetStoreForTidb(const std::string& tidb) const {
    auto it = tidb2store.find(tidb);
    if (it != tidb2store.end()) {
        return it->second;
    }
    return "";  // 如果找不到，返回空字符串
}

// 根据 Region ID 获取对应的主副本 Store
int LionRouter::GetStoreForRegion(int actual_region_id) const {
    int read_index = version_.load() % 2;  // 获取当前读取的缓冲区索引
    const MetaInfo& meta_info = meta_info_[read_index];

    auto it = meta_info.region_primary_store_id_.find(actual_region_id);
    if (it != meta_info.region_primary_store_id_.end()) {
        return it->second;  // 假设 Store 名称格式为 "storeX"
    }
    return -1;  // 如果找不到，返回 -1
}

void LionRouter::InitTikv2Store(const std::string& pd_url) {
    std::string response = FetchRemoteData(pd_url);
    // 更新 TiKV 和 Store 的映射关系
    UpdateTikv2Store(response);
}

void LionRouter::UpdateTikv2Store(const std::string& response) {
    try {
        // 解析 JSON 数据
        json json_data = json::parse(response);

        // 遍历 stores 数组
        for (const auto& store : json_data["stores"]) {
            int store_id = store["store"]["id"];
            std::string address = store["store"]["address"];

            // 提取 IP 地址
            std::string tikv_ip = address.substr(0, address.find(':'));

            // 填充映射关系
            tikv2storeID[tikv_ip] = store_id;
            storeID2tikv[store_id] = tikv_ip;
            // 更新 store_id
            store_ids_.insert(store_id);
        }
    } catch (const json::parse_error& e) {
        // JSON 解析错误
        std::cerr << "JSON parse error: " << e.what() << std::endl;
    } catch (const json::type_error& e) {
        // JSON 类型错误（例如访问不存在的字段）
        std::cerr << "JSON type error: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        // 其他异常
        std::cerr << "Error in UpdateTikv2Store: " << e.what() << std::endl;
    }
}

// 根据 TiKV IP 获取对应的 Store ID
int LionRouter::GetStoreIDForTiKV(const std::string& tikv_ip) const {
    auto it = tikv2storeID.find(tikv_ip);
    if (it != tikv2storeID.end()) {
        return it->second;
    }
    return -1;  // 未找到
}

// 根据 Store ID 获取对应的 TiKV IP
std::string LionRouter::GetTiKVForStoreID(int store_id) const {
    auto it = storeID2tikv.find(store_id);
    if (it != storeID2tikv.end()) {
        return it->second;
    }
    return "";  // 未找到
}

// 更新 Region 和 Store 的映射关系
void LionRouter::UpdateRegion2Store(const std::string& response) {
    try {
        json data = json::parse(response);
        auto regions = data.find("record_regions");
        if (regions == data.end()) {
            throw std::runtime_error("Invalid JSON data: missing 'record_regions'");
        }

        // 获取当前写入的缓冲区索引
        int write_index = (version_.load() + 1) % 2;
        MetaInfo& meta_info = meta_info_[write_index];

        // 清空旧数据
        meta_info.virtual_region_id_map_.clear();
        meta_info.region_primary_store_id_.clear();
        meta_info.region_secondary_store_id_.clear();

        // 更新数据
        for (size_t virtual_id = 0; virtual_id < regions->size(); ++virtual_id) {
            const auto& region = (*regions)[virtual_id];
            int actual_id = region["region_id"];
            meta_info.virtual_region_id_map_[virtual_id] = actual_id;

            const auto& leader = region["leader"];
            const auto& peers = region["peers"];

            // 更新主节点
            meta_info.region_primary_store_id_[actual_id] = leader["store_id"];

            // 更新从节点
            std::unordered_set<int> secondary_store_ids;
            for (const auto& peer : peers) {
                if (peer["id"] != leader["id"]) {
                    secondary_store_ids.insert(peer["store_id"].get<int>());
                }
            }
            meta_info.region_secondary_store_id_[actual_id] = secondary_store_ids;
        }

        // 更新版本号
        version_ = (version_.load() + 1) % 10;  // 防止溢出
    } catch (const std::exception& e) {
        printf("Error in UpdateRegion2Store: %s\n", e.what());
        // throw;
    }
}

// 获取某个虚拟 region 的主节点 store_id
int LionRouter::GetRegionPrimaryStoreId(int virtual_region_id) const {
    int read_index = version_.load() % 2;  // 获取当前读取的缓冲区索引
    const MetaInfo& meta_info = meta_info_[read_index];

    auto it = meta_info.virtual_region_id_map_.find(virtual_region_id);
    if (it == meta_info.virtual_region_id_map_.end()) {
        throw std::runtime_error("虚拟 region_id " + std::to_string(virtual_region_id) + " 不存在");
    }
    int actual_region_id = it->second;

    auto primary_it = meta_info.region_primary_store_id_.find(actual_region_id);
    if (primary_it == meta_info.region_primary_store_id_.end()) {
        throw std::runtime_error("实际 region_id " + std::to_string(actual_region_id) + " 没有主节点信息");
    }
    return primary_it->second;
}

// 获取某个虚拟 region 的从节点 store_id 列表
const std::unordered_set<int>& LionRouter::GetRegionSecondaryStoreId(int virtual_region_id) const {
    int read_index = version_.load() % 2;  // 获取当前读取的缓冲区索引
    const MetaInfo& meta_info = meta_info_[read_index];

    auto it = meta_info.virtual_region_id_map_.find(virtual_region_id);
    if (it == meta_info.virtual_region_id_map_.end()) {
        throw std::runtime_error("虚拟 region_id " + std::to_string(virtual_region_id) + " 不存在");
    }
    int actual_region_id = it->second;

    auto secondary_it = meta_info.region_secondary_store_id_.find(actual_region_id);
    if (secondary_it == meta_info.region_secondary_store_id_.end()) {
        throw std::runtime_error("实际 region_id " + std::to_string(actual_region_id) + " 没有从节点信息");
    }
    return secondary_it->second;
}

// 获取所有 store_id
const std::set<int>& LionRouter::GetAllStoreIds() const {
    return store_ids_;
}

// 解析 SQL 语句中的 YCSB_KEY，返回涉及的 region_id 数组
std::vector<int> LionRouter::ParseYcsbKey(const std::string& sql) {
    std::unordered_set<int> keys_set;  // 使用 unordered_set 去重

    // 查找 WHERE YCSB_KEY IN (...) 部分
    size_t where_pos = sql.find("WHERE YCSB_KEY IN (");
    if (where_pos == std::string::npos) {
        // 如果没有 WHERE 子句，直接返回空
        return std::vector<int>();
    }

    // 提取括号内的内容
    size_t start_pos = sql.find('(', where_pos);
    size_t end_pos = sql.find(')', start_pos);
    if (start_pos == std::string::npos || end_pos == std::string::npos) {
        // 如果括号不完整，直接返回空
        return std::vector<int>();
    }

    std::string key_list = sql.substr(start_pos + 1, end_pos - start_pos - 1);

    // 解析逗号分隔的 YCSB_KEY
    size_t pos = 0;
    while (pos < key_list.size()) {
        // 跳过空格
        while (pos < key_list.size() && std::isspace(key_list[pos])) {
            pos++;
        }

        // 提取数字
        size_t num_start = pos;
        while (pos < key_list.size() && std::isdigit(key_list[pos])) {
            pos++;
        }

        if (num_start < pos) {
            int key = std::stoi(key_list.substr(num_start, pos - num_start));
            keys_set.insert(key);
        }

        // 跳过逗号
        while (pos < key_list.size() && key_list[pos] != ',') {
            pos++;
        }
        pos++;  // 跳过逗号
    }
    // 将 unordered_set 转换为 vector
    return std::vector<int>(keys_set.begin(), keys_set.end());
}

// 根据 region_id 数组，计算最优的 hostgroupid
int LionRouter::EvaluateHost(const std::vector<int>& keys) {
    std::vector<int> best_store_ids(GetAllStoreIds().size());
    std::unordered_map<int, int> costs;
    int idx = 0;
    int min_cost = 0;
    
    // 遍历所有 store_id
    for (int store_id : GetAllStoreIds()) {
        int primary_count = 0;
        int secondary_count = 0;

        // 计算主副本和从副本的数量
        for (int key : keys) {
            int region_id = key / REGION_SIZE;
            int primary_store_id = GetRegionPrimaryStoreId(region_id);
            const std::unordered_set<int>& secondary_store_ids = GetRegionSecondaryStoreId(region_id);

            if (primary_store_id == store_id) {
                primary_count++;
            } else if (secondary_store_ids.count(store_id)) {
                secondary_count++;
            }
        }

        // 计算开销
        int cost = -(primary_count * weight_ + secondary_count);
        if(min_cost == cost){
            best_store_ids[idx ++ ] = store_id;
        } else if(min_cost > cost){
            min_cost = cost;
            idx = 0;
            best_store_ids[idx ++ ] = store_id;
        }
        costs[store_id] = cost;
    }

    // 更新均匀分布的范围
    int best_store_id = best_store_ids[random() % idx];
	RecordTransactionDetails(keys, best_store_id);

    // 找到与 store_id 相邻部署的 TiDB
    std::string best_tikv_ip = GetTiKVForStoreID(best_store_id);
    if (best_tikv_ip.empty()) {
        throw std::runtime_error("No TiKV found for store_id: " + std::to_string(best_store_id));
    }

    // 找到与 TiKV 相邻部署的 TiDB
    auto tidb_it = store2tidb.find(best_tikv_ip);
    if (tidb_it == store2tidb.end()) {
        throw std::runtime_error("No TiDB found for TiKV StoreID: " + std::to_string(best_store_id));
    }
    std::string best_tidb_ip = tidb_it->second;

    // 找到 TiDB 对应的 hostgroupid
    auto hostgroup_it = tidb2hostgroup.find(best_tidb_ip);
    if (hostgroup_it == tidb2hostgroup.end()) {
        throw std::runtime_error("No hostgroup found for TiDB IP: " + best_tidb_ip);
    }

    int hostgroup_id = hostgroup_it->second;
    // 尝试加锁，如果成功则更新统计
    if (store_sql_mutex.try_lock()) {
        store_sql_count[best_store_id]++;  // 统计路由到该 hostgroup 的 SQL 个数
        store_sql_mutex.unlock();
    }

    return hostgroup_id;  // 返回最优的 hostgroupid
}