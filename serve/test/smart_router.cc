#include "smart_router.h"

SmartRouter::SmartRouterResult SmartRouter::get_route_primary(tx_id_t tx_id, std::vector<table_id_t> &table_ids, std::vector<itemkey_t> &keys, 
        std::vector<pqxx::connection *> &thread_conns) {
    SmartRouterResult result;
    if (table_ids.size() != keys.size() || table_ids.empty()) {
        result.error_message = "Mismatched or empty table_ids and keys";
        return result;
    }

    std::unordered_map<uint64_t, node_id_t> page_to_node_map; // 高32位存table_id，低32位存page_id, for SYSTEM_MODE 3
    std::unordered_map<node_id_t, int> node_count_basedon_key_access_last; // 基于key_access_last的计数, for SYSTEM_MODE 5
    std::unordered_map<uint64_t, node_id_t> table_key_id;  // 高32位存table_id，低32位存key, for SYSTEM_MODE 6
    std::unordered_map<node_id_t, int> node_count_basedon_page_access_last; // 基于page_access_last的计数, for SYSTEM_MODE 7
    std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 找到ownership对应的节点的映射关系, for SYSTEM_MODE 8
    std::string debug_info;
    page_to_node_map.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if(SYSTEM_MODE == 3) {
            auto entry = lookup(table_ids[i], keys[i], thread_conns);
            // 计算page id
            if (entry.page == kInvalidPageId) {
                result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                    ", key=" + std::to_string(keys[i]) + ")";
                assert(false); // 这里不应该失败
            }
            // 方法1: 直接使用page_id
            uint64_t table_page = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
            page_to_node_map[table_page] = -1; // 初始化
            // 方法2: 每GroupPageAffinitySize个页面作为一个region, 这样可以减少图分区的大小
            // table_page_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | (entry.page / GroupPageAffinitySize));
            // 方法3: 直接对page_id做hash, 这样可以减少图分区的大小, 这会使得负载变得更加均衡
            // std::hash<page_id_t> h;
            // int region_id = h(entry.page) % (cfg_.partition_nums * 1000); // 10倍分区数的region
            // table_page_id.push_back((static_cast<uint64_t>(table_ids[i]) << 32) | region_id);
        }
        else if(SYSTEM_MODE == 5) {
            auto entry = lookup(table_ids[i], keys[i], thread_conns);
            // 如果是模式5，则统计key_access_last_node出现的次数, 按照上次key访问节点进行路由
            if (entry.key_access_last_node != -1) {
                node_count_basedon_key_access_last[entry.key_access_last_node]++;
            }
        }
        else if(SYSTEM_MODE == 6) {
            // 计算key id
            uint64_t table_key = (static_cast<uint64_t>(table_ids[i]) << 32) | keys[i];
            table_key_id[table_key] = -1; // 初始化
        }
        else if(SYSTEM_MODE == 7) {
            auto entry = lookup(table_ids[i], keys[i], thread_conns);
            // 计算page id
            if (entry.page == kInvalidPageId) {
                result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                    ", key=" + std::to_string(keys[i]) + ")";
                assert(false); // 这里不应该失败
            }
            auto last_node = ownership_table_->get_owner(table_ids[i], entry.page);
            if (last_node != -1) {
                node_count_basedon_page_access_last[last_node]++;
            }
            debug_info += "(table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                            ", page=" + std::to_string(entry.page) + ", last_node=" + std::to_string(last_node) + "); ";
        }
        else if(SYSTEM_MODE == 8) {
            // 计算page id
            auto entry = lookup(table_ids[i], keys[i], thread_conns);
            if (entry.page == kInvalidPageId) {
                result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                    ", key=" + std::to_string(keys[i]) + ")";
                assert(false); // 这里不应该失败
            }
            uint64_t table_page_id_val = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
            page_to_node_map[table_page_id_val] = -1; // 初始化
            auto last_node = ownership_table_->get_owner(table_ids[i], entry.page);
            if (last_node != -1) {
                page_ownership_to_node_map[table_page_id_val] = last_node;
                node_count_basedon_page_access_last[last_node]++;
            }
            debug_info += "(table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                            ", page=" + std::to_string(entry.page) + ", last_node=" + std::to_string(last_node) + "); ";
        }
        else { 
            assert(false); // unknown mode
        }
    }

    try {
        if (SYSTEM_MODE == 3) {
            // 基于page Metis的结果进行分区
            // 去重优化
            result.keys_processed = page_to_node_map.size();
            int ret_code = metis_->build_internal_graph(page_to_node_map, &result.smart_router_id);
            if(ret_code == 0) {
                result.sys_3_decision_type = 0; // metis no decision
            } else if (ret_code == -1) {
                result.sys_3_decision_type = 1; // metis missing
            }
            else if (ret_code == 1) {
                result.sys_3_decision_type = 2; // metis entirely
            }
            else if (ret_code == 2) {
                result.sys_3_decision_type = 3; // metis partial
            }
            else if (ret_code == 3) {
                result.sys_3_decision_type = 4; // metis cross
            }
            // result.smart_router_id = 0;
        }
        else if (SYSTEM_MODE == 5) {
            // !如果是模式5，则结合key_access_last_node的计数结果进行调整
            int max_count = 0;
            node_id_t candidate_node = -1;
            for (const auto& [node, count] : node_count_basedon_key_access_last) {
                if (count > max_count) {
                    max_count = count;
                    candidate_node = node;
                }
            }
            if (candidate_node != -1) {
                if (candidate_node != result.smart_router_id) {
                    // !直接选择出现次数最多的节点
                    result.smart_router_id = candidate_node;
                }
            }
            else {
                // 在运行前面一段时间, 因为初始化时没有last_node, 导致这里没有任何候选节点
                // 那就随机选择一个
                result.smart_router_id = rand() % cfg_.partition_nums;
            }
        }
        else if (SYSTEM_MODE == 6) {
            // SYSTEM_MODE 6: 按照key做亲和性划分
            // 去重优化
            result.keys_processed = table_key_id.size();
            metis_->build_internal_graph(table_key_id, &result.smart_router_id);
        }
        else if (SYSTEM_MODE == 7) {
            // SYSTEM_MODE 7: 按照page做亲和性划分, 结合page_access_last_node
            if(node_count_basedon_page_access_last.empty()) {
                this->stats_.ownership_random_txns++;
                result.smart_router_id = rand() % cfg_.partition_nums;
                logger->info("[SmartRouter Random] found no candidate node based on page access last node, randomly selected node " + 
                            std::to_string(result.smart_router_id));
            }
            else if (node_count_basedon_page_access_last.size() == 1) {
                this->stats_.ownership_entirely_txns++;
                // 只有一个候选节点, 直接选择
                result.smart_router_id = node_count_basedon_page_access_last.begin()->first;
                logger->info("[SmartRouter Entirely] " + debug_info + " based on page access last node directly to node " + 
                            std::to_string(result.smart_router_id));
            }
            else if (node_count_basedon_page_access_last.size() > 1) {
                this->stats_.ownership_cross_txns++;
                int max_count = 0;
                node_id_t candidate_node = -1;
                std::vector<node_id_t> candidates;
                for (const auto& [node, count] : node_count_basedon_page_access_last) {
                    if (count > max_count) {
                        max_count = count;
                        candidates.clear();
                        candidates.push_back(node);
                    } else if (count == max_count) {
                        candidates.push_back(node);
                    }
                }
                assert(!candidates.empty());
                // 随机选择出现次数最多的节点
                result.smart_router_id = candidates[rand() % candidates.size()];
                logger->info("[SmartRouter Cross] " + debug_info + " based on page access last node with count (" 
                            + std::to_string(max_count) + ") to node " + std::to_string(result.smart_router_id));
            }
        }
        else if(SYSTEM_MODE == 8) {
            // 基于page Metis的结果进行分区, 同时返回page到node的映射
            result.keys_processed = page_to_node_map.size();
            node_id_t metis_decision_node;
            int ret_code = metis_->build_internal_graph(page_to_node_map, &metis_decision_node);
            // page_ownership_to_node_map 是ownership_table_中记录的page到node的映射
            // page_to_node_map 是metis分区后得到的page到node的映射
            // 这里进行对比, 看看两者是否一致, 综合考虑page_to_node_map和ownership_table_的信息, 进行调整
            if  (ret_code == 0){ 
                // no decision
                assert(!page_to_node_map.empty());
                result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisNoDecision);
            }
            else if (ret_code == -1) {
                // missing, 没有任何page被映射到节点
                // 这个时候再检查一下ownership_table_的信息
                assert(!page_to_node_map.empty()); 
                if(node_count_basedon_page_access_last.empty()) {
                    // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                    result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipMissing);
                }
                else if(node_count_basedon_page_access_last.size() == 1) {
                    // ownership_table_中只有一个page的映射信息
                    result.smart_router_id = page_ownership_to_node_map.begin()->second;
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipEntirely);
                }
                else if(node_count_basedon_page_access_last.size() > 1) {
                    // ownership_table_中有多个page的映射信息
                    // 基于page_ownership_to_node_map进行计数, 选择出现次数最多的节点
                    int max_count = 0;
                    node_id_t candidate_node = -1;
                    std::vector<node_id_t> candidates;
                    for (const auto& [node, count] : node_count_basedon_page_access_last) {
                        if (count > max_count) {
                            max_count = count;
                            candidates.clear();
                            candidates.push_back(node);
                        } else if (count == max_count) {
                            candidates.push_back(node);
                        }
                    }
                    assert(!candidates.empty());
                    // 随机选择出现次数最多的节点
                    node_id_t min_txn_node = -1;
                    size_t min_txn_count = SIZE_MAX;
                    for (int i=0; i<candidates.size(); i++) {
                        auto node = candidates[i];
                        if (routed_txn_cnt_per_node[node].load() < min_txn_count) {
                            min_txn_count = routed_txn_cnt_per_node[node].load();
                            min_txn_node = node;
                        }
                    }
                    assert(min_txn_node != -1);
                    result.smart_router_id = min_txn_node;
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipCross);
                }
                else assert(false); // 不可能出现的情况
            }
            else if (ret_code == 1) {
                // entire affinity, 所有page都映射到同一个节点
                assert(!page_to_node_map.empty());
                assert(metis_decision_node != -1);
                if(node_count_basedon_page_access_last.empty()) {
                    // ownership_table_中没有任何page的映射信息, 即涉及到的页面
                    result.smart_router_id = metis_decision_node;
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipMissing);
                }
                else if(node_count_basedon_page_access_last.size() == 1) {
                    // ownership_table_中只有一个page的映射信息
                    auto ownership_node = page_ownership_to_node_map.begin()->second;
                    if(ownership_node == metis_decision_node) {
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyEqual);
                    }
                    else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = ownership_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyUnequal);
                    }
                }
                else if(node_count_basedon_page_access_last.size() > 1) {
                    // ownership_table_中有多个page的映射信息
                    int max_count = 0;
                    node_id_t candidate_node = -1;
                    std::vector<node_id_t> candidates;
                    for (const auto& [node, count] : node_count_basedon_page_access_last) {
                        if (count > max_count) {
                            max_count = count;
                            candidates.clear();
                            candidates.push_back(node);
                        } else if (count == max_count) {
                            candidates.push_back(node);
                        }
                    }
                    assert(!candidates.empty());
                    int i;
                    for(i=0; i<candidates.size(); i++) {
                        if (candidates[i] == metis_decision_node) {
                            candidate_node = candidates[i];
                            break;
                        }
                    }
                    if(i >= candidates.size()) {
                        // candidates中没有metis_decision_node, 那就随机选择一个
                        candidate_node = candidates[rand() % candidates.size()];
                    }
                    if(candidate_node == metis_decision_node) {
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossEqual);
                    }
                    else {
                        // !pay attention: 这里对于candidates 中有可能包含metis_decision_node的情况, 这种情况需要选择metis_decision_node
                        result.smart_router_id = candidate_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossUnequal);
                    }
                }
                else assert(false); // 不可能出现的情况
            }
            else if (ret_code == 2) {
                // partial affinity, 多个page映射到多个节点
                assert(!page_to_node_map.empty());
                // 这个时候再检查一下ownership_table_的信息
                if(node_count_basedon_page_access_last.empty()) {
                    // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                    result.smart_router_id = metis_decision_node;
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipMissing);
                }
                else if(node_count_basedon_page_access_last.size() == 1) {
                    // ownership_table_中只有一个page的映射信息
                    auto ownership_node = page_ownership_to_node_map.begin()->second;
                    if(ownership_node == metis_decision_node) {
                        // 两者一致
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyEqual);
                    }
                    else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = ownership_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyUnequal);
                    }
                }
                else if(node_count_basedon_page_access_last.size() > 1) {
                    // ownership_table_中有多个page的映射信息
                    int max_count = 0;
                    node_id_t candidate_node = -1;
                    std::vector<node_id_t> candidates;
                    for (const auto& [node, count] : node_count_basedon_page_access_last) {
                        if (count > max_count) {
                            max_count = count;
                            candidates.clear();
                            candidates.push_back(node);
                        } else if (count == max_count) {
                            candidates.push_back(node);
                        }
                    }
                    assert(!candidates.empty());
                    // 找到routed_txn_cnt_per_node中最小的节点
                    node_id_t min_txn_node = -1;
                    size_t min_txn_count = SIZE_MAX;
                    for (int i=0; i<candidates.size(); i++) {
                        auto node = candidates[i];
                        if (routed_txn_cnt_per_node[node].load() < min_txn_count) {
                            min_txn_count = routed_txn_cnt_per_node[node].load();
                            min_txn_node = node;
                        }
                    }
                    assert(min_txn_node != -1);
                    node_id_t ownership_node = min_txn_node;
                    if(ownership_node == metis_decision_node) {
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossEqual);
                    }
                    else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = ownership_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossUnequal);
                    }
                }
                else assert(false); // 不可能出现的情况
            }
            else if (ret_code == 3) {
                // cross affinity, 多个page映射到多个节点, 并且有些page没有映射到节点
                assert(!page_to_node_map.empty());
                // 这个时候再检查一下ownership_table_的信息
                if(node_count_basedon_page_access_last.empty()) {
                    // ownership_table_中也没有任何page的映射信息, 即涉及到的页面第一次被访问
                    result.smart_router_id = metis_decision_node;
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipMissing);
                }
                else if(node_count_basedon_page_access_last.size() == 1) {
                    // ownership_table_中只有一个page的映射信息
                    auto ownership_node = page_ownership_to_node_map.begin()->second;
                    if(ownership_node == metis_decision_node) {
                        // 两者一致
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyEqual);
                    }
                    else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = ownership_node; 
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyUnequal);
                    }
                }
                else if(node_count_basedon_page_access_last.size() > 1) {
                    // ownership_table_中有多个page的映射信息
                    int max_count = 0;
                    node_id_t candidate_node = -1;
                    std::vector<node_id_t> candidates;
                    for (const auto& [node, count] : node_count_basedon_page_access_last) {
                        if (count > max_count) {
                            max_count = count;
                            candidates.clear();
                            candidates.push_back(node);
                        } else if (count == max_count) {
                            candidates.push_back(node);
                        }
                    }
                    assert(!candidates.empty());
                    // 找到routed_txn_cnt_per_node中最小的节点
                    node_id_t min_txn_node = -1;
                    size_t min_txn_count = SIZE_MAX;
                    for (int i=0; i<candidates.size(); i++) {
                        auto node = candidates[i];
                        if (routed_txn_cnt_per_node[node].load() < min_txn_count) {
                            min_txn_count = routed_txn_cnt_per_node[node].load();
                            min_txn_node = node;
                        }
                    }
                    assert(min_txn_node != -1);
                    node_id_t ownership_node = min_txn_node;
                    if(ownership_node == metis_decision_node) {
                        result.smart_router_id = metis_decision_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossEqual);
                    }
                    else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = ownership_node;
                        log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossUnequal);
                    }
                }
                else assert(false); // 不可能出现的情况
            }
            else assert(false);
        }
        else {
            assert(false);
        }

        if(result.smart_router_id == -1) {
            // 如果没有决定, 则随机选择一个节点
            result.smart_router_id = rand() % cfg_.partition_nums; // 随机选择一个节点
        }
        result.success = true;
    }
    catch (const std::exception &e) {
        result.error_message = std::string("Exception during partitioning: ") + e.what();
    }
    return result;
}


std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> SmartRouter::get_route_primary_batch_schedule(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch,
        std::vector<pqxx::connection *> &thread_conns) {
    
    assert(SYSTEM_MODE == 10); // 仅支持模式10
    std::vector<SmartRouterResult> results;
    results.reserve(txn_batch->size());
    // 这几个变量是对整个batch, 记录一些基本的信息
    std::unordered_map<tx_id_t, SchedulingCandidateTxn*> txid_to_txn_map; // 记录每个事务ID对应的事务对象, 还没调度的事务
    std::unordered_map<uint64_t, std::vector<tx_id_t>> page_to_txn_map; // 记录每个页面对应的事务ID
    std::unordered_map<uint64_t, node_id_t> page_metis_to_node_map; // 记录每个页面对应的节点ID（Metis分区结果）
    std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 记录每个页面对应的节点ID（Ownership表结果）

    // ownership_ok_txn_queues链表存储着完全满足ownership table的要求
    std::vector<std::unordered_set<tx_id_t>> ownership_ok_txn_queues(ComputeNodeCount);
    // candidate_txn_queues链表存储着候选事务
    std::unordered_set<SchedulingCandidateTxn*> candidate_txn_queues;

    // !实际事务执行编排的事务列表
    std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> scheduled_txn_queues(new std::vector<std::queue<TxnQueueEntry*>>(ComputeNodeCount));

#if LOG_KROUTER_SCHEDULING_DEBUG
    if(WarmupEnd)
    logger->info("[SmartRouter Scheduling] Start scheduling for txn batch of size " + std::to_string(txn_batch->size()));
#endif
    // 0. 进行批事务的信息收集准备
    for (auto& txn : *txn_batch) { 
        auto txn_type = txn->txn_type;
        auto tx_id = txn->tx_id;
        SchedulingCandidateTxn* scheduling_candidate_txn = new SchedulingCandidateTxn{txn, {}, {}, -1};
        txid_to_txn_map[tx_id] = scheduling_candidate_txn;
        itemkey_t account1 = txn->accounts[0];
        itemkey_t account2 = txn->accounts[1];
        std::vector<itemkey_t> accounts_keys;
        std::vector<table_id_t> table_ids = smallbank_->get_table_ids_by_txn_type(txn_type); 
        assert(!table_ids.empty());
        smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
        assert(table_ids.size() == accounts_keys.size());

        // 获取涉及的页面列表
        std::unordered_map<uint64_t, node_id_t> table_page_ids; // 高32位存table_id，低32位存page_id
        for (size_t i = 0; i < accounts_keys.size(); ++i) {
            auto entry = lookup(table_ids[i], accounts_keys[i], thread_conns);
            // 计算page id
            if (entry.page == kInvalidPageId) {
                assert(false); // 这里不应该失败
            }
            uint64_t table_page_id = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
            table_page_ids[table_page_id] = -1; // 初始化
            page_to_txn_map[table_page_id].push_back(tx_id);
            scheduling_candidate_txn->involved_pages.push_back(table_page_id);
        }

        // 构建图
        if(!WarmupEnd) {
            // 如果还在Warmup阶段，让Metis构建图
            node_id_t metis_decision_node;
            // Warmup阶段，随机分区
            metis_->build_internal_graph(table_page_ids, &metis_decision_node);
        }

        // 填充page_to_node_map和page_ownership_to_node_map
        std::unordered_map<node_id_t, int> ownership_node_count;
        for (auto [table_page_id, _] : table_page_ids) {
            if(page_metis_to_node_map.find(table_page_id) == page_metis_to_node_map.end()) {
                node_id_t metis_node = metis_->get_metis_partitioning_result(table_page_id);
                page_metis_to_node_map[table_page_id] = metis_node;
            }
            if(page_ownership_to_node_map.find(table_page_id) == page_ownership_to_node_map.end()) {
                node_id_t ownership_node = ownership_table_->get_owner(table_page_id);
                if(ownership_node == -1) {
                    ownership_node = rand() % ComputeNodeCount; // 如果没有owner，则随机分配一个节点
                }
                page_ownership_to_node_map[table_page_id] = ownership_node;
            }
            ownership_node_count[page_ownership_to_node_map[table_page_id]]++;
        }

        if(ownership_node_count.size() == 1) {
            // 满足ownership entirely
            node_id_t ownership_node = ownership_node_count.begin()->first;
            ownership_ok_txn_queues[ownership_node].insert(tx_id);
            scheduling_candidate_txn->will_route_node = ownership_node;
        }
        else {
            // 记录为候选事务
            std::unordered_map<node_id_t, double> node_benefit_map;
            int sum_pages = scheduling_candidate_txn->involved_pages.size();
            for(const auto& [node_id, count] : ownership_node_count) {
                node_benefit_map[node_id] = static_cast<double>(count) / sum_pages; // 已经有ownership的页面比例
            }
            scheduling_candidate_txn->node_benefit_map = node_benefit_map;
            candidate_txn_queues.insert(scheduling_candidate_txn);
        }
    }
    
    #if LOG_KROUTER_SCHEDULING_DEBUG
    if(WarmupEnd){
        // print txid_to_txn_map
        for(const auto& [tx_id, scheduling_txn] : txid_to_txn_map) {
            logger->info("Txn " + std::to_string(tx_id) + 
                            " involves pages: " + [&]() {
                                std::string pages_str;
                                for(const auto& page : scheduling_txn->involved_pages) {
                                    pages_str += std::to_string(page) + " ";
                                }
                                return pages_str;
                            }());
        }
        // print page_to_txn_map
        for(const auto& [page, txn_ids] : page_to_txn_map) {
            logger->info("Page " + std::to_string(page) + 
                            " involved by txns: " + [&]() {
                                std::string txns_str;
                                for(const auto& tx_id : txn_ids) {
                                    txns_str += std::to_string(tx_id) + " ";
                                }
                                return txns_str;
                            }());
        }
        // print page_metis_to_node_map
        for(const auto& [page, node_id] : page_metis_to_node_map) {
            logger->info("Page " + std::to_string(page) + " metis assigned to node " + std::to_string(node_id));
        }
        // print page_ownership_to_node_map
        for(const auto& [page, node_id] : page_ownership_to_node_map) {
            logger->info("Page " + std::to_string(page) + " owned by node " + std::to_string(node_id));
        }
        // 
        // print ownership_ok_txn_queues
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
            std::string txns_str;
            for(const auto& tx_id : ownership_ok_txn_queues[node_id]) {
                txns_str += std::to_string(tx_id) + " ";
            }
            logger->info("Node " + std::to_string(node_id) + 
                            " ownership_ok_txn_queues: " + txns_str);
        }
    }
    #endif

    // 1. 进行调度决策
    while(!candidate_txn_queues.empty()){
        
        // 1.1 从候选事务中选择下一个要调度的事务进行所有权迁移的计划
        int min_txn_node = -1;
        int min_txn_count = INT32_MAX;
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            if(ownership_ok_txn_queues[node_id].size() < min_txn_count) {
                min_txn_count = ownership_ok_txn_queues[node_id].size();
                min_txn_node = node_id;
            }
        }
        double max_benefit_score = -1;
        SchedulingCandidateTxn* selected_candidate_txn = nullptr;
        for(auto& txn: candidate_txn_queues){
            // 遍历candidate_txn_queues，找到可以迁移到min_txn_node节点的事务
            auto it = txn->node_benefit_map.find(min_txn_node);
            if(it != txn->node_benefit_map.end()){
                double benefit_score = it->second;
                if(benefit_score > max_benefit_score){
                    max_benefit_score = benefit_score;
                    selected_candidate_txn = txn;
                }
            }
        }

    #if LOG_KROUTER_SCHEDULING_DEBUG
        if(WarmupEnd){
            if(selected_candidate_txn != nullptr){
                logger->info("[SmartRouter Scheduling] Selected txn " + std::to_string(selected_candidate_txn->txn->tx_id) + 
                            " to transfer pages to node " + std::to_string(min_txn_node) + 
                            " with benefit score " + std::to_string(max_benefit_score));
            }
            else {
                logger->info("[SmartRouter Scheduling] No suitable txn found to transfer pages to node " + std::to_string(min_txn_node));
            }
        }
    #endif
        
        // 1.2 找到合适的不满足ownership entirely的事务，进行页面转移计划的制订
        std::vector<uint64_t> transfer_pages;
        if(selected_candidate_txn != nullptr){
            auto tx_id = selected_candidate_txn->txn->tx_id;
            // 找到需要迁移的页面, 这些页面将从owner_node转移到min_txn_node
            for(const auto& page : selected_candidate_txn->involved_pages){
                node_id_t owner_node = page_ownership_to_node_map[page];
                if(owner_node != min_txn_node){
                    transfer_pages.push_back(page);
                }
            }
        }

        // 1.3 进行事务的编排，找到转移页面后，原来可以满足ownership entirely的事务可能不满足的，将这些事务先执行
        for(auto transfer_page : transfer_pages){
            page_ownership_to_node_map[transfer_page] = min_txn_node; // 更新ownership table中的页面归属信息
            #if LOG_KROUTER_SCHEDULING_DEBUG
                if(WarmupEnd)
                logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                " ownership transferred to node " + std::to_string(min_txn_node) + 
                                ", check affected txns:");
            #endif
            for(auto affected_txn_id : page_to_txn_map[transfer_page]){
                // 检查affected_txn_id是否在ownership_ok_txn_queues中
                auto it = txid_to_txn_map.find(affected_txn_id);
                if(it == txid_to_txn_map.end()) continue; // 说明这个事务已经被调度过了
                node_id_t will_route_node = it->second->will_route_node;
                if(will_route_node != -1) {
                    // 说明这个事务之前可以在will_route_node上执行， 但是这个transfer page转移到新的节点之后，他就不可以继续执行了
                    assert(ownership_ok_txn_queues[will_route_node].count(affected_txn_id) == 1);
                    // 将该事务从ownership_ok_txn_queues中删除，表示已不能执行
                    ownership_ok_txn_queues[will_route_node].erase(affected_txn_id);
                    // 将该事务加入到scheduled_txn_queues中, 可以执行
                    scheduled_txn_queues->at(will_route_node).push(it->second->txn);
                    delete it->second; // 释放内存
                    // 从txid_to_txn_map中删除该事务，表示已经调度完成
                    txid_to_txn_map.erase(it);
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                    " transferred to node " + std::to_string(min_txn_node) + 
                                    ", scheduling previously ownership_ok txn " + std::to_string(affected_txn_id) + 
                                    " to execute on node " + std::to_string(will_route_node));
                #endif
                }
                else {
                    // 确定当前事务在迁移页面后，能否执行
                    bool can_execute = true;
                    for(const auto& page : it->second->involved_pages){
                        node_id_t owner_node = page_ownership_to_node_map[page];
                        if(owner_node != min_txn_node){
                            // 说明当前事务仍然不能在min_txn_node上执行
                            can_execute = false;
                            break;
                        }
                    }
                    if(can_execute) {
                        // 说明当前事务现在可以在min_txn_node上执行
                        it->second->will_route_node = min_txn_node;
                        ownership_ok_txn_queues[min_txn_node].insert(affected_txn_id);
                        // 从candidate_txn_queues中删除该事务
                        candidate_txn_queues.erase(it->second);
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                        logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                        ", txn " + std::to_string(affected_txn_id) + 
                                        " MOVE to ownership_ok on node " + std::to_string(min_txn_node));
                #endif
                    }
                    else {
                        // 说明当前事务仍然不能在min_txn_node上执行，保持不变
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                        logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                        ", txn " + std::to_string(affected_txn_id) + 
                                        " still cannot execute on node " + std::to_string(min_txn_node));
                #endif
                    }
                }
            }
        }
    }

    // 2. 剩余的ownership_ok_txn_queues中的事务加入到scheduled_txn_queues中
    for(auto txn_queue: ownership_ok_txn_queues) {
        for(auto tx_id: txn_queue) {
            auto it = txid_to_txn_map.find(tx_id);
            assert(it != txid_to_txn_map.end());
            node_id_t will_route_node = it->second->will_route_node;
            scheduled_txn_queues->at(will_route_node).push(it->second->txn);
            delete it->second; // 释放内存
            txid_to_txn_map.erase(it);
        #if LOG_KROUTER_SCHEDULING_DEBUG
            if(WarmupEnd)
            logger->info("[SmartRouter Scheduling] Final Scheduling ownership_ok txn " + std::to_string(tx_id) + 
                            " to execute on node " + std::to_string(will_route_node));
        #endif
        }
    }
    // 3. 构造返回结果
    return scheduled_txn_queues;
}


std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> SmartRouter::get_route_primary_batch_schedule_v2(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch,
        std::vector<pqxx::connection *> &thread_conns) {
    
    assert(SYSTEM_MODE == 10); // 仅支持模式10
    std::vector<SmartRouterResult> results;
    results.reserve(txn_batch->size());
    // 这几个变量是对整个batch, 记录一些基本的信息
    std::unordered_map<tx_id_t, SchedulingCandidateTxn*> txid_to_txn_map; // 记录每个事务ID对应的事务对象, 还没调度的事务
    std::unordered_map<uint64_t, std::vector<tx_id_t>> page_to_txn_map; // 记录每个页面对应的事务ID
    std::unordered_map<uint64_t, node_id_t> page_metis_to_node_map; // 记录每个页面对应的节点ID（Metis分区结果）
    std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 记录每个页面对应的节点ID（Ownership表结果）

    // ownership_ok_txn_queues链表存储着完全满足ownership table的要求
    std::vector<std::unordered_set<tx_id_t>> ownership_ok_txn_queues(ComputeNodeCount);
    // candidate_txn_queues链表存储着候选事务
    std::unordered_set<SchedulingCandidateTxn*> candidate_txn_queues;

    // !实际事务执行编排的事务列表
    std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> scheduled_txn_queues(new std::vector<std::queue<TxnQueueEntry*>>(ComputeNodeCount));

#if LOG_KROUTER_SCHEDULING_DEBUG
    if(WarmupEnd)
    logger->info("[SmartRouter Scheduling] Start scheduling for txn batch of size " + std::to_string(txn_batch->size()));
#endif
    // 0. 进行批事务的信息收集准备
    for (auto& txn : *txn_batch) { 
        auto txn_type = txn->txn_type;
        auto tx_id = txn->tx_id;
        SchedulingCandidateTxn* scheduling_candidate_txn = new SchedulingCandidateTxn{txn, {}, {}, -1};
        txid_to_txn_map[tx_id] = scheduling_candidate_txn;
        itemkey_t account1 = txn->accounts[0];
        itemkey_t account2 = txn->accounts[1];
        std::vector<itemkey_t> accounts_keys;
        std::vector<table_id_t> table_ids = smallbank_->get_table_ids_by_txn_type(txn_type); 
        assert(!table_ids.empty());
        smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
        assert(table_ids.size() == accounts_keys.size());

        // 获取涉及的页面列表
        std::unordered_map<uint64_t, node_id_t> table_page_ids; // 高32位存table_id，低32位存page_id
        for (size_t i = 0; i < accounts_keys.size(); ++i) {
            auto entry = lookup(table_ids[i], accounts_keys[i], thread_conns);
            // 计算page id
            if (entry.page == kInvalidPageId) {
                assert(false); // 这里不应该失败
            }
            uint64_t table_page_id = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
            table_page_ids[table_page_id] = -1; // 初始化
            page_to_txn_map[table_page_id].push_back(tx_id);
            scheduling_candidate_txn->involved_pages.push_back(table_page_id);
        }

        // 构建图
        if(!WarmupEnd) {
            // 如果还在Warmup阶段，让Metis构建图
            node_id_t metis_decision_node;
            // Warmup阶段，随机分区
            metis_->build_internal_graph(table_page_ids, &metis_decision_node);
        }

        // 填充page_to_node_map和page_ownership_to_node_map
        std::unordered_map<node_id_t, int> ownership_node_count;
        for (auto [table_page_id, _] : table_page_ids) {
            if(page_metis_to_node_map.find(table_page_id) == page_metis_to_node_map.end()) {
                node_id_t metis_node = metis_->get_metis_partitioning_result(table_page_id);
                page_metis_to_node_map[table_page_id] = metis_node;
            }
            if(page_ownership_to_node_map.find(table_page_id) == page_ownership_to_node_map.end()) {
                node_id_t ownership_node = ownership_table_->get_owner(table_page_id);
                if(ownership_node == -1) {
                    ownership_node = rand() % ComputeNodeCount; // 如果没有owner，则随机分配一个节点
                }
                page_ownership_to_node_map[table_page_id] = ownership_node;
            }
            ownership_node_count[page_ownership_to_node_map[table_page_id]]++;
        }

        if(ownership_node_count.size() == 1) {
            // 满足ownership entirely
            node_id_t ownership_node = ownership_node_count.begin()->first;
            ownership_ok_txn_queues[ownership_node].insert(tx_id);
            scheduling_candidate_txn->will_route_node = ownership_node;
        }
        else {
            // 记录为候选事务
            std::unordered_map<node_id_t, double> node_benefit_map;
            int sum_pages = scheduling_candidate_txn->involved_pages.size();
            for(const auto& [node_id, count] : ownership_node_count) {
                node_benefit_map[node_id] = static_cast<double>(count) / sum_pages; // 已经有ownership的页面比例
            }
            scheduling_candidate_txn->node_benefit_map = node_benefit_map;
            candidate_txn_queues.insert(scheduling_candidate_txn);
        }
    }
    
    #if LOG_KROUTER_SCHEDULING_DEBUG
    if(WarmupEnd){
        // print txid_to_txn_map
        for(const auto& [tx_id, scheduling_txn] : txid_to_txn_map) {
            logger->info("Txn " + std::to_string(tx_id) + 
                            " involves pages: " + [&]() {
                                std::string pages_str;
                                for(const auto& page : scheduling_txn->involved_pages) {
                                    pages_str += std::to_string(page) + " ";
                                }
                                return pages_str;
                            }());
        }
        // print page_to_txn_map
        for(const auto& [page, txn_ids] : page_to_txn_map) {
            logger->info("Page " + std::to_string(page) + 
                            " involved by txns: " + [&]() {
                                std::string txns_str;
                                for(const auto& tx_id : txn_ids) {
                                    txns_str += std::to_string(tx_id) + " ";
                                }
                                return txns_str;
                            }());
        }
        // print page_metis_to_node_map
        for(const auto& [page, node_id] : page_metis_to_node_map) {
            logger->info("Page " + std::to_string(page) + " metis assigned to node " + std::to_string(node_id));
        }
        // print page_ownership_to_node_map
        for(const auto& [page, node_id] : page_ownership_to_node_map) {
            logger->info("Page " + std::to_string(page) + " owned by node " + std::to_string(node_id));
        }
        // 
        // print ownership_ok_txn_queues
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
            std::string txns_str;
            for(const auto& tx_id : ownership_ok_txn_queues[node_id]) {
                txns_str += std::to_string(tx_id) + " ";
            }
            logger->info("Node " + std::to_string(node_id) + 
                            " ownership_ok_txn_queues: " + txns_str);
        }
    }
    #endif

    // 1. 进行调度决策
    while(!candidate_txn_queues.empty()){
        
        // 1.1 从候选事务中选择下一个要调度的事务进行所有权迁移的计划
        int min_txn_node = -1;
        int min_txn_count = INT32_MAX;
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            if(ownership_ok_txn_queues[node_id].size() < min_txn_count) {
                min_txn_count = ownership_ok_txn_queues[node_id].size();
                min_txn_node = node_id;
            }
        }
        double max_benefit_score = -1;
        SchedulingCandidateTxn* selected_candidate_txn = nullptr;
        for(auto& txn: candidate_txn_queues){
            // 遍历candidate_txn_queues，找到可以迁移到min_txn_node节点的事务
            auto it = txn->node_benefit_map.find(min_txn_node);
            if(it != txn->node_benefit_map.end()){
                double benefit_score = it->second;
                if(benefit_score > max_benefit_score){
                    max_benefit_score = benefit_score;
                    selected_candidate_txn = txn;
                }
            }
        }

    #if LOG_KROUTER_SCHEDULING_DEBUG
        if(WarmupEnd){
            if(selected_candidate_txn != nullptr){
                logger->info("[SmartRouter Scheduling] Selected txn " + std::to_string(selected_candidate_txn->txn->tx_id) + 
                            " to transfer pages to node " + std::to_string(min_txn_node) + 
                            " with benefit score " + std::to_string(max_benefit_score));
            }
            else {
                logger->info("[SmartRouter Scheduling] No suitable txn found to transfer pages to node " + std::to_string(min_txn_node));
            }
        }
    #endif
        
        // 1.2 找到合适的不满足ownership entirely的事务，进行页面转移计划的制订
        std::vector<uint64_t> transfer_pages;
        if(selected_candidate_txn != nullptr){
            auto tx_id = selected_candidate_txn->txn->tx_id;
            // 找到需要迁移的页面, 这些页面将从owner_node转移到min_txn_node
            for(const auto& page : selected_candidate_txn->involved_pages){
                node_id_t owner_node = page_ownership_to_node_map[page];
                if(owner_node != min_txn_node){
                    transfer_pages.push_back(page);
                }
            }
        }

        // 1.3 进行事务的编排，找到转移页面后，原来可以满足ownership entirely的事务可能不满足的，将这些事务先执行
        for(auto transfer_page : transfer_pages){
            page_ownership_to_node_map[transfer_page] = min_txn_node; // 更新ownership table中的页面归属信息
            #if LOG_KROUTER_SCHEDULING_DEBUG
                if(WarmupEnd)
                logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                " ownership transferred to node " + std::to_string(min_txn_node) + 
                                ", check affected txns:");
            #endif
            for(auto affected_txn_id : page_to_txn_map[transfer_page]){
                // 检查affected_txn_id是否在ownership_ok_txn_queues中
                auto it = txid_to_txn_map.find(affected_txn_id);
                if(it == txid_to_txn_map.end()) continue; // 说明这个事务已经被调度过了
                node_id_t will_route_node = it->second->will_route_node;
                if(will_route_node != -1) {
                    // 说明这个事务之前可以在will_route_node上执行， 但是这个transfer page转移到新的节点之后，他就不可以继续执行了
                    assert(ownership_ok_txn_queues[will_route_node].count(affected_txn_id) == 1);
                    // 将该事务从ownership_ok_txn_queues中删除，表示已不能执行
                    ownership_ok_txn_queues[will_route_node].erase(affected_txn_id);
                    // 将该事务加入到scheduled_txn_queues中, 可以执行
                    scheduled_txn_queues->at(will_route_node).push(it->second->txn);
                    delete it->second; // 释放内存
                    // 从txid_to_txn_map中删除该事务，表示已经调度完成
                    txid_to_txn_map.erase(it);
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                    " transferred to node " + std::to_string(min_txn_node) + 
                                    ", scheduling previously ownership_ok txn " + std::to_string(affected_txn_id) + 
                                    " to execute on node " + std::to_string(will_route_node));
                #endif
                }
                else {
                    // 确定当前事务在迁移页面后，能否执行
                    bool can_execute = true;
                    for(const auto& page : it->second->involved_pages){
                        node_id_t owner_node = page_ownership_to_node_map[page];
                        if(owner_node != min_txn_node){
                            // 说明当前事务仍然不能在min_txn_node上执行
                            can_execute = false;
                            break;
                        }
                    }
                    if(can_execute) {
                        // 说明当前事务现在可以在min_txn_node上执行
                        it->second->will_route_node = min_txn_node;
                        ownership_ok_txn_queues[min_txn_node].insert(affected_txn_id);
                        // 从candidate_txn_queues中删除该事务
                        candidate_txn_queues.erase(it->second);
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                        logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                        ", txn " + std::to_string(affected_txn_id) + 
                                        " MOVE to ownership_ok on node " + std::to_string(min_txn_node));
                #endif
                    }
                    else {
                        // 说明当前事务仍然不能在min_txn_node上执行，保持不变
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                        logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                                        ", txn " + std::to_string(affected_txn_id) + 
                                        " still cannot execute on node " + std::to_string(min_txn_node));
                #endif
                    }
                }
            }
        }
    }

    // 2. 剩余的ownership_ok_txn_queues中的事务加入到scheduled_txn_queues中
    for(auto txn_queue: ownership_ok_txn_queues) {
        for(auto tx_id: txn_queue) {
            auto it = txid_to_txn_map.find(tx_id);
            assert(it != txid_to_txn_map.end());
            node_id_t will_route_node = it->second->will_route_node;
            scheduled_txn_queues->at(will_route_node).push(it->second->txn);
            delete it->second; // 释放内存
            txid_to_txn_map.erase(it);
        #if LOG_KROUTER_SCHEDULING_DEBUG
            if(WarmupEnd)
            logger->info("[SmartRouter Scheduling] Final Scheduling ownership_ok txn " + std::to_string(tx_id) + 
                            " to execute on node " + std::to_string(will_route_node));
        #endif
        }
    }
    // 3. 构造返回结果
    return scheduled_txn_queues;
}

