#include "smart_router.h"
#include "indexed_priority_queue.h"

SmartRouter::SmartRouterResult SmartRouter::get_route_primary(TxnQueueEntry* txn, std::vector<table_id_t> &table_ids, std::vector<itemkey_t> &keys, 
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
            auto entry = lookup(txn, table_ids[i], keys[i], thread_conns);
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
            auto entry = lookup(txn, table_ids[i], keys[i], thread_conns);
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
            auto entry = lookup(txn, table_ids[i], keys[i], thread_conns);
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
        else if(SYSTEM_MODE == 8 || SYSTEM_MODE == 13) {
            // 计算page id
            auto entry = lookup(txn, table_ids[i], keys[i], thread_conns);
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
                    // 选择负载最低的节点进行路由, 找到workload_balance_penalty_weights_ 中最大的那个
                    size_t max_txn_workload_penalty_weight = 0;
                    for (int i=0; i<candidates.size(); i++) {
                        auto node = candidates[i];
                        if (workload_balance_penalty_weights_[node] > max_txn_workload_penalty_weight) {
                            max_txn_workload_penalty_weight = workload_balance_penalty_weights_[node];
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
                    // 选择负载最低的节点进行路由, 找到workload_balance_penalty_weights_ 中最大的那个
                    size_t max_txn_workload_penalty_weight = 0;
                    for (int i=0; i<candidates.size(); i++) {
                        auto node = candidates[i];
                        if (workload_balance_penalty_weights_[node] > max_txn_workload_penalty_weight) {
                            max_txn_workload_penalty_weight = workload_balance_penalty_weights_[node];
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
                    // 选择负载最低的节点进行路由, 找到workload_balance_penalty_weights_ 中最大的那个
                    size_t max_txn_workload_penalty_weight = 0;
                    for (int i=0; i<candidates.size(); i++) {
                        auto node = candidates[i];
                        if (workload_balance_penalty_weights_[node] > max_txn_workload_penalty_weight) {
                            max_txn_workload_penalty_weight = workload_balance_penalty_weights_[node];
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
            // update now the ownership table
            if(table_ids.size() != txn->accessed_page_ids.size()) assert(false);
            for (int i=0; i<keys.size(); i++) {
                ownership_table_->set_owner(txn, table_ids[i], keys[i], txn->accessed_page_ids[i], result.smart_router_id);
            }
        }
        else if(SYSTEM_MODE == 13) {
            // 基于page Metis的结果进行分区, 同时返回page到node的映射
            if (!WarmupEnd) {
                node_id_t metis_decision_node;
                metis_->build_internal_graph(page_to_node_map, &metis_decision_node);
            }
            else {
                // 填充page_to_node_map
                for (auto &[table_page_id, _] : page_to_node_map) {
                    node_id_t metis_node = metis_->get_metis_partitioning_result(table_page_id);
                    page_to_node_map[table_page_id] = metis_node;
                }
            }
            std::unordered_map<node_id_t, double> node_benefit_map;
            int sum_page = page_to_node_map.size();
            // 计算Metis带来的好处
            std::vector<double> metis_benefit(ComputeNodeCount, 0.0);
            for (const auto& [table_page_id, metis_node] : page_to_node_map) {
                if (metis_node == -1) continue;
                metis_benefit[metis_node] += 1.0 / sum_page;
            }
            std::string debug_info;
            double benefit1, benefit2, benefit3;
            for (const auto& [node_id, count] : node_count_basedon_page_access_last) {
                // benefit 计算：已经有ownership的页面比例
                benefit1 = static_cast<double>(count) / sum_page;
                // benefit 计算：满足metis分区结果的页面比例
                benefit2 = 0 * metis_benefit[node_id];
                // benefit 计算： 负载均衡, 当前节点路由的事务越少，benefit越高
                benefit3 = 2 * workload_balance_penalty_weights_[node_id];
                double total_benefit = benefit1 + benefit2 + benefit3;
                node_benefit_map[node_id] = total_benefit;
                debug_info += "(node=" + std::to_string(node_id) + 
                                ", benefit1=" + std::to_string(benefit1) + 
                                ", benefit2=" + std::to_string(benefit2) + 
                                ", benefit3=" + std::to_string(benefit3) + 
                                ", total_benefit=" + std::to_string(total_benefit) + "); ";
            }
            // 选择benefit最大的节点
            double max_benefit = -1.0;
            node_id_t best_node = -1;
            for (const auto& [node_id, benefit] : node_benefit_map) {
                if (benefit > max_benefit) {
                    max_benefit = benefit;
                    best_node = node_id;
                }
            }
            if (best_node != -1) {
                result.smart_router_id = best_node;
            }
            if (WarmupEnd) {
                logger->info("[SmartRouter Mode 13] " + debug_info + " selected node " + std::to_string(result.smart_router_id));
            }
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
        SchedulingCandidateTxn* scheduling_candidate_txn = new SchedulingCandidateTxn{txn, {}, {}, {}, {}, -1};
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
            auto entry = lookup(txn, table_ids[i], accounts_keys[i], thread_conns);
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
                                    " to execute on node " + std::to_string(will_route_node) + "at this time txn queue size: " +
                                    this->get_txn_queue_now_status());
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


// 这里不再将 std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> 作为返回值，而是直接将txn_queues_作为输入，
// 这样做的好处是可以不等待这个函数处理完成整个batch后再返回结果，而是可以在函数内部直接将调度好的事务放入对应的txn_queues_中，
// 从而可以降低worker端等待事务调度完成的结果，pipeline效率更高。
void SmartRouter::get_route_primary_batch_schedule_v2(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch, 
        std::vector<pqxx::connection *> &thread_conns) {
    
    assert(SYSTEM_MODE == 11); // 仅支持模式11
    
#if LOG_KROUTER_SCHEDULING_DEBUG
    if (WarmupEnd)
        logger->info("[SmartRouter Scheduling] Start scheduling for txn batch of size " + std::to_string(txn_batch->size()));
#endif

    // 计时
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // 并行前处理：每个 txn 独立生成 SchedulingCandidateTxn 和 involved_pages 列表
    size_t n = txn_batch->size();
    size_t thread_count = std::min<size_t>(worker_threads_, n);
    // size_t thread_count = 1;
    if (thread_count == 0) thread_count = 1;
    size_t chunk = (n + thread_count - 1) / thread_count;

    // per-thread local containers
    std::vector<std::unordered_map<tx_id_t, std::unique_ptr<SchedulingCandidateTxn>>> local_txid_maps(thread_count);
    std::vector<std::vector<std::pair<uint64_t, tx_id_t>>> local_page_pairs(thread_count);

    std::vector<std::future<void>> futs;
    futs.reserve(thread_count);

    for (size_t t = 0; t < thread_count; ++t) {
        size_t start = t * chunk;
        size_t end = std::min(n, start + chunk);
        futs.push_back(threadpool.enqueue([this, &txn_batch, start, end, t, &local_txid_maps, &local_page_pairs, &thread_conns]() {
            auto &local_map = local_txid_maps[t];
            auto &local_pairs = local_page_pairs[t];

            for (size_t idx = start; idx < end; ++idx) {
                TxnQueueEntry* txn = (*txn_batch)[idx];
                tx_id_t tx_id = txn->tx_id;
                int txn_type = txn->txn_type;
                itemkey_t account1 = txn->accounts[0];
                itemkey_t account2 = txn->accounts[1];

                std::unique_ptr<SchedulingCandidateTxn> sc = std::make_unique<SchedulingCandidateTxn>();
                sc->txn = txn;
                sc->will_route_node = -1;
                sc->is_scheduled = false; // !这里做出逻辑的更改，当一个事务调度完成之后，不再从 txid_to_txn_map 中删除，而是设置 is_scheduled 标志

                // 获取 keys 和 table_ids（smallbank_ 的函数是线程安全的）
                std::vector<itemkey_t> accounts_keys;
                std::vector<table_id_t> table_ids = smallbank_->get_table_ids_by_txn_type(txn_type);
                smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
                assert(table_ids.size() == accounts_keys.size());

                // 获取涉及的页面列表
                std::unordered_map<uint64_t, node_id_t> table_page_ids; // 高32位存table_id，低32位存page_id
                // lookup（内部会加 hot_mutex_），构造 involved_pages，并收集 page->tx 映射对
                for (size_t i = 0; i < accounts_keys.size(); ++i) {
                    auto entry = lookup(txn, table_ids[i], accounts_keys[i], const_cast<std::vector<pqxx::connection*>&>(thread_conns));
                    if (entry.page == kInvalidPageId) {
                        assert(false); // 这里不应该失败
                    }
                    uint64_t table_page_id = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
                    table_page_ids[table_page_id] = -1; // 初始化
                    sc->involved_pages.push_back(table_page_id);
                    local_pairs.emplace_back(table_page_id, tx_id);
                }

                // Warmup 阶段仍需要把 page 信息加入 metis 内部图，保留原有行为
                if (!WarmupEnd) {
                    node_id_t metis_decision_node;
                    metis_->build_internal_graph(table_page_ids, &metis_decision_node);
                }

                // 填充page_to_node_map
                for (int i = 0; i < sc->involved_pages.size(); ++i) {
                    uint64_t table_page_id = sc->involved_pages[i];
                    // 这里不需要加锁，因为一个事务只会被一个线程处理   
                    node_id_t metis_node = metis_->get_metis_partitioning_result(table_page_id);
                    sc->page_to_metis_node_vec.push_back(metis_node);
                }

                local_map[tx_id] = std::move(sc); // 存储到 local map
            }
        }));
    }

    // join workers
    for (auto &fut : futs) fut.get(); // get() 会抛异常并传播任务异常

    // 计时
    struct timespec merge_start_time, merge_end_time;
    clock_gettime(CLOCK_MONOTONIC, &merge_start_time);
    // 合并 local 结果到全局结构（单线程执行）
    std::unordered_map<tx_id_t, std::unique_ptr<SchedulingCandidateTxn>> txid_to_txn_map; // 记录每个事务ID对应的事务对象, 还没调度的事务
    std::unordered_map<uint64_t, std::vector<tx_id_t>> page_to_txn_map; // 记录每个页面对应的事务ID
    // 计算map需要的总大小，提前分配空间
    size_t total_txn_count = 0;
    size_t total_page_pair_count = 0;
    for (size_t t = 0; t < thread_count; ++t) {
        total_txn_count += local_txid_maps[t].size();
        total_page_pair_count += local_page_pairs[t].size();
    }
    txid_to_txn_map.reserve(total_txn_count);
    page_to_txn_map.reserve(total_page_pair_count);
    // 将 local maps/pairs 合并
    for (size_t t = 0; t < thread_count; ++t) {
        for (auto &p : local_txid_maps[t]) {
            txid_to_txn_map.emplace(p.first, std::move(p.second));
        }
        for (auto &pr : local_page_pairs[t]) {
            page_to_txn_map[pr.first].push_back(pr.second);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &merge_end_time);
    time_stats_.merge_global_txid_to_txn_map_ms += 
        (merge_end_time.tv_sec - merge_start_time.tv_sec) * 1000.0 + (merge_end_time.tv_nsec - merge_start_time.tv_nsec) / 1000000.0;

    // 计时
    struct timespec compute_conflict_start_time, compute_conflict_end_time;
    clock_gettime(CLOCK_MONOTONIC, &compute_conflict_start_time);
    // 求解事务之间的页面冲突关系
    std::unordered_set<tx_id_t> conflicted_txns;
    std::unordered_set<tx_id_t> unconflicted_txns; // 记录没有冲突的事务
    for(const auto& [page, txn_ids] : page_to_txn_map) {
        if(txn_ids.size() <= 1) continue; // 只有一个事务访问该页面，不存在冲突
        for (auto tx_id : txn_ids) {
            conflicted_txns.insert(tx_id);
        }
    }

    for(const auto& [tx_id, scheduling_txn] : txid_to_txn_map) {
        if(conflicted_txns.find(tx_id) == conflicted_txns.end()) {
            unconflicted_txns.insert(tx_id);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &compute_conflict_end_time);
    time_stats_.compute_conflict_ms += 
        (compute_conflict_end_time.tv_sec - compute_conflict_start_time.tv_sec) * 1000.0 + (compute_conflict_end_time.tv_nsec - compute_conflict_start_time.tv_nsec) / 1000000.0;

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.preprocess_txn_ms += 
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    #if LOG_KROUTER_SCHEDULING_DEBUG
    if(WarmupEnd){
        // print the remained txn_queue size, txn_queue[node_id]
        logger->info("[SmartRouter Scheduling] After preprocessing, remained last batch txn size: " 
            + [&]() {
                std::string txns_str;
                txns_str += "[";
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
                    int queue_size = txn_queues_[node_id]->size();
                    txns_str += "Node " + std::to_string(node_id) + ": " + std::to_string(queue_size) + ", ";
                }
                txns_str += "]";
                return txns_str;
            }() 
            + [&]() {
                std::string txns_str;
                txns_str += " batch_finished_flags: [";
                for(int i=0; i<ComputeNodeCount; i++) {
                    txns_str += std::to_string(batch_finished_flags[i]) + " ";
                }
                txns_str += "]";
                return txns_str;
            }());
    }
    #endif 
    // 这里还没有获取 ownership 信息，后续再处理，合并之后首先要求事务之间的页面冲突关系，通过倒排索引
    // 以上为batch pipeline预处理的部分
    // !同步点，标志着上一个batch的事务执行完成

    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    // !0.0 等待上一个batch所有db connector线程完成该批次的路由
    for(int i=0; i<ComputeNodeCount; i++) {
        std::unique_lock<std::mutex> lock(batch_mutex); 
        batch_cv.wait(lock, [this, i]() { 
            // process batch id为-1表示该计算节点的db connector线程还没有开始处理事务, 
            // txn_queues_[i]->get_process_batch_id() >= batch_id 表示事务执行速度较快，batch_finished_flags 已经被重置，get_process_batch_id()表示已经处理完成的batch id
            // if(batch_finished_flags[i] >= 0.5 * db_con_worker_threads || txn_queues_[i]->get_have_finished_batch_id() == -1 || txn_queues_[i]->get_have_finished_batch_id() >= batch_id) 
            //     return true;
            for(int i=0; i<ComputeNodeCount; i++) {
                // check if the queue is empty
                if(txn_queues_[i]->size() == 0 && txn_queues_[i]->is_shared_queue_empty()) {
                    return true;
                }
            }
            return false;
        });
    }

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.wait_last_batch_finish_ms += 
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
    logger->info("Waiting last batch finish time: " + std::to_string(
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0) + " ms");

    logger->info("[SmartRouter Scheduling] one db connector threads finished in processing batch " + std::to_string(batch_id));

    // --!0.1 说明该计算节点的所有线程已经跑完事务了, 重置该节点的batch完成标志
    // !0.1 说明该计算节点的大部分线程已经跑完事务了，步进batch id，通知已完成db connector线程可以开始处理下一批次
    {
        std::unique_lock<std::mutex> lock(batch_mutex); 
        batch_id ++;
        for(int i=0; i<ComputeNodeCount; i++) {
            txn_queues_[i]->set_process_batch_id(batch_id);
        }
        batch_cv.notify_all();
    }
    
    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // ownership_ok_txn_queues链表存储着完全满足ownership table的要求
    std::vector<std::unordered_set<tx_id_t>> ownership_ok_txn_queues(ComputeNodeCount);
    std::vector<std::vector<std::unordered_set<tx_id_t>>> ownership_ok_txn_queues_per_thread(thread_count, std::vector<std::unordered_set<tx_id_t>>(ComputeNodeCount));
    // candidate_txn_queues链表存储着候选事务
    std::unordered_set<tx_id_t> candidate_txn_ids;
    std::vector<std::unordered_set<tx_id_t>> candidate_txn_queues_per_thread(thread_count);
    // indexed_priority_queue.h
    std::vector<IPQ<tx_id_t, double, std::greater<double>>> candidate_txn_benefit_ipq(ComputeNodeCount);
    // 期望的每个计算节点的迁移页面的数量
    std::vector<std::atomic<int>> expected_page_transfer_count_per_node(ComputeNodeCount);

    // !1. 获取ownership信息填充到page_to_ownership_node_vec，同时完成对非冲突事务的调度ownership_ok_txn_queues_per_thread, candidate_txn_queues_per_thread
    std::atomic<int> unconflict_and_ownership_ok_txn_cnt, unconflict_and_ownership_cross_txn_cnt, unconflict_and_shared_txn_cnt;
    std::vector<std::atomic<int>> schedule_txn_cnt_per_node_this_batch(ComputeNodeCount);
    futs.clear();
    futs.reserve(thread_count);
    std::vector<double> compute_node_workload_benefit = this->workload_balance_penalty_weights_; // 负载均衡因子
    
    for (size_t t = 0; t < thread_count; ++t) {
        size_t start = t * chunk;
        size_t end = std::min(n, start + chunk);
        futs.push_back(threadpool.enqueue([this, &txn_batch, &txid_to_txn_map, start, end, t, &compute_node_workload_benefit,
                &ownership_ok_txn_queues_per_thread, &candidate_txn_queues_per_thread, &unconflicted_txns, 
                &unconflict_and_ownership_ok_txn_cnt, &unconflict_and_ownership_cross_txn_cnt, &unconflict_and_shared_txn_cnt, 
                &schedule_txn_cnt_per_node_this_batch, &expected_page_transfer_count_per_node]() {
            for (size_t idx = start; idx < end; ++idx) {
                tx_id_t tx_id = txn_batch->at(idx)->tx_id;
                SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id].get();
                if(sc == nullptr) assert(false); // 不可能出现的情况
                std::unordered_map<node_id_t, int> ownership_node_count;
                for (int i = 0; i < sc->involved_pages.size(); ++i) {
                    auto page = sc->involved_pages[i];
                    node_id_t ownership_node = ownership_table_->get_owner(page); 
                    if(ownership_node == -1) {
                        ownership_node = sc->page_to_metis_node_vec[i];
                        if(ownership_node == -1) {
                            ownership_node = page % ComputeNodeCount; // 如果没有owner，则随机分配一个节点
                        }
                    }
                    sc->page_to_ownership_node_vec.push_back(ownership_node);
                    ownership_node_count[ownership_node]++;
                }
                if(ownership_node_count.size() == 1) {
                    // 满足ownership entirely
                    node_id_t ownership_node = ownership_node_count.begin()->first;
                    sc->will_route_node = ownership_node;
                    if(unconflicted_txns.count(tx_id) > 0) {
                        // ! 如果该事务没有冲突，直接加入txn_queues, 工作线程可以直接处理
                        txn_queues_[ownership_node]->push_txn(sc->txn); // txn_queues_ 是 SmartRouter 的成员变量, TxnQueue中的push_txn函数是线程安全的
                        sc->is_scheduled = true; // 标记该事务已经被调度
                        this->routed_txn_cnt_per_node[ownership_node]++; // 总数
                        load_tracker_.record(ownership_node); // 最近w个事务
                        unconflict_and_ownership_ok_txn_cnt++;
                        schedule_txn_cnt_per_node_this_batch[ownership_node]++;
                    } else {
                        // 有冲突的事务，加入ownership_ok_txn_queues
                        // 再写一遍, ownership_ok_txn_queues 和 candidate_txn_queues 是并列的关系，他们的集合是所有还没有调度的事务，即 txid_to_txn_map 中is_scheduled为false的事务
                        ownership_ok_txn_queues_per_thread[t][ownership_node].insert(tx_id);
                    }
                } else {
                    // 记录为候选事务
                    int sum_pages = sc->involved_pages.size();
                    std::vector<double> metis_benefit(ComputeNodeCount, 0.0);
                    for(int i=0; i<sum_pages; i++) {
                        node_id_t metis_node = sc->page_to_metis_node_vec[i];
                        if(metis_node == -1) continue;
                        metis_benefit[metis_node] += 1.0 / sum_pages;
                    }
                    for(const auto& [node_id, count] : ownership_node_count) {
                        // benefit 计算：已经有ownership的页面比例
                        double benefit1 = static_cast<double>(count) / sum_pages;
                        // benefit 计算: 满足metis分区结果的页面比例
                        double benefit2 = metis_benefit[node_id];
                        // benefit 计算: 负载均衡，当前节点路由的事务越少，benefit越高
                        double benefit3 = 2 * compute_node_workload_benefit[node_id];
                        // 综合benefit
                        sc->node_benefit_map[node_id] = benefit1 + benefit2 + benefit3;
                    }
                    if(unconflicted_txns.count(tx_id) > 0) {
                        // 如果该事务没有冲突，可以直接调度
                        // 选择benefit最高的节点作为will_route_node
                        double max_benefit = -1.0;
                        int best_node = -1;
                        double min_benefit = 1e9;
                        int worst_node = -1;
                        for(const auto& [node_id, benefit] : sc->node_benefit_map) {
                            if(benefit > max_benefit) {
                                max_benefit = benefit;
                                best_node = node_id;
                            }
                            if(benefit < min_benefit) {
                                min_benefit = benefit;
                                worst_node = node_id;
                            }
                        }
                        if(max_benefit - min_benefit < 0.5) {
                            // !如果best_node和worst_node的benefit差距不大，则可以加入到共享队列中
                            txn_queues_[best_node]->push_txn_into_shared_queue(sc->txn);
                            sc->is_scheduled = true; // 标记该事务已经被调度
                            unconflict_and_shared_txn_cnt++;
                        }
                        else{
                            sc->will_route_node = best_node;
                            // !直接加入txn_queues, 工作线程可以直接处理
                            txn_queues_[best_node]->push_txn(sc->txn); // txn_queues_ 是 SmartRouter 的成员变量, TxnQueue中的push_txn函数是线程安全的
                            sc->is_scheduled = true; // 标记该事务已经被调度
                            this->routed_txn_cnt_per_node[best_node]++;
                            load_tracker_.record(best_node);
                            unconflict_and_ownership_cross_txn_cnt++;
                            schedule_txn_cnt_per_node_this_batch[best_node]++;
                            // 记录预期的页面迁移数量
                            int expected_page_transfer_cnt = sum_pages - ownership_node_count[best_node];
                            expected_page_transfer_count_per_node[best_node] += expected_page_transfer_cnt;
                        }
                    }
                    else {
                        // 如果有事务冲突，则加入候选队列
                        candidate_txn_queues_per_thread[t].insert(tx_id);
                    }
                }
            }
        }));
    }    

    // join workers
    for (auto &fut : futs) fut.get(); // get() 会抛异常并传播任务异常

    // 合并 local 结果到全局结构（单线程执行）
    for (size_t t = 0; t < thread_count; ++t) {
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            ownership_ok_txn_queues[node_id].insert(
                ownership_ok_txn_queues_per_thread[t][node_id].begin(), 
                ownership_ok_txn_queues_per_thread[t][node_id].end());
        }
        candidate_txn_ids.insert(
            candidate_txn_queues_per_thread[t].begin(), 
            candidate_txn_queues_per_thread[t].end());
    }
    // 构造 candidate_txn_benefit_ipq
    for(const auto& tx_id : candidate_txn_ids) {
        SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id].get();
        for(const auto& [node_id, benefit] : sc->node_benefit_map) {
            candidate_txn_benefit_ipq[node_id].insert(tx_id, benefit);
        }
    }

    
    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.ownership_retrieval_and_devide_unconflicted_txn_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    int ownership_ok_txn_total = 0;
    for(int i=0; i<ComputeNodeCount; i++) {
        ownership_ok_txn_total += ownership_ok_txn_queues[i].size();
    }
    // 校验 ownership_ok_txn_total + candidate_txn_ids.size() + unconflict_and_ownership_cross_txn_cnt + unconflict_and_ownership_ok_txn_cnt == txid_to_txn_map.size()
    assert(txid_to_txn_map.size() == ownership_ok_txn_total + candidate_txn_ids.size() + 
        unconflict_and_ownership_cross_txn_cnt.load() + unconflict_and_ownership_ok_txn_cnt.load() + unconflict_and_shared_txn_cnt.load());

    #if LOG_KROUTER_SCHEDULING_DEBUG
    if(WarmupEnd){
        // Parallel router processing including txn involved pages, conflict map, ownership info retrieval, parallel unconflicted txn scheduling done
        logger->info("Batch id: " + std::to_string(batch_id) + "Parallel router processing done.");
        // print txn_conflict_map_size and unconflicted_txns size 
        logger->info("Txn conflict map size: " + std::to_string(conflicted_txns.size()));
        logger->info("Ownership ok txn total size: " + std::to_string(ownership_ok_txn_total));
        for(int i=0; i<ComputeNodeCount; i++){
            logger->info("Node " + std::to_string(i) + 
                            " ownership_ok txn count: " + std::to_string(ownership_ok_txn_queues[i].size()));
        }
        logger->info("Candidate txn ids size: " + std::to_string(candidate_txn_ids.size()));
        logger->info("Unconflicted txns size: " + std::to_string(unconflicted_txns.size()));
        logger->info("Unconflicted and ownership ok txns scheduled: " + std::to_string(unconflict_and_ownership_ok_txn_cnt));
        logger->info("Unconflicted and ownership cross txns scheduled: " + std::to_string(unconflict_and_ownership_cross_txn_cnt));
        logger->info("Unconflicted and shared txns scheduled: " + std::to_string(unconflict_and_shared_txn_cnt));
        for(int i=0; i<ComputeNodeCount; i++){
            logger->info("Node " + std::to_string(i) + 
                            " scheduled unconflict txn count: " + std::to_string(schedule_txn_cnt_per_node_this_batch[i]));
        }
        for(int i=0; i<ComputeNodeCount; i++){
            logger->info("Node " + std::to_string(i) + 
                            " expected page transfer count: " + std::to_string(expected_page_transfer_count_per_node[i].load()));
        }
        // print txn_conflict_map
        // for(const auto& tx_id : conflicted_txns) {
        //     auto iter = txid_to_txn_map.find(tx_id); 
        //     logger->info("Txn " + std::to_string(tx_id) + 
        //                     " involves pages: " + [&]() {
        //                         std::string pages_str;
        //                         for(const auto& page : iter->second->involved_pages) {
        //                             pages_str += std::to_string(page) + " ";
        //                         }
        //                         pages_str += " page_to_metis_node: ";
        //                         for(const auto& metis_node : iter->second->page_to_metis_node_vec) {
        //                             pages_str += std::to_string(metis_node) + " ";
        //                         }
        //                         pages_str += " page_ownership_node: ";
        //                         for(const auto& ownership_node : iter->second->page_to_ownership_node_vec) {
        //                             pages_str += std::to_string(ownership_node) + " ";
        //                         }
        //                         return pages_str;
        //                     }());
        // }
        // // print conflict page_to_txn_map
        // for(const auto& [page, txn_ids] : page_to_txn_map) {
        //     if(txn_ids.size() <= 1) continue; // 说明是非冲突的页面，没必要输出，直接往下走
        //     logger->info("Page " + std::to_string(page) + 
        //                     " involved by txns: " + [&]() {
        //                         std::string txns_str;
        //                         for(const auto& tx_id : txn_ids) {
        //                             txns_str += std::to_string(tx_id) + " ";
        //                         }
        //                         return txns_str;
        //                     }());
        // }
        // // print ownership_ok_txn_queues
        // for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
        //     std::string txns_str;
        //     for(const auto& tx_id : ownership_ok_txn_queues[node_id]) {
        //         txns_str += std::to_string(tx_id) + " ";
        //     }
        //     logger->info("Node " + std::to_string(node_id) + 
        //                     " ownership_ok_txn_queues: " + txns_str);
        // }
    }
    #endif

    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // !2. 单线程进行调度决策
    while(!candidate_txn_ids.empty()){
        
        // 1.1 从候选事务中选择下一个要调度的事务进行所有权迁移的计划
        int min_txn_node = -1;
        int min_txn_count = INT32_MAX;
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            // int workload = schedule_txn_cnt_per_node_this_batch[node_id] + ownership_ok_txn_queues[node_id].size(); 
            // !采用更准确的当前负载作为衡量标准, 实时看一下txn_queues_的大小, 而不是schedule_txn_cnt_per_node_this_batch
            // int workload = txn_queues_[node_id]->size() + ownership_ok_txn_queues[node_id].size(); 
            int workload = expected_page_transfer_count_per_node[node_id].load(); // 采用预期的页面迁移数量作为负载衡量标准
            if(workload < min_txn_count) { 
                min_txn_count = workload; 
                min_txn_node = node_id;
            }
        }
        double max_benefit_score = -1;
        SchedulingCandidateTxn* selected_candidate_txn = nullptr;
        // ! 这里不再使用for循环遍历candidate_txn_queues来选择事务，而是使用candidate_txn_benefit_ipq来选择
        assert(candidate_txn_benefit_ipq[min_txn_node].size() > 0); // 这里不可能为空
        auto [best_tx_id, best_benefit] = candidate_txn_benefit_ipq[min_txn_node].pop();
        selected_candidate_txn = txid_to_txn_map[best_tx_id].get();
        if(selected_candidate_txn->is_scheduled == true) {
            // 说明该事务已经被调度过了，继续下一个循环
            continue;
        }
        max_benefit_score = best_benefit;

    // #if LOG_KROUTER_SCHEDULING_DEBUG
    //     if(WarmupEnd){
    //         if(selected_candidate_txn != nullptr){
    //             logger->info("[SmartRouter Scheduling] Selected txn " + std::to_string(selected_candidate_txn->txn->tx_id) + 
    //                         " to transfer pages to node " + std::to_string(min_txn_node) + 
    //                         " with benefit score " + std::to_string(max_benefit_score));
    //         }
    //         else {
    //             logger->info("[SmartRouter Scheduling] No suitable txn found to transfer pages to node " + std::to_string(min_txn_node));
    //         }
    //     }
    // #endif
        
        // 1.2 找到合适的不满足ownership entirely的事务，进行页面转移计划的制订
        std::vector<uint64_t> transfer_pages;
        if(selected_candidate_txn != nullptr){
            assert(selected_candidate_txn->page_to_ownership_node_vec.size() == selected_candidate_txn->involved_pages.size());
            for(int i=0; i<selected_candidate_txn->involved_pages.size(); i++){
                auto page = selected_candidate_txn->involved_pages[i];
                node_id_t owner_node = selected_candidate_txn->page_to_ownership_node_vec[i];
                if(owner_node != min_txn_node){
                    transfer_pages.push_back(page);
                }
            }
        }
        // 记录预期的页面迁移数量
        expected_page_transfer_count_per_node[min_txn_node] += transfer_pages.size();

        // 1.3 进行事务的编排，找到转移页面后，原来可以满足ownership entirely的事务可能不满足的，将这些事务先执行
        std::vector<TxnQueueEntry*> schedule_txn;
        node_id_t schedule_node = -1;
        std::vector<TxnQueueEntry*> next_time_schedule_txn; // 记录下次调度的事务
        for(auto transfer_page : transfer_pages){
            // #if LOG_KROUTER_SCHEDULING_DEBUG
            //     if(WarmupEnd)
            //     logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
            //                     " ownership transferred to node " + std::to_string(min_txn_node) + 
            //                     ", check affected txns:");
            // #endif
            for(auto affected_txn_id : page_to_txn_map[transfer_page]){
                // 检查affected_txn_id是否在ownership_ok_txn_queues中
                auto it = txid_to_txn_map.find(affected_txn_id);
                if (it->second->is_scheduled) continue; // 说明这个事务已经被调度过了
                node_id_t will_route_node = it->second->will_route_node;
                if(will_route_node != -1) {
                    // 说明这个事务之前可以在will_route_node上执行， 但是这个transfer page转移到新的节点之后，他就不可以继续执行了
                    assert(ownership_ok_txn_queues[will_route_node].count(affected_txn_id) == 1);
                    // 将该事务从ownership_ok_txn_queues中删除，表示已不能执行
                    ownership_ok_txn_queues[will_route_node].erase(affected_txn_id);
                    // !将该事务加入到txn_queues中, 可以执行
                    schedule_txn.push_back(it->second->txn);
                    if(schedule_node == -1) schedule_node = will_route_node; 
                    else if(schedule_node != will_route_node) 
                        logger->warning("[SmartRouter Scheduling] Warning: schedule_node " + std::to_string(schedule_node) + 
                                        " different from will_route_node " + std::to_string(will_route_node) + 
                                        " for affected txn " + std::to_string(affected_txn_id));
                    // else assert(schedule_node == will_route_node); // 说明这些事务之前都是在同一个节点上执行的
                    this->routed_txn_cnt_per_node[will_route_node]++;
                    load_tracker_.record(will_route_node);
                    schedule_txn_cnt_per_node_this_batch[will_route_node]++;
                    // !标记该事务已经被调度
                    it->second->is_scheduled = true;
                // #if LOG_KROUTER_SCHEDULING_DEBUG
                //     if(WarmupEnd)
                //     logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                //                     " transferred to node " + std::to_string(min_txn_node) + 
                //                     ", scheduling previously ownership_ok txn " + std::to_string(affected_txn_id) + 
                //                     " to execute on node " + std::to_string(will_route_node) + "at this time txn queue size: " +
                //                     this->get_txn_queue_now_status());
                // #endif
                }
                else {
                    // 加入next_time_schedule_txn
                    next_time_schedule_txn.push_back(it->second->txn);
                    // 如果不能执行，首先更新迁移页面的owner信息
                    for(int i=0; i<it->second->involved_pages.size(); i++){
                        auto page = it->second->involved_pages[i];
                        node_id_t owner_node = it->second->page_to_ownership_node_vec[i];
                        if(page == transfer_page){
                            it->second->page_to_ownership_node_vec[i] = min_txn_node;
                        }
                    }
                    // 确定当前事务在迁移页面后，能否执行
                    bool can_execute = true;
                    for(int i=0; i<it->second->involved_pages.size(); i++){ 
                        auto page = it->second->involved_pages[i];
                        node_id_t owner_node = it->second->page_to_ownership_node_vec[i];
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
                        candidate_txn_ids.erase(affected_txn_id);
                // #if LOG_KROUTER_SCHEDULING_DEBUG
                //     if(WarmupEnd)
                //         logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                //                         ", txn " + std::to_string(affected_txn_id) + 
                //                         " MOVE to ownership_ok on node " + std::to_string(min_txn_node));
                // #endif
                    }
                    else {
                        // 说明当前事务仍然不能在min_txn_node上执行，保持不变
                        // 更新candidate_txn_benefit_ipq中的benefit值
                        int sum_pages = it->second->involved_pages.size();
                        std::vector<double> metis_benefit(ComputeNodeCount, 0.0);
                        
                        for(int i=0; i<sum_pages; i++) {
                            auto page = it->second->involved_pages[i];
                            node_id_t metis_node = it->second->page_to_metis_node_vec[i];
                            if(metis_node == -1) continue;
                            metis_benefit[metis_node] += 1.0 / sum_pages;
                        }
                        std::vector<double> ownership_benefit(ComputeNodeCount, 0.0);
                        for(int i=0; i<sum_pages; i++) {
                            auto page = it->second->involved_pages[i];
                            node_id_t ownership_node = it->second->page_to_ownership_node_vec[i];
                            ownership_benefit[ownership_node] += 1.0 / sum_pages;
                        }
                        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                            // benefit 计算：已经有ownership的页面比例
                            double benefit1 = ownership_benefit[node_id];
                            // benefit 计算: 满足metis分区结果的页面比例
                            double benefit2 = metis_benefit[node_id];
                            // benefit 计算: 负载均衡，当前节点路由的事务越少，benefit越高
                            double benefit3 = 2 * compute_node_workload_benefit[node_id];
                            // 综合benefit
                            double total_benefit = benefit1 + benefit2 + benefit3;
                            // 更新ipq中的benefit值
                            candidate_txn_benefit_ipq[node_id].update(affected_txn_id, total_benefit);
                        }
                // #if LOG_KROUTER_SCHEDULING_DEBUG
                //     if(WarmupEnd)
                //         logger->info("[SmartRouter Scheduling] Page " + std::to_string(transfer_page) + 
                //                         ", txn " + std::to_string(affected_txn_id) + 
                //                         " still cannot execute on node " + std::to_string(min_txn_node));
                // #endif
                    }
                }
            }
        }
        // ! 将schedule_txn中的事务加入到txn_queues_头部先执行了
        if(!schedule_txn.empty()) txn_queues_[schedule_node]->push_txn_front(schedule_txn);
        // !记录事务偏序依赖
        for(auto txn: next_time_schedule_txn){
            for(auto prior_txn : schedule_txn){
                txn->dependencies.push_back(prior_txn->tx_id);
                // #if LOG_KROUTER_SCHEDULING_DEBUG
                //     if(WarmupEnd)
                //     logger->info("[SmartRouter Scheduling] Txn " + std::to_string(txn->tx_id) + 
                //                     " depends on prior txn " + std::to_string(prior_txn->tx_id) );
                // #endif
            }
        }
    }

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.process_conflicted_txn_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    // 2. 剩余的ownership_ok_txn_queues中的事务加入到txn_queues_中
    futs.clear();
    futs.reserve(ComputeNodeCount);
    for(int i=0; i<ComputeNodeCount; i++) {
        auto& txn_queue = ownership_ok_txn_queues[i];
        futs.push_back(threadpool.enqueue([this, i, &txn_queue, &txid_to_txn_map, &schedule_txn_cnt_per_node_this_batch]() {
            std::vector<TxnQueueEntry*> to_schedule_txns;
            // 构建to_schedule_txns
            for(auto tx_id: txn_queue) {
                auto it = txid_to_txn_map.find(tx_id);
                assert(it != txid_to_txn_map.end());
                assert(it->second->is_scheduled == false); // 说明这个事务还没有被调度
                to_schedule_txns.push_back(it->second->txn);
                this->routed_txn_cnt_per_node[i]++;
                load_tracker_.record(i); // 记录负载
                schedule_txn_cnt_per_node_this_batch[i]++;
                it->second->is_scheduled = true; // 标记该事务已经被调度
            }
            // 批量加入txn_queues_
            txn_queues_[i]->push_txn_back_batch(to_schedule_txns);
            #if LOG_KROUTER_SCHEDULING_DEBUG
                if(WarmupEnd)
                logger->info("[SmartRouter Scheduling] Final Scheduling ownership_ok txn to execute on node " + std::to_string(i) + 
                                ", count: " + std::to_string(to_schedule_txns.size()));
            #endif
        }));
    }
    // join workers
    for (auto &fut : futs) fut.get(); // get() 会抛异常并传播任务异常
    logger->info("batch schedule finish" + this->get_txn_queue_now_status() +
                ", expected page transfer count per node: " + [&]() {
                    std::string s;
                    for (size_t i = 0; i < expected_page_transfer_count_per_node.size(); ++i) {
                        s += "Node " + std::to_string(i) + ": " + std::to_string(expected_page_transfer_count_per_node[i].load()) + ", ";
                    }
                    return s;
                }());
    return ;
}

// // 计算负载均衡相关的惩罚权重
// std::vector<double> SmartRouter::compute_load_balance_penalty_weights() {
//     std::vector<double> penalty_weights(ComputeNodeCount, 0.0);
//     std::vector<int> compute_vector(ComputeNodeCount, 0);
//     // for(int i=0; i<ComputeNodeCount; i++) {
//     //     compute_vector[i] = this->txn_queues_[i]->size();
//     // }

//     for(int i=0; i<ComputeNodeCount; i++) {
//         compute_vector[i] = this->routed_txn_cnt_per_node[i];
//     }
//     // 计算平均事务
//     int total_routed_txn = 0;
//     for(int i=0; i<ComputeNodeCount; i++) {
//         total_routed_txn += compute_vector[i];
//     }
//     double average_routed_txn = static_cast<double>(total_routed_txn) / ComputeNodeCount;

//     // 计算标准差
//     double sum_squared_diff = 0.0;
//     for(int i=0; i<ComputeNodeCount; i++) {
//         double diff = static_cast<double>(compute_vector[i]) - average_routed_txn;
//         sum_squared_diff += diff * diff;
//     }
//     double stddev = std::sqrt(sum_squared_diff / ComputeNodeCount) + 10.0; // 防止除以0

//     // 计算惩罚权重, 这里用Z-score的方法
//     for(int i=0; i<ComputeNodeCount; i++) {
//         double z_score = (static_cast<double>(compute_vector[i]) - average_routed_txn) / stddev;
        
//         // Sigmoid函数映射到(0, 1)
//         penalty_weights[i] = 1.0 / (1.0 + std::exp(z_score));
//     }
//     logger->info("Compute node workload benefit: " + [&]() {
//         std::string s;
//         for (size_t i = 0; i < penalty_weights.size(); ++i) {
//             s += "Node " + std::to_string(i) + ": " + std::to_string(penalty_weights[i]) + ", ";
//         }
//         return s;
//     }());

//     workload_balance_penalty_weights_ = penalty_weights; // 更新成员变量

//     return penalty_weights;
// }


// smoothstep: 3t^2 - 2t^3, t in [0,1]
static double smoothstep(double t) {
    if (t <= 0.0) return 0.0;
    if (t >= 1.0) return 1.0;
    return t * t * (3.0 - 2.0 * t);
}

/*
 * 说明：
 *  - ComputeNodeCount: 节点数（类成员或全局）
 *  - routed_txn_cnt_per_node: 各节点已路由事务计数（类成员）
 */
std::vector<double> SmartRouter::compute_load_balance_penalty_weights() {
    const double EPS = 0.10;     // 小变动阈值 10%
    const double THR = 0.30;     // 明显区分阈值 30%
    const double small_m = 0.02; // 在 <=EPS 时的最大微小幅度（可调）
    const double large_m = 0.30; // 在 >=THR 时的最大幅度（可调）
    const double base = 0.5;     // 中心值

    std::vector<double> penalty_weights(ComputeNodeCount, base);
    std::vector<int> compute_vector(ComputeNodeCount, 0);

    // 复制计数
    long long total = 0;
    // for(int i=0; i<ComputeNodeCount; i++) {
    //     compute_vector[i] = this->txn_queues_[i]->size();
    //     total += compute_vector[i];
    // }
    // for (int i = 0; i < ComputeNodeCount; ++i) {
    //     compute_vector[i] = this->routed_txn_cnt_per_node[i];
    //     total += compute_vector[i];
    // }
    compute_vector = load_tracker_.get_loads(); // 使用滑动窗口负载
    for (int i = 0; i < ComputeNodeCount; ++i) {
        total += compute_vector[i];
    }
    double avg = (ComputeNodeCount > 0) ? static_cast<double>(total) / ComputeNodeCount : 0.0;
    if (avg <= 0.0) {
        // 避免除0：都返回base
        std::fill(penalty_weights.begin(), penalty_weights.end(), base);
        workload_balance_penalty_weights_ = penalty_weights;
        return penalty_weights;
    }

    for (int i = 0; i < ComputeNodeCount; ++i) {
        double r = (static_cast<double>(compute_vector[i]) - avg) / avg; // 相对差
        double sgn = (r >= 0.0) ? -1.0 : +1.0;
        double absr = std::abs(r);

        double mag = 0.0;
        if (absr <= EPS) {
            // 线性微小增长到 small_m
            mag = (absr / EPS) * small_m;
        } else if (absr >= THR) {
            // 超过 THR，直接最大幅度
            mag = large_m;
        } else {
            // 在 (EPS, THR) 之间做平滑过渡
            double t = (absr - EPS) / (THR - EPS); // 0..1
            double s = smoothstep(t);
            mag = small_m + s * (large_m - small_m);
        }

        double p = base + sgn * mag;
        // 限幅到 0..1（一般可选地限制到 [0.2,0.8]）
        p = std::clamp(p, 0.0, 1.0);
        penalty_weights[i] = p;
    }

    // 记录到成员变量（如果需要）
    workload_balance_penalty_weights_ = penalty_weights;

    // 可选：打印/记录用于调试
    logger->info("Compute node workload penalty: " + [&]() {
        std::string s;
        for (size_t i = 0; i < penalty_weights.size(); ++i) {
            s += "Node " + std::to_string(i) + " workload:" + std::to_string(compute_vector[i]) + 
                 " Penalty:" + std::to_string(penalty_weights[i]) + ", ";
        }
        return s;
    }());

    return penalty_weights;
}
