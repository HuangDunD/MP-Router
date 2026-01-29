#include "smart_router.h"
#include "indexed_priority_queue.h"

// check 是否事务满足 ownership, 如果满足, 返回满足的节点集合
// 同时传入 ownership_node_count 以便于后续计算分数
std::vector<node_id_t> SmartRouter::checkif_txn_ownership_ok(SchedulingCandidateTxn* sc){
    std::vector<node_id_t> ownership_ok_nodes;
    // 判断该事务是否满足ownership entirely
    int max_ownership_count = 0;
    std::vector<node_id_t> candidate_ownership_nodes;
    for(int node_id = 0; node_id < ComputeNodeCount; ++node_id) {
        int count = sc->ownership_node_count[node_id];
        if (count == 0) continue;
        
        if(count > max_ownership_count) {
            max_ownership_count = count;
            candidate_ownership_nodes.clear();
            candidate_ownership_nodes.push_back(node_id);
        } else if(count == max_ownership_count) {
            candidate_ownership_nodes.push_back(node_id);
        }
    }
    if(max_ownership_count == sc->involved_pages.size()) {
        // ownership entirely
        for(const auto& node_id : candidate_ownership_nodes) {
            ownership_ok_nodes.push_back(node_id);
        }
    }
    else{
        // ownership not entirely
        ownership_ok_nodes = {};
    }
    return ownership_ok_nodes;
}

void SmartRouter::update_sc_ownership_count(SchedulingCandidateTxn* sc, int page_idx, const std::pair<std::vector<node_id_t>, bool>& old_ownership, const std::pair<std::vector<node_id_t>, bool>& new_ownership) {
    // 1. revert old ownership count
    bool rw = sc->rw_flags[page_idx];
    
    // 如果old_ownership为空（初始化情况），则不需要decrement
    if (!old_ownership.first.empty()) {
        for(const auto& ownership_node : old_ownership.first) {
            if(old_ownership.second == 1) { // Exclusive
                 sc->ownership_node_count[ownership_node]--;
            } else{ // Shared
                if(!rw) { // Read op
                    sc->ownership_node_count[ownership_node]--;
                }
            }
        }
    }

    // 2. add new ownership count
    for(const auto& ownership_node : new_ownership.first) {
        if(new_ownership.second == 1) { // Exclusive
             sc->ownership_node_count[ownership_node]++;
        } else{ // Shared
            if(!rw) { // Read op
                sc->ownership_node_count[ownership_node]++;
            }
        }
    }
}

void SmartRouter::compute_benefit_for_node( SchedulingCandidateTxn* sc, std::vector<int>& ownership_node_count, std::vector<double> &compute_node_workload_benefit,
        double metis_benefit_weight, double ownership_benefit_weight, double load_balance_benefit_weight) { 
    // 计算综合收益
    int sum_pages = sc->involved_pages.size();
    if(sc->node_benefit_map.size() != ComputeNodeCount) sc->node_benefit_map.assign(ComputeNodeCount, 0.0);
    std::vector<double> metis_benefit_vec(ComputeNodeCount, 0.0);
    if(metis_benefit_weight > 0 ) {
        for(int i=0; i<sum_pages; i++) {
            node_id_t metis_node = sc->page_to_metis_node_vec[i];
            if(metis_node == -1) continue;
            metis_benefit_vec[metis_node] += 1.0 / sum_pages;
        }
    }
    for(int i=0; i<ComputeNodeCount; i++) {
        // 计算每一个节点的benefit
        // benefit 计算：已经有ownership的页面比例
        double benefit1 = ownership_node_count[i] * ownership_benefit_weight / sum_pages;
        // benefit 计算: 满足metis分区结果的页面比例
        double benefit2 = 0.0;
        if(metis_benefit_weight > 0) {
            benefit2 = metis_benefit_weight * metis_benefit_vec[i];
        }
        // benefit 计算: 负载均衡，当前节点路由的事务越少，benefit越高
        double benefit3 = load_balance_benefit_weight * compute_node_workload_benefit[i];
        // 综合benefit
        sc->node_benefit_map[i] = benefit1 + benefit2 + benefit3; 
    }
}

SmartRouter::SmartRouterResult SmartRouter::get_route_primary(TxnQueueEntry* txn, std::vector<table_id_t> &table_ids, std::vector<itemkey_t> &keys, std::vector<bool> &rw) {
    SmartRouterResult result;
    if (table_ids.size() != keys.size() || table_ids.empty()) {
        result.error_message = "Mismatched or empty table_ids and keys";
        return result;
    }

    std::unordered_map<uint64_t, node_id_t> page_to_node_map; // 高32位存table_id，低32位存page_id, for SYSTEM_MODE 3
    std::unordered_map<node_id_t, int> node_count_basedon_key_access_last; // 基于key_access_last的计数, for SYSTEM_MODE 5
    std::unordered_map<uint64_t, node_id_t> table_key_id;  // 高32位存table_id，低32位存key, for SYSTEM_MODE 6
    std::unordered_map<node_id_t, int> node_count_basedon_page_access_last; // 基于page_access_last的计数, for SYSTEM_MODE 7
    // std::unordered_map<uint64_t, node_id_t> page_ownership_to_node_map; // 找到ownership对应的节点的映射关系, for SYSTEM_MODE 8
    std::string debug_info;
    #if LOG_METIS_OWNERSHIP_DECISION
    debug_info = "txn_type: " + std::to_string(txn->txn_type) + "; ";
    #endif
    page_to_node_map.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if(SYSTEM_MODE == 2) {
            auto entry = lookup(txn, table_ids[i], keys[i]);
            // 计算page id
            if (entry.page == kInvalidPageId) {
                result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                    ", key=" + std::to_string(keys[i]) + ")";
                assert(false); // 这里不应该失败
            }
            uint64_t table_page = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
            page_to_node_map[table_page] = -1;
        }
        else if(SYSTEM_MODE == 3) {
            auto entry = lookup(txn, table_ids[i], keys[i]);
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
            auto entry = lookup(txn, table_ids[i], keys[i]);
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
            auto entry = lookup(txn, table_ids[i], keys[i]);
            // 计算page id
            if (entry.page == kInvalidPageId) {
                result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                    ", key=" + std::to_string(keys[i]) + ")";
                assert(false); // 这里不应该失败
            }
            bool rw_flag = rw[i];
            auto owner_stats = ownership_table_->get_owner(table_ids[i], entry.page);
            for(auto node: owner_stats.first) {
                node_count_basedon_page_access_last[node]++;
            }
            #if LOG_METIS_OWNERSHIP_DECISION
            debug_info += "(table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                            ", page=" + std::to_string(entry.page) + ", owners=[";
            for(auto node: owner_stats.first) {
                debug_info += std::to_string(node) + ",";
            }
            debug_info += "], mode=" + std::to_string(owner_stats.second) + "); ";
            #endif
        }
        else if(SYSTEM_MODE == 8 || SYSTEM_MODE == 13 || (SYSTEM_MODE >= 23 && SYSTEM_MODE <= 25)) {
            // 计算page id
            auto entry = lookup(txn, table_ids[i], keys[i]);
            if (entry.page == kInvalidPageId) {
                result.error_message = "[warning] Lookup failed for (table_id=" + std::to_string(table_ids[i]) +
                                    ", key=" + std::to_string(keys[i]) + ")";
                assert(false); // 这里不应该失败
            }
            uint64_t table_page_id_val = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
            page_to_node_map[table_page_id_val] = -1; // 初始化
            bool rw_flag = rw[i];
            auto owner_stats = ownership_table_->get_owner(table_ids[i], entry.page);
            for(auto node: owner_stats.first) {
                node_count_basedon_page_access_last[node]++;
            }
            #if LOG_METIS_OWNERSHIP_DECISION
            debug_info += "(table_id=" + std::to_string(table_ids[i]) + ", key=" + std::to_string(keys[i]) + 
                            ", page=" + std::to_string(entry.page) + ", owners=[";
            for(auto node: owner_stats.first) {
                debug_info += std::to_string(node) + ",";
            }
            debug_info += "], mode=" + std::to_string(owner_stats.second) + "); ";
            #endif
        }
        else { 
            assert(false); // unknown mode
        }
    }

    try {
        if (SYSTEM_MODE == 2) {
            // 对 page 做hash
            std::vector<int> key_hash_cnt(ComputeNodeCount, 0);
            for(auto page: page_to_node_map) { 
                // 使用乘法Hash打散，避免直接取模导致的热点集中
                size_t hash_val = page.first * 9973; 
                key_hash_cnt[hash_val % ComputeNodeCount]++;
            }
            // 找出最大值
            // int max_val = *std::max_element(key_hash_cnt.begin(), key_hash_cnt.end());

            // // 收集所有拥有最大值的节点
            // std::vector<int> candidates;
            // for(int i=0; i<ComputeNodeCount; i++) {
            //     if(key_hash_cnt[i] == max_val) {
            //         candidates.push_back(i);
            //     }
            // }

            // if(candidates.size() == 1) {
            //     result.smart_router_id = candidates[0];
            // } else {
            //     result.smart_router_id = candidates[rand() % candidates.size()];
            // }
            std::unordered_map<node_id_t, double> node_benefit_map;
            double benefit1, benefit3;
            for(int i=0; i<ComputeNodeCount; i++) {
                // benefit 计算：满足metis分区结果的页面比例，这里是hash分区结果
                benefit1 = static_cast<double>(key_hash_cnt[i]) / page_to_node_map.size();
                // benefit 计算: 负载均衡，当前节点路由的事务越少，benefit越高
                benefit3 = workload_balance_penalty_weights_[i] + remain_queue_balance_penalty_weights_[i];
                // 综合benefit
                double total_benefit = benefit1 + benefit3; 
                node_benefit_map[i] = total_benefit;
            }
            // 找出最大benefit的节点
            double max_benefit = -1.0;
            std::vector<node_id_t> candidate_nodes;
            for (const auto& [node, benefit] : node_benefit_map) {
                if (benefit > max_benefit) {
                    max_benefit = benefit;
                    candidate_nodes.clear();
                    candidate_nodes.push_back(node);
                } else if (benefit == max_benefit) {
                    candidate_nodes.push_back(node);
                }
            }
            if (!candidate_nodes.empty()) {
                result.smart_router_id = candidate_nodes[rand() % candidate_nodes.size()];
            } else {
                result.smart_router_id = rand() % ComputeNodeCount;
            }
        }
        else if (SYSTEM_MODE == 3) {
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
            // 找出满足所有权的节点候选集
            int max_owner_count = 0;
            std::vector<node_id_t> owner_candidates;
            for (const auto& [node, count] : node_count_basedon_page_access_last) {
                if (count > max_owner_count) {
                    max_owner_count = count;
                    owner_candidates.clear();
                    owner_candidates.push_back(node);
                } else if (count == max_owner_count) {
                    owner_candidates.push_back(node);
                }
            }
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
                else if(max_owner_count == keys.size()) {
                    // ownership_table_中只有一个page的映射信息
                    // 随机一个ownership_node从owner_candidates中
                    assert(!owner_candidates.empty());
                    result.smart_router_id = owner_candidates[rand() % owner_candidates.size()];
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisMissingAndOwnershipEntirely);
                }
                else if(max_owner_count < keys.size()) {
                    // ownership_table_中有多个page的映射信息
                    assert(!owner_candidates.empty());
                    // 随机选择出现次数最多的节点
                    node_id_t min_txn_node = -1;
                    // 选择负载最低的节点进行路由, 找到workload_balance_penalty_weights_ 中最大的那个
                    size_t max_txn_workload_penalty_weight = 0;
                    for (int i=0; i<owner_candidates.size(); i++) {
                        auto node = owner_candidates[i];
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
            else if (ret_code == 1 || ret_code == 2) {
                // entire affinity, 所有page都映射到同一个节点
                assert(!page_to_node_map.empty());
                assert(metis_decision_node != -1);
                if(node_count_basedon_page_access_last.empty()) {
                    // ownership_table_中没有任何page的映射信息, 即涉及到的页面
                    result.smart_router_id = metis_decision_node;
                    log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipMissing);
                }
                else {
                    assert(!owner_candidates.empty());
                    node_id_t candidate_node = -1;
                    int i;
                    for(i=0; i<owner_candidates.size(); i++) {
                        if (owner_candidates[i] == metis_decision_node) {
                            candidate_node = owner_candidates[i];
                            break;
                        }
                    }
                    if(i >= owner_candidates.size()) {
                        // candidates中没有metis_decision_node, 那就根据负载选择一个节点
                        node_id_t min_txn_node = -1;
                        // 选择负载最低的节点进行路由, 找到workload_balance_penalty_weights_ 中最大的那个
                        size_t max_txn_workload_penalty_weight = 0;
                        for (int i=0; i<owner_candidates.size(); i++) {
                            auto node = owner_candidates[i];
                            if (workload_balance_penalty_weights_[node] > max_txn_workload_penalty_weight) {
                                max_txn_workload_penalty_weight = workload_balance_penalty_weights_[node];
                                min_txn_node = node;
                            }
                        }
                        assert(min_txn_node != -1);
                        candidate_node = min_txn_node;
                    }
                    if(candidate_node == metis_decision_node) {
                        // !两者一致
                        result.smart_router_id = metis_decision_node;
                        if(ret_code == 1){
                            if(max_owner_count == keys.size()) {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyEqual);
                            }
                            else {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossEqual);
                            }
                        }
                        else if(ret_code == 2) {
                            if(max_owner_count == keys.size()) {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyEqual);
                            }
                            else {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossEqual);
                            }
                        }
                    }
                    else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = candidate_node;
                        if(ret_code == 1){
                            if(max_owner_count == keys.size()) {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipEntirelyUnequal);
                            }
                            else {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisEntirelyAndOwnershipCrossUnequal);
                            }
                        }
                        else if(ret_code == 2) {
                            if(max_owner_count == keys.size()) {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipEntirelyUnequal);
                            }
                            else {
                                log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisPartialAndOwnershipCrossUnequal);
                            }
                        }
                    }
                }
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
                else{
                    assert(!owner_candidates.empty());
                    // 找到routed_txn_cnt_per_node中最小的节点
                    node_id_t min_txn_node = -1;
                    // 选择负载最低的节点进行路由, 找到workload_balance_penalty_weights_ 中最大的那个
                    size_t max_txn_workload_penalty_weight = 0;
                    for (int i=0; i<owner_candidates.size(); i++) {
                        auto node = owner_candidates[i];
                        if (workload_balance_penalty_weights_[node] > max_txn_workload_penalty_weight) {
                            max_txn_workload_penalty_weight = workload_balance_penalty_weights_[node];
                            min_txn_node = node;
                        }
                    }
                    assert(min_txn_node != -1);
                    node_id_t ownership_node = min_txn_node;
                    if(ownership_node == metis_decision_node) {
                        // !两者一致
                        result.smart_router_id = metis_decision_node;
                        if(max_owner_count == keys.size()) {
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyEqual);
                        }
                        else {
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossEqual);
                        }
                    } else {
                        // !两者不一致, 优先选择ownership_node
                        result.smart_router_id = ownership_node;
                        if(max_owner_count == keys.size()) {
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipEntirelyUnequal);
                        }
                        else {
                            log_metis_ownership_based_router_result(result, debug_info, MetisOwnershipDecisionType::MetisCrossAndOwnershipCrossUnequal);
                        }
                    }
                }
            }
            else assert(false);
            // update now the ownership table
            if(table_ids.size() != txn->accessed_page_ids.size()) assert(false);
            for (int i=0; i<keys.size(); i++) {
                ownership_table_->set_owner(txn, table_ids[i], keys[i], rw[i], txn->accessed_page_ids[i], result.smart_router_id);
            }
        }
        else if(SYSTEM_MODE == 13 || (SYSTEM_MODE >= 23 && SYSTEM_MODE <= 25)) {
            // 基于page Metis的结果进行分区, 同时返回page到node的映射
            if(SYSTEM_MODE == 23) {
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
            }
            std::unordered_map<node_id_t, double> node_benefit_map;
            int sum_page = page_to_node_map.size();
            // 计算Metis带来的好处
            std::vector<double> metis_benefit(ComputeNodeCount, 0.0);
            for (const auto& [table_page_id, metis_node] : page_to_node_map) {
                if (metis_node == -1) continue;
                metis_benefit[metis_node] += 1.0 / sum_page;
            }
            double benefit1, benefit2, benefit3;

            for(int i=0; i<ComputeNodeCount; i++) {
                // benefit 计算：已经有ownership的页面比例
                benefit1 = 1.0 * node_count_basedon_page_access_last[i] / sum_page;
                // benefit 计算：满足metis分区结果的页面比例
                benefit2 = metis_benefit[i];
                // benefit 计算： 负载均衡, 当前节点路由的事务越少，benefit越高
                // benefit3 = 2 * workload_balance_penalty_weights_[i];
                benefit3 = workload_balance_penalty_weights_[i] + remain_queue_balance_penalty_weights_[i];
                double total_benefit = 0.0;
                if(SYSTEM_MODE == 23) {
                    // 考虑Metis 分区和Load Balance
                    total_benefit = benefit2 + benefit3;
                }
                else if(SYSTEM_MODE == 24) {
                    // 考虑Ownership和Load Balance
                    total_benefit = benefit1 + benefit3;
                }
                else if(SYSTEM_MODE == 25) {
                    // 仅考虑Load Balance
                    total_benefit = remain_queue_balance_penalty_weights_[i];
                }
                else if (SYSTEM_MODE == 13){ 
                    // 考虑Ownership, Metis 分区和Load Balance
                    total_benefit = benefit1 + benefit2 + benefit3;
                }
                else {
                    assert(false);
                }
                node_benefit_map[i] = total_benefit;
                #if LOG_METIS_OWNERSHIP_DECISION
                if(WarmupEnd)
                debug_info += "(node=" + std::to_string(i) + 
                                ", benefit1=" + std::to_string(benefit1) + 
                                ", benefit2=" + std::to_string(benefit2) + 
                                ", benefit3=" + std::to_string(benefit3) + 
                                ", total_benefit=" + std::to_string(total_benefit) + "); ";
                #endif
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
            #if LOG_METIS_OWNERSHIP_DECISION
            if (WarmupEnd) {
                logger->info("[SmartRouter Mode 13] " + debug_info + " selected node " + std::to_string(result.smart_router_id));
            }
            #endif
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


std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> SmartRouter::get_route_primary_batch_schedule(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch) {
    
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

        // 获取 keys 和 table_ids（smallbank_ 的函数是线程安全的）
        std::vector<itemkey_t> accounts_keys;
        std::vector<table_id_t> table_ids; 
        
        if(Workload_Type == 0) {
            itemkey_t account1 = txn->accounts[0];
            itemkey_t account2 = txn->accounts[1];
            table_ids = smallbank_->get_table_ids_by_txn_type(txn_type);
            smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
        } else if (Workload_Type == 1) { 
            table_ids = ycsb_->get_table_ids_by_txn_type();
            accounts_keys = txn->ycsb_keys;
        } else if (Workload_Type == 2) {
            accounts_keys = txn->tpcc_keys;
            table_ids = tpcc_->get_table_ids_by_txn_type(txn_type, accounts_keys.size());
        }
        else assert(false); // 不可能出现的情况
        assert(table_ids.size() == accounts_keys.size());

        SchedulingCandidateTxn* scheduling_candidate_txn = new SchedulingCandidateTxn{txn, {}, {}, std::vector<int>(ComputeNodeCount, 0), {}, std::vector<double>(ComputeNodeCount, 0.0), -1};
        txid_to_txn_map[tx_id] = scheduling_candidate_txn;

        // 获取涉及的页面列表
        std::unordered_map<uint64_t, node_id_t> table_page_ids; // 高32位存table_id，低32位存page_id
        for (size_t i = 0; i < accounts_keys.size(); ++i) {
            auto entry = lookup(txn, table_ids[i], accounts_keys[i]);
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
                auto owner_stats = ownership_table_->get_owner(table_page_id);
                node_id_t ownership_node = -1;
                if(owner_stats.first.empty()) {
                    // 如果没有owner，则随机分配一个节点
                    ownership_node = rand() % ComputeNodeCount;
                } else {
                    ownership_node = owner_stats.first[0]; // 选择第一个owner作为ownership_node
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
            std::vector<double> node_benefit_map(ComputeNodeCount, 0.0);
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
            double benefit_score = txn->node_benefit_map[min_txn_node];
            if(benefit_score > 0){
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
void SmartRouter::get_route_primary_batch_schedule_v2(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch) {
    
    assert(SYSTEM_MODE == 11); // 仅支持模式11
    
    if (WarmupEnd)
        logger->info("[SmartRouter Scheduling] Start scheduling for txn batch of size " + std::to_string(txn_batch->size()));

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
    std::vector<std::unordered_map<tx_id_t, SchedulingCandidateTxn*>> local_txid_maps(thread_count);
    std::vector<std::vector<std::pair<uint64_t, tx_id_t>>> local_page_pairs(thread_count);
    forbid_update_hot_entry();
    // for debug, i want see the key 1 is on which page
    std::unordered_map<uint64_t, std::string> debug_pages;
    for(size_t i=0; i < hottest_keys.size(); i++) {
        uint64_t page_id0 = (static_cast<uint64_t>(0) << 32) | hot_key_map.find({0, hottest_keys[i]})->second.page; 
        debug_pages[page_id0] = "table: 0, key: " + std::to_string(hottest_keys[i]);
        uint64_t page_id1 = (static_cast<uint64_t>(1) << 32) | hot_key_map.find({1, hottest_keys[i]})->second.page; 
        debug_pages[page_id1] = "table: 1, key: " + std::to_string(hottest_keys[i]);
    }
    // uint64_t debug_page0, debug_page1;
    // {
    //     std::shared_lock<std::shared_mutex> lock(hot_mutex_);
    //     debug_page0 =(static_cast<uint64_t>(0) << 32) | hot_key_map.find({0, 1})->second.page;
    //     debug_page1 =(static_cast<uint64_t>(1) << 32) | hot_key_map.find({1, 1})->second.page;
    // }

    std::vector<std::future<void>> futs;
    futs.reserve(thread_count);

    for (size_t t = 0; t < thread_count; ++t) {
        size_t start = t * chunk;
        size_t end = std::min(n, start + chunk);
        futs.push_back(threadpool.enqueue([this, &txn_batch, start, end, t, &local_txid_maps, &local_page_pairs]() {
            auto &local_map = local_txid_maps[t];
            auto &local_pairs = local_page_pairs[t];

            for (size_t idx = start; idx < end; ++idx) {
                TxnQueueEntry* txn = (*txn_batch)[idx];
                tx_id_t tx_id = txn->tx_id;
                int txn_type = txn->txn_type;

                // 获取 keys 和 table_ids（smallbank_ 的函数是线程安全的）
                std::vector<itemkey_t> accounts_keys;
                std::vector<table_id_t> table_ids; 
                std::vector<bool> rw;
                if(Workload_Type == 0) {
                    table_ids = smallbank_->get_table_ids_by_txn_type(txn_type);                
                    if (txn_type == 6) {
                        accounts_keys = txn->accounts;
                    } else {
                        itemkey_t account1 = txn->accounts[0];
                        itemkey_t account2 = txn->accounts[1];
                        smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
                    }
                    rw = smallbank_->get_rw_by_txn_type(txn_type);
                } else if (Workload_Type == 1) { 
                    table_ids = ycsb_->get_table_ids_by_txn_type();
                    accounts_keys = txn->ycsb_keys;
                    rw = ycsb_->get_rw_flags();
                } else if (Workload_Type == 2) {
                    accounts_keys = txn->tpcc_keys;
                    table_ids = tpcc_->get_table_ids_by_txn_type(txn_type, accounts_keys.size());
                    rw = tpcc_->get_rw_flags_by_txn_type(txn_type, accounts_keys.size());
                }
                else assert(false); // 不可能出现的情况
                assert(table_ids.size() == accounts_keys.size());

                SchedulingCandidateTxn* sc = new SchedulingCandidateTxn();
                sc->ownership_node_count.assign(ComputeNodeCount, 0);
                sc->node_benefit_map.assign(ComputeNodeCount, 0.0);
                sc->txn = txn;
                sc->will_route_node = -1;
                sc->is_scheduled = false; // !这里做出逻辑的更改，当一个事务调度完成之后，不再从 txid_to_txn_map 中删除，而是设置 is_scheduled 标志
                sc->rw_flags = std::move(rw);
                sc->involved_pages.reserve(accounts_keys.size());

                // 获取涉及的页面列表
                std::unordered_map<uint64_t, node_id_t> table_page_ids; // 高32位存table_id，低32位存page_id
                // lookup（内部会加 hot_mutex_），构造 involved_pages，并收集 page->tx 映射对
                for (size_t i = 0; i < accounts_keys.size(); ++i) {
                    auto entry = lookup(txn, table_ids[i], accounts_keys[i]);
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
    allow_update_hot_entry(); // 开放 hot entry 更新

    // 计时
    struct timespec merge_start_time, merge_end_time;
    clock_gettime(CLOCK_MONOTONIC, &merge_start_time);
    // 合并 local 结果到全局结构（单线程执行）
    std::unordered_map<tx_id_t, SchedulingCandidateTxn*> txid_to_txn_map; // 记录每个事务ID对应的事务对象, 还没调度的事务
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
    for(const auto& [page, txn_ids] : page_to_txn_map) {
        if(txn_ids.size() <= 1) continue; // 只有一个事务访问该页面，不存在冲突
        for (auto tx_id : txn_ids) {
            conflicted_txns.insert(tx_id);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &compute_conflict_end_time);
    time_stats_.compute_conflict_ms += 
        (compute_conflict_end_time.tv_sec - compute_conflict_start_time.tv_sec) * 1000.0 + (compute_conflict_end_time.tv_nsec - compute_conflict_start_time.tv_nsec) / 1000000.0;

    // !构建所有涉及页面的列表，并建立 ownership 快照，保证并发读取一致
    std::unordered_map<uint64_t, std::pair<std::vector<node_id_t>, bool>> page_ownership_to_node_map; // 记录每个页面对应的节点ID（Ownership表结果）
    page_ownership_to_node_map.reserve(page_to_txn_map.size());
    for (const auto& kv : page_to_txn_map) {
        auto owner_stats = std::move(ownership_table_->get_owner(kv.first));
        page_ownership_to_node_map[kv.first] = std::move(owner_stats);
    }

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.preprocess_txn_ms += 
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

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

    // 这里还没有获取 ownership 信息，后续再处理，合并之后首先要求事务之间的页面冲突关系，通过倒排索引
    // 以上为batch pipeline预处理的部分
    // !同步点，标志着上一个batch的事务执行完成
    
    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    // waiting the pending txns to be pushed to txn_queues_
    if(batch_id >= 0) {
        // 如果是第一个batch，则不需要等待
        pending_txn_queue_->wait_for_pending_txn_empty();
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            // 把该批的事务都分发完成，设置batch处理完成标志
            txn_queues_[node_id]->set_batch_finished();
        }
    }
    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.wait_pending_txn_push_ms += 
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
    logger->info("Waiting pending txn push time: " + std::to_string(
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0) + " ms");

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
                // // check if all the queue is empty
                // if(txn_queues_[i]->size() != 0 || !txn_queues_[i]->is_shared_queue_empty()) {
                //     return false;
                // }
                // 宽松 check if the queue is empty
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
    // print txn_queue size in each compute node
    // if(WarmupEnd){
        logger->info("[SmartRouter Scheduling] After waiting last batch finish, remained last batch txn size: " 
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
        );
    // }

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
                &ownership_ok_txn_queues_per_thread, &candidate_txn_queues_per_thread, conflicted_txns, &page_ownership_to_node_map, 
                &unconflict_and_ownership_ok_txn_cnt, &unconflict_and_ownership_cross_txn_cnt, &unconflict_and_shared_txn_cnt, 
                &schedule_txn_cnt_per_node_this_batch, &expected_page_transfer_count_per_node]() {
            
            std::vector<std::vector<TxnQueueEntry*>> node_routed_txns(ComputeNodeCount);
            for (size_t idx = start; idx < end; ++idx) {
                tx_id_t tx_id = txn_batch->at(idx)->tx_id;
                SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id];
                if(sc == nullptr) assert(false); // 不可能出现的情况
                // 填充 page_to_ownership_node_vec
                if(sc->ownership_node_count.size() != ComputeNodeCount) sc->ownership_node_count.assign(ComputeNodeCount, 0);
                for (int i = 0; i < sc->involved_pages.size(); ++i) {
                    auto page = sc->involved_pages[i];
                    const auto &owner_pair = page_ownership_to_node_map.at(page); // 只读访问，避免并发修改
                    update_sc_ownership_count(sc, i, {{}, false}, owner_pair); // update to correct owner
                }
                // 计算是否有计算节点满足页面所有权
                std::vector<node_id_t> candidate_ownership_nodes = checkif_txn_ownership_ok(sc);
                if(!candidate_ownership_nodes.empty()) {
                    // 满足ownership entirely
                    if(candidate_ownership_nodes.size() > 1) {
                        // 多个节点满足ownership entirely，选择负载最轻的节点
                        node_id_t best_node = -1;
                        int max_compute_node_workload_benefit = -1;
                        for(const auto& node_id : candidate_ownership_nodes) {
                            if(compute_node_workload_benefit[node_id] > max_compute_node_workload_benefit) {
                                max_compute_node_workload_benefit = compute_node_workload_benefit[node_id];
                                best_node = node_id;
                            }
                        }
                        sc->will_route_node = best_node;
                    }
                    else {
                        sc->will_route_node = candidate_ownership_nodes.front();
                    }
                    node_id_t ownership_node = sc->will_route_node;
                    if(conflicted_txns.count(tx_id) == 0) {
                        {
                            // ! 如果该事务没有冲突，直接加入txn_queues, 工作线程可以直接处理
                            // 计时
                            struct timespec push_begin_time, push_end_time;
                            clock_gettime(CLOCK_MONOTONIC, &push_begin_time);
                            node_routed_txns[ownership_node].push_back(sc->txn);
                            sc->txn->schedule_type = TxnScheduleType::UNCONFLICT;
                            // 如果达到批量大小，则批量推送
                            if(node_routed_txns[ownership_node].size() >= BatchExecutorPOPTxnSize){ 
                                txn_queues_[ownership_node]->push_txn_back_batch(node_routed_txns[ownership_node]);
                                node_routed_txns[ownership_node].clear();
                            }
                            clock_gettime(CLOCK_MONOTONIC, &push_end_time);
                            time_stats_.push_txn_to_queue_ms_per_thread[t] += 
                                (push_end_time.tv_sec - push_begin_time.tv_sec) * 1000.0 + 
                                (push_end_time.tv_nsec - push_begin_time.tv_nsec) / 1000000.0;
                        }
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
                    for(int i=0; i<ComputeNodeCount; i++) {
                        // 计算每一个节点的benefit
                        // benefit 计算：已经有ownership的页面比例
                        double benefit1 = 1.0 * sc->ownership_node_count[i] / sum_pages;
                        // benefit 计算: 满足metis分区结果的页面比例
                        double benefit2 = metis_benefit[i];
                        // benefit 计算: 负载均衡，当前节点路由的事务越少，benefit越高
                        double benefit3 = 2 * compute_node_workload_benefit[i];
                        // 综合benefit
                        sc->node_benefit_map[i] = benefit1 + benefit2 + benefit3;
                    }
                    assert(sc->node_benefit_map.size() == ComputeNodeCount);
                    if(conflicted_txns.count(tx_id) == 0) {
                        // 如果该事务没有冲突，可以直接调度
                        // 选择benefit最高的节点作为will_route_node
                        double max_benefit = -1.0;
                        int best_node = -1;
                        double min_benefit = 1e9;
                        int worst_node = -1;
                        for(int node_id = 0; node_id < ComputeNodeCount; ++node_id) {
                            double benefit = sc->node_benefit_map[node_id];
                            if(benefit > max_benefit) {
                                max_benefit = benefit;
                                best_node = node_id;
                            }
                            if(benefit < min_benefit) {
                                min_benefit = benefit;
                                worst_node = node_id;
                            }
                        }
                        if(max_benefit - min_benefit < 0) {
                            // !如果best_node和worst_node的benefit差距不大，则可以加入到共享队列中
                            txn_queues_[best_node]->push_txn_into_shared_queue(sc->txn);
                            sc->is_scheduled = true; // 标记该事务已经被调度
                            unconflict_and_shared_txn_cnt++;
                        }
                        else{
                            sc->will_route_node = best_node;
                            {
                                // !直接加入txn_queues, 工作线程可以直接处理
                                // 计时
                                struct timespec push_begin_time, push_end_time;
                                clock_gettime(CLOCK_MONOTONIC, &push_begin_time);
                                node_routed_txns[best_node].push_back(sc->txn);
                                // 如果达到批量大小，则批量推送
                                if(node_routed_txns[best_node].size() >= BatchExecutorPOPTxnSize){ 
                                    txn_queues_[best_node]->push_txn_back_batch(node_routed_txns[best_node]);
                                    node_routed_txns[best_node].clear();
                                }
                                clock_gettime(CLOCK_MONOTONIC, &push_end_time);
                                time_stats_.push_txn_to_queue_ms_per_thread[t] += 
                                    (push_end_time.tv_sec - push_begin_time.tv_sec) * 1000.0 + 
                                    (push_end_time.tv_nsec - push_begin_time.tv_nsec) / 1000000.0;
                            }
                            sc->is_scheduled = true; // 标记该事务已经被调度
                            this->routed_txn_cnt_per_node[best_node]++;
                            load_tracker_.record(best_node);
                            unconflict_and_ownership_cross_txn_cnt++;
                            schedule_txn_cnt_per_node_this_batch[best_node]++;
                            // 记录预期的页面迁移数量
                            int expected_page_transfer_cnt = sum_pages - sc->ownership_node_count[best_node];
                            expected_page_transfer_count_per_node[best_node] += expected_page_transfer_cnt;
                        }
                    }
                    else {
                        // 如果有事务冲突，则加入候选队列
                        candidate_txn_queues_per_thread[t].insert(tx_id);
                    }
                }
            }
            // 将剩余的node_routed_txns批量推送到txn_queues_
            struct timespec push_begin_time, push_end_time;
            clock_gettime(CLOCK_MONOTONIC, &push_begin_time);
            for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                if(!node_routed_txns[node_id].empty()) {
                    txn_queues_[node_id]->push_txn_back_batch(node_routed_txns[node_id]);
                    node_routed_txns[node_id].clear();
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &push_end_time);
            time_stats_.push_txn_to_queue_ms_per_thread[t] += 
                (push_end_time.tv_sec - push_begin_time.tv_sec) * 1000.0 + 
                (push_end_time.tv_nsec - push_begin_time.tv_nsec) / 1000000.0;
        }));
    }    

    // join workers
    for (auto &fut : futs) fut.get(); // get() 会抛异常并传播任务异常

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.ownership_retrieval_and_devide_unconflicted_txn_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    
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
        SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id];
        for(int node_id = 0; node_id < ComputeNodeCount; ++node_id) {
            double benefit = sc->node_benefit_map[node_id];
            candidate_txn_benefit_ipq[node_id].insert(tx_id, benefit);
        }
    }

    
    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.merge_and_construct_ipq_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    int ownership_ok_txn_total = 0;
    for(int i=0; i<ComputeNodeCount; i++) {
        ownership_ok_txn_total += ownership_ok_txn_queues[i].size();
    }
    // 校验 ownership_ok_txn_total + candidate_txn_ids.size() + unconflict_and_ownership_cross_txn_cnt + unconflict_and_ownership_ok_txn_cnt == txid_to_txn_map.size()
    assert(txid_to_txn_map.size() == ownership_ok_txn_total + candidate_txn_ids.size() + 
        unconflict_and_ownership_cross_txn_cnt.load() + unconflict_and_ownership_ok_txn_cnt.load() + unconflict_and_shared_txn_cnt.load());

    // if(WarmupEnd){
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
        logger->info("Unconflicted txns size: " + std::to_string(txid_to_txn_map.size() - conflicted_txns.size()));
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
    #if LOG_KROUTER_SCHEDULING_DEBUG
        // print txn_conflict_map
        for(const auto& tx_id : conflicted_txns) {
            auto iter = txid_to_txn_map.find(tx_id); 
            logger->info("Txn " + std::to_string(tx_id) + 
                            " involves pages: " + [&]() {
                                std::string pages_str;
                                for(const auto& page : iter->second->involved_pages) {
                                    pages_str += std::to_string(page) + " ";
                                }
                                pages_str += " page_to_metis_node: ";
                                for(const auto& metis_node : iter->second->page_to_metis_node_vec) {
                                    pages_str += std::to_string(metis_node) + " ";
                                }
                                pages_str += " page_ownership_node: ";
                                for(const auto& ownership_node : iter->second->page_to_ownership_node_vec) {
                                    pages_str += "{";
                                    for(const auto& node : ownership_node.first) {
                                        pages_str += std::to_string(node) + " ";
                                    }
                                    pages_str += "} ";
                                }
                                return pages_str;
                            }());
        }
        // print conflict page_to_txn_map
        for(const auto& [page, txn_ids] : page_to_txn_map) {
            if(txn_ids.size() <= 1) continue; // 说明是非冲突的页面，没必要输出，直接往下走
            logger->info("Page " + std::to_string(page) + 
                            " involved by txns: " + [&]() {
                                std::string txns_str;
                                for(const auto& tx_id : txn_ids) {
                                    txns_str += std::to_string(tx_id) + " ";
                                }
                                return txns_str;
                            }());
        }
        // print ownership_ok_txn_queues
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
            std::string txns_str;
            for(const auto& tx_id : ownership_ok_txn_queues[node_id]) {
                txns_str += std::to_string(tx_id) + " ";
            }
            logger->info("Node " + std::to_string(node_id) + 
                            " ownership_ok_txn_queues: " + txns_str);
        }
    #endif
    // }

    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // !2. 单线程进行调度决策
    int scheduled_front_txn_cnt = 0;
    int scheduled_counter = 0;
    while(!candidate_txn_ids.empty()){
        scheduled_counter++;
        // 1.1 从候选事务中选择下一个要调度的事务进行所有权迁移的计划
        int min_txn_node = -1;
        int min_txn_count = INT32_MAX;
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            // int workload = schedule_txn_cnt_per_node_this_batch[node_id] + ownership_ok_txn_queues[node_id].size(); 
            // !采用更准确的当前负载作为衡量标准, 实时看一下txn_queues_的大小, 而不是schedule_txn_cnt_per_node_this_batch
            int workload = txn_queues_[node_id]->size() + ownership_ok_txn_queues[node_id].size(); 
            // int workload = expected_page_transfer_count_per_node[node_id].load(); // 采用预期的页面迁移数量作为负载衡量标准
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
        selected_candidate_txn = txid_to_txn_map[best_tx_id];
        if(selected_candidate_txn->is_scheduled == true) {
            // 说明该事务已经被调度过了，继续下一个循环
            continue;
        }
        max_benefit_score = best_benefit;

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
        std::unordered_map<uint64_t, std::pair<std::vector<node_id_t>, bool>> transfer_pages;
        if(selected_candidate_txn != nullptr){
            for(int i=0; i<selected_candidate_txn->involved_pages.size(); i++){
                auto page = selected_candidate_txn->involved_pages[i];
                auto owner_stats = page_ownership_to_node_map.at(page);
                bool rw = selected_candidate_txn->rw_flags[i];
                std::pair<std::vector<node_id_t>, bool> new_ownership_stats;
                if(rw){
                    // write operation, need exclusive ownership
                    if(!(owner_stats.second == 1 && owner_stats.first.size() == 1 && owner_stats.first[0] == min_txn_node)){
                        new_ownership_stats = {{min_txn_node}, 1}; // exclusive ownership
                        // check if already in transfer_pages
                        transfer_pages[page] = new_ownership_stats;
                    }
                } else {
                    // check if already in transfer_pages
                    auto it = transfer_pages.find(page);
                    if (it != transfer_pages.end()){
                        // already in transfer_pages
                        continue;
                    }
                    else {
                        // read operation, shared ownership is ok
                        bool has_shared_ownership = false;
                        for(const auto& ownership_node : owner_stats.first){
                            if(ownership_node == min_txn_node){
                                has_shared_ownership = true;
                                break;
                            }
                        }
                        if(!has_shared_ownership){
                            owner_stats.first.push_back(min_txn_node);
                            owner_stats.second = false; // shared ownership
                            transfer_pages[page] = owner_stats;
                        } 
                    }
                }
            }
        }
        // 记录预期的页面迁移数量
        expected_page_transfer_count_per_node[min_txn_node] += transfer_pages.size();
        for(const auto& [page, new_ownership_stats] : transfer_pages) {
            if(debug_pages.find(page) != debug_pages.end()) {
                logger->info("[SmartRouter Scheduling] Debug page (" + debug_pages[page] + ")" + 
                                " page: " + std::to_string(page) +
                                " will transfer to node " + std::to_string(min_txn_node) + 
                                " new_ownership_stats: " + [&]() {
                                    std::string stats_str;
                                    stats_str += "{";
                                    for(const auto& node : new_ownership_stats.first) {
                                        stats_str += std::to_string(node) + " ";
                                    }
                                    stats_str += "}, is_exclusive: " + std::to_string(new_ownership_stats.second);
                                    return stats_str;
                                }());   
            }
            // if(page == debug_page0) {
            //     logger->info("[SmartRouter Scheduling] table 0 key 1 page " + std::to_string(page) + 
            //                     " will transfer to node " + std::to_string(min_txn_node) + 
            //                     " new_ownership_stats: " + [&]() {
            //                         std::string stats_str;
            //                         stats_str += "{";
            //                         for(const auto& node : new_ownership_stats.first) {
            //                             stats_str += std::to_string(node) + " ";
            //                         }
            //                         stats_str += "}, is_exclusive: " + std::to_string(new_ownership_stats.second);
            //                         return stats_str;
            //                     }());   
            // }
            // if(page == debug_page1) {
            //     logger->info("[SmartRouter Scheduling] table 1 key 1 page " + std::to_string(page) + 
            //                     " will transfer to node " + std::to_string(min_txn_node) + 
            //                     " new_ownership_stats: " + [&]() {
            //                         std::string stats_str;
            //                         stats_str += "{";
            //                         for(const auto& node : new_ownership_stats.first) {
            //                             stats_str += std::to_string(node) + " ";
            //                         }
            //                         stats_str += "}, is_exclusive: " + std::to_string(new_ownership_stats.second);
            //                         return stats_str;
            //                     }());
            // }
        }
        // 1.3 找到所有转移页面所涉及到的事务
        std::unordered_set<tx_id_t> affected_txns;
        for(auto [transfer_page, new_ownership_stats] : transfer_pages) {
            page_ownership_to_node_map[transfer_page] = new_ownership_stats;
            for(auto affected_txn_id : page_to_txn_map[transfer_page]) {
                auto it = txid_to_txn_map.find(affected_txn_id);
                if (it->second->is_scheduled) continue; // 说明这个事务已经被调度过了
                affected_txns.insert(affected_txn_id);
            }
        }

        // 1.4 进行事务的编排，找到转移页面后，原来可以满足ownership entirely的事务可能不满足的，将这些事务先执行
        std::vector<TxnQueueEntry*> schedule_txn_prior; // 记录本次调度的事务
        node_id_t schedule_node = -1;
        std::vector<TxnQueueEntry*> next_time_schedule_txn; // 记录下次调度的事务
        for(auto affected_txn_id : affected_txns) {
            auto it = txid_to_txn_map.find(affected_txn_id);
            assert(it != txid_to_txn_map.end());
            assert(it->second->is_scheduled == false); // 说明这个事务还没有被调度过
            // 更新所有涉及页面的所有权
            for(int i=0; i<it->second->involved_pages.size(); i++){
                auto page = it->second->involved_pages[i];
                auto tp_it = transfer_pages.find(page);
                if (tp_it != transfer_pages.end()) {
                    update_sc_ownership_count(it->second, i, page_ownership_to_node_map.at(page), tp_it->second);
                } 
            }
            // check if the affected_txn_id can still execute
            std::vector<node_id_t> candidate_ownership_nodes = checkif_txn_ownership_ok(it->second);
            #if LOG_KROUTER_SCHEDULING_DEBUG
            if(WarmupEnd)
            logger->info("[SmartRouter Scheduling] Affected txn " + std::to_string(affected_txn_id) + 
                            " ownership_node_count: " + [&]() {
                                std::string counts_str;
                                for(int node_id = 0; node_id < ComputeNodeCount; ++node_id) {
                                    if(it->second->ownership_node_count[node_id] > 0)
                                        counts_str += "Node " + std::to_string(node_id) + ": " + std::to_string(it->second->ownership_node_count[node_id]) + ", ";
                                }
                                counts_str += "candidate_ownership_nodes: ";
                                for(const auto& node_id : candidate_ownership_nodes) {
                                    counts_str += std::to_string(node_id) + " ";
                                }
                                return counts_str;
                            }());
            #endif
            // print ownership status for debug
            if(!candidate_ownership_nodes.empty()) {
                // 加入next_time_schedule_txn
                next_time_schedule_txn.push_back(it->second->txn);
                node_id_t last_can_execute_node = it->second->will_route_node;
                // 注意这里尽管向min_txn_node执行，但是实际上可能会选择其他节点执行, 因为可能只是向min_txn_node增加了shared ownership
                bool execute_node_not_changed = false;
                for(const auto& node_id : candidate_ownership_nodes) {
                    if(node_id == last_can_execute_node) {
                        execute_node_not_changed = true;
                        break;
                    }
                }
                // ! case0: 之前可以执行, 页面所有权转移之后仍然可以执行, 且执行节点不变
                if(execute_node_not_changed) continue; // 说明之前就已经在该节点上执行，不需要变动
                bool min_txn_node_in_candidates = false;
                for(const auto& node_id : candidate_ownership_nodes) {
                    if(node_id == min_txn_node) {
                        min_txn_node_in_candidates = true;  
                        break;
                    }
                }
                node_id_t now_can_execute_node = -1;
                if(min_txn_node_in_candidates) {
                    now_can_execute_node = min_txn_node;
                } else {
                    now_can_execute_node = candidate_ownership_nodes.front(); // 选择第一个可执行节点
                }

                if(last_can_execute_node != -1) {
                    // ! case1: 之前可以执行, 但现在需要变动到新的节点上执行, 但执行节点变了
                    assert(ownership_ok_txn_queues[last_can_execute_node].count(affected_txn_id) == 1);
                    // 需要变动到新的节点上执行, 将该事务从ownership_ok_txn_queues中删除，表示已不能执行
                    ownership_ok_txn_queues[last_can_execute_node].erase(affected_txn_id);  
                    // 记录调度信息
                    #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Page " + [&]() {
                                        std::string pages_str;
                                        for(const auto& [page, _] : transfer_pages) {
                                            pages_str += std::to_string(page) + " ";
                                        }
                                        return pages_str;
                                    }() +
                                    " transferred to node " + std::to_string(min_txn_node) + 
                                    ", affected txn " + std::to_string(affected_txn_id) + 
                                    " will change execution node from " + std::to_string(last_can_execute_node) + 
                                    " to " + std::to_string(now_can_execute_node) + " at this time txn queue size: " +
                                    this->get_txn_queue_now_status());
                    #endif
                } else{
                    // ! case2: 之前不可以执行, 现在可以执行, 将该事务从candidate_txn_ids中删除，表示现在可以执行
                    assert(candidate_txn_ids.count(affected_txn_id) == 1);
                    candidate_txn_ids.erase(affected_txn_id);
                    #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Page " + [&]() {
                                        std::string pages_str;
                                        for(const auto& [page, _] : transfer_pages) {
                                            pages_str += std::to_string(page) + " ";
                                        }
                                        return pages_str;
                                    }() +
                                    " transferred to node " + std::to_string(min_txn_node) + 
                                    ", affected txn " + std::to_string(affected_txn_id) + 
                                    " can now execute on node " + std::to_string(min_txn_node) + " at this time txn queue size: " +
                                    this->get_txn_queue_now_status());
                    #endif
                }
                it->second->will_route_node = now_can_execute_node;
                ownership_ok_txn_queues[now_can_execute_node].insert(affected_txn_id);
            }
            else{
                node_id_t will_route_node = it->second->will_route_node;
                if(will_route_node != -1) {
                    // !case 3: 之前可以执行, 页面所有权转移之后不能执行
                    assert(ownership_ok_txn_queues[will_route_node].count(affected_txn_id) == 1);
                    // 将该事务从ownership_ok_txn_queues中删除，表示已不能执行
                    ownership_ok_txn_queues[will_route_node].erase(affected_txn_id);
                    // !表示这轮计划的迁移所造成的不能执行的事务, 需要先执行, 但这些事务本身也可能存在依赖关系, 比如他如果依赖的事务还没有执行完成, 那么他现在就得等待一会
                    schedule_txn_prior.push_back(it->second->txn); // schedule_txn_prior 只是用作记录依赖关系, 并不是真正的调度
                    it->second->txn->schedule_type = TxnScheduleType::SCHEDULE_PRIOR;
                    if(schedule_node == -1) schedule_node = will_route_node; 
                    else if(schedule_node != will_route_node) 
                        logger->warning("[SmartRouter Scheduling] Warning: schedule_node " + std::to_string(schedule_node) + 
                                        " different from will_route_node " + std::to_string(will_route_node) + 
                                        " for affected txn " + std::to_string(affected_txn_id));
                    this->routed_txn_cnt_per_node[will_route_node]++;
                    load_tracker_.record(will_route_node);
                    schedule_txn_cnt_per_node_this_batch[will_route_node]++;
                    // !标记该事务已经被调度
                    it->second->is_scheduled = true;
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Page " + [&]() {
                                        std::string pages_str;
                                        for(const auto& [page, _] : transfer_pages) {
                                            pages_str += std::to_string(page) + " ";
                                        }
                                        return pages_str;
                                    }() +
                                " transferred to node " + std::to_string(min_txn_node) + 
                                ", scheduling previously ownership_ok txn " + std::to_string(affected_txn_id) + 
                                " to execute on node " + std::to_string(will_route_node) + "at this time txn queue size: " +
                                this->get_txn_queue_now_status());
                #endif
                } else{
                    // ! case4: 之前不可以执行, 页面所有权转移之后仍然不能执行
                    // 说明当前事务仍然不能在min_txn_node上执行，保持不变
                    // 更新candidate_txn_benefit_ipq中的benefit值
                    int sum_pages = it->second->involved_pages.size();
                    std::vector<double> metis_benefit(ComputeNodeCount, 0.0);
                    for(int i=0; i<sum_pages; i++) {
                        node_id_t metis_node = it->second->page_to_metis_node_vec[i];
                        if(metis_node == -1) continue;
                        metis_benefit[metis_node] += 1.0 / sum_pages;
                    }
                    for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                        // benefit 计算：已经有ownership的页面比例
                        double benefit1 = 1.0 * it->second->ownership_node_count[node_id] / sum_pages;
                        // benefit 计算: 满足metis分区结果的页面比例
                        double benefit2 = metis_benefit[node_id];
                        // benefit 计算: 负载均衡，当前节点路由的事务越少，benefit越高
                        double benefit3 = 2 * compute_node_workload_benefit[node_id];
                        // 综合benefit
                        double total_benefit = benefit1 + benefit2 + benefit3;
                        // 更新ipq中的benefit值
                        candidate_txn_benefit_ipq[node_id].update(affected_txn_id, total_benefit);
                        it->second->node_benefit_map[node_id] = total_benefit;
                    }
                    // #if LOG_KROUTER_SCHEDULING_DEBUG
                        // if(WarmupEnd)
                            // logger->info("[SmartRouter Scheduling] Page " + [&]() {
                            //                     std::string pages_str;
                            //                     for(const auto& [page, _] : transfer_pages) {
                            //                         pages_str += std::to_string(page) + " ";
                            //                     }
                            //                     return pages_str;
                            //                 }() +
                            //                 ", txn " + std::to_string(affected_txn_id) + 
                            //                 " still cannot execute on node " + std::to_string(min_txn_node));
                    // #endif
                }
            }
        }
        
        // 1.5 记录事务偏序依赖
        std::string dependency_info = "[SmartRouter Scheduling] batch_id: " + std::to_string(batch_id) +
        " min_txn_node: " + std::to_string(min_txn_node) +
        " scheduled_counter: " + std::to_string(scheduled_counter) + 
        " now scheduling prior txns in this batch" + std::to_string(scheduled_front_txn_cnt) +
        " Dependency info: [";
        for(auto txn: schedule_txn_prior){
            dependency_info += std::to_string(txn->tx_id) + " ";
        }
        dependency_info += "] -> [";
        for(auto txn: next_time_schedule_txn){
            dependency_info += std::to_string(txn->tx_id) + " ";
        }
        dependency_info += "]";
        logger->info(dependency_info);

        int group_id = rand(); // 生成一个随机的group_id用于标识本次调度的事务组
        for(auto prior_txn : schedule_txn_prior) {
            prior_txn->group_id = group_id;
            prior_txn->batch_id = batch_id;
        }
        for(auto txn: next_time_schedule_txn ){
            txn->batch_id = batch_id;
            txn->dependency_group_id.push_back(group_id);
        }

        // ! 注入依赖, not used!!!!
        assert(!next_time_schedule_txn.empty()); // 由于选择了该事务，一定会有后续事务需要调度
        if(!schedule_txn_prior.empty()) {
            // 创建Group, Group 一旦创建, group 中所有的事务都会在这里添加，之后不会添加新的事务
            auto group = std::make_shared<DependencyGroup>();
            group->unfinish_txn_count = static_cast<int>(schedule_txn_prior.size()); // 记录ref, 之后只会减少

            // Prior notify group
            for(auto prior_txn : schedule_txn_prior) {
                prior_txn->notification_groups.push_back(group);
            }
            
            // Next wait for group
            std::unique_lock<std::mutex> lock(group->notify_mutex);
            for(auto txn : next_time_schedule_txn) {
                txn->ref++;
                group->after_txns.push_back(txn); // 记录那些事务依赖于这个group，只有当这个group的事务全部完成后，才会认为这个页面已经在我期望的节点上了
            }
        }
        // 这里应该是遍历 next_time_schedule_txn 的每个页面，看看他们现在的页面是不是都已经过来了，如果有group还没完成，就要加到依赖中
        
        /* Legacy explicit dependency
        for(auto txn: next_time_schedule_txn){
            // txn 是拓扑排序的后续事务
            for(auto prior_txn : schedule_txn_prior){
                if(txn->tx_id == prior_txn->tx_id) assert(false); // 不可能相等
                txn->dependencies.push_back(prior_txn->tx_id); // prior_txn 是 txn 的前驱事务
                txn->ref++; // 增加引用计数
                prior_txn->after_txns.push_back(txn); // prior_txn 的后继事务增加 txn（存指针以便执行层快速定位）
                #if LOG_KROUTER_SCHEDULING_DEBUG
                    if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Txn " + std::to_string(txn->tx_id) + 
                                    " depends on prior txn " + std::to_string(prior_txn->tx_id) );
                #endif
            }
        }
        */
        
        // 1.5 将schedule_txn_prior中的事务加入到txn_queues_中先执行
        // ! 检查schedule_txn_prior 的事务是否满足依赖关系
        std::vector<TxnQueueEntry*> dag_ready_txn;
        for(auto txn: schedule_txn_prior) {
            int ref_now = txn->ref.load(std::memory_order_acquire);
            if(ref_now == 0) {
                dag_ready_txn.push_back(txn);
            } else {
                this->register_pending_txn(txn, schedule_node);
                // logger->info("[SmartRouter Scheduling] Txn " + std::to_string(txn->tx_id) + 
                //                 " is not dag ready, ref count: " + std::to_string(ref_now) + 
                //                 ", registered as pending txn at node " + std::to_string(schedule_node) + 
                //                 ", at this time txn queue size: " + this->get_txn_queue_now_status());
            }
        } 
        // 将dag_ready_txn加入到txn_queues_中先执行
        if(!dag_ready_txn.empty()) {
            txn_queues_[schedule_node]->push_txn_dag_ready(dag_ready_txn); // 放到最前面执行, dag_ready_txn 是绑定到一个连接的
            scheduled_front_txn_cnt += dag_ready_txn.size();
        }
        assert(candidate_txn_ids.count(selected_candidate_txn->txn->tx_id) == 0); // 由于选择了该事务，一定不在candidate_txn_ids中了
    }
    logger->info("Batch id: " + std::to_string(batch_id) + 
                    ", SmartRouter scheduled front txn cnt due to ownership transfer without pending: " + std::to_string(scheduled_front_txn_cnt)); 

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.process_conflicted_txn_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    // 3. 剩余的ownership_ok_txn_queues中的事务加入到txn_queues_中
    futs.clear();
    futs.reserve(ComputeNodeCount);
    for(int i=0; i<ComputeNodeCount; i++) {
        auto& txn_queue = ownership_ok_txn_queues[i];
        futs.push_back(threadpool.enqueue([this, i, &txn_queue, &txid_to_txn_map, &schedule_txn_cnt_per_node_this_batch]() {
            std::vector<TxnQueueEntry*> to_schedule_txns;
            {
                // 构建to_schedule_txns
                for(auto tx_id: txn_queue) {
                    auto it = txid_to_txn_map.find(tx_id);
                    assert(it != txid_to_txn_map.end());
                    assert(it->second->is_scheduled == false); // 说明这个事务还没有被调度
                    it->second->txn->schedule_type = TxnScheduleType::OWNERSHIP_OK;
                    this->routed_txn_cnt_per_node[i]++;
                    load_tracker_.record(i); // 记录负载
                    schedule_txn_cnt_per_node_this_batch[i]++;
                    it->second->is_scheduled = true; // 标记该事务已经被调度
                    if(it->second->txn->ref.load() == 0) {
                        // 说明该事务dag ready，可以直接调度
                        to_schedule_txns.push_back(it->second->txn);
                    }
                    else{
                        // 说明该事务还没有dag ready，注册为pending txn
                        this->register_pending_txn(it->second->txn, i);
                    }
                }
                // 批量加入txn_queues_
                txn_queues_[i]->push_txn_back_batch(to_schedule_txns);
            }
            // if(WarmupEnd)
                logger->info("[SmartRouter Scheduling] Final Scheduling ownership_ok txn to execute on node " + std::to_string(i) + 
                                ", count: " + std::to_string(to_schedule_txns.size()) + " at this time txn queue size: " +
                                this->get_txn_queue_now_status());
                logger->info("Node " + std::to_string(i) + 
                                " Final end, dag not ready pending txn count: " + std::to_string(this->get_pending_txn_count()));
        }));
    }
    // join workers
    for (auto &fut : futs) fut.get(); // get() 会抛异常并传播任务异常
    // if(WarmupEnd)
    logger->info("batch schedule finish" + this->get_txn_queue_now_status() +
                ", expected page transfer count per node: " + [&]() {
                    std::string s;
                    for (size_t i = 0; i < expected_page_transfer_count_per_node.size(); ++i) {
                        s += "Node " + std::to_string(i) + ": " + std::to_string(expected_page_transfer_count_per_node[i].load()) + ", ";
                    }
                    return s;
                }());
    
    // 清理并发导致的ref=0但尚未调度的pending事务，安全迭代并擦除
    std::vector<std::vector<TxnQueueEntry*>> to_schedule;
    to_schedule = std::move(pending_txn_queue_->self_check_ref_is_zero());
    for (size_t node_id = 0; node_id < to_schedule.size(); ++node_id) {
        if (!to_schedule[node_id].empty()) {
            txn_queues_[node_id]->push_txn_back_batch(std::move(to_schedule[node_id]));
            // logger->warning("[SmartRouter Scheduling] Clean pending txns pushed to node " + std::to_string(nid) +
                //                 ", count: " + std::to_string(per_node[nid].size()) +
                //                 ", at this time txn queue size: " + this->get_txn_queue_now_status());
        }
    }

    // delete SchedulingCandidateTxn objects to avoid memory leak
    for(auto& [tx_id, sc] : txid_to_txn_map) {
        delete sc;
    }
    return ;
}

// 这里不再将 std::unique_ptr<std::vector<std::queue<TxnQueueEntry*>>> 作为返回值，而是直接将txn_queues_作为输入，
// 这样做的好处是可以不等待这个函数处理完成整个batch后再返回结果，而是可以在函数内部直接将调度好的事务放入对应的txn_queues_中，
// 从而可以降低worker端等待事务调度完成的结果，pipeline效率更高。
void SmartRouter::get_route_primary_batch_schedule_v3(std::unique_ptr<std::vector<TxnQueueEntry*>> &txn_batch) {
    
    assert(SYSTEM_MODE == 11); // 仅支持模式11
    
    if (WarmupEnd)
        logger->info("[SmartRouter Scheduling] Start scheduling for txn batch of size " + std::to_string(txn_batch->size()));

    // 计时
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // 并行前处理：每个 txn 独立生成 SchedulingCandidateTxn 和 involved_pages 列表
    size_t n = txn_batch->size();
    
    // 全局数据结构, 事务ID 到 事务对象 的映射
    std::unordered_map<tx_id_t, SchedulingCandidateTxn*> txid_to_txn_map; // 记录每个事务ID对应的事务对象, 还没调度的事务
    // 替代 page_to_txn_map 的高效结构: CSR storage, 这里存储着conflicting 事务的所有页面
    std::vector<std::pair<uint64_t, tx_id_t>> global_page_pairs;
    std::unordered_map<uint64_t, std::pair<uint32_t, uint32_t>> page_to_txn_range_map; 
    
    txid_to_txn_map.reserve(n);
    global_page_pairs.reserve(n * 4); // Heuristic

    forbid_update_hot_entry();
    // for debug, i want see the key 1 is on which page
    std::unordered_map<uint64_t, std::string> debug_pages;
    for(size_t i=0; i < hottest_keys.size(); i++) {
        uint64_t page_id0 = (static_cast<uint64_t>(0) << 32) | hot_key_map.find({0, hottest_keys[i]})->second.page; 
        debug_pages[page_id0] = "table: 0, key: " + std::to_string(hottest_keys[i]);
        uint64_t page_id1 = (static_cast<uint64_t>(1) << 32) | hot_key_map.find({1, hottest_keys[i]})->second.page; 
        debug_pages[page_id1] = "table: 1, key: " + std::to_string(hottest_keys[i]);
    }
    
    // Shared variables for parallel execution context
    size_t thread_count = std::min<size_t>(worker_threads_, n);
    if (thread_count == 0) thread_count = 1;
    size_t chunk = (n + thread_count - 1) / thread_count;
    std::vector<std::future<void>> futs;
    // Optimization: avoid new/delete overhead
    std::vector<SchedulingCandidateTxn> candidates(n);

    // ! 求解事务之间的页面冲突关系
    std::unordered_set<tx_id_t> conflicted_txns;

    // per-thread local containers
    std::vector<std::unordered_map<tx_id_t, SchedulingCandidateTxn*>> local_txid_maps(thread_count);
    
    // Optimization: Weak conflict detection
    std::vector<std::unordered_set<tx_id_t>> local_conflicted_txns(thread_count);

    futs.reserve(thread_count);

    for (size_t t = 0; t < thread_count; ++t) {
        size_t start = t * chunk;
        size_t end = std::min(n, start + chunk);
        futs.push_back(threadpool.enqueue([this, &txn_batch, start, end, t, &local_txid_maps, &local_conflicted_txns, &candidates]() {
            auto &local_map = local_txid_maps[t];
            auto &local_conflicts = local_conflicted_txns[t];
            
            std::unordered_map<uint64_t, tx_id_t> local_page_tracker;

            for (size_t idx = start; idx < end; ++idx) {
                TxnQueueEntry* txn = (*txn_batch)[idx];
                tx_id_t tx_id = txn->tx_id;
                int txn_type = txn->txn_type;

                // 获取 keys 和 table_ids（smallbank_ 的函数是线程安全的）
                std::vector<itemkey_t> accounts_keys;
                std::vector<table_id_t> table_ids; 
                std::vector<bool> rw;
                if(Workload_Type == 0) {
                    table_ids = smallbank_->get_table_ids_by_txn_type(txn_type);                
                    if (txn_type == 6) {
                        accounts_keys = txn->accounts;
                    } else {
                        itemkey_t account1 = txn->accounts[0];
                        itemkey_t account2 = txn->accounts[1];
                        smallbank_->get_keys_by_txn_type(txn_type, account1, account2, accounts_keys);
                    }
                    rw = smallbank_->get_rw_by_txn_type(txn_type);
                } else if (Workload_Type == 1) { 
                    table_ids = ycsb_->get_table_ids_by_txn_type();
                    accounts_keys = txn->ycsb_keys;
                    rw = ycsb_->get_rw_flags();
                } else if (Workload_Type == 2) {
                    accounts_keys = txn->tpcc_keys;
                    table_ids = tpcc_->get_table_ids_by_txn_type(txn_type, accounts_keys.size());
                    rw = tpcc_->get_rw_flags_by_txn_type(txn_type, accounts_keys.size());
                }
                else assert(false); // 不可能出现的情况
                assert(table_ids.size() == accounts_keys.size());

                SchedulingCandidateTxn* sc = &candidates[idx];
                sc->ownership_node_count.assign(ComputeNodeCount, 0);
                sc->node_benefit_map.assign(ComputeNodeCount, 0.0);
                sc->txn = txn;
                sc->will_route_node = -1;
                sc->is_scheduled = false; // !这里做出逻辑的更改，当一个事务调度完成之后，不再从 txid_to_txn_map 中删除，而是设置 is_scheduled 标志
                sc->rw_flags = std::move(rw);
                sc->involved_pages.reserve(accounts_keys.size());

                bool potential_conflict = false;
                // 获取涉及的页面列表
                // lookup（内部会加 hot_mutex_），构造 involved_pages，并收集 page->tx 映射对
                for (size_t i = 0; i < accounts_keys.size(); ++i) {
                    auto entry = lookup(txn, table_ids[i], accounts_keys[i]);
                    if (entry.page == kInvalidPageId) {
                        assert(false); // 这里不应该失败
                    }
                    uint64_t table_page_id = (static_cast<uint64_t>(table_ids[i]) << 32) | entry.page;
                    sc->involved_pages.push_back(table_page_id);
                    
                    // Week conflict detection
                    if (local_page_tracker.count(table_page_id)) {
                        if(local_page_tracker[table_page_id] != tx_id) {
                            potential_conflict = true;
                            local_conflicts.insert(local_page_tracker[table_page_id]); 
                        }
                    }
                    local_page_tracker[table_page_id] = tx_id; 
                }
                if (potential_conflict) {
                    local_conflicts.insert(tx_id);
                }

                for (size_t i = 0; i < sc->involved_pages.size(); ++i) {
                    sc->page_to_metis_node_vec.push_back(-1); // 不考虑 metis 分区结果了
                }

                local_map[tx_id] = sc; 
            }
        }));
    }

    // join workers
    for (auto &fut : futs) fut.get(); // get() 会抛异常并传播任务异常
    
    allow_update_hot_entry(); // 开放 hot entry 更新

    // 计时
    struct timespec merge_start_time, merge_end_time;
    clock_gettime(CLOCK_MONOTONIC, &merge_start_time);

    // 将 local maps/pairs 合并
    std::vector<uint64_t> unique_conflict_pages; // Lifted up for optimization
    unique_conflict_pages.reserve(n * 4); 
    std::vector<tx_id_t> global_conflicted_txids;
    global_conflicted_txids.reserve(n);

    for (size_t t = 0; t < thread_count; ++t) {
        for (auto &p : local_txid_maps[t]) {
            txid_to_txn_map.emplace(p.first, std::move(p.second));
        }
        
        // Merge conflicts and collect involved pages simultaneously
        for(auto tx_id : local_conflicted_txns[t]) {
            conflicted_txns.insert(tx_id);
            global_conflicted_txids.push_back(tx_id);
            // sc is guaranteed to be valid and pointers are stable
            SchedulingCandidateTxn* sc = local_txid_maps[t].at(tx_id);
            unique_conflict_pages.insert(unique_conflict_pages.end(), sc->involved_pages.begin(), sc->involved_pages.end());
        }
    }
    // !构建page-txn映射的CSR存储结构，倒排索引, global page pairs 现在只需要存储冲突事务涉及的页面
    for(auto tx_id : conflicted_txns) {
        SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id];
        for(auto page : sc->involved_pages) {
            global_page_pairs.emplace_back(page, tx_id);
        }
    }

    std::sort(global_page_pairs.begin(), global_page_pairs.end(), [](const auto& a, const auto& b){
        return a.first < b.first;
    });
    
    for(size_t i=0; i<global_page_pairs.size(); ) {
        uint64_t page = global_page_pairs[i].first;
        size_t start = i;
        while(i < global_page_pairs.size() && global_page_pairs[i].first == page) {
            i++;
        }
        page_to_txn_range_map[page] = {static_cast<uint32_t>(start), static_cast<uint32_t>(i - start)};
    }

    clock_gettime(CLOCK_MONOTONIC, &merge_end_time);
    time_stats_.merge_global_txid_to_txn_map_ms += 
        (merge_end_time.tv_sec - merge_start_time.tv_sec) * 1000.0 + (merge_end_time.tv_nsec - merge_start_time.tv_nsec) / 1000000.0;

    // ! merge 结束，计算冲突事务的并查集，开始计时
    struct timespec compute_union_start_time, compute_union_end_time;
    clock_gettime(CLOCK_MONOTONIC, &compute_union_start_time);
        
    // 使用并查集将冲突事务划分为若干个不重叠的分区
    std::unordered_map<tx_id_t, tx_id_t> parent;
    std::function<tx_id_t(tx_id_t)> find = [&](tx_id_t i) {
        if (parent.find(i) == parent.end()) parent[i] = i;
        if (parent[i] == i) return i;
        return parent[i] = find(parent[i]);
    };
    auto union_op = [&](tx_id_t i, tx_id_t j) {
        tx_id_t root_i = find(i);
        tx_id_t root_j = find(j);
        if (root_i != root_j) parent[root_i] = root_j;
    };

    // If using parallel weak conflict, conflicted_txns is already populated.
    // We run Union-Find to group them into connected partitions based on shared page access.
    // Optimization: Iterate directly over the range map contained limited pages involved in conflicts.
    
    for(const auto& [page, range] : page_to_txn_range_map) {
        if(range.second <= 1) continue; // Only one conflicted txn touches this page, no new link
        
        tx_id_t first = global_page_pairs[range.first].second;
        // find(first); // 'first' is guaranteed to be in parent map if initialized lazily or pre-filled
        
        for (size_t i = 1; i < range.second; ++i) {
            tx_id_t curr = global_page_pairs[range.first + i].second;
            union_op(first, curr);
        }
    }

    // 导出分区
    std::unordered_map<tx_id_t, std::vector<tx_id_t>> partitions;
    for(auto tx_id : conflicted_txns) {
        partitions[find(tx_id)].push_back(tx_id);
    }
    std::vector<std::vector<tx_id_t>> conflicted_txn_partitions;
    for(auto& p : partitions) {
        conflicted_txn_partitions.push_back(std::move(p.second));
    }
    clock_gettime(CLOCK_MONOTONIC, &compute_union_end_time);
    time_stats_.compute_union_ms += 
        (compute_union_end_time.tv_sec - compute_union_start_time.tv_sec) * 1000.0 + (compute_union_end_time.tv_nsec - compute_union_start_time.tv_nsec) / 1000000.0;

    // !预处理结束, 计时
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.preprocess_txn_ms += 
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

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

    // !------------------------------------------preprocess end------------------------------------------   
    // !-----------------------------*****************************************----------------------------
    // 这里还没有获取 ownership 信息，后续再处理，合并之后首先要求事务之间的页面冲突关系，通过倒排索引
    // 以上为batch pipeline预处理的部分
    // !同步点，标志着上一个batch的事务执行完成
    
    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    // waiting the pending txns to be pushed to txn_queues_
    if(batch_id >= 0) {
        // 如果是第一个batch，则不需要等待
        pending_txn_queue_->wait_for_pending_txn_empty();
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
            // 把该批的事务都分发完成，设置batch处理完成标志
            txn_queues_[node_id]->set_batch_finished();
        }
    }
    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.wait_pending_txn_push_ms += 
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;
    logger->info("Waiting pending txn push time: " + std::to_string(
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0) + " ms");

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
                // // check if all the queue is empty
                // if(txn_queues_[i]->size() != 0 || !txn_queues_[i]->is_shared_queue_empty()) {
                //     return false;
                // }
                // 宽松 check if the queue is empty
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
    // print txn_queue size in each compute node
    // if(WarmupEnd){
        logger->info("[SmartRouter Scheduling] After waiting last batch finish, remained last batch txn size: " 
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
        );
    // }

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


    // !1. 获取ownership信息填充到page_to_ownership_node_vec，同时完成对非冲突事务的调度ownership_ok_txn_queues_per_thread, candidate_txn_queues_per_thread
    // !构建所有涉及页面的列表，并建立 ownership 快照，保证并发读取一致
    struct timespec ownership_start_time, ownership_end_time;
    clock_gettime(CLOCK_MONOTONIC, &ownership_start_time);
    std::unordered_map<uint64_t, std::pair<std::vector<node_id_t>, bool>> page_ownership_to_node_map; // 记录每个页面对应的节点ID（Ownership表结果）
    page_ownership_to_node_map.reserve(unique_conflict_pages.size());
    
    // 只获取冲突事务涉及的页面的 ownership
    // unique_conflict_pages 已经在 Merge 阶段收集完毕，此处只需去重
    std::sort(unique_conflict_pages.begin(), unique_conflict_pages.end());
    auto last_p = std::unique(unique_conflict_pages.begin(), unique_conflict_pages.end());
    unique_conflict_pages.erase(last_p, unique_conflict_pages.end());

    // 并行获取 conflict pages ownership
    size_t num_pages = unique_conflict_pages.size();
    size_t ownership_thread_count = std::min<size_t>(worker_threads_, (num_pages + 1000) / 1000); 
    if(ownership_thread_count == 0) ownership_thread_count = 1;

    std::vector<std::vector<std::pair<uint64_t, std::pair<std::vector<node_id_t>, bool>>>> local_ownership_results(ownership_thread_count);
    std::vector<std::future<void>> ownership_futs;
    ownership_futs.reserve(ownership_thread_count);
    
    size_t ownership_chunk = (num_pages + ownership_thread_count - 1) / ownership_thread_count;

    for(size_t t = 0; t < ownership_thread_count; ++t) {
        size_t start = t * ownership_chunk;
        size_t end = std::min(num_pages, start + ownership_chunk);
        
        ownership_futs.push_back(threadpool.enqueue([this, start, end, t, &unique_conflict_pages, &local_ownership_results]() {
            auto& local_res = local_ownership_results[t];
            local_res.reserve(end - start);
            for(size_t i = start; i < end; ++i) {
                uint64_t page = unique_conflict_pages[i];
                auto owner_stats = std::move(ownership_table_->get_owner(page));
                local_res.emplace_back(page, std::move(owner_stats));
            }
        }));
    }

    // Don't wait here.
    // for(auto& fut : ownership_futs) fut.get();

    std::atomic<int> unconflict_and_ownership_ok_txn_cnt, unconflict_and_ownership_cross_txn_cnt, unconflict_and_shared_txn_cnt;
    std::vector<std::atomic<int>> schedule_txn_cnt_per_node_this_batch(ComputeNodeCount);
    std::atomic<int> candidate_txn_cnt;
    std::vector<std::atomic<int>> ownership_ok_txn_cnt_per_node(ComputeNodeCount);
    std::vector<std::atomic<int>> expected_page_transfer_count_per_node(ComputeNodeCount);
    futs.clear();
    futs.reserve(thread_count);
    std::vector<double> compute_node_workload_benefit = this->workload_balance_penalty_weights_; // 负载均衡因子
    std::vector<double> remain_queue_balance_penalty_weights = this->remain_queue_balance_penalty_weights_; // 剩余队列负载均衡因子
    std::vector<double>  total_load_balance_penalty_weights(ComputeNodeCount);
    for(int i=0; i<ComputeNodeCount; i++) {
        total_load_balance_penalty_weights[i] = compute_node_workload_benefit[i] + remain_queue_balance_penalty_weights[i];
    }
    
    for (size_t t = 0; t < thread_count; ++t) {
        size_t start = t * chunk;
        size_t end = std::min(n, start + chunk);
        futs.push_back(threadpool.enqueue([this, &txn_batch, &txid_to_txn_map, start, end, t, &total_load_balance_penalty_weights, 
                conflicted_txns, &page_ownership_to_node_map, &candidate_txn_cnt, &ownership_ok_txn_cnt_per_node, &page_to_txn_range_map, 
                &unconflict_and_ownership_ok_txn_cnt, &unconflict_and_ownership_cross_txn_cnt, &unconflict_and_shared_txn_cnt, 
                &schedule_txn_cnt_per_node_this_batch, &expected_page_transfer_count_per_node]() {
            
            std::vector<std::list<TxnQueueEntry*>> node_routed_txns(ComputeNodeCount);
            for (size_t idx = start; idx < end; ++idx) {
                tx_id_t tx_id = txn_batch->at(idx)->tx_id;
                
                // ! Phase A: Only process Non-Conflicting transactions
                if (conflicted_txns.count(tx_id)) continue;

                SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id];
                if(sc == nullptr) assert(false); // 不可能出现的情况
                // 填充 page_to_ownership_node_vec
                if(sc->ownership_node_count.size() != ComputeNodeCount) sc->ownership_node_count.assign(ComputeNodeCount, 0);

                // Non-conflict txn: fetch ownership directly (lazy fetch)
                for (int i = 0; i < sc->involved_pages.size(); ++i) {
                    auto page = sc->involved_pages[i];
                    auto owner_stats = ownership_table_->get_owner(page);
                    update_sc_ownership_count(sc, i, {{}, false}, {owner_stats.first, owner_stats.second});
                    // 热度级别累加
                    sc->hot_level += page_to_txn_range_map.count(page) ? page_to_txn_range_map.at(page).second : 0; 
                }

                // 计算是否有计算节点满足页面所有权
                std::vector<node_id_t> candidate_ownership_nodes = checkif_txn_ownership_ok(sc);
                if(!candidate_ownership_nodes.empty()) {
                    // 满足ownership entirely
                    if(candidate_ownership_nodes.size() > 1) {
                        // 多个节点满足ownership entirely，选择负载最轻的节点
                        node_id_t best_node = -1;
                        int max_compute_node_workload_benefit = -1;
                        for(const auto& node_id : candidate_ownership_nodes) {
                            if(total_load_balance_penalty_weights[node_id] > max_compute_node_workload_benefit) {
                                max_compute_node_workload_benefit = total_load_balance_penalty_weights[node_id];
                                best_node = node_id;
                            }
                        }
                        sc->will_route_node = best_node;
                    }
                    else {
                        sc->will_route_node = candidate_ownership_nodes.front();
                    }
                    node_id_t ownership_node = sc->will_route_node;
                    
                    {
                        // ! 如果该事务没有冲突，直接加入txn_queues, 工作线程可以直接处理
                        // 计时
                        struct timespec push_begin_time, push_end_time;
                        clock_gettime(CLOCK_MONOTONIC, &push_begin_time);
                        node_routed_txns[ownership_node].push_back(sc->txn);
                        sc->txn->schedule_type = TxnScheduleType::UNCONFLICT;
                        // 如果达到批量大小，则批量推送
                        if(node_routed_txns[ownership_node].size() >= BatchExecutorPOPTxnSize){ 
                            int push_cnt = node_routed_txns[ownership_node].size();
                            this->routed_txn_cnt_per_node[ownership_node] += push_cnt;
                            load_tracker_.record(ownership_node, push_cnt); // 最近w个事务
                            schedule_txn_cnt_per_node_this_batch[ownership_node] += push_cnt;

                            txn_queues_[ownership_node]->push_txn_back_batch(node_routed_txns[ownership_node]);
                            node_routed_txns[ownership_node].clear();
                        }
                        clock_gettime(CLOCK_MONOTONIC, &push_end_time);
                        time_stats_.push_txn_to_queue_ms_per_thread[t] += 
                            (push_end_time.tv_sec - push_begin_time.tv_sec) * 1000.0 + 
                            (push_end_time.tv_nsec - push_begin_time.tv_nsec) / 1000000.0;
                    }
                    sc->is_scheduled = true; // 标记该事务已经被调度
                    unconflict_and_ownership_ok_txn_cnt++;
                } else {
                    // 记录为候选事务
                    // 使用历史负载均衡信息直接快速计算
                    compute_benefit_for_node(sc, sc->ownership_node_count, total_load_balance_penalty_weights, 0.0 , 1.0, 1.0);
                    assert(sc->node_benefit_map.size() == ComputeNodeCount);
                    // 如果该事务没有冲突，可以直接调度
                    // 选择benefit最高的节点作为will_route_node
                    double max_benefit = -1.0;
                    int best_node = -1;
                    double min_benefit = 1e9;
                    int worst_node = -1;
                    for(int node_id = 0; node_id < ComputeNodeCount; ++node_id) {
                        double benefit = sc->node_benefit_map[node_id];
                        if(benefit > max_benefit) {
                            max_benefit = benefit;
                            best_node = node_id;
                        }
                        if(benefit < min_benefit) {
                            min_benefit = benefit;
                            worst_node = node_id;
                        }
                    }
                    if(max_benefit - min_benefit < 0) {
                        // !如果best_node和worst_node的benefit差距不大，则可以加入到共享队列中
                        txn_queues_[best_node]->push_txn_into_shared_queue(sc->txn);
                        sc->is_scheduled = true; // 标记该事务已经被调度
                        unconflict_and_shared_txn_cnt++;
                    }
                    else{
                        sc->will_route_node = best_node;
                        {
                            // !直接加入txn_queues, 工作线程可以直接处理
                            // 计时
                            struct timespec push_begin_time, push_end_time;
                            clock_gettime(CLOCK_MONOTONIC, &push_begin_time);
                            node_routed_txns[best_node].push_back(sc->txn);
                            // 如果达到批量大小，则批量推送
                            if(node_routed_txns[best_node].size() >= BatchExecutorPOPTxnSize){ 
                                int push_cnt = node_routed_txns[best_node].size();
                                this->routed_txn_cnt_per_node[best_node] += push_cnt;
                                load_tracker_.record(best_node, push_cnt); // 最近w个事务
                                schedule_txn_cnt_per_node_this_batch[best_node] += push_cnt;
                                txn_queues_[best_node]->push_txn_back_batch(node_routed_txns[best_node]);
                                node_routed_txns[best_node].clear();
                            }
                            clock_gettime(CLOCK_MONOTONIC, &push_end_time);
                            time_stats_.push_txn_to_queue_ms_per_thread[t] += 
                                (push_end_time.tv_sec - push_begin_time.tv_sec) * 1000.0 + 
                                (push_end_time.tv_nsec - push_begin_time.tv_nsec) / 1000000.0;
                        }
                        sc->is_scheduled = true; // 标记该事务已经被调度
                        unconflict_and_ownership_cross_txn_cnt++;
                        // 记录预期的页面迁移数量
                        int expected_page_transfer_cnt = sc->involved_pages.size() - sc->ownership_node_count[best_node];
                        expected_page_transfer_count_per_node[best_node] += expected_page_transfer_cnt;
                    }
                }
            } // End of Non-Conflict Loop

            // 将剩余的node_routed_txns批量推送到txn_queues_
            struct timespec push_begin_time, push_end_time;
            clock_gettime(CLOCK_MONOTONIC, &push_begin_time);
            for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                if(!node_routed_txns[node_id].empty()) {
                    int push_cnt = node_routed_txns[node_id].size();
                    this->routed_txn_cnt_per_node[node_id] += push_cnt;
                    load_tracker_.record(node_id, push_cnt); // 最近w个事务
                    schedule_txn_cnt_per_node_this_batch[node_id] += push_cnt;
                    txn_queues_[node_id]->push_txn_back_batch(node_routed_txns[node_id]);
                    node_routed_txns[node_id].clear();
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &push_end_time);
            time_stats_.push_txn_to_queue_ms_per_thread[t] += 
                (push_end_time.tv_sec - push_begin_time.tv_sec) * 1000.0 + 
                (push_end_time.tv_nsec - push_begin_time.tv_nsec) / 1000000.0;
        }));
    }    

    // ! Wait for Ownership Fetching First
    for(auto& fut : ownership_futs) fut.get();

    // Merge Ownership Results (Now safe, single threaded or we can parallelize if map supports it. Standard map doesn't)
    for(size_t t = 0; t < ownership_thread_count; ++t) {
        for(auto& pair : local_ownership_results[t]) {
            page_ownership_to_node_map.emplace(pair.first, std::move(pair.second));
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &ownership_end_time);
    time_stats_.get_page_ownership_ms += 
        (ownership_end_time.tv_sec - ownership_start_time.tv_sec) * 1000.0 + (ownership_end_time.tv_nsec - ownership_start_time.tv_nsec) / 1000000.0;


    // ! Phase B: Process Conflicting Transactions (Now that ownership is ready)
    // We launch new tasks for this to parallelize over global_conflicted_txids
    std::vector<std::future<void>> conflict_dispatch_futs;
    size_t num_conflicts = global_conflicted_txids.size();
    if (num_conflicts > 0) {
        size_t c_thread_count = std::min<size_t>(worker_threads_, (num_conflicts + 100) / 100); 
        if(c_thread_count == 0) c_thread_count = 1;
        size_t c_chunk = (num_conflicts + c_thread_count - 1) / c_thread_count;
        
        conflict_dispatch_futs.reserve(c_thread_count);
        for(size_t t = 0; t < c_thread_count; ++t) {
            size_t start = t * c_chunk;
            size_t end = std::min(num_conflicts, start + c_chunk);
            
            conflict_dispatch_futs.push_back(threadpool.enqueue([this, start, end, t, &global_conflicted_txids, &txid_to_txn_map, 
                &page_ownership_to_node_map, &page_to_txn_range_map, &ownership_ok_txn_cnt_per_node, &total_load_balance_penalty_weights,
                &candidate_txn_cnt]() {
                
                for(size_t i = start; i < end; ++i) {
                    tx_id_t tx_id = global_conflicted_txids[i];
                    SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id];
                    
                    if(sc->ownership_node_count.size() != ComputeNodeCount) sc->ownership_node_count.assign(ComputeNodeCount, 0);

                    // Conflict txn: must rely on the consistent snapshot in page_ownership_to_node_map
                    for (int j = 0; j < sc->involved_pages.size(); ++j) {
                    auto page = sc->involved_pages[j];
                    // assert(page_ownership_to_node_map.count(page)); 
                    // Note: If assert fails, it means we missed a conflict page in 'unique_conflict_pages' collection
                    if (page_ownership_to_node_map.count(page)) {
                            const auto &owner_pair = page_ownership_to_node_map.at(page); 
                            update_sc_ownership_count(sc, j, {{}, false}, owner_pair); 
                    } else {
                            // Fallback? Should not happen if logic is correct.
                            // But involved_pages might contain non-conflict pages too?
                            // No, unique_conflict_pages logic collected ALL involved pages of ALL conflict txns.
                            // So it must be there.
                            // assert(false);
                            // If missing (maybe logic error), fetch it? Unsafe in parallel if not guarded.
                            // But let's assume correct collection.
                    }
                    sc->hot_level += page_to_txn_range_map.count(page) ? page_to_txn_range_map.at(page).second : 0; 
                    }

                    // Check ownership
                    std::vector<node_id_t> candidate_ownership_nodes = checkif_txn_ownership_ok(sc);
                    if(!candidate_ownership_nodes.empty()) {
                    int max_ownership_count = 0;
                    int ownership_node = -1;
                    for(auto node_id : candidate_ownership_nodes){
                        if(sc->ownership_node_count[node_id] > max_ownership_count){
                            max_ownership_count = sc->ownership_node_count[node_id];
                            ownership_node = node_id;
                        }
                    }
                    if(ownership_node == -1) ownership_node = candidate_ownership_nodes[0];
                    
                    ownership_ok_txn_cnt_per_node[ownership_node]++;
                    sc->will_route_node = ownership_node; 
                    } else {
                        candidate_txn_cnt++;
                        // Conflicting txn, failed ownership -> candidate.
                        // Do we need to call compute_benefit_for_node?
                        compute_benefit_for_node(sc, sc->ownership_node_count, total_load_balance_penalty_weights, 0.0 , 1.0, 0.0);
                    }
                }
            }));
        }
    }

    // join workers
    for (auto &fut : futs) fut.get(); // Non-Conflicts
    for (auto &fut : conflict_dispatch_futs) fut.get(); // Conflicts

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.ownership_retrieval_and_devide_unconflicted_txn_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    // // 计时
    // clock_gettime(CLOCK_MONOTONIC, &start_time);
        
    // // 计时结束
    // clock_gettime(CLOCK_MONOTONIC, &end_time);
    // time_stats_.merge_and_construct_ipq_ms +=
    //     (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    int ownership_ok_txn_total = 0;
    for(int i=0; i<ComputeNodeCount; i++) {
        ownership_ok_txn_total += ownership_ok_txn_cnt_per_node[i].load();
    }
    // 校验 ownership_ok_txn_total + candidate_txn_ids.size() + unconflict_and_ownership_cross_txn_cnt + unconflict_and_ownership_ok_txn_cnt == txid_to_txn_map.size()
    assert(txid_to_txn_map.size() == ownership_ok_txn_total + candidate_txn_cnt.load() +
        unconflict_and_ownership_cross_txn_cnt.load() + unconflict_and_ownership_ok_txn_cnt.load() + unconflict_and_shared_txn_cnt.load());

    // if(WarmupEnd){
        // Parallel router processing including txn involved pages, conflict map, ownership info retrieval, parallel unconflicted txn scheduling done
        logger->info("Batch id: " + std::to_string(batch_id) + "Parallel router processing done.");
        // print txn_conflict_map_size and unconflicted_txns size 
        logger->info("Txn conflict map size: " + std::to_string(conflicted_txns.size()));
        logger->info("Ownership ok txn total size: " + std::to_string(ownership_ok_txn_total));
        for(int i=0; i<ComputeNodeCount; i++){
            logger->info("Node " + std::to_string(i) + 
                            " ownership_ok txn count: " + std::to_string(ownership_ok_txn_cnt_per_node[i].load()));
        }
        logger->info("Candidate txn ids size: " + std::to_string(candidate_txn_cnt.load()));
        logger->info("Unconflicted txns size: " + std::to_string(txid_to_txn_map.size() - conflicted_txns.size()));
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
    #if LOG_KROUTER_SCHEDULING_DEBUG
        // print txn_conflict_map
        for(const auto& tx_id : conflicted_txns) {
            auto iter = txid_to_txn_map.find(tx_id); 
            logger->info("Txn " + std::to_string(tx_id) + 
                            " involves pages: " + [&]() {
                                std::string pages_str;
                                for(const auto& page : iter->second->involved_pages) {
                                    pages_str += std::to_string(page) + " ";
                                }
                                pages_str += " page_to_metis_node: ";
                                for(const auto& metis_node : iter->second->page_to_metis_node_vec) {
                                    pages_str += std::to_string(metis_node) + " ";
                                }
                                pages_str += " page_ownership_node: ";
                                for(const auto& ownership_node : iter->second->page_to_ownership_node_vec) {
                                    pages_str += "{";
                                    for(const auto& node : ownership_node.first) {
                                        pages_str += std::to_string(node) + " ";
                                    }
                                    pages_str += "} ";
                                }
                                return pages_str;
                            }());
        }
        // print conflict page_to_txn_map
        for(const auto& [page, txn_ids] : page_to_txn_map) {
            if(txn_ids.size() <= 1) continue; // 说明是非冲突的页面，没必要输出，直接往下走
            logger->info("Page " + std::to_string(page) + 
                            " involved by txns: " + [&]() {
                                std::string txns_str;
                                for(const auto& tx_id : txn_ids) {
                                    txns_str += std::to_string(tx_id) + " ";
                                }
                                return txns_str;
                            }());
        }
        // print ownership_ok_txn_queues
        for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
            std::string txns_str;
            for(const auto& tx_id : ownership_ok_txn_queues[node_id]) {
                txns_str += std::to_string(tx_id) + " ";
            }
            logger->info("Node " + std::to_string(node_id) + 
                            " ownership_ok_txn_queues: " + txns_str);
        }
    #endif
    // }

    // 计时
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // !2. 多线程进行调度决策
    // 2.1 从并查集合并小的分区
    std::vector<std::vector<tx_id_t>> merged_partitions(thread_count);
    std::vector<int> merged_partitions_per_thread_count(thread_count, 0);
    std::vector<size_t> partition_sizes(thread_count, 0);
    
    // Sort partitions by size descending to help load balancing
    std::sort(conflicted_txn_partitions.begin(), conflicted_txn_partitions.end(), 
        [](const std::vector<tx_id_t>& a, const std::vector<tx_id_t>& b) {
            return a.size() > b.size();
        });

    for(const auto& part : conflicted_txn_partitions) {
        // Find thread with min txns
        size_t min_idx = 0;
        size_t min_size = partition_sizes[0];
        for(size_t i=1; i<thread_count; ++i) {
            if(partition_sizes[i] < min_size) {
                min_size = partition_sizes[i];
                min_idx = i;
            }
        }
        merged_partitions[min_idx].insert(merged_partitions[min_idx].end(), part.begin(), part.end());
        partition_sizes[min_idx] += part.size();
        merged_partitions_per_thread_count[min_idx] += 1;
    }

    logger->info("[Multi-Thread Scheduling] Batch id: " + std::to_string(batch_id) + 
                    " Merged conflicted txn partitions into " + std::to_string(thread_count) + " threads." + 
                    " Merged partitions per thread count: " + [&]() {
                        std::string counts_str;
                        counts_str += "[";
                        for(size_t i=0; i<thread_count; ++i) {
                            counts_str += "( size: " + std::to_string(partition_sizes[i]) + ", count: ";
                            counts_str += std::to_string(merged_partitions_per_thread_count[i]) + "), ";
                        }
                        counts_str += "]";
                        return counts_str;
                    }());

    // 2.2 并行调度
    futs.clear();
    std::vector<std::vector<std::pair<node_id_t, std::vector<TxnQueueEntry*>>>> local_dag_ready_txns(thread_count);
    std::vector<int> local_scheduled_front_txn_cnt(thread_count, 0);
    std::vector<std::atomic<int>> ownership_queue_sizes(ComputeNodeCount);
    for(int i=0; i<ComputeNodeCount; ++i) ownership_queue_sizes[i] = ownership_ok_txn_cnt_per_node[i].load();
    
    std::vector<std::vector<std::unordered_set<tx_id_t>>> local_ownership_ok_txn_queues_list(thread_count);
    std::atomic<int> scheduled_counter, scheduled_front_txn_cnt = 0;
    for (size_t t = 0; t < thread_count; ++t) {
        // Resize the local vector for this thread
        local_ownership_ok_txn_queues_list[t].resize(ComputeNodeCount);
        if (merged_partitions[t].empty()) continue;

        futs.push_back(threadpool.enqueue([this, t, &merged_partitions, &local_dag_ready_txns, &local_scheduled_front_txn_cnt, 
                                            &txid_to_txn_map, &page_ownership_to_node_map, &global_page_pairs, &page_to_txn_range_map, &scheduled_front_txn_cnt,
                                            &local_ownership_ok_txn_queues_list, &ownership_queue_sizes, &scheduled_counter,
                                            &expected_page_transfer_count_per_node, &schedule_txn_cnt_per_node_this_batch, 
                                            &compute_node_workload_benefit, &debug_pages]() {
            
            struct timespec r_start_time, r_end_time;
            clock_gettime(CLOCK_MONOTONIC, &r_start_time);

            auto& my_txns = merged_partitions[t];
            auto& local_ownership_ok_txn_queues = local_ownership_ok_txn_queues_list[t];
            
            // 构建页面-not_ok_txn_cnt 的数量, 一个页面要么ownership_ok，要么schedule_front (之前ok), 要么还不ok过
            std::unordered_map<uint64_t, int> page_not_ok_txn_cnt;

            // 构建本地 IPQ
            std::vector<DenseIPQ<std::pair<double, int>, std::greater<std::pair<double, int>>>> local_ipq(ComputeNodeCount);
            for(int i=0; i<ComputeNodeCount; i++) local_ipq[i].reserve(my_txns.size());

            std::vector<SchedulingCandidateTxn*> local_lookup;
            local_lookup.reserve(my_txns.size());
            std::vector<uint8_t> local_candidates_active; 
            local_candidates_active.reserve(my_txns.size());
            int active_candidate_count = 0;
            
            for(auto tx_id : my_txns) {
                SchedulingCandidateTxn* sc = txid_to_txn_map[tx_id];
                assert(sc != nullptr);
                assert(!sc->is_scheduled); // 不可能已经被调度过
                
                // Populate local_ownership_ok_txn_queues
                if (sc->will_route_node != -1) {
                    local_ownership_ok_txn_queues[sc->will_route_node].insert(tx_id);
                } else {
                    sc->dense_id = local_lookup.size();
                    local_lookup.push_back(sc);
                    local_candidates_active.push_back(1);
                    active_candidate_count++;

                    for(int node_id = 0; node_id < ComputeNodeCount; ++node_id) {
                        double benefit = sc->node_benefit_map[node_id];
                        local_ipq[node_id].insert(sc->dense_id, {benefit, sc->hot_level});
                    }
                    for(const auto& page : sc->involved_pages) {
                        page_not_ok_txn_cnt[page]++;
                    }
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &r_end_time);
            if(t==0) // only log for thread 0
            time_stats_.merge_and_construct_ipq_ms +=
                (r_end_time.tv_sec - r_start_time.tv_sec) * 1000.0 + (r_end_time.tv_nsec - r_start_time.tv_nsec) / 1000000.0;

            if(t==0)
            logger->info("[Multi-Thread Scheduling Init Success. ] Batch id: " + std::to_string(batch_id) + 
                            " Thread " + std::to_string(t) + 
                            " local candidate txn ids size: " + std::to_string(active_candidate_count) + 
                            " local ownership_ok_txn_queues size: " + [&]() {
                                std::string sizes_str;
                                sizes_str += "[";
                                for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
                                    int queue_size = local_ownership_ok_txn_queues[node_id].size();
                                    sizes_str += "Node " + std::to_string(node_id) + ": " + std::to_string(queue_size) + ", ";
                                }
                                sizes_str += "]";
                                return sizes_str;
                            }());
            
            // 移动到 while 循环外，避免频繁申请内存
            std::vector<std::vector<TxnQueueEntry*>> dag_ready_txn(ComputeNodeCount);
            
            std::unordered_map<uint64_t, std::shared_ptr<DependencyGroup>> page_fences;

            while(active_candidate_count > 0) {
                // Clear and reuse dag_ready_txn explicitly if needed, though they are moved-from.
                // Assuming moved-from vectors are empty or we resize them.
                for(auto& v : dag_ready_txn) { if(!v.empty()) v.clear(); }

                scheduled_counter++;
                
                // 1.1 选择节点
                struct timespec select_start_time, select_end_time;
                clock_gettime(CLOCK_MONOTONIC, &select_start_time);

                int min_txn_node = -1;
                int min_txn_count = INT32_MAX;
                
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                    if (local_ipq[node_id].empty()) continue;

                    int workload = txn_queues_[node_id]->size() + ownership_queue_sizes[node_id].load();
                    
                    if(workload < min_txn_count) {
                        min_txn_count = workload;
                        min_txn_node = node_id;
                    }
                }
                
                if (min_txn_node == -1) break; 

                // Pop best txn
                auto [best_dense_id, best_benefit_pair] = local_ipq[min_txn_node].pop();
                double best_benefit = best_benefit_pair.first;

                // Check if the txn is still a candidate
                if (!local_candidates_active[best_dense_id]) {
                    continue;
                }

                SchedulingCandidateTxn* selected_candidate_txn = local_lookup[best_dense_id];
                tx_id_t best_tx_id = selected_candidate_txn->txn->tx_id;
                
                if(selected_candidate_txn->is_scheduled) {
                    continue;
                }
                
                double max_benefit_score = best_benefit;

                clock_gettime(CLOCK_MONOTONIC, &select_end_time);
                time_stats_.select_condidate_txns_ms +=
                    (select_end_time.tv_sec - select_start_time.tv_sec) * 1000.0 + (select_end_time.tv_nsec - select_start_time.tv_nsec) / 1000000.0;

            #if LOG_BATCH_ROUTER
                if(t==0)
                logger->info("[SmartRouter Scheduling] Batch id: " + std::to_string(batch_id) + 
                                " Thread " + std::to_string(t) + 
                                " Selected txn " + std::to_string(selected_candidate_txn->txn->tx_id) + 
                                " from candidate ids, benefit score " + std::to_string(max_benefit_score) + 
                                " on node " + std::to_string(min_txn_node) + " all score: " + [&]() {
                                    std::string scores_str;
                                    scores_str += "[";
                                    for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                                        scores_str += std::to_string(selected_candidate_txn->node_benefit_map[node_id]) + ", ";
                                    }
                                    scores_str += "]";
                                    return scores_str;
                                }()
                                + "hot level: " + std::to_string(selected_candidate_txn->hot_level)
                            );
            #endif
            #if LOG_KROUTER_SCHEDULING_DEBUG
                if(WarmupEnd){
                    if(selected_candidate_txn != nullptr){
                        logger->info("[SmartRouter Scheduling] Selected txn " + std::to_string(selected_candidate_txn->txn->tx_id) + 
                                    " to transfer pages to node " + std::to_string(min_txn_node) + 
                                    " with benefit score " + std::to_string(max_benefit_score));
                    }
                }
            #endif
                
                // 1.2 找到合适的不满足ownership entirely的事务，进行页面转移计划的制订
                struct timespec compute_transfer_start_time, compute_transfer_end_time;
                clock_gettime(CLOCK_MONOTONIC, &compute_transfer_start_time);
                std::unordered_map<uint64_t, std::pair<std::vector<node_id_t>, bool>> transfer_pages;
                if(selected_candidate_txn != nullptr){
                    size_t page_count = selected_candidate_txn->involved_pages.size();
                    transfer_pages.reserve(page_count); 
                    for(int i=0; i<page_count; i++){
                        auto page = selected_candidate_txn->involved_pages[i];
                        // Optimization: Use reference to avoid expensive copy of vector
                        const auto& owner_stats = page_ownership_to_node_map.at(page);
                        bool rw = selected_candidate_txn->rw_flags[i];
                        
                        if(rw){
                            // write operation, need exclusive ownership
                            if(!(owner_stats.second == 1 && owner_stats.first.size() == 1 && owner_stats.first[0] == min_txn_node)){
                                // check if already in transfer_pages ? 
                                // In original logic: if write, just overwrite.
                                transfer_pages[page] = {{min_txn_node}, 1}; // exclusive ownership
                            }
                        } else {
                            // check if already in transfer_pages
                            auto it = transfer_pages.find(page);
                            if (it != transfer_pages.end()){
                                // already in transfer_pages
                                continue;
                            }
                            else {
                                // read operation, shared ownership is ok
                                bool has_shared_ownership = false;
                                for(const auto& ownership_node : owner_stats.first){
                                    if(ownership_node == min_txn_node){
                                        has_shared_ownership = true;
                                        break;
                                    }
                                }
                                if(!has_shared_ownership){
                                    // Optimization: Only copy when needed
                                    std::pair<std::vector<node_id_t>, bool> new_ownership_stats = owner_stats;
                                    new_ownership_stats.first.push_back(min_txn_node);
                                    new_ownership_stats.second = false; // shared ownership
                                    transfer_pages[page] = std::move(new_ownership_stats);
                                } 
                            }
                        }
                    }
                }
                // 记录预期的页面迁移数量
                expected_page_transfer_count_per_node[min_txn_node] += transfer_pages.size();
            #if LOG_BATCH_ROUTER
                for(const auto& [page, new_ownership_stats] : transfer_pages) {
                    if(debug_pages.find(page) != debug_pages.end()) {
                        if(t==0)
                        logger->info("[SmartRouter Scheduling] Batch id: " + std::to_string(batch_id) + 
                                        " Debug page (" + debug_pages[page] + ")" + 
                                        " page: " + std::to_string(page) +
                                        " will transfer to node " + std::to_string(min_txn_node) + 
                                        " new_ownership_stats: " + [&]() {
                                            std::string stats_str;
                                            stats_str += "{";
                                            for(const auto& node : new_ownership_stats.first) {
                                                stats_str += std::to_string(node) + " ";
                                            }
                                            stats_str += "}, is_exclusive: " + std::to_string(new_ownership_stats.second);
                                            return stats_str;
                                        }());   
                    }
                    // if(page == debug_page0) {
                    //     logger->info("[SmartRouter Scheduling] table 0 key 1 page " + std::to_string(page) + 
                    //                     " will transfer to node " + std::to_string(min_txn_node));   
                    // }
                    // if(page == debug_page1) {
                    //     logger->info("[SmartRouter Scheduling] table 1 key 1 page " + std::to_string(page) + 
                    //                     " will transfer to node " + std::to_string(min_txn_node));
                    // }
                }
            #endif
                clock_gettime(CLOCK_MONOTONIC, &compute_transfer_end_time);
                time_stats_.compute_transfer_page_ms += (compute_transfer_end_time.tv_sec - compute_transfer_start_time.tv_sec) * 1000.0 + 
                    (compute_transfer_end_time.tv_nsec - compute_transfer_start_time.tv_nsec) / 1000000.0;
                
                // 1.3 找到所有转移页面所涉及到的事务
                struct timespec find_affected_start_time, find_affected_end_time;
                clock_gettime(CLOCK_MONOTONIC, &find_affected_start_time);
                std::unordered_set<tx_id_t> affected_txns;
                for(auto [transfer_page, new_ownership_stats] : transfer_pages) {

                    if (page_to_txn_range_map.find(transfer_page) == page_to_txn_range_map.end()) continue;
                    auto range = page_to_txn_range_map[transfer_page];

                    for(size_t i=0; i<range.second; ++i) {
                        tx_id_t affected_txn_id = global_page_pairs[range.first + i].second;
                        if(txid_to_txn_map[affected_txn_id]->is_scheduled) continue; // ! Lazy skip

                        affected_txns.insert(affected_txn_id);
                    }
                }
                clock_gettime(CLOCK_MONOTONIC, &find_affected_end_time);
                time_stats_.find_affected_txns_ms += (find_affected_end_time.tv_sec - find_affected_start_time.tv_sec) * 1000.0 + 
                    (find_affected_end_time.tv_nsec - find_affected_start_time.tv_nsec) / 1000000.0;

                // 1.4 进行事务的编排，找到转移页面后，原来可以满足ownership entirely的事务可能不满足的，将这些事务先执行
                struct timespec decide_schedule_start_time, decide_schedule_end_time;
                clock_gettime(CLOCK_MONOTONIC, &decide_schedule_start_time);
                std::vector<SchedulingCandidateTxn*> schedule_txn_prior; // 记录本次调度的事务
                std::vector<SchedulingCandidateTxn*> next_time_schedule_txn; // 记录下次调度的事务
                int new_ownership_ok_txn_cnt = 0, change_ownership_node_txn_cnt = 0, ownership_still_not_ok_txn_cnt = 0, must_schedule_prior_txn_cnt = 0;
                std::string still_not_ok_txn_debug;
                for(auto affected_txn_id : affected_txns) {
                    auto it = txid_to_txn_map.find(affected_txn_id);
                    assert(it != txid_to_txn_map.end());
                    assert(it->second->is_scheduled == false); // 说明这个事务还没有被调度过
                    // 更新所有涉及页面的所有权
                    for(int i=0; i<it->second->involved_pages.size(); i++){
                        auto page = it->second->involved_pages[i];
                        auto tp_it = transfer_pages.find(page);
                        if (tp_it != transfer_pages.end()) {
                            // tp_it->second 是 new_ownership
                            // 我们需要 old_ownership，它在 page_ownership_to_node_map 中
                            update_sc_ownership_count(it->second, i, page_ownership_to_node_map.at(page), tp_it->second);
                        } 
                    }
                    // check if the affected_txn_id can still execute
                    std::vector<node_id_t> candidate_ownership_nodes = checkif_txn_ownership_ok(it->second);
                    
                    if(!candidate_ownership_nodes.empty()) {
                        node_id_t last_can_execute_node = it->second->will_route_node;
                        // 注意这里尽管向min_txn_node执行，但是实际上可能会选择其他节点执行, 因为可能只是向min_txn_node增加了shared ownership
                        bool execute_node_not_changed = false;
                        for(const auto& node_id : candidate_ownership_nodes) {
                            if(node_id == last_can_execute_node) {
                                execute_node_not_changed = true;
                                break;
                            }
                        }
                        // ! case0: 之前可以执行, 页面所有权转移之后仍然可以执行, 且执行节点不变
                        if(execute_node_not_changed) continue; // 说明之前就已经在该节点上执行，不需要变动
                        bool min_txn_node_in_candidates = false;
                        for(const auto& node_id : candidate_ownership_nodes) {
                            if(node_id == min_txn_node) {
                                min_txn_node_in_candidates = true;  
                                break;
                            }
                        }
                        node_id_t now_can_execute_node = -1;
                        if(min_txn_node_in_candidates) {
                            now_can_execute_node = min_txn_node;
                        } else {
                            now_can_execute_node = candidate_ownership_nodes.front(); // 选择第一个可执行节点
                        }

                        if(last_can_execute_node != -1) {
                            // ! 修改逻辑, 这里不盲目的去新的节点执行, 改为随机吧
                            bool last_node_better =  rand() % 2;
                            if (last_node_better) {
                                // !! 就在原来节点执行
                                local_ownership_ok_txn_queues[last_can_execute_node].erase(affected_txn_id);
                                ownership_queue_sizes[last_can_execute_node]--;
                                // !表示这轮计划的迁移所造成的不能执行的事务, 需要先执行
                                schedule_txn_prior.push_back(it->second); 
                                it->second->txn->schedule_type = TxnScheduleType::SCHEDULE_PRIOR;
                                
                                this->routed_txn_cnt_per_node[last_can_execute_node]++;
                                load_tracker_.record(last_can_execute_node);
                                schedule_txn_cnt_per_node_this_batch[last_can_execute_node]++;
                                // cnt
                                must_schedule_prior_txn_cnt++;
                                // !标记该事务已经被调度
                                it->second->is_scheduled = true;
                            }
                            else {      
                                // ! case1: 之前可以执行, 但现在需要变动到新的节点上执行, 但执行节点变了
                                // 加入next_time_schedule_txn
                                next_time_schedule_txn.push_back(it->second);
                                local_ownership_ok_txn_queues[last_can_execute_node].erase(affected_txn_id);
                                ownership_queue_sizes[last_can_execute_node]--;
                                it->second->will_route_node = now_can_execute_node;
                                local_ownership_ok_txn_queues[now_can_execute_node].insert(affected_txn_id);
                                ownership_queue_sizes[now_can_execute_node]++;
                                // cnt
                                change_ownership_node_txn_cnt++;
                            #if LOG_BATCH_ROUTER
                                if(t==0)
                                logger->info("[SmartRouter Scheduling] Batch id: " + std::to_string(batch_id) + 
                                            " Thread " + std::to_string(t) + 
                                            " affected txn " + std::to_string(affected_txn_id) + 
                                            " will change execute node from " + std::to_string(last_can_execute_node) + 
                                            " to " + std::to_string(now_can_execute_node));
                            #endif
                            }
                        } else{
                            // ! case2: 之前不可以执行, 现在可以执行, 将该事务从candidate_txn_ids中删除，表示现在可以执行
                            // 加入next_time_schedule_txn
                            next_time_schedule_txn.push_back(it->second);
                            local_candidates_active[it->second->dense_id] = 0;
                            active_candidate_count--;
                            for(const auto& page : it->second->involved_pages) {
                                page_not_ok_txn_cnt[page]--;
                            }
                            it->second->will_route_node = now_can_execute_node;
                            local_ownership_ok_txn_queues[now_can_execute_node].insert(affected_txn_id);
                            ownership_queue_sizes[now_can_execute_node]++;
                            // cnt
                            new_ownership_ok_txn_cnt++;
                        }
                    }
                    else{
                        node_id_t will_route_node = it->second->will_route_node;
                        if(will_route_node != -1) {
                            // !case 3: 之前可以执行, 页面所有权转移之后不能执行
                            local_ownership_ok_txn_queues[will_route_node].erase(affected_txn_id);
                            ownership_queue_sizes[will_route_node]--;
                            // !表示这轮计划的迁移所造成的不能执行的事务, 需要先执行
                            schedule_txn_prior.push_back(it->second); 
                            it->second->txn->schedule_type = TxnScheduleType::SCHEDULE_PRIOR;
                            
                            this->routed_txn_cnt_per_node[will_route_node]++;
                            load_tracker_.record(will_route_node);
                            schedule_txn_cnt_per_node_this_batch[will_route_node]++;
                            // cnt
                            must_schedule_prior_txn_cnt++;
                            // !标记该事务已经被调度
                            it->second->is_scheduled = true;
                        } else{
                            // ! case4: 之前不可以执行, 页面所有权转移之后仍然不能执行
                            // 更新candidate_txn_benefit_ipq中的benefit值
                            compute_benefit_for_node(it->second, it->second->ownership_node_count, compute_node_workload_benefit, 0.0, 1.0, 0.0); 
                            it->second->hot_level = 0; 
                            for(auto& page : it->second->involved_pages) {
                                // 重新计算hot level, 大致约等于未调度的事务数量
                                it->second->hot_level += page_to_txn_range_map.count(page) ? page_to_txn_range_map.at(page).second : 0; 
                            }
                            for(node_id_t node_id = 0; node_id < ComputeNodeCount; node_id++) {
                                // update the benefit value in local_ipq
                                local_ipq[node_id].update(it->second->dense_id, {it->second->node_benefit_map[node_id], it->second->hot_level});
                            }
                            // cnt
                            ownership_still_not_ok_txn_cnt++;
                        #if LOG_BATCH_ROUTER
                            if(t==0)
                                still_not_ok_txn_debug += std::to_string(affected_txn_id) + " " + 
                                " access keys: " + [&]() {
                                    std::string keys_str;
                                    for(const auto& key : it->second->txn->accounts) {
                                        keys_str += std::to_string(key) + " ";
                                    }
                                    return keys_str;
                                }() +
                                " access pages: " + [&]() {
                                    std::string pages_str;
                                    for(const auto& page : it->second->involved_pages) {
                                        pages_str += std::to_string(page) + " ";
                                    }
                                    return pages_str;
                                }() +
                                " Page ownership after transfer: " + [&]() {
                                    std::string ownership_str;
                                    for(int i=0; i<it->second->involved_pages.size(); i++){
                                        auto page = it->second->involved_pages[i];
                                        // auto ownership_stats = it->second->page_to_ownership_node_vec[i];
                                        auto ownership_stats = transfer_pages.count(page) ? transfer_pages.at(page) : page_ownership_to_node_map.at(page);
                                        ownership_str += " Page " + std::to_string(page) + ": {";
                                        for(const auto& node : ownership_stats.first) {
                                            ownership_str += std::to_string(node) + " ";
                                        }
                                        ownership_str += "}, is_exclusive: " + std::to_string(ownership_stats.second) + " ; ";
                                    }
                                    return ownership_str;
                                }() +
                                " now score: " + std::to_string(it->second->node_benefit_map[min_txn_node]) + "hot level: " + std::to_string(it->second->hot_level) +
                                " now ipq index: " + std::to_string(local_ipq[min_txn_node].get_item_heap_idx(affected_txn_id)) + ". ";
                        #endif
                        }
                    }
                }
                
                // Update global map
                for(auto [transfer_page, new_ownership_stats] : transfer_pages) {
                    page_ownership_to_node_map[transfer_page] = new_ownership_stats;
                }
                
                clock_gettime(CLOCK_MONOTONIC, &decide_schedule_end_time);
                time_stats_.decide_txn_schedule_ms += (decide_schedule_end_time.tv_sec - decide_schedule_start_time.tv_sec) * 1000.0 + 
                    (decide_schedule_end_time.tv_nsec - decide_schedule_start_time.tv_nsec) / 1000000.0;

                // 1.5 记录事务偏序依赖
                struct timespec record_dependency_start_time, record_dependency_end_time;
                clock_gettime(CLOCK_MONOTONIC, &record_dependency_start_time);
            #if LOG_BATCH_ROUTER
                std::string dependency_info = "[SmartRouter Scheduling] batch_id: " + std::to_string(batch_id) + 
                "thread id: " + std::to_string(t) +
                " min_txn_node: " + std::to_string(min_txn_node) +
                " scheduled_counter: " + std::to_string(scheduled_counter) + 
                " affected_txns size: " + std::to_string(affected_txns.size()) +
                " transfer pages: " + [&]() {
                    std::string pages_str;
                    for(const auto& [page, _] : transfer_pages) {
                        pages_str += std::to_string(page) + " ";
                    }
                    return pages_str;
                }() +
                " new_ownership_ok_txn_cnt: " + std::to_string(new_ownership_ok_txn_cnt) +
                " change_ownership_node_txn_cnt: " + std::to_string(change_ownership_node_txn_cnt) +
                " ownership_still_not_ok_txn_cnt: " + std::to_string(ownership_still_not_ok_txn_cnt) +
                " must_schedule_prior_txn_cnt: " + std::to_string(must_schedule_prior_txn_cnt) +
                " now scheduling prior txns in this batch " + std::to_string(scheduled_front_txn_cnt) +
                " Dependency info: [";
                for(auto sc: schedule_txn_prior){
                    dependency_info += std::to_string(sc->txn->tx_id) + " ";
                }
                dependency_info += "] -> [";
                for(auto txn: next_time_schedule_txn){
                    dependency_info += std::to_string(txn->txn->tx_id) + " ";
                }
                dependency_info += "], now ownership_ok_txn_queue sizes: ";
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
                    int queue_size = local_ownership_ok_txn_queues[node_id].size();
                    dependency_info += "Node " + std::to_string(node_id) + ": " + std::to_string(queue_size) + ", ";
                }
                dependency_info += ". Still not ok txns: " + still_not_ok_txn_debug;
                if(t==0)
                logger->info(dependency_info);
            #endif

                assert(selected_candidate_txn->will_route_node != -1); 

                int group_id = rand(); 
                for(auto prior_txn_sc : schedule_txn_prior) {
                    prior_txn_sc->txn->group_id = group_id;
                    prior_txn_sc->txn->batch_id = batch_id;
                }
                for(auto txn_sc: next_time_schedule_txn ){
                    txn_sc->txn->batch_id = batch_id;
                    txn_sc->txn->dependency_group_id.push_back(group_id);
                }

                // ! 生成group作为障碍, 并且关联后续事务与对应的屏障（注入依赖）
                // 1.5.1. Create Group for Prior Txns (Victims) and Update Fences
                assert(next_time_schedule_txn.size() > 0);
                if(!schedule_txn_prior.empty()) {
                    std::shared_ptr<DependencyGroup> group_prior = std::make_shared<DependencyGroup>(); // 创建新的依赖组，唯一创建group的地方
                    group_prior->group_id = group_id;
                    group_prior->unfinish_txn_count = static_cast<int>(schedule_txn_prior.size());
                    
                    for(auto prior_txn_sc : schedule_txn_prior) {
                        prior_txn_sc->txn->notification_groups.push_back(group_prior);
                        // Update Fences: Future transactions (Next) must wait for Prior
                        for(auto page : prior_txn_sc->involved_pages) {
                            page_fences[page] = group_prior;
                        }
                    }
                #if LOG_DEPENDENCY
                    if(t==0)
                    logger->info("[SmartRouter Scheduling] Batch id: " + std::to_string(batch_id) + 
                                    " Thread " + std::to_string(t) + 
                                    " Created fence group id: " + std::to_string(group_id) + 
                                    " for prior txns count: " + std::to_string(schedule_txn_prior.size()));
                #endif
                }

                // 1.5.2. Identify Dependencies for Next Txns (Checking Fences)
                for(auto txn_sc: next_time_schedule_txn) {
                    
                    for(auto page : txn_sc->involved_pages) {
                        if(page_fences.count(page)) {
                            auto fence_group = page_fences[page];
                            // Check if group is still active
                            // Note: fence_group could be group_prior if set above, or an old group
                            std::unique_lock<std::mutex> lock(fence_group->notify_mutex);
                            if(fence_group->unfinish_txn_count.load() > 0) {
                                txn_sc->txn->ref++;
                                fence_group->after_txns.push_back(txn_sc->txn);
                            #if LOG_DEPENDENCY
                                if(t==0)
                                logger->info("[SmartRouter Scheduling] Batch id: " + std::to_string(batch_id) + 
                                                " Thread " + std::to_string(t) + 
                                                " Txn " + std::to_string(txn_sc->txn->tx_id) + 
                                                " added dependency on fence group id: " + std::to_string(fence_group->group_id) + 
                                                " on page " + std::to_string(page) +
                                                " with unfinish_txn_count " + 
                                                std::to_string(fence_group->unfinish_txn_count.load()));
                            #endif
                            }
                            else {
                                // Fence already cleared
                                // clear the fence for this page
                                page_fences.erase(page); // lazy removal
                            }
                        }
                    }
                }

                clock_gettime(CLOCK_MONOTONIC, &record_dependency_end_time);
                time_stats_.add_txn_dependency_ms += (record_dependency_end_time.tv_sec - record_dependency_start_time.tv_sec) * 1000.0 + 
                    (record_dependency_end_time.tv_nsec - record_dependency_start_time.tv_nsec) / 1000000.0;
                

                // 1.6 将schedule_txn_prior中的事务加入到txn_queues_中先执行
                struct timespec push_txn_start_time, push_txn_end_time;
                clock_gettime(CLOCK_MONOTONIC, &push_txn_start_time);
                // std::vector<std::vector<TxnQueueEntry*>> dag_ready_txn(ComputeNodeCount); // Moved outside
                for(auto txn_sc: schedule_txn_prior) {
                    int ref_now = txn_sc->txn->ref.load(std::memory_order_acquire);
                    if(ref_now == 0) {
                        assert(txn_sc->will_route_node != -1);
                        dag_ready_txn[txn_sc->will_route_node].push_back(txn_sc->txn);
                    } else {
                        assert(txn_sc->will_route_node != -1);
                        this->register_pending_txn(txn_sc->txn, txn_sc->will_route_node);
                    #if LOG_DEPENDENCY
                        if(t==0)
                        logger->info("[SmartRouter Scheduling] Txn " + std::to_string(txn_sc->txn->tx_id) + 
                                        " is not dag ready, ref count: " + std::to_string(ref_now) + 
                                        [&]() {
                                            if(txn_sc->txn->group_id) {
                                                return " it is in group id: " + std::to_string(txn_sc->txn->group_id);
                                            } 
                                            return std::string(" ");
                                        }() +
                                        ", registered as pending txn at node " + std::to_string(txn_sc->will_route_node) + 
                                        ", at this time txn queue size: " + this->get_txn_queue_now_status());
                    #endif 
                    }
                } 
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                    if(dag_ready_txn[node_id].empty() ) continue;
                    size_t cnt = dag_ready_txn[node_id].size(); // Capture size before move
                    txn_queues_[node_id]->push_txn_dag_ready(std::move(dag_ready_txn[node_id])); // 放到最前面执行
                    scheduled_front_txn_cnt += cnt;
                    local_scheduled_front_txn_cnt[t] += cnt; 
                    assert(dag_ready_txn[node_id].empty());
                }

                assert(!local_candidates_active[selected_candidate_txn->dense_id]); // 由于选择了该事务，一定不在candidate_txn_ids中了

                clock_gettime(CLOCK_MONOTONIC, &push_txn_end_time);
                time_stats_.push_prioritized_txns_ms += (push_txn_end_time.tv_sec - push_txn_start_time.tv_sec) * 1000.0 + 
                    (push_txn_end_time.tv_nsec - push_txn_start_time.tv_nsec) / 1000000.0;

                // !1.65 check 是否当前txn_queue中几乎没有事务了，如果没有事务，则调度一小部分ownership_ok_txn 事务，防止计算节点没有事情做, 填充流水线气泡
                struct timespec fill_bubble_start_time, fill_bubble_end_time;
                clock_gettime(CLOCK_MONOTONIC, &fill_bubble_start_time);
                for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                    if(txn_queues_[node_id]->size() < 5) {
                        auto& ok_queue = local_ownership_ok_txn_queues_list[t][node_id];
                        if(ok_queue.empty()) continue;
                        
                        int fetch_count = 0;
                        int max_fetch = 5; // 最多调度5个事务

                        // 随机选取一些事务(begin iterator is effectively random)
                        for(auto it = ok_queue.begin(); it != ok_queue.end(); ) {
                            tx_id_t tid = *it;
                            SchedulingCandidateTxn* sc = txid_to_txn_map[tid];
                            // check dependency, ensuring ref == 0
                            if(sc->txn->ref.load(std::memory_order_acquire) == 0) {
                                dag_ready_txn[node_id].push_back(sc->txn);
                                sc->is_scheduled = true;
                                it = ok_queue.erase(it);
                                fetch_count++;
                                if(fetch_count >= max_fetch) break;
                            } else {
                                ++it;
                            }
                        }
                        
                        size_t cnt = dag_ready_txn[node_id].size();
                        if(cnt > 0) {
                            txn_queues_[node_id]->push_txn_back_batch(std::move(dag_ready_txn[node_id]));
                            assert(dag_ready_txn[node_id].empty());
                            scheduled_front_txn_cnt += cnt;
                            local_scheduled_front_txn_cnt[t] += cnt; 
                        #if LOG_BATCH_ROUTER
                            if(t==0)
                            logger->info("[SmartRouter Scheduling] Batch id: " + std::to_string(batch_id) + 
                                            " Thread " + std::to_string(t) + 
                                            " Fill pipeline bubble: Scheduling " + std::to_string(cnt) + 
                                            " ownership_ok txns to node " + std::to_string(node_id) + 
                                            " to fill txn queue, now txn queue size: " + std::to_string(txn_queues_[node_id]->size()));
                        #endif
                        }
                    }
                }
                clock_gettime(CLOCK_MONOTONIC, &fill_bubble_end_time);
                time_stats_.fill_pipeline_bubble_ms += (fill_bubble_end_time.tv_sec - fill_bubble_start_time.tv_sec) * 1000.0 + 
                    (fill_bubble_end_time.tv_nsec - fill_bubble_start_time.tv_nsec) / 1000000.0;
                    
                // 1.7 check 是否ownership_ok_txn中有没有后续依赖的事务
                struct timespec check_ownership_ok_start_time, check_ownership_ok_end_time;
                clock_gettime(CLOCK_MONOTONIC, &check_ownership_ok_start_time);
                bool push_end_optimization = false;
                if (push_end_optimization) {
                    bool has_no_dependency_ownership_ok_txn = false;
                    std::vector<std::vector<TxnQueueEntry*>> to_schedule_txns_batch(ComputeNodeCount);
                    int register_txn_cnt = 0;

                    // Optimization: Collect potentially unblocked transactions first to avoid duplicate checks
                    // A transaction might be affected by multiple transferred pages.
                    std::unordered_set<tx_id_t> potential_unblocked_txns;

                    for(auto [transfer_page, _] : transfer_pages) {
                        if(page_not_ok_txn_cnt[transfer_page] > 0) continue; // 说明该页面还有未调度的事务，不能ownership_ok
                        
                        if (page_to_txn_range_map.find(transfer_page) == page_to_txn_range_map.end()) continue;
                        auto range = page_to_txn_range_map[transfer_page];

                        for(size_t i=0; i<range.second; ++i) {
                            potential_unblocked_txns.insert(global_page_pairs[range.first + i].second);
                        }
                    }
                    
                    for(auto affected_txn_id : potential_unblocked_txns) {
                        // tx_id_t affected_txn_id = global_page_pairs[range.first + i].second;
                        // if(txid_to_txn_map[affected_txn_id]->is_scheduled) continue; // ! Lazy skip
                        auto it = txid_to_txn_map.find(affected_txn_id);
                        if (it->second->is_scheduled) continue; // 说明这个事务已经被调度过了
                        node_id_t will_route_node = it->second->will_route_node;
                        assert(will_route_node != -1); // 一定不在candidate_txn_ids中了, 一定在ownership_ok_txn_queues中
                        assert(local_ownership_ok_txn_queues[will_route_node].count(affected_txn_id) == 1); // 一定在ownership_ok_txn_queues中
                        bool no_affected_it_txn = true; 
                        for(auto page: it->second->involved_pages) {
                            if(page_not_ok_txn_cnt[page] > 0) {
                                no_affected_it_txn = false  ;
                                break;
                            }
                        }
                        if(no_affected_it_txn) {
                            // !说明不再有其他事务可以影响这个事务
                            has_no_dependency_ownership_ok_txn = true;
                            // 这里先判断一下这个事务前序依赖是否都已经调度完成
                            if(it->second->txn->ref.load() == 0) {
                                // 说明该事务dag ready，可以直接调度
                                to_schedule_txns_batch[will_route_node].push_back(it->second->txn);
                            }
                            else{
                                // 说明该事务还没有dag ready，注册为pending txn
                                this->register_pending_txn(it->second->txn, will_route_node);
                                register_txn_cnt++;
                            }
                            local_ownership_ok_txn_queues[will_route_node].erase(affected_txn_id);
                            ownership_queue_sizes[will_route_node]--;
                            it->second->is_scheduled = true; // 标记该事务已经被调度
                            it->second->txn->schedule_type = TxnScheduleType::OWNERSHIP_OK;
                            this->routed_txn_cnt_per_node[will_route_node]++;
                            load_tracker_.record(will_route_node);
                            schedule_txn_cnt_per_node_this_batch[will_route_node]++;
                        }
                    }
                    for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
                        if(to_schedule_txns_batch[node_id].empty()) continue;
                        txn_queues_[node_id]->push_txn_back_batch(to_schedule_txns_batch[node_id]);
                    }
                    #if LOG_BATCH_ROUTER
                    if(has_no_dependency_ownership_ok_txn)
                    if(t==0)
                    logger->info("[SmartRouter Scheduling] Batch id : " + std::to_string(batch_id) + " thread " + std::to_string(t) +
                                    " Scheduling no dependency ownership_ok txns to execute (no dependency) on nodes with count: " + [&]() {
                                        std::string counts_str;
                                        counts_str += "[";
                                        for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
                                            counts_str += "Node " + std::to_string(node_id) + ": " + std::to_string(to_schedule_txns_batch[node_id].size()) + ", ";
                                        }
                                        counts_str += "]";
                                        return counts_str;
                                    }() + 
                                    " register txns: " + std::to_string(register_txn_cnt) +
                                    " at this time ownership_ok txn queue sizes: " + [&]() {
                                        std::string sizes_str;
                                        sizes_str += "[";
                                        for(int node_id = 0; node_id < ComputeNodeCount; node_id++){
                                            int queue_size = local_ownership_ok_txn_queues[node_id].size();
                                            sizes_str += "Node " + std::to_string(node_id) + ": " + std::to_string(queue_size) + ", ";
                                        }
                                        sizes_str += "]";
                                        return sizes_str;
                                    }());
                    #endif
                }
                clock_gettime(CLOCK_MONOTONIC, &check_ownership_ok_end_time);
                time_stats_.push_end_txns_ms += (check_ownership_ok_end_time.tv_sec - check_ownership_ok_start_time.tv_sec) * 1000.0 + 
                    (check_ownership_ok_end_time.tv_nsec - check_ownership_ok_start_time.tv_nsec) / 1000000.0;
            }
            #if LOG_BATCH_ROUTER
            if(t==0)
            logger->info("Batch id: " + std::to_string(batch_id) + 
                            ", Thread " + std::to_string(t) + 
                            ", SmartRouter scheduled front txn cnt due to ownership transfer without pending: " + 
                            std::to_string(local_scheduled_front_txn_cnt[t]));
            #endif
            
        
            // 3. 将local_ownership_ok_txn_queues 中剩余的事务并行加到txn_queues_中
            struct timespec push_remaining_start_time, push_remaining_end_time;
            clock_gettime(CLOCK_MONOTONIC, &push_remaining_start_time);
            
            std::vector<std::future<void>> push_futs;
            auto push_func = [&](int node_id) {
                auto& txn_queue = local_ownership_ok_txn_queues[node_id];
                if (txn_queue.empty()) return; // Skip if empty

                std::vector<TxnQueueEntry*> to_schedule_txns;
                to_schedule_txns.reserve(txn_queue.size());
                int node_txn_count = 0;

                // 构建to_schedule_txns
                {
                    for(auto tx_id: txn_queue) {
                        auto it = txid_to_txn_map.find(tx_id);
                        // assert(it != txid_to_txn_map.end());
                        SchedulingCandidateTxn* sc = it->second;

                        sc->txn->schedule_type = TxnScheduleType::OWNERSHIP_OK;
                        
                        node_txn_count++;

                        sc->is_scheduled = true; // 标记该事务已经被调度
                        if(sc->txn->ref.load(std::memory_order_acquire) == 0) {
                            // 说明该事务dag ready，可以直接调度
                            to_schedule_txns.push_back(sc->txn);
                        }
                        else{
                            // 说明该事务还没有dag ready，注册为pending txn
                            this->register_pending_txn(sc->txn, node_id);
                        #if LOG_DEPENDENCY
                            if(t==0)
                            logger->info("[SmartRouter Scheduling] Txn " + std::to_string(sc->txn->tx_id) + 
                                            " is not dag ready, ref count: " + std::to_string(sc->txn->ref.load()) + 
                                            [&]() {
                                                if(sc->txn->group_id) {
                                                    return " it is in group id: " + std::to_string(sc->txn->group_id);
                                                } 
                                                return std::string(" ");
                                            }() +
                                            ", registered as pending txn at node " + std::to_string(node_id) + 
                                            ", at this time txn queue size: " + this->get_txn_queue_now_status());
                        #endif
                        }
                    }
                    // Batch atomic updates
                    this->routed_txn_cnt_per_node[node_id].fetch_add(node_txn_count, std::memory_order_relaxed);
                    load_tracker_.record(node_id, node_txn_count);
                    schedule_txn_cnt_per_node_this_batch[node_id].fetch_add(node_txn_count, std::memory_order_relaxed);

                    // 批量加入txn_queues_
                    txn_queues_[node_id]->push_txn_back_batch(to_schedule_txns);
                }
                // if(WarmupEnd)
                    logger->info("[SmartRouter Scheduling] Batch id : " + std::to_string(batch_id) + " thread " + std::to_string(t) +
                                    " final Scheduling ownership_ok txn to execute on node " + std::to_string(node_id) + 
                                    ", count: " + std::to_string(to_schedule_txns.size()) + " at this time txn queue size: " +
                                    this->get_txn_queue_now_status());
                    logger->info("Node " + std::to_string(node_id) + 
                                    " Final end, dag not ready pending txn count: " + std::to_string(this->get_pending_txn_count()) 
                                    + " pending txn: " + [&]() {
                                        auto txn_ids = this->get_pending_txns_ids();
                                        std::string ids_str;
                                        ids_str += "[";
                                        for (size_t i = 0; i < txn_ids.size(); ++i) {
                                            ids_str += std::to_string(txn_ids[i]);
                                            if (i != txn_ids.size() - 1) {
                                                ids_str += ", ";
                                            }
                                        }
                                        ids_str += "]";
                                        return ids_str;
                                    }()
                                );
            };

            for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
                // 如果队列中元素比较多(>20)，则使用线程池并发处理，避免串行阻塞；否则直接串行处理，避免线程调度开销和死锁风险
                if (local_ownership_ok_txn_queues[node_id].size() > 20) {
                    push_futs.emplace_back(threadpool.enqueue([&, node_id](){ push_func(node_id); }));
                } else {
                    push_func(node_id);
                }
            }

            for(auto& fut : push_futs) {
                fut.get();
            }
            clock_gettime(CLOCK_MONOTONIC, &push_remaining_end_time);
            time_stats_.final_push_to_queues_ms += (push_remaining_end_time.tv_sec - push_remaining_start_time.tv_sec) * 1000.0 + 
                (push_remaining_end_time.tv_nsec - push_remaining_start_time.tv_nsec) / 1000000.0;
        }));
    }

    // Join workers
    for(auto& fut : futs) fut.get();

    logger->info("Batch id: " + std::to_string(batch_id) + 
                    ", SmartRouter scheduled front txn cnt due to ownership transfer without pending: " + std::to_string(scheduled_front_txn_cnt)); 

    // 计时结束
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    time_stats_.process_conflicted_txn_ms +=
        (end_time.tv_sec - start_time.tv_sec) * 1000.0 + (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

    // if(WarmupEnd)
    logger->info("batch schedule finish" + this->get_txn_queue_now_status() + "dag not ready pending txn count:" + 
                [&]() {
                    std::string s;
                    for(int i=0; i<ComputeNodeCount; i++) {
                        s += "Node " + std::to_string(i) + ": " + std::to_string(this->get_pending_txn_count_on_node(i)) + ", ";
                    }
                    return s;
                }() + 
                ", expected page transfer count per node: " + [&]() {
                    std::string s;
                    for (size_t i = 0; i < expected_page_transfer_count_per_node.size(); ++i) {
                        s += "Node " + std::to_string(i) + ": " + std::to_string(expected_page_transfer_count_per_node[i].load()) + ", ";
                    }
                    return s;
                }());
    
    // 清理并发导致的ref=0但尚未调度的pending事务，安全迭代并擦除
    std::vector<std::vector<TxnQueueEntry*>> to_schedule;
    to_schedule = std::move(pending_txn_queue_->self_check_ref_is_zero());
    for (size_t node_id = 0; node_id < to_schedule.size(); ++node_id) {
        if (!to_schedule[node_id].empty()) {
            txn_queues_[node_id]->push_txn_back_batch(std::move(to_schedule[node_id]));
        #if LOG_DEPENDENCY
            logger->warning("[SmartRouter Scheduling] Clean pending txns pushed to node " + std::to_string(node_id) + 
                            ", count: " + std::to_string(to_schedule[node_id].size()) + 
                            ", at this time txn queue size: " + this->get_txn_queue_now_status());
        #endif 
        }
    }

    // delete SchedulingCandidateTxn objects to avoid memory leak
    // for(auto& [tx_id, sc] : txid_to_txn_map) {
    //    delete sc;
    // }
    return ;
}

// TIT通知后续事务ready时调用：立即将其推入对应节点队列执行
void SmartRouter::schedule_ready_txn(std::vector<TxnQueueEntry*> entries, int finish_call_id) {
    if (entries.empty()) return;
    // 这里entries不一定都没调度呢
    std::vector<std::vector<TxnQueueEntry*>> to_schedule;
    to_schedule = std::move(pending_txn_queue_->pop_dag_ready_txns(entries));
    // 只调度to_schedule中的事务
    if (to_schedule.empty()) return;
    // logger->info("[SmartRouter Scheduling] DAG successor txn " + [&]() {
    //     std::string ids;
    //     for (size_t node_id = 0; node_id < to_schedule.size(); ++node_id) {
    //         ids += "Node " + std::to_string(node_id) + ": ";
    //         for (auto entry : to_schedule[node_id]) {
    //             ids += std::to_string(entry->tx_id) + " ";
    //         }
    //     }
    //     return ids;
    // }() + " ready to schedule.");
    
    // logger->info("Finish call id: " + std::to_string(finish_call_id) + 
    //                 ", SmartRouter scheduling ready txn cnt: " + std::to_string([&]() {
    //                     size_t cnt = 0;
    //                     for (const auto &vec : to_schedule) {
    //                         cnt += vec.size();
    //                     }
    //                     return cnt;
    //                 }()));
    for (size_t node_id = 0; node_id < to_schedule.size(); ++node_id) {
        if (!to_schedule[node_id].empty()) {
            txn_queues_[node_id]->push_txn_dag_ready(std::move(to_schedule[node_id]));
        }
    }
}

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
    // // 在添加上当前队列的大小
    // for(int i=0; i<ComputeNodeCount; i++) {
    //     int queue_size = this->txn_queues_[i]->size();
    //     compute_vector[i] += queue_size;
    //     total += queue_size;
    // }

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

std::vector<double> SmartRouter::compute_remain_queue_balance_penalty_weights() {
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
    // compute_vector = load_tracker_.get_loads(); // 使用滑动窗口负载
    // for (int i = 0; i < ComputeNodeCount; ++i) {
    //     total += compute_vector[i];
    // }
    // // 在添加上当前队列的大小
    for(int i=0; i<ComputeNodeCount; i++) {
        int queue_size = this->txn_queues_[i]->size();
        compute_vector[i] += queue_size;
        total += queue_size;
    }

    double avg = (ComputeNodeCount > 0) ? static_cast<double>(total) / ComputeNodeCount : 0.0;
    if (avg <= 0.0) {
        // 避免除0：都返回base
        std::fill(penalty_weights.begin(), penalty_weights.end(), base);
        remain_queue_balance_penalty_weights_ = penalty_weights;
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
    remain_queue_balance_penalty_weights_ = penalty_weights;

    // 可选：打印/记录用于调试
    logger->info("Compute node remain queue penalty: " + [&]() {
        std::string s;
        for (size_t i = 0; i < penalty_weights.size(); ++i) {
            s += "Node " + std::to_string(i) + " workload:" + std::to_string(compute_vector[i]) + 
                 " Penalty:" + std::to_string(penalty_weights[i]) + ", ";
        }
        return s;
    }());

    return penalty_weights;
}
