#include <metis.h>
#include <partition.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <memory>

void metis_partition_graph(const std::string& output_path_) {
    /**
     * @brief 
     * 
     */

    std::vector<idx_t> xadj(0);   // 压缩邻接矩阵
    std::vector<idx_t> adjncy(0); // 压缩图表示
    std::vector<idx_t> adjwgt(0); // 边权重
    std::vector<idx_t> vwgt(0);   // 点权重
    

    std::unordered_map<uint64_t, int32_t> key_coordinator_cache;

    for(size_t i = 0 ; i < hottest_tuple_index_seq.size(); i ++ ){                
        uint64_t key = hottest_tuple_index_seq[i];

        xadj.push_back(adjncy.size()); // 
        vwgt.push_back(hottest_tuple[key]);

        myValueType* it = (myValueType*)record_degree.search_value(&key);
        
        for(auto edge: *it){
            adjncy.push_back(hottest_tuple_index_[edge.first]); // 节点id从0开始
            adjwgt.push_back(edge.second.degree);

            // cache coordinator
            key_coordinator_cache[edge.second.from] = edge.second.from_c_id;
            key_coordinator_cache[edge.second.to] = edge.second.to_c_id;
        }
    }
    xadj.push_back(adjncy.size());
    
    idx_t nVertices = xadj.size() - 1;      // 节点数
    idx_t nWeights = 1;                     // 节点权重维数
    idx_t nParts = key_coordinator_cache.size() / 50;   // 子图个数≥2
    idx_t objval;                           // 目标函数值
    std::vector<idx_t> parts(nVertices, 0); // 划分结果
    std::cout << key_coordinator_cache.size() << " " << key_coordinator_cache.size() / 50 << std::endl;
    int ret = METIS_PartGraphKway(&nVertices, &nWeights, xadj.data(), adjncy.data(),
        vwgt.data(), NULL, adjwgt.data(), &nParts, NULL,
        NULL, NULL, &objval, parts.data());

    if (ret != rstatus_et::METIS_OK) { 
        std::cout << "METIS_ERROR" << std::endl; 
    }

    // print for logs
    std::cout << "METIS_OK" << std::endl;
    std::cout << "objval: " << objval << std::endl;
    
    std::vector<std::shared_ptr<myMove<WorkloadType>>> metis_move(nParts);
    for(size_t j = 0; j < nParts; j ++ ){
        metis_move[j] = std::make_shared<myMove<WorkloadType>>();
        metis_move[j]->metis_dest_coordinator_id = j;
    }

    for(size_t i = 0 ; i < parts.size(); i ++ ){
        uint64_t key_ = hottest_tuple_index_seq[i];
        int32_t source_c_id = key_coordinator_cache[key_];
        int32_t dest_c_id = parts[i];

        MoveRecord<WorkloadType> new_move_rec;
        new_move_rec.set_real_key(key_);

        metis_move[dest_c_id]->records.push_back(new_move_rec);
    }


    std::ofstream outpartition(output_path_);
    for (size_t i = 0; i < parts.size(); i++) { 
    }
    
    for(size_t j = 0; j < metis_move.size(); j ++ ){
        if(metis_move[j]->records.size() > 0){
            outpartition << j << "\t";
            for(int i = 0 ; i < metis_move[j]->records.size(); i ++ ){
                outpartition << metis_move[j]->records[i].record_key_ << "\t";
            }
            outpartition << "\n";
            move_plans.push_no_wait(metis_move[j]);
        }
    }
    outpartition.close();

    movable_flag.store(false);
}

