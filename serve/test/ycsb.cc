#include "ycsb.h"

void YCSB::generate_ycsb_txns_worker(int thread_id, TxnPool* txn_pool) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("txn_gen_t_" + std::to_string(thread_id)).c_str());

    std::vector<itemkey_t> keys_vec(txn_length_);
    std::vector<bool> rw_flags(txn_length_);
    std::mt19937 rw_rng(static_cast<uint32_t>(GetCPUCycle() ^ (thread_id * 0x9e3779b9U)));
    ZipfGen* zipfian_gen = nullptr;
    FiniteZipfGen* finite_zipfian_gen = nullptr;
    uint64_t zipf_seed = 2 * thread_id * GetCPUCycle();
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    if (use_finite_zipfian_) {
        finite_zipfian_gen = new FiniteZipfGen(get_record_count(), zipfian_theta_, zipf_seed & zipf_seed_mask, NumBucket);
    } else {
        zipfian_gen = new ZipfGen(get_record_count(), zipfian_theta_, zipf_seed & zipf_seed_mask, NumBucket);
    }

    // 全局一共进行 MetisWarmupRound * PARTITION_INTERVAL的冷启动事务生成，每个工作节点具有worker_threads个线程，每个线程生成try_count个事务
    int total_txn_to_generate = MetisWarmupRound * PARTITION_INTERVAL + try_count * worker_threads * ComputeNodeCount;
    while(time_based_run || generated_txn_count < total_txn_to_generate) {
        if (time_based_run && stop_benchmark.load(std::memory_order_relaxed)) {
            break;
        }
        std::vector<TxnQueueEntry*> txn_batch;
        for (int i = 0; i < 100; i++){
            if (time_based_run && stop_benchmark.load(std::memory_order_relaxed)) {
                break;
            }
            generated_txn_count++;
            tx_id_t tx_id = tx_id_generator++; // global atomic transaction ID
            // Simulate some work
            // Randomly select a transaction type and accounts
            int txn_type = generate_txn_type();
            generate_keys(keys_vec, rw_flags, zipfian_gen, finite_zipfian_gen, rw_rng);
            // Create a new transaction object
            TxnQueueEntry* txn_entry = new TxnQueueEntry(tx_id, txn_type, {}, keys_vec);
            txn_entry->ycsb_rw_flags = rw_flags;
            txn_batch.push_back(txn_entry);
        }
        if (txn_batch.empty()) {
            break;
        }
        // Enqueue the transaction into the global transaction pool
        // txn_pool->receive_txn_from_client(txn_entry);
        // if(SYSTEM_MODE != 11){
        //     txn_pool->receive_txn_from_client_batch(txn_batch, thread_id);
        // }
        // else {
        //     txn_pool->receive_txn_from_client_batch(txn_batch, 0);
        // }
        txn_pool->receive_txn_from_client_batch(txn_batch, 0);
    }
    txn_pool->stop_pool();

    // 清理Zipfian生成器
    if (zipfian_gen) {
        delete zipfian_gen;
        zipfian_gen = nullptr;
    }
    if (finite_zipfian_gen) {
        delete finite_zipfian_gen;
        finite_zipfian_gen = nullptr;
    }
}

// 装载数据
void YCSB::load_data(pqxx::connection* conn0) {
    std::cout << "Loading YCSB data... count=" << record_count_ << std::endl;
    int num_threads = 50;
    std::vector<std::thread> threads;
    int chunk = (record_count_ + num_threads - 1) / num_threads;
    auto worker = [&](int start_id, int end_id) {
        pqxx::connection conn(DBConnection[0]);
        if (!conn.is_open()) {
            std::cerr << "Failed to connect loader conn" << std::endl;
            return;
        }
        try {
            pqxx::work txn(conn);
            for (int id = start_id; id < end_id; ++id) {
                std::vector<std::string> fields(10);
                for (int f = 0; f < 10; ++f) {
                    fields[f] = random_string(field_len_);
                }
                std::string sql = "INSERT INTO usertable (id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (" +
                                  std::to_string(id) + ", " +
                                  "'" + txn.esc(fields[0]) + "', " +
                                  "'" + txn.esc(fields[1]) + "', " +
                                  "'" + txn.esc(fields[2]) + "', " +
                                  "'" + txn.esc(fields[3]) + "', " +
                                  "'" + txn.esc(fields[4]) + "', " +
                                  "'" + txn.esc(fields[5]) + "', " +
                                  "'" + txn.esc(fields[6]) + "', " +
                                  "'" + txn.esc(fields[7]) + "', " +
                                  "'" + txn.esc(fields[8]) + "', " +
                                  "'" + txn.esc(fields[9]) + "')";
                txn.exec(sql);
            }
            txn.commit();
        } catch (const std::exception& e) {
            std::cerr << "YCSB load worker error: " << e.what() << std::endl;
        }
    };

    for (int t = 0; t < num_threads; ++t) {
        int s = t * chunk + 1;
        int e = std::min(record_count_ + 1, (t + 1) * chunk + 1);
        threads.emplace_back(worker, s, e);
    }
    for (auto& th : threads) th.join();
    std::cout << "YCSB data loaded." << std::endl;

    // 输出一些导入数据的统计信息
    try{
        auto txn = pqxx::work(*conn0);
        pqxx::result usertable_size = txn.exec("select sys_size_pretty(sys_relation_size('usertable'))");
        std::cout << "YCSB table sizes after data load:" << std::endl;
        if(!usertable_size.empty()){
            std::cout << "  Usertable table size: " << usertable_size[0][0].as<std::string>() << std::endl;
        }
        txn.commit();
    }catch(const std::exception &e) {
        std::cerr << "Error while getting YCSB table size after data load: " << e.what() << std::endl;
    }
}

// Create or replace YCSB stored procedures
void YCSB::create_ycsb_stored_procedures(pqxx::connection* conn) {
    std::cout << "Creating YCSB stored procedures..." << std::endl;
    try {
        pqxx::work txn(*conn);
        // Create read_user procedure
        txn.exec(R"(
            CREATE OR REPLACE FUNCTION ycsb_multi_rw(
                IN  read_keys  INT[],   -- 待读 key 列表
                IN  write_keys INT[],   -- 待写 key 列表
                IN  p_seed BIGINT       -- 确定性生成写入值
            )
            RETURNS TABLE(id INT, ctid TID, txid BIGINT)
            LANGUAGE plpgsql
            AS $$
            DECLARE
                read_key INT;
                write_key INT;
            BEGIN
                --------------------------------------------------------------------
                -- Read phase
                --------------------------------------------------------------------
                FOREACH read_key IN ARRAY read_keys LOOP
                    RETURN QUERY
                    SELECT
                        t.id,
                        t.ctid,
                        txid_current() AS txid
                    FROM usertable AS t
                    WHERE t.id = read_key;
                END LOOP;

                --------------------------------------------------------------------
                -- Write phase
                --------------------------------------------------------------------
                FOREACH write_key IN ARRAY write_keys LOOP
                    RETURN QUERY
                    UPDATE usertable AS t
                    SET field1 = md5(p_seed::text || ':' || write_key::text)
                    WHERE t.id = write_key
                    RETURNING
                        t.id,
                        t.ctid,
                        txid_current() AS txid;
                END LOOP;

            END;
            $$;
        )");

        txn.commit();
        std::cout << "YCSB stored procedures created." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error creating YCSB stored procedures: " << e.what() << std::endl;
    }
}
