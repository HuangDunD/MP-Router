#include "smallbank.h"

void SmallBank::generate_smallbank_txns_worker(int thread_id, TxnPool* txn_pool) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("txn_gen_t_" + std::to_string(thread_id)).c_str());

    std::vector<itemkey_t> accounts_vec(2);
    ZipfGen* zipfian_gen = nullptr;
    if (!zipfian_gen) {
        uint64_t zipf_seed = 2 * thread_id * GetCPUCycle();
        uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
        zipfian_gen = new ZipfGen(get_account_count(), zipfian_theta, zipf_seed & zipf_seed_mask);
    }

    // 全局一共进行 MetisWarmupRound * PARTITION_INTERVAL的冷启动事务生成，每个工作节点具有worker_threads个线程，每个线程生成try_count个事务
    int total_txn_to_generate = MetisWarmupRound * PARTITION_INTERVAL + try_count * worker_threads * ComputeNodeCount;
    while(generated_txn_count < total_txn_to_generate) {
        std::vector<TxnQueueEntry*> txn_batch;
        for (int i = 0; i < 0.1 * BatchRouterProcessSize; i++){
            generated_txn_count++;
            tx_id_t tx_id = tx_id_generator++; // global atomic transaction ID
            // Simulate some work
            // Randomly select a transaction type and accounts
            int txn_type = generate_txn_type();
            if(txn_type == 0 || txn_type == 1) { // TxAmagamate or TxSendPayment
                generate_two_account_ids(accounts_vec[0], accounts_vec[1], zipfian_gen);
            } else {
                generate_account_id(accounts_vec[0], zipfian_gen);
            }
            // Create a new transaction object
            TxnQueueEntry* txn_entry = new TxnQueueEntry(tx_id, txn_type, accounts_vec);
            txn_batch.push_back(txn_entry);
        }
        // Enqueue the transaction into the global transaction pool
        // txn_pool->receive_txn_from_client(txn_entry);
        if(SYSTEM_MODE != 11){
            txn_pool->receive_txn_from_client_batch(txn_batch, thread_id);
        }
        else {
            txn_pool->receive_txn_from_client_batch(txn_batch, 0);
        }
    }
    txn_pool->stop_pool();

    // 清理Zipfian生成器
    if (zipfian_gen) {
        delete zipfian_gen;
        zipfian_gen = nullptr;
    }
}

void SmallBank::load_data(pqxx::connection *conn0) {
    auto smallbank_account = this->get_account_count();
    std::cout << "Loading data..." << std::endl;
    std::cout << "Will load " << smallbank_account << " accounts into checking and savings tables" << std::endl;
    // Load data into the database if needed
    // Insert data into checking and savings tables
    const int num_threads = 16;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;
    // 这里创建一个导入数据的账户id的列表, 随机导入
    std::vector<int> id_list;
    for(int i = 1; i <= smallbank_account; i++) id_list.push_back(i);
    // 随机打乱 id_list
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(id_list.begin(), id_list.end(), g);
    auto worker = [&id_list, smallbank_account](int start, int end) {
        pqxx::connection conn00(DBConnection[0]);
        if (!conn00.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        int city_cnt = static_cast<int>(SmallBankCityType::Count);
        for(int i = start; i < end; i++) {
            int id = id_list[i];
            int balance = 1000 + ((id - 1) % 1000); // Random balance
            std::string name = "Account_" + std::to_string(id);
            int city = ((id - 1) / (smallbank_account / city_cnt) ) % city_cnt;
            assert(city >=0 && city < city_cnt);
            // 使用RETURNING子句获取插入数据的位置信息
            std::string insert_checking_sql = "INSERT INTO checking (id, balance, city, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", " +
                                        std::to_string(city) + ", '" +
                                        name + "') RETURNING ctid, id";
            std::string insert_savings_sql = "INSERT INTO savings (id, balance, city, name) VALUES (" +
                                        std::to_string(id) + ", " +
                                        std::to_string(balance) + ", " +
                                        std::to_string(city) + ", '" +
                                        name + "') RETURNING ctid, id";

            try {
                pqxx::work txn_create(conn00);
                // 执行checking表插入并获取位置信息
                pqxx::result checking_result = txn_create.exec(insert_checking_sql);                
                if (!checking_result.empty()) {
                    std::string ctid = checking_result[0]["ctid"].as<std::string>();
                    // ctid 为 (page_id, tuple_index) 格式, 这里要把ctid转换为page_id
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = checking_result[0]["id"].as<int>();
                }
                
                // 执行savings表插入并获取位置信息
                pqxx::result savings_result = txn_create.exec(insert_savings_sql);
                if (!savings_result.empty()) {
                    std::string ctid = savings_result[0]["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = savings_result[0]["id"].as<int>();
                }
                
                txn_create.commit();
            } catch (const std::exception &e) {
                std::cerr << "Error while inserting data: " << e.what() << std::endl;
            }
        }
    }; 

    // Create and start threads
    for(int i = 0; i < num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? smallbank_account : (i + 1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }
    
    // Wait for all threads to complete
    for(auto& thread : threads) {
        thread.join();
    }
    std::cout << "Data loaded successfully." << std::endl;

    // 输出一些导入数据的统计信息
    try{
        auto txn = pqxx::work(*conn0);
        pqxx::result checking_size = txn.exec("select sys_size_pretty(sys_relation_size('checking')) ");
        pqxx::result savings_size = txn.exec("select sys_size_pretty(sys_relation_size('savings')) ");
        if(!checking_size.empty()){
            std::cout << "Checking table size: " << checking_size[0][0].as<std::string>() << std::endl;
        }
        if(!savings_size.empty()){
            std::cout << "Savings table size: " << savings_size[0][0].as<std::string>() << std::endl;
        }
        txn.commit();
    }catch(const std::exception &e) {
        std::cerr << "Error while getting table size: " << e.what() << std::endl;
    }
}

// Create or replace SmallBank stored procedures
void SmallBank::create_smallbank_stored_procedures(pqxx::connection* conn) {
    std::cout << "Creating stored procedures..." << std::endl;
    try {
            pqxx::work txn(*conn);

            // Amalgamate: zero checking/savings of a1, deposit total into a2.checking
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_amalgamate(a1 INT, a2 INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                c1_ctid TID;
                s1_ctid TID;
                c2_ctid TID;
                c1_bal INT;
                s1_bal INT;
                total_bal INT;
            BEGIN
            -- ① update checking(a1)
            UPDATE checking c
            SET balance = 0
            WHERE c.id = a1
            RETURNING c.ctid, c.balance INTO c1_ctid, c1_bal;

            -- ② update savings(a1)
            UPDATE savings s
            SET balance = 0
            WHERE s.id = a1
            RETURNING s.ctid, s.balance INTO s1_ctid, s1_bal;

            -- ③ 合并
            total_bal := COALESCE(c1_bal,0) + COALESCE(s1_bal,0);

            -- ④ deposit into checking(a2)
            UPDATE checking c2
            SET balance = total_bal
            WHERE c2.id = a2
            RETURNING c2.ctid, c2.balance INTO c2_ctid, balance;

            --------------------------------------------------------------------
            --  最终一次性返回三个修改结果（每个 tuple 的最终 ctid) 
            --------------------------------------------------------------------
            RETURN QUERY 
                SELECT 'checking'::text, a1, c1_ctid, 0, txid_current();      -- 更新后的 checking(a1)
            RETURN QUERY 
                SELECT 'savings'::text, a1, s1_ctid, 0, txid_current();       -- 更新后的 savings(a1)
            RETURN QUERY 
                SELECT 'checking'::text, a2, c2_ctid, balance, txid_current();-- checking(a2) 的最终余额
            END;
            $$;
            )SQL");

            // SendPayment: a1.checking -= 10, a2.checking += 10
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_send_payment(a1 INT, a2 INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                c1_ctid TID;
                c2_ctid TID;
                c1_bal INT;
                c2_bal INT;
            BEGIN
                UPDATE checking c1
                SET balance = c1.balance - 10
                WHERE c1.id = a1
                RETURNING c1.ctid, c1.balance INTO c1_ctid, c1_bal;

                UPDATE checking c2
                SET balance = c2.balance + 10
                WHERE c2.id = a2
                RETURNING c2.ctid, c2.balance INTO c2_ctid, c2_bal;

                RETURN QUERY SELECT 'checking'::text, a1, c1_ctid, c1_bal, txid_current();
                RETURN QUERY SELECT 'checking'::text, a2, c2_ctid, c2_bal, txid_current();
            END; $$;
            )SQL");

            // DepositChecking: a1.checking += 100
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_deposit_checking(a1 INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                c_ctid TID;
                c_bal INT;
            BEGIN
                UPDATE checking c
                SET balance = c.balance + 100
                WHERE c.id = a1
                RETURNING c.ctid, c.balance INTO c_ctid, c_bal;

                RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
            END; $$;
            )SQL");

            // WriteCheck: read savings(a1), update checking(a1) -= 50
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_write_check(a1 INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                s_ctid TID;
                s_bal  INT;
                c_ctid TID;
                c_bal  INT;
            BEGIN
                SELECT s.ctid, s.balance INTO s_ctid, s_bal
                FROM savings s WHERE s.id = a1;

                UPDATE checking c
                SET balance = c.balance - 50
                WHERE c.id = a1
                RETURNING c.ctid, c.balance INTO c_ctid, c_bal;

                RETURN QUERY SELECT 'savings'::text,  a1, s_ctid, s_bal, txid_current();
                RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
            END; $$;
            )SQL");

            // Balance: read checking(a1), savings(a1)
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_balance(a1 INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                c_ctid TID;
                c_bal  INT;
                s_ctid TID;
                s_bal  INT;
            BEGIN
                SELECT c.ctid, c.balance INTO c_ctid, c_bal
                FROM checking c WHERE c.id = a1;

                SELECT s.ctid, s.balance INTO s_ctid, s_bal
                FROM savings s WHERE s.id = a1;

                RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
                RETURN QUERY SELECT 'savings'::text,  a1, s_ctid, s_bal, txid_current();
            END; $$;
            )SQL");

            // TransactSavings: a1.savings += 20
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_transact_savings(a1 INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                s_ctid TID;
                s_bal  INT;
            BEGIN
                UPDATE savings s
                SET balance = s.balance + 20
                WHERE s.id = a1
                RETURNING s.ctid, s.balance INTO s_ctid, s_bal;

                RETURN QUERY SELECT 'savings'::text, a1, s_ctid, s_bal, txid_current();
            END; $$;
            )SQL");

            txn.commit();
            std::cout << "Stored procedures created." << std::endl;
    } catch (const std::exception &e) {
            std::cerr << "Error creating stored procedures: " << e.what() << std::endl;
    }
}