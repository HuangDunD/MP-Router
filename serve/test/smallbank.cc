#include "smallbank.h"
#include <regex>
#include <sstream>
#include <vector>
#include <iostream>

// Try to include yacli.h if available, otherwise we might need to mock or ensure path is correct
// Assuming it is in the same directory or include path
#include "yacli.h" 

#define YAC_CALL_VOID(proc)                      \
    do {                                         \
        if ((YacResult)(proc) != YAC_SUCCESS) {  \
            std::cerr << "YashanDB Error in " << #proc << std::endl; \
            return;                              \
        }                                        \
    } while (0)

#define YAC_CALL_RET(proc, ret_val)              \
    do {                                         \
        if ((YacResult)(proc) != YAC_SUCCESS) {  \
            std::cerr << "YashanDB Error in " << #proc << std::endl; \
            return ret_val;                      \
        }                                        \
    } while (0)

// Helper class for RAII of Yashan handles
class YacHandleGuard {
public:
    YacHandleGuard(YacHandle& h, YacHandleType t) : handle(h), type(t) {}
    ~YacHandleGuard() {
        if (handle != NULL) {
            yacFreeHandle(type, handle);
            handle = NULL;
        }
    }
    YacHandle& handle;
    YacHandleType type;
};

void SmallBank::create_table_yashan() { 
    std::cout << "Creating tables in YashanDB..." << std::endl;
    YashanConnInfo info = YashanDBConnections[0]; // Use the first connection info for table creation

    YacHandle env = NULL;
    YacHandle conn = NULL;
    YacHandle stmt = NULL;

    yacAllocHandle(YAC_HANDLE_ENV, NULL, &env);
    yacAllocHandle(YAC_HANDLE_DBC, env, &conn);
    
    auto YacResult = yacConnect(conn, (YacChar*)info.ip_port.c_str(), YAC_NULL_TERM_STR, 
                   (YacChar*)info.user.c_str(), YAC_NULL_TERM_STR, 
                   (YacChar*)info.password.c_str(), YAC_NULL_TERM_STR);
    if (YacResult != YAC_SUCCESS) {
        std::cerr << "Failed to connect to YashanDB for create table" << std::endl;
        // Print error detailed... omitted for brevity
        yacFreeHandle(YAC_HANDLE_DBC, conn);
        yacFreeHandle(YAC_HANDLE_ENV, env);
        return;
    }
    
    yacAllocHandle(YAC_HANDLE_STMT, conn, &stmt);

    // Helper lambda to run execute
    auto execSQL = [&](const char* sql) {
        if(yacDirectExecute(stmt, (YacChar*)sql, YAC_NULL_TERM_STR) != YAC_SUCCESS) {
            std::cerr << "Failed to execute: " << sql << std::endl;
            YacInt32 errCode;
            char msg[1024];
            YacTextPos pos;
            yacGetDiagRec(&errCode, msg, sizeof(msg), NULL, NULL, 0, &pos);
            std::cerr << "Error Code: " << errCode << ", Message: " << msg << std::endl;
        }
    };

    // 1. Drop tables
    // execSQL("DROP TABLE checking CASCADE CONSTRAINTS"); // Oracle syntax often needs CASCADE CONSTRAINTS
    // execSQL("DROP TABLE savings CASCADE CONSTRAINTS");

    // 2. Create tables. Yashan/Oracle types: INT/INTEGER -> NUMBER(38), or INTEGER.
    // Using INTEGER is fine.
    // checking(id INT PRIMARY KEY, user_id INT, balance INT)
    // savings(id INT PRIMARY KEY, user_id INT, balance INT)
    
    // Note: Yashan might not support DROP IF EXISTS directly in standard SQL mode like PG, usually handled by catching error or PL/SQL block.
    // Here we just tried direct execute.
    
    const char* create_checking = "CREATE TABLE checking ("
                                  "id INTEGER PRIMARY KEY, "
                                  "balance INTEGER, "
                                  "city INTEGER, "
                                  "name CHAR(200))";
    
    const char* create_savings = "CREATE TABLE savings ("
                                 "id INTEGER PRIMARY KEY, "
                                 "balance INTEGER, "
                                 "city INTEGER, "
                                 "name CHAR(200))";

    execSQL(create_checking);
    execSQL(create_savings);
    
    std::cout << "YashanDB tables created." << std::endl;
    
    yacFreeHandle(YAC_HANDLE_STMT, stmt);
    yacDisconnect(conn);
    yacFreeHandle(YAC_HANDLE_DBC, conn);
    yacFreeHandle(YAC_HANDLE_ENV, env);
}

SmallBank::TableKeyPageMap SmallBank::load_data_yashan() {
    std::cout << "Loading data into YashanDB..." << std::endl;
    auto smallbank_account = this->get_account_count();
    YashanConnInfo info = YashanDBConnections[0]; // Use the first connection info for data loading
    
    // Same shuffle logic as PG load_data to ensure consistent distribution if using consistent seed
    std::vector<int> id_list;
    id_list.reserve(smallbank_account);
    for(int i = 1; i <= smallbank_account; i++) id_list.push_back(i);
    std::random_device rd;
    // std::mt19937 g(rd()); 
    // Use fixed seed for reproducibility if needed, or rd()
    std::mt19937 g(0); // Using fixed seed 0 to match what might be needed for determinism, or use rd()
    std::shuffle(id_list.begin(), id_list.end(), g);

    TableKeyPageMap ret;
    ret.checking_page.resize(smallbank_account + 1, 0);
    ret.savings_page.resize(smallbank_account + 1, 0);

    const int num_threads = 50; // Increased threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;

    auto worker = [&](int start_index, int end_index) {
        YacHandle env = NULL;
        YacHandle conn = NULL;
        YacHandle stmt = NULL;

        yacAllocHandle(YAC_HANDLE_ENV, NULL, &env);
        yacAllocHandle(YAC_HANDLE_DBC, env, &conn);
        if (yacConnect(conn, (YacChar*)info.ip_port.c_str(), YAC_NULL_TERM_STR, 
                    (YacChar*)info.user.c_str(), YAC_NULL_TERM_STR, 
                    (YacChar*)info.password.c_str(), YAC_NULL_TERM_STR) != YAC_SUCCESS) {
            std::cerr << "Worker thread failed to connect YashanDB" << std::endl;
            yacFreeHandle(YAC_HANDLE_DBC, conn);
            yacFreeHandle(YAC_HANDLE_ENV, env);
            return;
        }
        
        // Turn off AutoCommit for batch speed
        yacSetConnAttr(conn, YAC_ATTR_AUTOCOMMIT, (YacPointer)0, 0);

        // Prepare statements
        // CHECKING
        const char* ins_check = "INSERT INTO checking (id, balance, city, name) VALUES (?, ?, ?, ?)";
        YacHandle stmt_check = NULL;
        yacAllocHandle(YAC_HANDLE_STMT, conn, &stmt_check);
        yacPrepare(stmt_check, (YacChar*)ins_check, YAC_NULL_TERM_STR);
        
        // SAVINGS
        const char* ins_save = "INSERT INTO savings (id, balance, city, name) VALUES (?, ?, ?, ?)";
        YacHandle stmt_save = NULL;
        yacAllocHandle(YAC_HANDLE_STMT, conn, &stmt_save);
        yacPrepare(stmt_save, (YacChar*)ins_save, YAC_NULL_TERM_STR);

        // Batch size
        const int BATCH_SIZE = 4000;
        YacUint32 paramSetSize = BATCH_SIZE;

        YacInt32 id_arr[BATCH_SIZE];
        YacInt32 bal_arr[BATCH_SIZE];
        YacInt32 city_arr[BATCH_SIZE];
        char name_arr[BATCH_SIZE][201]; // Flat array for strings?? No, C multidim array is flat.
        
        YacInt32 bal_save_arr[BATCH_SIZE];
        
        // Bind parameters for checking
        yacSetStmtAttr(stmt_check, YAC_ATTR_PARAMSET_SIZE, &paramSetSize, sizeof(YacUint32));
        yacBindParameter(stmt_check, 1, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, id_arr, sizeof(YacInt32), sizeof(YacInt32), NULL);
        yacBindParameter(stmt_check, 2, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, bal_arr, sizeof(YacInt32), sizeof(YacInt32), NULL);
        yacBindParameter(stmt_check, 3, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, city_arr, sizeof(YacInt32), sizeof(YacInt32), NULL);
        yacBindParameter(stmt_check, 4, YAC_PARAM_INPUT, YAC_SQLT_CHAR, name_arr, 200, 201, NULL); // len=200, stride=201

        // Bind parameters for savings
        yacSetStmtAttr(stmt_save, YAC_ATTR_PARAMSET_SIZE, &paramSetSize, sizeof(YacUint32));
        yacBindParameter(stmt_save, 1, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, id_arr, sizeof(YacInt32), sizeof(YacInt32), NULL);
        yacBindParameter(stmt_save, 2, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, bal_save_arr, sizeof(YacInt32), sizeof(YacInt32), NULL);
        yacBindParameter(stmt_save, 3, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, city_arr, sizeof(YacInt32), sizeof(YacInt32), NULL);
        yacBindParameter(stmt_save, 4, YAC_PARAM_INPUT, YAC_SQLT_CHAR, name_arr, 200, 201, NULL);

        int count = 0;
        int city_cnt = static_cast<int>(SmallBankCityType::Count);

        for(int i = start_index; i < end_index; i++) {
            // id comes from shuffled list
            if (i >= id_list.size()) break;
            int cur_id = id_list[i];
            int idx = count;
            
            id_arr[idx] = cur_id;
            bal_arr[idx] = 1000 + ((cur_id - 1) % 1000);
            bal_save_arr[idx] = 1000 + ((cur_id - 1) % 1000);
            city_arr[idx] = ((cur_id - 1) / (smallbank_account / city_cnt) ) % city_cnt;
            
            std::string name = "Account_" + std::to_string(cur_id);
            strncpy(name_arr[idx], name.c_str(), 200);
            name_arr[idx][200] = '\0'; // Ensure null term if needed, though CHAR(200) fixed width
            
            count++;
            if(count == BATCH_SIZE) {
                if (yacExecute(stmt_check) != YAC_SUCCESS) std::cerr << "Insert Check Failed" << std::endl;
                if (yacExecute(stmt_save) != YAC_SUCCESS) std::cerr << "Insert Save Failed" << std::endl;
                count = 0;
            }
        }
        
        if(count > 0) {
            YacUint32 remain = count;
            yacSetStmtAttr(stmt_check, YAC_ATTR_PARAMSET_SIZE, &remain, sizeof(YacUint32));
            if (yacExecute(stmt_check) != YAC_SUCCESS) std::cerr << "Insert Check Remain Failed" << std::endl;
            
            yacSetStmtAttr(stmt_save, YAC_ATTR_PARAMSET_SIZE, &remain, sizeof(YacUint32));
            if (yacExecute(stmt_save) != YAC_SUCCESS) std::cerr << "Insert Save Remain Failed" << std::endl;
        }

        yacCommit(conn);
        
        yacFreeHandle(YAC_HANDLE_STMT, stmt_check);
        yacFreeHandle(YAC_HANDLE_STMT, stmt_save);
        yacDisconnect(conn);
        yacFreeHandle(YAC_HANDLE_DBC, conn);
        yacFreeHandle(YAC_HANDLE_ENV, env);
    };

    for(int i=0; i<num_threads; i++) {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? smallbank_account : (i+1) * chunk_size;
        threads.emplace_back(worker, start, end);
    }
    
    for(auto& t : threads) t.join();
    
    // Fetch all ROWIDs to populate ret
    {
        std::cout << "Fetching all ROWIDs..." << std::endl;
        YacHandle env = NULL;
        YacHandle conn = NULL;
        yacAllocHandle(YAC_HANDLE_ENV, NULL, &env);
        yacAllocHandle(YAC_HANDLE_DBC, env, &conn);
        if(yacConnect(conn, (YacChar*)info.ip_port.c_str(), YAC_NULL_TERM_STR, 
                   (YacChar*)info.user.c_str(), YAC_NULL_TERM_STR, 
                   (YacChar*)info.password.c_str(), YAC_NULL_TERM_STR) == YAC_SUCCESS) {
            
            auto fetch_rowids = [&](const char* table_name, std::vector<page_id_t>& page_map) {
                YacHandle stmt = NULL;
                yacAllocHandle(YAC_HANDLE_STMT, conn, &stmt);
                std::string sql = "SELECT id, ROWID FROM " + std::string(table_name);
                if (yacDirectExecute(stmt, (YacChar*)sql.c_str(), YAC_NULL_TERM_STR) == YAC_SUCCESS) {
                    YacInt32 id;
                    YacChar rowid_buf[64];
                    YacInt32 ind;
                    
                    yacBindColumn(stmt, 1, YAC_SQLT_INTEGER, &id, sizeof(id), &ind);
                    yacBindColumn(stmt, 2, YAC_SQLT_CHAR, rowid_buf, sizeof(rowid_buf), &ind);
                    
                    YacUint32 fetched_rows = 0;
                    while(true) {
                        YacResult fres = yacFetch(stmt, &fetched_rows);
                        if (fres == YAC_SUCCESS || fres == YAC_SUCCESS_WITH_INFO) {
                            // Single row fetch default if not array bound? 
                            // yacFetch usually requires array binding to return > 1 row, 
                            // or it returns 1 row if no array size set. 
                            // Assuming default param size 1.
                            if (id >= 0 && id < page_map.size()) {
                                page_map[id] = parse_yashan_rowid((char*)rowid_buf);
                            }
                        } else if (fres == 100) { // SQL_NO_DATA
                            break;
                        } else {
                            break;
                        }
                    }
                } else {
                     std::cerr << "Failed to select ROWID from " << table_name << std::endl;
                }
                yacFreeHandle(YAC_HANDLE_STMT, stmt);
            };
            
            fetch_rowids("checking", ret.checking_page);
            fetch_rowids("savings", ret.savings_page);
            
            yacDisconnect(conn);
            yacFreeHandle(YAC_HANDLE_DBC, conn);
            yacFreeHandle(YAC_HANDLE_ENV, env);
        } else {
            std::cerr << "Failed to connect for fetching ROWIDs" << std::endl;
        }
    }
    
    std::cout << "YashanDB data loaded." << std::endl;
    // Note: Yashan ROWID retrieval in bulk load is not straightforward without RETURNING BULK COLLECT
    // For now, we skip populating `ret` map with exact page IDs.
    
    return ret;
}

void SmallBank::create_smallbank_stored_procedures_yashan() {
    std::cout << "Creating stored procedures in YashanDB..." << std::endl;
    YashanConnInfo info = YashanDBConnections[0];

    YacHandle env = NULL;
    YacHandle conn = NULL;
    YacHandle stmt = NULL;

    yacAllocHandle(YAC_HANDLE_ENV, NULL, &env);
    yacAllocHandle(YAC_HANDLE_DBC, env, &conn);
    
    if (yacConnect(conn, (YacChar*)info.ip_port.c_str(), YAC_NULL_TERM_STR, 
                   (YacChar*)info.user.c_str(), YAC_NULL_TERM_STR, 
                   (YacChar*)info.password.c_str(), YAC_NULL_TERM_STR) != YAC_SUCCESS) {
        std::cerr << "Connect failed." << std::endl;
        yacFreeHandle(YAC_HANDLE_DBC, conn);
        yacFreeHandle(YAC_HANDLE_ENV, env);
        return;
    }
    
    yacAllocHandle(YAC_HANDLE_STMT, conn, &stmt);
    auto execSQL = [&](const std::string& sql) {
        if(yacDirectExecute(stmt, (YacChar*)sql.c_str(), YAC_NULL_TERM_STR) != YAC_SUCCESS) {
             std::cerr << "Failed to execute Schema change." << std::endl;
             YacInt32 errCode;
             char msg[1024];
             YacTextPos pos;
             yacGetDiagRec(&errCode, msg, sizeof(msg), NULL, NULL, 0, &pos);
             std::cerr << "Error Code: " << errCode << ", Message: " << msg << std::endl;
        }
    };

    // sp_amalgamate
    // Using OUT parameter REFCURSOR for returning results
    std::string sp_amalgamate = R"(
    CREATE OR REPLACE PROCEDURE sp_amalgamate(
        a1 IN INTEGER, 
        a2 IN INTEGER, 
        p_cursor OUT SYS_REFCURSOR
    ) AS
        v_bal1 INTEGER;
        v_bal2 INTEGER;
        v_total_bal INTEGER;
        v_final_bal INTEGER;
        v_rid1 VARCHAR(100);
        v_rid2 VARCHAR(100);
        v_rid3 VARCHAR(100);
    BEGIN
        -- 1. Get current balance and lock row for checking a1
        SELECT balance, ROWID INTO v_bal1, v_rid1 FROM checking WHERE id = a1 FOR UPDATE;
        
        -- 2. Get current balance and lock row for savings a1
        SELECT balance, ROWID INTO v_bal2, v_rid2 FROM savings WHERE id = a1 FOR UPDATE;
        
        -- Calculate total
        v_total_bal := COALESCE(v_bal1, 0) + COALESCE(v_bal2, 0);
        
        -- Update a1 accounts to 0
        UPDATE checking SET balance = 0 WHERE ROWID = v_rid1;
        UPDATE savings SET balance = 0 WHERE ROWID = v_rid2;
        
        -- 3. Update checking a2
        SELECT balance, ROWID INTO v_final_bal, v_rid3 FROM checking WHERE id = a2 FOR UPDATE;
        v_final_bal := COALESCE(v_final_bal, 0) + v_total_bal;
        UPDATE checking SET balance = v_final_bal WHERE ROWID = v_rid3;
        
        OPEN p_cursor FOR
            SELECT 'checking' AS rel, a1 AS id, v_rid1 AS ctid, 0 AS balance FROM DUAL
            UNION ALL
            SELECT 'savings', a1, v_rid2, 0 FROM DUAL
            UNION ALL
            SELECT 'checking', a2, v_rid3, v_final_bal FROM DUAL;
    END;
    )";
    
    // sp_send_payment
    std::string sp_send_payment = R"(
    CREATE OR REPLACE PROCEDURE sp_send_payment(
        a1 IN INTEGER, 
        a2 IN INTEGER,
        p_cursor OUT SYS_REFCURSOR
    ) AS
        v_rid1 VARCHAR(100);
        v_bal1 INTEGER;
        v_rid2 VARCHAR(100);
        v_bal2 INTEGER;
    BEGIN
        -- Update a1
        SELECT balance, ROWID INTO v_bal1, v_rid1 FROM checking WHERE id = a1 FOR UPDATE;
        v_bal1 := v_bal1 - 10;
        UPDATE checking SET balance = v_bal1 WHERE ROWID = v_rid1;
        
        -- Update a2
        SELECT balance, ROWID INTO v_bal2, v_rid2 FROM checking WHERE id = a2 FOR UPDATE;
        v_bal2 := v_bal2 + 10;
        UPDATE checking SET balance = v_bal2 WHERE ROWID = v_rid2;
        
        OPEN p_cursor FOR
            SELECT 'checking' AS rel, a1 AS id, v_rid1 AS ctid, v_bal1 AS balance FROM DUAL
            UNION ALL
            SELECT 'checking' AS rel, a2 AS id, v_rid2 AS ctid, v_bal2 AS balance FROM DUAL;
    END;
    )";
    
    // sp_deposit_checking
    std::string sp_deposit_checking = R"(
    CREATE OR REPLACE PROCEDURE sp_deposit_checking(
        a1 IN INTEGER,
        p_amount IN INTEGER,
        p_cursor OUT SYS_REFCURSOR
    ) AS
        v_rid1 VARCHAR(100);
        v_bal1 INTEGER;
    BEGIN
        SELECT balance, ROWID INTO v_bal1, v_rid1 FROM checking WHERE id = a1 FOR UPDATE;
        v_bal1 := v_bal1 + p_amount;
        UPDATE checking SET balance = v_bal1 WHERE ROWID = v_rid1;
        
        OPEN p_cursor FOR SELECT 'checking' AS rel, a1 AS id, v_rid1 AS ctid, v_bal1 AS balance FROM DUAL;
    END;
    )";

    // sp_write_check
    std::string sp_write_check = R"(
    CREATE OR REPLACE PROCEDURE sp_write_check(
        a1 IN INTEGER,
        p_amount IN INTEGER,
        p_cursor OUT SYS_REFCURSOR
    ) AS
        v_rid1 VARCHAR(100);
        v_bal1 INTEGER;
        v_rid2 VARCHAR(100);
        v_bal2 INTEGER;
    BEGIN
        SELECT balance, ROWID INTO v_bal1, v_rid1 FROM savings WHERE id = a1 FOR UPDATE;
        v_bal1 := v_bal1 - p_amount;
        UPDATE savings SET balance = v_bal1 WHERE ROWID = v_rid1;
        
        IF v_bal1 < 0 THEN
             SELECT balance, ROWID INTO v_bal2, v_rid2 FROM checking WHERE id = a1 FOR UPDATE;
             v_bal2 := v_bal2 + v_bal1 - 1;
             UPDATE checking SET balance = v_bal2 WHERE ROWID = v_rid2;
             
             v_bal1 := 0;
             UPDATE savings SET balance = 0 WHERE ROWID = v_rid1;
        ELSE
             v_rid2 := ''; v_bal2 := 0; -- No change to checking
        END IF;

        OPEN p_cursor FOR 
            SELECT 'savings' AS rel, a1 AS id, v_rid1 AS ctid, v_bal1 AS balance FROM DUAL;
    END;
    )";
    
    // sp_balance
    std::string sp_balance = R"(
    CREATE OR REPLACE PROCEDURE sp_balance(
        a1 IN INTEGER,
        p_cursor OUT SYS_REFCURSOR
    ) AS
        v_bal1 INTEGER;
        v_bal2 INTEGER;
    BEGIN
        SELECT balance INTO v_bal1 FROM savings WHERE id = a1;
        SELECT balance INTO v_bal2 FROM checking WHERE id = a1;
        OPEN p_cursor FOR
            SELECT 'savings' AS rel, a1 AS id, '' AS ctid, v_bal1 AS balance FROM DUAL
            UNION ALL
            SELECT 'checking' AS rel, a1 AS id, '' AS ctid, v_bal2 AS balance FROM DUAL;
    END;
    )";

    // sp_transact_savings
    std::string sp_transact_savings = R"(
    CREATE OR REPLACE PROCEDURE sp_transact_savings(
        a1 IN INTEGER,
        p_amount IN INTEGER,
        p_cursor OUT SYS_REFCURSOR
    ) AS
        v_rid1 VARCHAR(100);
        v_bal1 INTEGER;
    BEGIN
        SELECT balance, ROWID INTO v_bal1, v_rid1 FROM savings WHERE id = a1 FOR UPDATE;
        v_bal1 := v_bal1 + p_amount;
        UPDATE savings SET balance = v_bal1 WHERE ROWID = v_rid1;
        
        OPEN p_cursor FOR SELECT 'savings' AS rel, a1 AS id, v_rid1 AS ctid, v_bal1 AS balance FROM DUAL;
    END;
    )";

    execSQL(sp_amalgamate);
    execSQL(sp_send_payment);
    execSQL(sp_deposit_checking);
    execSQL(sp_write_check);
    execSQL(sp_balance);
    execSQL(sp_transact_savings);

    std::cout << "YashanDB stored procedures created." << std::endl;

    yacFreeHandle(YAC_HANDLE_STMT, stmt);
    yacDisconnect(conn);
    yacFreeHandle(YAC_HANDLE_DBC, conn);
    yacFreeHandle(YAC_HANDLE_ENV, env);
}

void SmallBank::generate_smallbank_txns_worker(int thread_id, TxnPool* txn_pool) {
    // 设置线程名
    pthread_setname_np(pthread_self(), ("txn_gen_t_" + std::to_string(thread_id)).c_str());

    std::vector<itemkey_t> accounts_vec(2);
    ZipfGen* zipfian_gen = nullptr;
    if (!zipfian_gen) {
        uint64_t zipf_seed = 2 * thread_id * GetCPUCycle();
        uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
        // 仅让线程0负责填充全局hottest_keys，避免并发写冲突和重复填充
        std::vector<uint64_t>* hot_keys_ptr = (thread_id == 0) ? &hottest_keys : nullptr;
        zipfian_gen = new ZipfGen(get_account_count(), zipfian_theta, zipf_seed & zipf_seed_mask, NumBucket, hot_keys_ptr);
    }

    // 全局一共进行 MetisWarmupRound * PARTITION_INTERVAL的冷启动事务生成，每个工作节点具有worker_threads个线程，每个线程生成try_count个事务
    int total_txn_to_generate = MetisWarmupRound * PARTITION_INTERVAL + try_count * worker_threads * ComputeNodeCount;
    
    // Experiment Control: Transaction Length
    bool enable_multi_update_experiment = Enable_Long_Txn; // 从配置中读取是否启用长事务实验
    int multi_update_length = Long_Txn_Length; // 从配置中读取长事务的长度
    
    while(generated_txn_count < total_txn_to_generate) {
        std::vector<TxnQueueEntry*> txn_batch;
        for (int i = 0; i < 0.1 * BatchRouterProcessSize; i++){
            generated_txn_count++;
            tx_id_t tx_id = tx_id_generator++; // global atomic transaction ID
            
            TxnQueueEntry* txn_entry = nullptr;

            if (enable_multi_update_experiment) {
                // Generate MultiUpdate Transaction (Type 6)
                int txn_type = 6; 
                std::vector<itemkey_t> multi_accounts;
                multi_accounts.reserve(multi_update_length);
                for(int k = 0; k < multi_update_length; k++) {
                    itemkey_t acc;
                    generate_account_id(acc, zipfian_gen);
                    multi_accounts.push_back(acc);
                }
                // Create with variable number of accounts
                txn_entry = new TxnQueueEntry(tx_id, txn_type, multi_accounts);
            } else {
                // Simulate some work
                // Randomly select a transaction type and accounts
                int txn_type = generate_txn_type();
                if(txn_type == 0 || txn_type == 1) { // TxAmagamate or TxSendPayment
                    generate_two_account_ids(accounts_vec[0], accounts_vec[1], zipfian_gen);
                } else {
                    generate_account_id(accounts_vec[0], zipfian_gen);
                }
                // Create a new transaction object
                txn_entry = new TxnQueueEntry(tx_id, txn_type, accounts_vec);
            }
            txn_batch.push_back(txn_entry);
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
}

SmallBank::TableKeyPageMap SmallBank::load_data(pqxx::connection *conn0) {
    auto smallbank_account = this->get_account_count();
    std::cout << "Loading data..." << std::endl;
    std::cout << "Will load " << smallbank_account << " accounts into checking and savings tables" << std::endl;
    // Load data into the database if needed
    // Insert data into checking and savings tables
    const int num_threads = 50;  // Number of worker threads
    std::vector<std::thread> threads;
    const int chunk_size = smallbank_account / num_threads;
    // 这里创建一个导入数据的账户id的列表, 随机导入
    std::vector<int> id_list;
    for(int i = 1; i <= smallbank_account; i++) id_list.push_back(i);
    // 随机打乱 id_list
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(id_list.begin(), id_list.end(), g);
    // 返回映射，按 id 直接索引，避免额外拷贝与查找开销
    TableKeyPageMap ret;
    ret.checking_page.resize(smallbank_account + 1);
    ret.savings_page.resize(smallbank_account + 1);

    auto worker = [&id_list, smallbank_account, &ret](int start, int end) {
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
                    // 按 id 直接记录页号，避免拷贝 pair
                    ret.checking_page[inserted_id] = page_id;
                }
                
                // 执行savings表插入并获取位置信息
                pqxx::result savings_result = txn_create.exec(insert_savings_sql);
                if (!savings_result.empty()) {
                    std::string ctid = savings_result[0]["ctid"].as<std::string>();
                    auto [page_id, tuple_index] = parse_page_id_from_ctid(ctid);
                    int inserted_id = savings_result[0]["id"].as<int>();
                    ret.savings_page[inserted_id] = page_id;
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

    // try vacuum freeze 
    // try {
    //     pqxx::nontransaction txn_vacuum(*conn0);
    //     txn_vacuum.exec("VACUUM FREEZE checking;");
    //     txn_vacuum.exec("VACUUM FREEZE savings;");
    //     std::cout << "Vacuum freeze completed successfully." << std::endl;
    // }catch (const std::exception &e) {
    //     std::cerr << "Error during vacuum freeze: " << e.what() << std::endl;
    // }

    std::thread extend_thread1([](){
        pqxx::connection conn_extend(DBConnection[0]);
        if (!conn_extend.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        try{
            // pg not support
            pqxx::nontransaction txn(conn_extend);
            // pre-extend table to avoid frequent page extend during txn processing
            std::string extend_sql = "SELECT sys_extend('checking', " + std::to_string(PreExtendPageSize) + ")";
            txn.exec(extend_sql);
            std::cout << "Pre-extended checking table." << std::endl;
        }
        catch (const std::exception &e) {
            std::cerr << "Error while pre-extending checking table: " << e.what() << std::endl;
        }
    });

    std::thread extend_thread2([](){
        pqxx::connection conn_extend(DBConnection[0]);
        if (!conn_extend.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        try{
            // pg not support
            pqxx::nontransaction txn(conn_extend);
            // pre-extend table to avoid frequent page extend during txn processing
            std::string extend_sql = "SELECT sys_extend('savings', " + std::to_string(PreExtendPageSize) + ")";
            txn.exec(extend_sql);
            std::cout << "Pre-extended savings table." << std::endl;
        }
        catch (const std::exception &e) {
            std::cerr << "Error while pre-extending savings table: " << e.what() << std::endl;
        }
    });

    std::thread extend_thread3([](){
        pqxx::connection conn_extend(DBConnection[0]);
        if (!conn_extend.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        try{
            // pg not support
            pqxx::nontransaction txn(conn_extend);
            // pre-extend table to avoid frequent page extend during txn processing
            std::string extend_sql = "SELECT sys_extend('idx_checking_id', " + std::to_string(PreExtendIndexPageSize) + ")";
            txn.exec(extend_sql);
            std::cout << "Pre-extended idx_checking_id index." << std::endl;
        }
        catch (const std::exception &e) {
            std::cerr << "Error while pre-extending idx_checking_id index: " << e.what() << std::endl;
        }
    });

    std::thread extend_thread4([](){
        pqxx::connection conn_extend(DBConnection[0]);
        if (!conn_extend.is_open()) {
            std::cerr << "Failed to connect to the database. conninfo" + DBConnection[0] << std::endl;
            return;
        }
        try{
            // pg not support
            pqxx::nontransaction txn(conn_extend);
            // pre-extend table to avoid frequent page extend during txn processing
            std::string extend_sql = "SELECT sys_extend('idx_savings_id', " + std::to_string(PreExtendIndexPageSize) + ")";
            txn.exec(extend_sql);
            std::cout << "Pre-extended idx_savings_id index." << std::endl;
        }
        catch (const std::exception &e) {
            std::cerr << "Error while pre-extending idx_savings_id index: " << e.what() << std::endl;
        }
    });

    extend_thread1.join();
    extend_thread2.join();
    extend_thread3.join();
    extend_thread4.join();

    std::cout << "Table creation and pre-extension completed." << std::endl;

    // try analyze
    // try {
    //     pqxx::nontransaction txn_analyze(*conn0);
    //     txn_analyze.exec("ANALYZE checking;");
    //     txn_analyze.exec("ANALYZE savings;");
    //     std::cout << "Analyze completed successfully." << std::endl;
    // }catch (const std::exception &e) {
    //     std::cerr << "Error during analyze: " << e.what() << std::endl;
    // }

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

    // // 在各个节点都执行一次select count(*)，确保数据同步完成
    // try{
    //     std::vector<std::thread> count_threads;
    //     for(int node_id = 0; node_id < ComputeNodeCount; node_id++) {
    //         count_threads.emplace_back([node_id]() {
    //             pqxx::connection conn_count(DBConnection[node_id]);
    //             if (!conn_count.is_open()) {
    //                 std::cerr << "Failed to connect to the database. conninfo" + DBConnection[node_id] << std::endl;
    //                 return;
    //             }
    //             try{
    //                 pqxx::nontransaction txn(conn_count);
    //                 pqxx::result checking_count = txn.exec("SELECT COUNT(*) FROM checking;");
    //                 pqxx::result savings_count = txn.exec("SELECT COUNT(*) FROM savings;");
    //                 if(!checking_count.empty()){
    //                     std::cout << "Node " << node_id << " - Checking table count: " << checking_count[0][0].as<int>() << std::endl;
    //                 }
    //                 if(!savings_count.empty()){
    //                     std::cout << "Node " << node_id << " - Savings table count: " << savings_count[0][0].as<int>() << std::endl;
    //                 }
    //             }catch(const std::exception &e) {
    //                 std::cerr << "Error while getting table count on node " << node_id << ": " << e.what() << std::endl;
    //             }
    //         });
    //     }
    //     for(auto& t : count_threads) {
    //         t.join();
    //     }
    // }catch(const std::exception &e) {
    //     std::cerr << "Error while executing count threads: " << e.what() << std::endl;
    // }

    // 返回映射供上层初始化 SmartRouter 的 key->page
    return ret;
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

            // txn.exec(R"SQL(
            // CREATE OR REPLACE FUNCTION sp_amalgamate(a1 INT, a2 INT)
            // RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            // LANGUAGE plpgsql AS $$
            // DECLARE
            //     c1_ctid TID;
            //     s1_ctid TID;
            //     c2_ctid TID;
            //     c1_bal INT;
            //     s1_bal INT;
            //     total_bal INT;
            // BEGIN
            //     -- 1) 先锁 checking 中 a1/a2 (固定顺序)
            //     PERFORM 1
            //     FROM checking c
            //     WHERE c.id IN (a1, a2)
            //     ORDER BY c.id
            //     FOR UPDATE;

            //     -- 2) 再锁 savings 中 a1 (固定在 checking 之后拿锁，形成全局顺序)
            //     PERFORM 1
            //     FROM savings s
            //     WHERE s.id = a1
            //     FOR UPDATE;

            //     -- 3) 更新 checking(a1) -> 0
            //     UPDATE checking c
            //     SET balance = 0
            //     WHERE c.id = a1
            //     RETURNING c.ctid, c.balance INTO c1_ctid, c1_bal;

            //     -- 4) 更新 savings(a1) -> 0
            //     UPDATE savings s
            //     SET balance = 0
            //     WHERE s.id = a1
            //     RETURNING s.ctid, s.balance INTO s1_ctid, s1_bal;

            //     total_bal := COALESCE(c1_bal,0) + COALESCE(s1_bal,0);

            //     -- 5) 更新 checking(a2) -> total
            //     UPDATE checking c2
            //     SET balance = total_bal
            //     WHERE c2.id = a2
            //     RETURNING c2.ctid, c2.balance INTO c2_ctid, balance;

            //     RETURN QUERY SELECT 'checking'::text, a1, c1_ctid, 0, txid_current();
            //     RETURN QUERY SELECT 'savings'::text,  a1, s1_ctid, 0, txid_current();
            //     RETURN QUERY SELECT 'checking'::text, a2, c2_ctid, balance, txid_current();
            // END;
            // $$;
            // )SQL");

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

            // SendPayment: a1.checking -= 10, a2.checking += 10
            // txn.exec(R"SQL(
            // CREATE OR REPLACE FUNCTION sp_send_payment(a1 INT, a2 INT)
            // RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            // LANGUAGE plpgsql AS $$
            // DECLARE
            //     v_txid BIGINT;
            // BEGIN
            //     v_txid := txid_current();

            //     -- a1 == a2 时按常识应是 no-op(否则 CASE 会只命中第一条，变成 -10)
            //     IF a1 = a2 THEN
            //         RETURN QUERY
            //         SELECT 'checking'::text, c.id, c.ctid, c.balance, v_txid
            //         FROM checking c
            //         WHERE c.id = a1;
            //         RETURN;
            //     END IF;

            //     RETURN QUERY
            //     WITH upd AS (
            //         UPDATE checking AS c
            //         SET balance = c.balance + CASE
            //             WHEN c.id = a1 THEN -10
            //             WHEN c.id = a2 THEN  10
            //         END
            //         WHERE c.id IN (a1, a2)
            //         RETURNING c.id, c.ctid, c.balance
            //     )
            //     SELECT 'checking'::text, u.id, u.ctid, u.balance, v_txid
            //     FROM upd u
            //     ORDER BY u.id;

            // END;
            // $$;
            // )SQL");

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

            // // new WriteCheck: update savings(a1) -= 50, read checking(a1)
            // txn.exec(R"SQL(
            // CREATE OR REPLACE FUNCTION sp_write_check(a1 INT)
            // RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            // LANGUAGE plpgsql AS $$
            // DECLARE
            //     c_ctid TID;
            //     c_bal  INT;
            //     s_ctid TID;
            //     s_bal  INT;
            // BEGIN
            //     SELECT c.ctid, c.balance INTO c_ctid, c_bal
            //     FROM checking c WHERE c.id = a1;

            //     UPDATE savings s
            //     SET balance = s.balance - 50
            //     WHERE s.id = a1
            //     RETURNING s.ctid, s.balance INTO s_ctid, s_bal;

            //     RETURN QUERY SELECT 'checking'::text, a1, c_ctid, c_bal, txid_current();
            //     RETURN QUERY SELECT 'savings'::text,  a1, s_ctid, s_bal, txid_current();
            // END; $$;
            // )SQL");

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

            // MultiUpdate: extended transaction that updates multiple accounts
            // Used for variable transaction length experiments
            txn.exec(R"SQL(
            CREATE OR REPLACE FUNCTION sp_multi_update(ids INT[], val INT)
            RETURNS TABLE(rel TEXT, id INT, ctid TID, balance INT, txid BIGINT)
            LANGUAGE plpgsql AS $$
            DECLARE
                target_id INT;
                c_ctid TID;
                c_bal INT;
            BEGIN
                FOREACH target_id IN ARRAY ids
                LOOP
                    UPDATE checking c
                    SET balance = c.balance + val
                    WHERE c.id = target_id
                    RETURNING c.ctid, c.balance INTO c_ctid, c_bal;
                    
                    RETURN QUERY SELECT 'checking'::text, target_id, c_ctid, c_bal, txid_current();
                END LOOP;
            END; $$;
            )SQL");

            txn.commit();
            std::cout << "Stored procedures created." << std::endl;
    } catch (const std::exception &e) {
            std::cerr << "Error creating stored procedures: " << e.what() << std::endl;
    }
}