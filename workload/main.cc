#include "config.h"
#include "common.h"
#include "smallbank_db.h"

int main(){
    int total_page_num = 300000;
    SmallBank* smallbank_client = new SmallBank(total_page_num);

    uint64_t seed = 0xdeadbeef;

    std::fstream file;
    file.open(LOG_FILE_NAME, std::ios::out);
    for(int i = 0; i < ATTEMPTED_NUM; i++){
        uint64_t page_id_0;
        uint64_t page_id_1;
        bool par = (FastRand(&seed) % 100 < CrossNodeAccessRatio * 100);
        bool node_id = FastRand(&seed) % ComputeNodeCount;
        if(FastRand(&seed) % 2 == 0){
            smallbank_client->get_two_accounts(&seed, &page_id_0, &page_id_1, node_id, par);
            // 输出到LOG_FILE中
            file << i << " " << "2 " << page_id_0 << " " << page_id_1 << std::endl;
        }else{
            smallbank_client->get_account(&seed, &page_id_0, par, node_id);
            // 输出到LOG_FILE中
            file << i << " " << "1 " << page_id_0 << std::endl;
        }
    }
}