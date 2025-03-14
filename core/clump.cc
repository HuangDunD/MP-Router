#include "partition.h"

int main(){
    Metis* metis = new Metis();
    metis->process_transactions_to_graph("../workload/LOG_FILE", "graph.txt");
    metis->metis_partition_graph("graph.txt","partition.txt");
    return 0;
}