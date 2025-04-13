#include "queryplan_cardinality.h"

int main() {
    std::string conninfo = "host=localhost port=15432 dbname=template1 user=gpadmin password=gpadmin";
    std::string query = "SELECT C.C_NAME, O.O_ORDERSTATUS FROM CUSTOMER C JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY;";

    int cardinality = get_query_plan_cardinality(query, conninfo);
    if (cardinality != -1) {
        std::cout << "Estimated cardinality: " << cardinality << std::endl;
    }

    return 0;
}
