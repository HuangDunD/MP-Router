#include "parse.h"
#include <iostream>

int main() {
    std::string selectSQL = "*** SELECT COUNT(*) FROM CUSTOMER \n WHERE CUSTOMER.c_custkey IN (103054, 9384, 101518, 97586, 10643, 100155, 104663, 101584, 106879, 102584);";
    std::string updateSQL = "UPDATE PART\nSET P_COMMENT = CASE \n\tWHEN p_partkey = 212777 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 209751 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 241230 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 237845 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 215006 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 237942 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 204020 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 234152 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 204980 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tWHEN p_partkey = 227011 THEN SUBSTRING(MD5(RAND()), 1, 10)\n\tELSE P_COMMENT END\nWHERE p_partkey IN (212777, 209751, 241230, 237845, 215006, 237942, 204020, 234152, 204980, 227011);\n";
    // std::string updateSQL = "UPDATE CUSTOMER ************* WHERE ps_partkey IN (732400, 752384, 707676,781470, 798267, 755211, 782611,745634, 791234, 727513);";
    std::string joinSQL = "*** SELECT * FROM tpch.CUSTOMER JOIN tpch.ORDERS ON CUSTOMER.c_customer = ORDERS.o_customer";

    SQLInfo selectInfo ;
    parseTPCHSQL(selectSQL,selectInfo);
    SQLInfo updateInfo ;
    parseTPCHSQL(updateSQL,updateInfo);
    SQLInfo joinInfo ;
    parseTPCHSQL(joinSQL,updateInfo);

    // 打印解析结果
    assert(selectInfo.type == SQLType::SELECT);
    std::cout << "SELECT Statement:" << std::endl;
    std::cout << "Table Names: ";
    for (const auto& table : selectInfo.tableNames) {
        std::cout << table << " ";
    }
    std::cout << std::endl;
    std::cout << "Column Names: ";
    for (const auto& column : selectInfo.columnNames) {
        std::cout << column << " ";
    }
    std::cout << std::endl;
    std::cout << "Key Vector: ";
    for (const auto& key : selectInfo.keyVector) {
        std::cout << key << " ";
    }
    std::cout << std::endl;

    assert(updateInfo.type == SQLType::UPDATE);
    std::cout << "UPDATE Statement:" << std::endl;
    std::cout << "Table Names: ";
    for (const auto& table : updateInfo.tableNames) {
        std::cout << table << " ";
    }
    std::cout << std::endl;
    std::cout << "Column Names: ";
    for (const auto& column : updateInfo.columnNames) {
        std::cout << column << " ";
    }
    std::cout << std::endl;
    std::cout << "Key Vector: ";
    for (const auto& key : updateInfo.keyVector) {
        std::cout << key << " ";
    }
    std::cout << std::endl;

    assert(joinInfo.type == SQLType::JOIN);
    std::cout << "JOIN Statement:" << std::endl;
    std::cout << "Table Names: ";
    for (const auto& table : joinInfo.tableNames) {
        std::cout << table << " ";
    }
    std::cout << std::endl;
    std::cout << "Column Names: ";
    for (const auto& column : joinInfo.columnNames) {
        std::cout << column << " ";
    }
    std::cout << std::endl;
    std::cout << "Key Vector: ";
    for (const auto& key : joinInfo.keyVector) {
        std::cout << key << " ";
    }
    std::cout << std::endl;

    return 0;
}
    