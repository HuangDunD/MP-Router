package mock.bench.Tpcc.WorkLoad.Application.Tpcc;

import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.jTPCCTData;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;


public class OrderStatus {
    public static String executeOrderStatus(jTPCCTData.OrderStatusData orderStatus, jTPCCRandom rnd) throws Exception {

        List<String> sqlHeaderList = new ArrayList<>();
        List<String> sqlList = new ArrayList<>();
        String stmt, header;
        StringBuilder sqlBuilder = new StringBuilder();

        String last_stmt = "unknown";
        header = "WareHouse[1]:" + orderStatus.w_id; // 标记主仓库id
        sqlHeaderList.add(header);
        header = "Remote[1]:0";
        sqlHeaderList.add(header + "\n");

        // Select the CUSTOMER ID. 100% by ID 修改注意
        last_stmt = "stmtOrderStatusSelectCustomer";
        header = "Table[1]:0\n" +
                "Column[3]:10,11,12\n" +
                "Key[3]:{0},{1},{2}\n";
        header = MessageFormat.format(header, String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(orderStatus.c_id));
        sqlHeaderList.add(header);

        stmt = "SELECT c_first, c_middle, c_last, c_balance "
                + "    FROM bmsql_customer "
                + "    WHERE c_w_id = {0} AND c_d_id = {1} AND c_id = {2}";
        stmt = MessageFormat.format(stmt, String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(orderStatus.c_id));
        sqlList.add(stmt);



        // Select the last ORDER for this customer.
        last_stmt = "stmtOrderStatusSelectLastOrder";
        header = "Table[1]:5\n" +
                "Column[3]:22,23,24\n" +
                "Key[3]:{0},{1},{2}\n";
        header = MessageFormat.format(header, String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(orderStatus.c_id));
        sqlHeaderList.add(header);

        stmt = "SELECT o_id, o_entry_d, o_carrier_id "
                + "    FROM bmsql_oorder "
                + "    WHERE o_w_id = {0} AND o_d_id = {1} AND o_c_id = {2} "
                + "      AND o_id = ("
                + "          SELECT max(o_id) "
                + "              FROM bmsql_oorder "
                + "              WHERE o_w_id = {0} AND o_d_id = {1} AND o_c_id = {2}"
                + "          )";
        stmt = MessageFormat.format(stmt, String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(orderStatus.c_id),
                String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(orderStatus.c_id));
        sqlList.add(stmt);



        // Select the ORDER LINE for this order.
        last_stmt = "stmtOrderStatusSelectOrderLine";
        int ol_d_id = rnd.nextInt(1, 2999);
        header = "Table[1]:6\n" +
                "Column[3]:25,26,27\n" +
                "Key[3]:{0},{1},{2}\n";
        header = MessageFormat.format(header, String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(ol_d_id));
        sqlHeaderList.add(header);

        stmt = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, "
                + "       ol_amount, ol_delivery_d "
                + "    FROM bmsql_order_line "
                + "    WHERE ol_w_id = {0} AND ol_d_id = {1} AND ol_o_id = {2} "
                + "    ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number";
        stmt = MessageFormat.format(stmt, String.valueOf(orderStatus.w_id), String.valueOf(orderStatus.d_id), String.valueOf(ol_d_id)); // o_id 随机生成
        sqlList.add(stmt);

        sqlBuilder.append("***Header_Start***\n");
        for (String headerSql : sqlHeaderList) {
            sqlBuilder.append(headerSql).append("\n");
        }
        sqlBuilder.append("***Header_End***\n");

        sqlBuilder.append("***Txn_Start***\n");
        sqlBuilder.append("BEGIN;\n");
        for (String sql : sqlList) {
            sqlBuilder.append(sql).append(";\n");
        }
        sqlBuilder.append("COMMIT;\n");
        sqlBuilder.append("***Txn_End***\n");
        return sqlBuilder.toString();
    }
}
