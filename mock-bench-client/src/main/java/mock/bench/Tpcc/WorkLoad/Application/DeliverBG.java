package mock.bench.Tpcc.WorkLoad.Application;

import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.jTPCCTData;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static mock.bench.Tpcc.WorkLoad.jTPCC.min_o_id;

public class DeliverBG {
    public static String executeDeliveryBG(jTPCCTData.DeliveryBGData deliveryBG, jTPCCRandom rnd) throws Exception {
        int d_id;
        int o_id;
        long now = System.currentTimeMillis();

        deliveryBG.delivered_o_id = new int[15];

        List<String> sqlHeaderList = new ArrayList<>();
        List<String> sqlList = new ArrayList<>();
        String stmt, header;
        StringBuilder sqlBuilder = new StringBuilder();

        String last_stmt = "unknown";
        header = "WareHouse[1]:" + deliveryBG.w_id; // 标记主仓库id
        sqlHeaderList.add(header);
        header = "Remote[1]:0";
        sqlHeaderList.add(header + "\n");

        for (d_id = 1; d_id <= 10; d_id++) {
            /*
             * Try to find the oldest undelivered order for this DISTRICT. There may not be one, which
             * is a case that needs to be reportd.
             */
            last_stmt = "stmtDeliveryBGSelectOldestNewOrder";
            header = "Table[1]:4\n" +
                    "Column[2]:18,19\n" +
                    "Key[2]:{0},{1}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id));
            sqlHeaderList.add(header);

            stmt = "SELECT no_o_id "
                    + "    FROM bmsql_new_order "
                    + "    WHERE no_w_id = {0} AND no_d_id = {1} "
                    + "    ORDER BY no_o_id ASC";
            stmt = MessageFormat.format(stmt, String.valueOf(deliveryBG.w_id), String.valueOf(d_id));
            sqlList.add(stmt);



            last_stmt = "stmtDeliveryBGDeleteOldestNewOrder";
            // get the min o_id and update it
            o_id = min_o_id[deliveryBG.w_id][d_id];
            min_o_id[deliveryBG.w_id][d_id] = o_id + 1;
            header = "Table[1]:4\n" +
                    "Column[3]:18,19,20\n" +
                    "Key[3]:{0},{1},{2}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlHeaderList.add(header);

            stmt = "DELETE FROM bmsql_new_order "
                    + "    WHERE no_w_id = {0} AND no_d_id = {1} AND no_o_id = {2}";
            stmt = MessageFormat.format(stmt, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlList.add(stmt);



            // Update the ORDER setting the o_carrier_id.
            last_stmt = "stmtDeliveryBGUpdateOrder";
            header = "Table[1]:5\n" +
                    "Column[3]:22,23,21\n" +
                    "Key[3]:{0},{1},{2}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlHeaderList.add(header);

            stmt = "UPDATE bmsql_oorder "
                    + "    SET o_carrier_id = {0} "
                    + "    WHERE o_w_id = {1} AND o_d_id = {2} AND o_id = {3}";
            stmt = MessageFormat.format(stmt, String.valueOf(deliveryBG.o_carrier_id), String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlList.add(stmt);



            // Get the o_c_id from the ORDER.
            last_stmt = "stmtDeliveryBGSelectOrder";
            header = "Table[1]:5\n" +
                    "Column[3]:22,23,21\n" +
                    "Key[3]:{0},{1},{2}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlHeaderList.add(header);

            stmt = "SELECT o_c_id "
                    + "    FROM bmsql_oorder "
                    + "    WHERE o_w_id = {0} AND o_d_id = {1} AND o_id = {2}";
            stmt = MessageFormat.format(stmt, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlList.add(stmt);



            // Update ORDER_LINE setting the ol_delivery_d.
            last_stmt = "stmtDeliveryBGUpdateOrderLine";
            header = "Table[1]:6\n" +
                    "Column[3]:25,26,27\n" +
                    "Key[3]:{0},{1},{2}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlHeaderList.add(header);

            stmt = "UPDATE bmsql_order_line "
                    + "    SET ol_delivery_d = {0} "
                    + "    WHERE ol_w_id = {1} AND ol_d_id = {2} AND ol_o_id = {3}";
            stmt = MessageFormat.format(stmt, "'" + new Timestamp(now).toString() + "'" , String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlList.add(stmt);



            // Select the sum(ol_amount) from ORDER_LINE.
            last_stmt = "stmtDeliveryBGSelectSumOLAmount";
            header = "Table[1]:6\n" +
                    "Column[3]:25,26,27\n" +
                    "Key[3]:{0},{1},{2}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlHeaderList.add(header);

            stmt = "SELECT sum(ol_amount) AS sum_ol_amount "
                    + "    FROM bmsql_order_line "
                    + "    WHERE ol_w_id = {0} AND ol_d_id = {1} AND ol_o_id = {2}";
            stmt = MessageFormat.format(stmt, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(o_id));
            sqlList.add(stmt);



            // Update the CUSTOMER.
            last_stmt = "stmtDeliveryBGUpdateCustomer";
            int c_id = rnd.getCustomerID();
            header = "Table[1]:0\n" +
                    "Column[3]:10,11,12\n" +
                    "Key[3]:{0},{1},{2}\n";
            header = MessageFormat.format(header, String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(c_id));
            sqlHeaderList.add(header);

            stmt = "UPDATE bmsql_customer "
                    + "    SET c_balance = {0}, "
                    + "        c_delivery_cnt = c_delivery_cnt + 1 "
                    + "    WHERE c_w_id = {1} AND c_d_id = {2} AND c_id = {3}";
            stmt = MessageFormat.format(stmt, String.valueOf(rnd.nextDouble(0, 2000)), String.valueOf(deliveryBG.w_id), String.valueOf(d_id), String.valueOf(c_id));
            sqlList.add(stmt);

            // Recored the delivered O_ID in the DELIVERY_BG
            deliveryBG.delivered_o_id[d_id - 1] = o_id;
        }

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
