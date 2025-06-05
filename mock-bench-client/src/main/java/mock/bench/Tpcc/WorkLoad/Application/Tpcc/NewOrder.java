package mock.bench.Tpcc.WorkLoad.Application.Tpcc;

import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.jTPCCTData;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static mock.bench.Tpcc.WorkLoad.jTPCC.next_o_id;

public class NewOrder {

    public static String executeNewOrder(jTPCCTData.NewOrderData newOrder, jTPCCRandom rnd) throws Exception {

        List<String> sqlHeaderList = new ArrayList<>();
        List<String> sqlList = new ArrayList<>();
        String stmt, header;
        StringBuilder sqlBuilder = new StringBuilder();

        int o_id = 0; // TODO：需要全局维护 Done
        int o_all_local = 1; // 是否全在一家
        long o_entry_d;
        int ol_cnt;

        int ol_seq[] = new int[15]; // 15个购买记录

        // The o_entry_d is now.
        o_entry_d = System.currentTimeMillis();
        newOrder.o_entry_d = new Timestamp(o_entry_d).toString();
        for (ol_cnt = 0; ol_cnt < 15 && newOrder.ol_i_id[ol_cnt] != 0; ol_cnt++) {
            ol_seq[ol_cnt] = ol_cnt;

            // While looping we also determine o_all_local.
            if (newOrder.ol_supply_w_id[ol_cnt] != newOrder.w_id)
                o_all_local = 0;
        }

        for (int x = 0; x < ol_cnt - 1; x++) {
            for (int y = x + 1; y < ol_cnt; y++) {
                if (newOrder.ol_supply_w_id[ol_seq[y]] < newOrder.ol_supply_w_id[ol_seq[x]]) {
                    int tmp = ol_seq[x];
                    ol_seq[x] = ol_seq[y];
                    ol_seq[y] = tmp;
                } else if (newOrder.ol_supply_w_id[ol_seq[y]] == newOrder.ol_supply_w_id[ol_seq[x]]
                        && newOrder.ol_i_id[ol_seq[y]] < newOrder.ol_i_id[ol_seq[x]]) {
                    int tmp = ol_seq[x];
                    ol_seq[x] = ol_seq[y];
                    ol_seq[y] = tmp;
                }
            }
        }

        // The above also provided the output value for o_ol_cnt;
        newOrder.o_ol_cnt = ol_cnt;

        String last_stmt = "unknown";
        header = "WareHouse[1]:" + newOrder.w_id; // 标记主仓库id
        sqlHeaderList.add(header);

        boolean remote_warehouse = false;
        for (int i = 0; i < ol_cnt; i++) {
            int seq = ol_seq[i];
            if (newOrder.ol_supply_w_id[seq] != newOrder.w_id) { // 仓库是否相同
                remote_warehouse = true;
                break;
            }
        }
        if (remote_warehouse)
            header = "Remote[1]:1";
        else
            header = "Remote[1]:0";
        sqlHeaderList.add(header + "\n");



        // Retrieve the required data from DISTRICT
        last_stmt = "stmtNewOrderSelectDist";
        header = "Table[1]:1\n" +
                "Column[2]:13,14\n" +
                "Key[2]:{0},{1}\n";
        header = MessageFormat.format(header, String.valueOf(newOrder.w_id), String.valueOf(newOrder.d_id));
        sqlHeaderList.add(header);

        stmt = "SELECT d_tax, d_next_o_id "
                + "    FROM bmsql_district "
                + "    WHERE d_w_id = {0} AND d_id = {1} "
                + "    FOR UPDATE";
        stmt = MessageFormat.format(stmt, String.valueOf(newOrder.w_id), String.valueOf(newOrder.d_id));
        sqlList.add(stmt);



        // Retrieve the required data from CUSTOMER and WAREHOUSE
        last_stmt = "stmtNewOrderSelectWhseCust";
        header = "Table[2]:0,8\n" +
                "Column[4]:9,10,11,12\n" +
                "Key[4]:{0},{1},{2},{3}\n";
        header = MessageFormat.format(header, String.valueOf(newOrder.w_id), String.valueOf(newOrder.w_id), String.valueOf(newOrder.d_id), String.valueOf(newOrder.c_id));
        sqlHeaderList.add(header);

        stmt = "SELECT c_discount, c_last, c_credit, w_tax "
                + "    FROM bmsql_customer "
                + "    JOIN bmsql_warehouse ON (w_id = c_w_id) " // c_w_id 传
                + "    WHERE c_w_id = {0} AND c_d_id = {1} AND c_id = {2}";
        stmt = MessageFormat.format(stmt, String.valueOf(newOrder.w_id), String.valueOf(newOrder.d_id), String.valueOf(newOrder.c_id));
        sqlList.add(stmt);



        // Update the DISTRICT bumping the D_NEXT_O_ID 全局维护的d_next_o_id
        last_stmt = "stmtNewOrderUpdateDist";
        header = "Table[1]:1\n" +
                "Column[2]:13,14\n" +
                "Key[2]:{0},{1}\n";
        header = MessageFormat.format(header, String.valueOf(newOrder.w_id), String.valueOf(newOrder.d_id));
        sqlHeaderList.add(header);

        stmt = "UPDATE bmsql_district "
                + "    SET d_next_o_id = d_next_o_id + 1 "
                + "    WHERE d_w_id = {0} AND d_id = {1}";
        stmt = MessageFormat.format(stmt, String.valueOf(newOrder.w_id), String.valueOf(newOrder.d_id));
        sqlList.add(stmt);



        // Insert the ORDER row
        last_stmt = "stmtNewOrderInsertOrder";
        o_id = next_o_id[newOrder.w_id][newOrder.d_id];
        newOrder.o_id = o_id;
        next_o_id[newOrder.w_id][newOrder.d_id] = o_id + 1;

        stmt = "INSERT INTO bmsql_oorder ("
                + "    o_id, o_d_id, o_w_id, o_c_id, o_entry_d, "
                + "    o_ol_cnt, o_all_local) "
                + "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})";
        stmt = MessageFormat.format(stmt, String.valueOf(o_id), String.valueOf(newOrder.d_id), String.valueOf(newOrder.w_id),
                String.valueOf(newOrder.c_id), "'" + new Timestamp(System.currentTimeMillis()).toString() + "'", String.valueOf(ol_cnt), String.valueOf(o_all_local));
        sqlList.add(stmt);



        // Insert the NEW_ORDER row
        last_stmt = "stmtNewOrderInsertNewOrder";
        stmt = "INSERT INTO bmsql_new_order ("
                + "    no_o_id, no_d_id, no_w_id) "
                + "VALUES ({0}, {1}, {2})";
        stmt = MessageFormat.format(stmt, String.valueOf(o_id), String.valueOf(newOrder.d_id), String.valueOf(newOrder.w_id));
        sqlList.add(stmt);


        for (int i = 0; i < ol_cnt; i++) {
            int seq = ol_seq[i];

            last_stmt = "stmtNewOrderSelectItem";
            header = "Table[1]:3\n" +
                    "Column[1]:15\n" +
                    "Key[1]:{0}\n";
            header = MessageFormat.format(header, String.valueOf(newOrder.ol_i_id[seq]));
            sqlHeaderList.add(header);

            stmt = "SELECT i_price, i_name, i_data "
                    + "    FROM bmsql_item "
                    + "    WHERE i_id = {0}";
            stmt = MessageFormat.format(stmt, String.valueOf(newOrder.ol_i_id[seq]));
            sqlList.add(stmt);


            // Select STOCK for update.
            last_stmt = "stmtNewOrderSelectStock";
            header = "Table[1]:7\n" +
                    "Column[2]:16,17\n" +
                    "Key[2]:{0},{1}\n";
            header = MessageFormat.format(header, String.valueOf(newOrder.ol_supply_w_id[seq]), String.valueOf(newOrder.ol_i_id[seq]));
            sqlHeaderList.add(header);

            stmt = "SELECT s_quantity, s_data, "
                    + "       s_dist_01, s_dist_02, s_dist_03, s_dist_04, "
                    + "       s_dist_05, s_dist_06, s_dist_07, s_dist_08, "
                    + "       s_dist_09, s_dist_10 "
                    + "    FROM bmsql_stock "
                    + "    WHERE s_w_id = {0} AND s_i_id = {1} "
                    + "    FOR UPDATE";
            stmt = MessageFormat.format(stmt, String.valueOf(newOrder.ol_supply_w_id[seq]), String.valueOf(newOrder.ol_i_id[seq]));
            sqlList.add(stmt);


            last_stmt = "stmtNewOrderUpdateStock";
            header = "Table[1]:7\n" +
                    "Column[2]:16,17\n" +
                    "Key[2]:{0},{1}\n";
            header = MessageFormat.format(header, String.valueOf(newOrder.ol_supply_w_id[seq]), String.valueOf(newOrder.ol_i_id[seq]));
            sqlHeaderList.add(header);

            stmt = "UPDATE bmsql_stock "
                    + "    SET s_quantity = {0}, s_ytd = s_ytd + {1}, "
                    + "        s_order_cnt = s_order_cnt + 1, "
                    + "        s_remote_cnt = s_remote_cnt + {2} "
                    + "    WHERE s_w_id = {3} AND s_i_id = {4}";

            int s_remote_cnt = 0;
            if (newOrder.ol_supply_w_id[seq] != newOrder.w_id) { // 仓库是否相同
                s_remote_cnt = 1;
                remote_warehouse = true;
            }

            stmt = MessageFormat.format(stmt, String.valueOf(newOrder.ol_quantity[seq] + 10), String.valueOf(newOrder.ol_quantity[seq]),
                    String.valueOf(s_remote_cnt), String.valueOf(newOrder.ol_supply_w_id[seq]), String.valueOf(newOrder.ol_i_id[seq]));
            sqlList.add(stmt);


            // Insert the ORDER_LINE row.
            last_stmt = "stmtNewOrderInsertOrderLine";
            stmt = "INSERT INTO bmsql_order_line ("
                    + "    ol_o_id, ol_d_id, ol_w_id, ol_number, "
                    + "    ol_i_id, ol_supply_w_id, ol_quantity, "
                    + "    ol_amount, ol_dist_info) "
                    + "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})";
            stmt = MessageFormat.format(stmt, String.valueOf(o_id), String.valueOf(newOrder.d_id), String.valueOf(newOrder.w_id),
                    String.valueOf(seq + 1), String.valueOf(newOrder.ol_i_id[seq]), String.valueOf(newOrder.ol_supply_w_id[seq]),
                    String.valueOf(newOrder.ol_quantity[seq]), String.valueOf(newOrder.ol_amount[seq]), rnd.getAString_24()); // 随机设置一个dist_info字符串
        }

        newOrder.execution_status = new String("Order placed"); // 订单完成

        // 生成SQL语句
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
