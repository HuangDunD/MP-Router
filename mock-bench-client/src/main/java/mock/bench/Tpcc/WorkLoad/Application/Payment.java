package mock.bench.Tpcc.WorkLoad.Application;

import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.jTPCCTData;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class Payment {
    public static String executePayment(jTPCCTData.PaymentData payment, jTPCCRandom rnd) throws Exception {

        List<String> sqlHeaderList = new ArrayList<>();
        List<String> sqlList = new ArrayList<>();
        String stmt, header;
        StringBuilder sqlBuilder = new StringBuilder();

        long h_date = System.currentTimeMillis();

        String last_stmt = "unknown";
        header = "WareHouse[1]:" + payment.w_id; // 标记主仓库id
        sqlHeaderList.add(header);

        // Update the DISTRICT.
        last_stmt = "stmtPaymentUpdateDistrict";
        header = "Table[1]:1\n" +
                "Column[2]:13,14\n" +
                "Key[2]:{0},{1}\n";
        header = MessageFormat.format(header, String.valueOf(payment.w_id), String.valueOf(payment.d_id));
        sqlHeaderList.add(header);

        stmt = "UPDATE bmsql_district "
                + "    SET d_ytd = d_ytd + {0} "
                + "    WHERE d_w_id = {1} AND d_id = {2}";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.h_amount), String.valueOf(payment.w_id), String.valueOf(payment.d_id));
        sqlList.add(stmt);



        // Select the DISTRICT.
        last_stmt = "stmtPaymentSelectDistrict";
        header = "Table[1]:1\n" +
                "Column[2]:13,14\n" +
                "Key[2]:{0},{1}\n";
        header = MessageFormat.format(header, String.valueOf(payment.w_id), String.valueOf(payment.d_id));
        sqlHeaderList.add(header);

        stmt = "SELECT d_name, d_street_1, d_street_2, d_city, "
                + "       d_state, d_zip "
                + "    FROM bmsql_district "
                + "    WHERE d_w_id = {0} AND d_id = {1}";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.w_id), String.valueOf(payment.d_id));
        sqlList.add(stmt);



        // Update the WAREHOUSE.
        last_stmt = "stmtPaymentUpdateWarehouse";
        header = "Table[1]:8\n" +
                "Column[1]:9\n" +
                "Key[1]:{0}\n";
        header = MessageFormat.format(header, String.valueOf(payment.w_id));
        sqlHeaderList.add(header);

        stmt = "UPDATE bmsql_warehouse "
                + "    SET w_ytd = w_ytd + {0} "
                + "    WHERE w_id = {1}";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.h_amount), String.valueOf(payment.w_id));
        sqlList.add(stmt);


        // Select the WAREHOUSE.
        last_stmt = "stmtPaymentSelectWarehouse";
        header = "Table[1]:8\n" +
                "Column[1]:9\n" +
                "Key[1]:{0}\n";
        header = MessageFormat.format(header, String.valueOf(payment.w_id));
        sqlHeaderList.add(header);


        stmt = "SELECT w_name, w_street_1, w_street_2, w_city, "
                + "       w_state, w_zip "
                + "    FROM bmsql_warehouse "
                + "    WHERE w_id = {0} ";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.w_id));
        sqlList.add(stmt);



        // Select the CUSTOMER.
        last_stmt = "stmtPaymentSelectCustomer";
        header = "Table[1]:0\n" +
                "Column[3]:10,11,12\n" +
                "Key[3]:{0},{1},{2}\n";
        header = MessageFormat.format(header, String.valueOf(payment.c_w_id), String.valueOf(payment.c_d_id), String.valueOf(payment.c_id));
        sqlHeaderList.add(header);

        stmt = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
                + "       c_city, c_state, c_zip, c_phone, c_since, c_credit, "
                + "       c_credit_lim, c_discount, c_balance "
                + "    FROM bmsql_customer "
                + "    WHERE c_w_id = {0} AND c_d_id = {1} AND c_id = {2} "
                + "    FOR UPDATE";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.c_w_id), String.valueOf(payment.c_d_id), String.valueOf(payment.c_id));
        sqlList.add(stmt);



        last_stmt = "stmtPaymentUpdateCustomer";
        header = "Table[1]:0\n" +
                "Column[3]:10,11,12\n" +
                "Key[3]:{0},{1},{2}\n";
        header = MessageFormat.format(header, String.valueOf(payment.c_w_id), String.valueOf(payment.c_d_id), String.valueOf(payment.c_id));
        sqlHeaderList.add(header);

        stmt = "UPDATE bmsql_customer "
                + "    SET c_balance = {0}, "
                + "        c_ytd_payment = c_ytd_payment + {1}, "
                + "        c_payment_cnt = c_payment_cnt + 1 "
                + "    WHERE c_w_id = {2} AND c_d_id = {3} AND c_id = {4}";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.h_amount + 10), String.valueOf(payment.h_amount),
                String.valueOf(payment.c_w_id), String.valueOf(payment.c_d_id), String.valueOf(payment.c_id));
        sqlList.add(stmt);


        // Insert the HISORY row.
        last_stmt = "stmtPaymentInsertHistory";
        stmt = "INSERT INTO bmsql_history ("
                + "    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, "
                + "    h_date, h_amount, h_data) "
                + "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})";
        stmt = MessageFormat.format(stmt, String.valueOf(payment.c_id), String.valueOf(payment.c_d_id), String.valueOf(payment.c_w_id),
                String.valueOf(payment.d_id), String.valueOf(payment.w_id), "'" + String.valueOf(h_date) + "'", String.valueOf(payment.h_amount),
                rnd.getAString_12_24()); // TODO: 随机生成 DONE

        if (payment.w_id != payment.c_w_id) // 判断有没有remote
            header = "Remote[1]:1";
        else
            header = "Remote[1]:0";
        sqlHeaderList.add(header);

        payment.h_date = new Timestamp(h_date).toString();

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
