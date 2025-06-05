package mock.bench.Tpcc.WorkLoad.Application.Tpcc;

import mock.bench.Tpcc.WorkLoad.jTPCCTData;
import java.text.MessageFormat;

public class StockLevel {
    public static String executeStockLevel(jTPCCTData.StockLevelData stockLevel) throws Exception { // 不使用
        String last_stmt = "unknown";

        String stmt;
        StringBuilder sqlBuilder = new StringBuilder();

        last_stmt = "stmtStockLevelSelectLow";

        stmt = "SELECT count(*) AS low_stock FROM (" //删
                + "    SELECT s_w_id, s_i_id, s_quantity "
                + "        FROM bmsql_stock "
                + "        WHERE s_w_id = {0} AND s_quantity < {1} AND s_i_id IN ("
                + "            SELECT ol_i_id "
                + "                FROM bmsql_district "
                + "                JOIN bmsql_order_line ON ol_w_id = d_w_id "
                + "                 AND ol_d_id = d_id "
                + "                 AND ol_o_id >= d_next_o_id - 20 "
                + "                 AND ol_o_id < d_next_o_id "
                + "                WHERE d_w_id = {2} AND d_id = {3} "
                + "        ) "
                + "    ) AS L";
        stmt = MessageFormat.format(stmt, stockLevel.w_id, stockLevel.threshold, stockLevel.w_id, stockLevel.d_id);

//        stmt.setInt(1, stockLevel.w_id);
//        stmt.setInt(2, stockLevel.threshold);
//        stmt.setInt(3, stockLevel.w_id);
//        stmt.setInt(4, stockLevel.d_id);
//      rs = stmt.executeQuery();
//      if (!rs.next()) {
//        throw new Exception("Failed to get low-stock for" + " W_ID=" + stockLevel.w_id + " D_ID="
//            + stockLevel.d_id);
//      }
//      stockLevel.low_stock = rs.getInt("low_stock");
//      rs.close();

//        dbConn.commit();
        sqlBuilder.append("BEGIN;\n");
        sqlBuilder.append(stmt);
        sqlBuilder.append(";\n");
        sqlBuilder.append("COMMIT;\n");
        return sqlBuilder.toString();
    }
}
