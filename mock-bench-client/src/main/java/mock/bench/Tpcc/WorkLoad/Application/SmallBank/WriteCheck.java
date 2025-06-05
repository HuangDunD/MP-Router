package mock.bench.Tpcc.WorkLoad.Application.SmallBank;

import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.jTPCCTData;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class WriteCheck {

    public static String executeWriteCheck(jTPCCTData.WriteCheckData writeCheck, jTPCCRandom rnd) throws Exception {

        List<String> sqlHeaderList = new ArrayList<>();
        List<String> sqlList = new ArrayList<>();
        String stmt, header;
        StringBuilder sqlBuilder = new StringBuilder();


        String last_stmt = "unknown";
        header = "WareHouse[1]:" + "0"; // 标记主仓库id
        sqlHeaderList.add(header);

        boolean remote_warehouse = false;
        if (remote_warehouse)
            header = "Remote[1]:1";
        else
            header = "Remote[1]:0";
        sqlHeaderList.add(header + "\n");


        // TODO: 修改custid

        // GetAccount
        last_stmt = "stmtWriteCheckGetAccount";
        header = "Table[1]:28\n" +
                "Column[1]:31\n" +
                "Key[1]:{0}\n";
        header = MessageFormat.format(header, String.valueOf(writeCheck.acctID));
        sqlHeaderList.add(header);

        stmt = "SELECT * "
                + "    FROM accounts "
                + "    WHERE custid = {0}";
        stmt = MessageFormat.format(stmt, String.valueOf(writeCheck.acctID));
        sqlList.add(stmt);


        // GetSavingsBalance
        last_stmt = "stmtWriteCheckGetSavingsBalance";
        header = "Table[1]:30\n" +
                "Column[1]:31\n" +
                "Key[1]:{0}\n";
        header = MessageFormat.format(header, String.valueOf(writeCheck.acctID));
        sqlHeaderList.add(header);

        stmt = "SELECT bal "
                + "    FROM savings "
                + "    WHERE custid = {0}";
        stmt = MessageFormat.format(stmt, String.valueOf(writeCheck.acctID));
        sqlList.add(stmt);


        // GetCheckingBalance
        last_stmt = "stmtWriteCheckGetCheckingBalance";
        header = "Table[1]:29\n" +
                "Column[1]:31\n" +
                "Key[1]:{0}\n";
        header = MessageFormat.format(header, String.valueOf(writeCheck.acctID));
        sqlHeaderList.add(header);

        stmt = "SELECT bal "
                + "    FROM checking "
                + "    WHERE custid = {0}";
        stmt = MessageFormat.format(stmt, String.valueOf(writeCheck.acctID));
        sqlList.add(stmt);


        // UpdateCheckingBalance
        last_stmt = "stmtWriteCheckUpdateCheckingBalance";
        header = "Table[1]:29\n" +
                "Column[1]:31\n" +
                "Key[1]:{0}\n";
        header = MessageFormat.format(header, String.valueOf(writeCheck.acctID));
        sqlHeaderList.add(header);

        // 特化处理改为 +
        stmt = "UPDATE checking "
                + "    SET bal = bal + {0} "
                + "    WHERE custid = {1}";
        stmt = MessageFormat.format(stmt, String.valueOf(writeCheck.updateValue), String.valueOf(writeCheck.acctID));
        sqlList.add(stmt);


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
