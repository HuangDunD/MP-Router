package mock.bench;

import java.sql.*;
import java.util.HashMap;


public class Throughput {
    // 统计吞吐量
    private long total_time = 0; // 总时间
    private long total_count = 0; // 总计数

    public Throughput() {
        // 初始化
        this.total_time = 0;
        this.total_count = 0;
    }

    // 连接数据库执行查询语句，统计吞吐量
    public void throughputCalculate(HashMap<String, String> dbInformation) {
        String host = dbInformation.get("database_host");
        String port = dbInformation.get("database_port");
        String database = dbInformation.get("database_name");
        String user = dbInformation.get("database_user");
        String password = dbInformation.get("database_password");
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        try {
            // 加载 PostgreSQL 驱动类
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            for (Table table : Workload_ddl.tables) {
                String query = "SELECT COUNT(*) AS row_count FROM " + table.tableName + ";";
                long startTime = System.nanoTime();
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(query);
                    // 记录查询结束时间
                    long endTime = System.nanoTime();
                    total_time = endTime - startTime; // 计算总时间
                    if (rs.next()) {
                        long rowCount = rs.getInt("row_count");
                        total_count += rowCount; // 计算总计数
                    }
                    rs.close();
                }
            }
            // 计算吞吐量（单位：row/ms）
            conn.close();
            double throughput = total_count / (total_time / 1_000_000.0); // 每秒查询的行数
            System.out.println("Throughput: " + String.format("%.3f", throughput) + " rows/ms");
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
