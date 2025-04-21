package mock.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.sql.*;
import java.util.regex.*;

import static java.lang.Integer.min;

class Affinity_Class{
    int partition_num; // 分区数
    List<Integer> key_start_list; // 每个分区的起始key
    List<Integer> key_num_list; // 每个分区的key数量
    public Affinity_Class(int start_key, int end_key, int key_partition_num) {
        this.key_start_list = new ArrayList<>();
        this.key_num_list = new ArrayList<>();
        for (int i = start_key; i < end_key; i += key_partition_num) {
            this.key_start_list.add(i);
            this.key_num_list.add(min(key_partition_num, end_key - i));
        }
        this.partition_num = key_start_list.size();
    }
}

class Table {
    String tableName;
    int rowCount;
    List<Column> columns;
    // for partition
    int affinity_class_num;
    List<Affinity_Class> affinity_classes;
    String write_column;
    String partition_column;

    public Table(String tableName) {
        this.rowCount = 0;
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        this.affinity_classes = new ArrayList<>();
    }

    public Table(String tableName, int rowCount) {
        this.rowCount = rowCount;
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        this.affinity_classes = new ArrayList<>();
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public void initWriteAndPartitionColumns() { // 初始化写入列和分区列
        List<Column> candidate_write_columns = new ArrayList<>();
        for (Column column : columns) {
            if (column.columnType.contains("VARCHAR")) {
                candidate_write_columns.add(column);
            }
        }
        this.write_column = candidate_write_columns.get(new Random().nextInt(candidate_write_columns.size())).columnName;
        switch (tableName.toLowerCase()) {
            case "lineitem": this.partition_column = "l_orderkey"; break;
            case "orders": this.partition_column = "o_orderkey"; break;
            case "customer": this.partition_column = "c_custkey"; break;
            case "nation": this.partition_column = "n_nationkey"; break;
            case "region": this.partition_column = "r_regionkey"; break;
            case "part": this.partition_column = "p_partkey"; break;
            case "partsupp": this.partition_column = "ps_partkey"; break;
            case "supplier": this.partition_column = "s_suppkey"; break;
            case "ycsb_table": this.partition_column = "YCSB_KEY"; break;
            default: throw new IllegalStateException("Unexpected value: " + tableName);
        }
    }

    public void initAffinityClass(HashMap<String, Integer> ptInformation){  // 初始化亲和性类
        Integer affinity_class_num = ptInformation.get("affinity_class_num");
        Integer key_cnt_per_partition = ptInformation.get("key_cnt_per_partition");
        int affinity_class_max_key_num = (rowCount / affinity_class_num / 1000) * 1000; // 向下取整到1000
        if (affinity_class_max_key_num == 0) {
            affinity_class_max_key_num = 1000;
        }
        this.affinity_class_num = affinity_class_num;
        for (int i = 0; i < rowCount; i += affinity_class_max_key_num) { // init affinity_classes
            affinity_classes.add(new Affinity_Class(i, min(i + affinity_class_max_key_num, rowCount), key_cnt_per_partition));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(tableName).append(" ").append(rowCount).append(" Rows").append("\n");
        for (Column column : columns) {
            sb.append("  ").append(column).append("\n");
        }
        // 输出亲和性类信息
        sb.append("Affinity Classes: ").append(affinity_class_num);
//        for (int i = 0; i < affinity_classes.size(); i++) {
//            Affinity_Class ac = affinity_classes.get(i);
//            sb.append("\n   Affinity Class ").append(i).append(": ");
//            sb.append("Partition Num: ").append(ac.partition_num);
//            sb.append("Key Start List: ").append(ac.key_start_list).append("\n");
//            sb.append("Key Num List: ").append(ac.key_num_list).append("\n");
//        }
        sb.append("\n");
        return sb.toString();
    }
}

class Column {
    String columnName;
    String columnType;

    public Column(String columnName, String columnType) {
        this.columnName = columnName;
        this.columnType = columnType.toUpperCase();
    }

    public boolean IsInt(){
        return columnType.contains("INT") || columnType.contains("INTEGER") || columnType.contains("SMALLINT") || columnType.contains("BIGINT");
    }

    @Override
    public String toString() {
        return columnName + " (" + columnType + ")";
    }
}


public class Workload_ddl {
    static String sql = "";
    static String workload_type = "";
    static List<Table> tables = new ArrayList<>();

    private static String loadDDL(){
        try {
            String filePath = "tpch_ddl/table_ddl.sql"; // 文件路径
            sql = Files.readString(Path.of(filePath)); // 读取文件内容为字符串
        } catch (IOException e) {
            System.err.println("Error loading ddl file: " + e.getMessage());
        }
        return sql;
    }

    private static void fetchTableRowSize(HashMap<String, String> dbInformation) {
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
            for (Table table : tables) {
                String query = "SELECT COUNT(*) AS row_count FROM " + table.tableName + ";";
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(query);
                    if (rs.next()) {
                        table.rowCount = rs.getInt("row_count");
                    }
                    rs.close();
                }
            }
            conn.close();
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void fetchTableRowSizeOffline() {
        for (Table table : tables) {
            String table_name = table.tableName.toLowerCase();
            if (table_name.equals("lineitem")) {
                table.rowCount = 6001215;
            } else if (table_name.equals("orders")) {
                table.rowCount = 1500000;
            } else if (table_name.equals("customer")) {
                table.rowCount = 150000;
            } else if (table_name.equals("nation")) {
                table.rowCount = 25;
            } else if (table_name.equals("region")) {
                table.rowCount = 5;
            } else if (table_name.equals("part")) {
                table.rowCount = 200000;
            } else if (table_name.equals("partsupp")) {
                table.rowCount = 800000;
            } else if (table_name.equals("supplier")) {
                table.rowCount = 10000;
            }
        }
    }

    private static void initTables(HashMap<String, Integer> ptInformation) {
        for (Table table : tables) {
            table.initWriteAndPartitionColumns();
            table.initAffinityClass(ptInformation);
        }
    }

    public static void loadTables(HashMap<String, String> dbInformation, HashMap<String, Integer> ptInformation) {
        if (dbInformation.get("workload_type").equals("tpch")){
            workload_type = "tpch";
            loadDDL();
            Pattern tablePattern = Pattern.compile("CREATE TABLE (\\w+)\\s*\\((.*?)\\)\\s*DISTRIBUTED (.*?);", Pattern.DOTALL);
            Matcher tableMatcher = tablePattern.matcher(sql);
            while (tableMatcher.find()) {
                String tableName = tableMatcher.group(1);
                String tableBody = tableMatcher.group(2);
                Table table = new Table(tableName);
                Pattern columnPattern = Pattern.compile("(\\w+)\\s+(\\S+)(?:\\s+NOT NULL)?", Pattern.DOTALL);   // 匹配列定义
                Matcher columnMatcher = columnPattern.matcher(tableBody);
                while (columnMatcher.find()) {
                    String columnName = columnMatcher.group(1);
                    String columnType = columnMatcher.group(2);
                    table.addColumn(new Column(columnName, columnType));
                }
                tables.add(table);
            }
            if (dbInformation.get("connect_to_database").equals("true"))
                fetchTableRowSize(dbInformation);             // 获取每个表的行数
            else
                fetchTableRowSizeOffline();
            initTables(ptInformation);       // 初始化每个表的亲和性类
        } else if (dbInformation.get("workload_type").equals("ycsb")) { // TODO: 完善ycsb
            workload_type = "ycsb";
            tables.add(new Table("ycsb_table", 80000));
            tables.get(0).addColumn(new Column("YCSB_KEY", "BIGINT"));
            tables.get(0).addColumn(new Column("FIELD1", "VARCHAR(100)"));
            initTables(ptInformation);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Table table : tables) {
            sb.append(table).append("\n");
        }
        return sb.toString();
    }
}