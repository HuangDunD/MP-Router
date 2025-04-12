package mock.bench;

import java.security.SecureRandom;
import java.util.*;

public class Workload {

    private final SecureRandom random;
    public int cross_num = 0;

    public Workload() {
        random = new SecureRandom();
    }

    private Meta_Data generate_write_key(int num, double cross_ratio, int execution_count, Random random_global) {
        // 从Workload_DDL中随机获取Table
        List<Integer> table_idx = new ArrayList<>();
        for (int i = 0; i < Workload_DDL.tables.size(); i++)
            table_idx.add(i);
        Collections.shuffle(table_idx);
        int shuffle_list_idx = 0;
        Table table = Workload_DDL.tables.get(table_idx.get(shuffle_list_idx));
        while (table.rowCount < 8000) { // 表的行数小于8000的不要点写
            shuffle_list_idx++;
            table = Workload_DDL.tables.get(table_idx.get(shuffle_list_idx));
        }
        Meta_Data meta_data = new Meta_Data(Workload_DDL.workload_type, table.tableName, table.write_column, table.partition_column);
        random.setSeed(System.currentTimeMillis() + execution_count); // 设置随机数种子，避免重复
        // 判断当次是否生成跨分区的key
        if (random_global.nextDouble() % 1 < cross_ratio) { // 产生跨亲和类的分区key
            cross_num++;
            // 跨亲和类的数量
            int cross_num = random.nextInt(table.affinity_class_num - 2) + 2;
            List<Affinity_Class> cross_ac = new ArrayList<>();

            for (int i = 0; i < cross_num; i++) { // 添加跨亲和类
                int ac_id = random.nextInt(table.affinity_class_num);
                while (cross_ac.contains(table.affinity_classes.get(ac_id)))
                    ac_id  = random.nextInt(table.affinity_class_num);
                cross_ac.add(table.affinity_classes.get(ac_id));
            }

            for (int i = 0; i < num; i++) { // 从跨亲和类中随机选取key
                int ac_id = random.nextInt(cross_num);
                Affinity_Class ac = cross_ac.get(ac_id);
                int key_start_idx = random.nextInt(ac.partition_num);  // 随机选择分区中的key
                int ac_start = ac.key_start_list.get(key_start_idx);
                int ac_key_num = ac.key_num_list.get(key_start_idx);
                int key = ac_start + random.nextInt(ac_key_num);
                meta_data.partition_key.add(key);
            }
        } else { // 产生同一亲和类的分区key
            int ac_id = random.nextInt(table.affinity_class_num);
            Affinity_Class ac = table.affinity_classes.get(ac_id);
            for (int i = 0; i < num; i++) {
                int key_start_idx = random.nextInt(ac.partition_num);
                int ac_start = ac.key_start_list.get(key_start_idx);
                int ac_key_num = ac.key_num_list.get(key_start_idx);
                int key = ac_start + random.nextInt(ac_key_num);
                meta_data.partition_key.add(key);
            }
        }
        return meta_data;
    }

    public String generate_write(int num, double cross_ratio, int execution_count, Random random_global) {
        Meta_Data meta_data = generate_write_key(num, cross_ratio, execution_count, random_global);
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(meta_data.db_table_name());
        sb.append("\n");
        sb.append("SET ");
        sb.append(meta_data.write_column);
        sb.append(" = CASE \n");
        for (Integer integer : meta_data.partition_key) {
            sb.append("\tWHEN ");
            sb.append(meta_data.partition_column);
            sb.append(" = ");
            sb.append(integer);
            sb.append(" THEN ");
            sb.append("SUBSTRING(MD5(RAND()), 1, 10)\n");
        }
        sb.append("\tELSE ");
        sb.append(meta_data.write_column);
        sb.append(" END\n");
        sb.append("WHERE ");
        sb.append(meta_data.partition_column);
        sb.append(" IN (");
        for (int i = 0; i < meta_data.partition_key.size(); i++) {
            sb.append(meta_data.partition_key.get(i));
            if (i != meta_data.partition_key.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        return sb.toString();
    }

    private boolean type_equal(Column c1, Column c2) {
//        if (c1.IsDouble() && c2.IsDouble()) return true;
        if (c1.IsInt() && c2.IsInt()) return true;
        return false;
    }

    private Meta_Data generate_read_key(int num, double cross_ratio, int execution_count, Random random_global, boolean is_join) {
        // 从Workload_DDL中随机获取Table
        List<Integer> table_idx = new ArrayList<>();
        for (int i = 0; i < Workload_DDL.tables.size(); i++)  table_idx.add(i);
        Collections.shuffle(table_idx);
        int shuffle_list_idx = 0;
        Table table = Workload_DDL.tables.get(table_idx.get(shuffle_list_idx));
        while (table.rowCount < 8000 && shuffle_list_idx < Workload_DDL.tables.size()) { // 表的行数小于8000的不要点读
            shuffle_list_idx++;
            table = Workload_DDL.tables.get(table_idx.get(shuffle_list_idx));
        }
        Meta_Data meta_data = new Meta_Data(Workload_DDL.workload_type, table.tableName, table.partition_column);
        random.setSeed(System.currentTimeMillis() + execution_count); // 设置随机数种子，避免重复
        if (!is_join){
            // 判断当次是否生成跨分区的key
            if (random_global.nextDouble() % 1 < cross_ratio) { // 产生跨亲和类的分区key
                cross_num++;
                // 跨亲和类的数量
                int cross_num = random.nextInt(table.affinity_class_num - 2) + 2;
                List<Affinity_Class> cross_ac = new ArrayList<>();

                for (int i = 0; i < cross_num; i++) { // 添加跨亲和类
                    int ac_id = random.nextInt(table.affinity_class_num);
                    while (cross_ac.contains(table.affinity_classes.get(ac_id)))
                        ac_id  = random.nextInt(table.affinity_class_num);
                    cross_ac.add(table.affinity_classes.get(ac_id));
                }

                for (int i = 0; i < num; i++) { // 从跨亲和类中随机选取key
                    int ac_id = random.nextInt(cross_num);
                    Affinity_Class ac = cross_ac.get(ac_id);
                    int key_start_idx = random.nextInt(ac.partition_num);  // 随机选择分区中的key
                    int ac_start = ac.key_start_list.get(key_start_idx);
                    int ac_key_num = ac.key_num_list.get(key_start_idx);
                    int key = ac_start + random.nextInt(ac_key_num);
                    meta_data.partition_key.add(key);
                }
            } else { // 产生同一亲和类的分区key
                int ac_id = random.nextInt(table.affinity_class_num);
                Affinity_Class ac = table.affinity_classes.get(ac_id);
                for (int i = 0; i < num; i++) {
                    int key_start_idx = random.nextInt(ac.partition_num);
                    int ac_start = ac.key_start_list.get(key_start_idx);
                    int ac_key_num = ac.key_num_list.get(key_start_idx);
                    int key = ac_start + random.nextInt(ac_key_num);
                    meta_data.partition_key.add(key);
                }
            }
            // 添加读列 只要1列
//            int column_idx = random.nextInt(table.columns.size());
//            String column = table.tableName + "." + table.columns.get(column_idx).columnName;
//            while (meta_data.read_columns.contains(column) || column.equals(meta_data.partition_column)) { // 过滤相同列和分区列
//                column_idx = random.nextInt(table.columns.size());
//                column = table.tableName + "." + table.columns.get(column_idx).columnName;
//            }
//            meta_data.read_columns.add(column);
        } else  {
            Join_Table join_table = new Join_Table(Workload_DDL.workload_type, table.tableName);
            // 遍历左表所有列，查看是否有其他表的列的属性与左表的列相同
            List<Integer> table_idx1 = new ArrayList<>();             // 生成表遍历序列并打乱
            for (int i = 0; i < Workload_DDL.tables.size(); i++)
                table_idx1.add(i);
            Collections.shuffle(table_idx1);
            // 遍历所有表
            for (int i = 0; i < Workload_DDL.tables.size(); i++) {
                Table table1 = Workload_DDL.tables.get(table_idx1.get(i)); // 每次随机选择一张表
                if (table1.tableName.equals(table.tableName)) continue; // 跳过自身JOIN

                for (int j = 0; j < table.columns.size(); j++) { // 遍历左表的列
                    Column column = table.columns.get(j);
                    if (column.columnName.equals(meta_data.partition_column)) continue; // 跳过分区列

                    for (int k = 0; k < table1.columns.size(); k++) {
                        Column column1 = table1.columns.get(k);
                        if (column1.columnName.equals(table1.partition_column)) continue; // 跳过分区列

                        if (type_equal(column, column1)) {
                            join_table.right_table_name = table1.tableName;
                            join_table.left_table_column = table.tableName + "." + column.columnName;
                            join_table.right_table_column = table1.tableName + "." + column1.columnName;
                            // 从两张表中随机选择查询列，只选择1列
//                            for (int l = 0; l < table.columns.size() / 2; l++) {

//                            int column_idx = random.nextInt(table.columns.size());
//                            String column_str = table.tableName + "." + table.columns.get(column_idx).columnName;
//                            while (meta_data.read_columns.contains(column_str) || column_str.equals(meta_data.partition_column)) { // 过滤相同列和分区列
//                                column_idx = random.nextInt(table.columns.size());
//                                column_str = table.tableName + "." + table.columns.get(column_idx).columnName;
//                            }
//                            meta_data.read_columns.add(column_str);
//                            break;

//                            }
//                            for (int l = 0; l < table1.columns.size() / 2; l++) {
//                                int column_idx = random.nextInt(table1.columns.size());
//                                String column1_str = table1.tableName + "." + table1.columns.get(column_idx).columnName;
//                                while (meta_data.read_columns.contains(column1_str) || column1_str.equals(meta_data.partition_column)) { // 过滤相同列和分区列
//                                    column_idx = random.nextInt(table1.columns.size());
//                                    column1_str = table1.tableName + "." + table1.columns.get(column_idx).columnName;
//                                }
//                                meta_data.read_columns.add(column1_str);
//                            }
                        }
                    }
                }
            }
            meta_data.join_table = join_table;
        }
        return meta_data;
    }

    public String generate_read(int num, double cross_ratio, int execution_count, Random random_global) {
        boolean is_join = random_global.nextDouble() % 1.0 < 0.3; // 50%概率选择JOIN
        Meta_Data meta_data = generate_read_key(num, cross_ratio, execution_count, random_global, is_join);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*)");
//        for (int i = 0; i < meta_data.read_columns.size(); i++) {
//            sb.append(" COUNT(");
//            sb.append(meta_data.read_columns.get(i));
//            sb.append(")");
//            if (i != meta_data.read_columns.size() - 1) {
//                sb.append(", ");
//            }
//        }
//        sb.append("\n");
        sb.append(" FROM ");
        if (!is_join) { // 只读一张表
            sb.append(meta_data.db_table_name());
            sb.append("\n");
            sb.append("WHERE ");
            sb.append(meta_data.partition_column);
            sb.append(" IN (");
            for (int i = 0; i < meta_data.partition_key.size(); i++) {
                sb.append(meta_data.partition_key.get(i));
                if (i != meta_data.partition_key.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(");");
        } else {
            sb.append(meta_data.join_table.left_table_name());
            sb.append(" JOIN ");
            sb.append(meta_data.join_table.right_table_name());
            sb.append(" ON ");
            sb.append(meta_data.join_table.left_table_column_name());
            sb.append(" = ");
            sb.append(meta_data.join_table.right_table_column_name());
//            sb.append("WHERE "); // 写啥
            sb.append(";");
        }

        return sb.toString();
    }
}

class Meta_Data {
    String database_name;
    String table_name;
    String write_column;
    String partition_column;
    List<Integer> partition_key;
    List<String> read_columns;
    Join_Table join_table;

    public Meta_Data(String database_name, String table_name, String partition_column) { // for read
        this.database_name = database_name;
        this.table_name = table_name;
        this.partition_column = table_name + "." + partition_column; // 读的时候分区列加表名
        this.partition_key = new ArrayList<>();
        this.read_columns = new ArrayList<>();
    }

    public Meta_Data(String database_name, String table_name, String write_column, String partition_column) { // for write
        this.database_name = database_name;
        this.table_name = table_name;
        this.write_column = write_column;
        this.partition_column = partition_column;
        this.partition_key = new ArrayList<>();
    }

    public String db_table_name(){
        return database_name + "." + table_name;
    }
}

class Join_Table{
    String database_name;
    String left_table_name;
    String right_table_name;
    String left_table_column;
    String right_table_column;

    public Join_Table(String database_name, String left_table_name){
        this.database_name = database_name;
        this.left_table_name = left_table_name;
    }

    public String left_table_name(){
        return database_name + "." + left_table_name;
    }
    public String right_table_name(){
        return database_name + "." + right_table_name;
    }
    public String left_table_column_name(){
        return database_name + "." + left_table_column;
    }
    public String right_table_column_name(){
        return database_name + "." + right_table_column;
    }
}