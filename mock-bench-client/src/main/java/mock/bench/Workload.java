package mock.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Workload {
    private List<Integer> ycsb_key;
    private List<Affinity_Class> affinity_class;
    private int affinity_class_num = 8;

    public Workload(int affinity_class_num, int affinity_class_partition_num) {
        this.affinity_class_num = affinity_class_num;
        ycsb_key = new ArrayList<>();
        affinity_class = new ArrayList<>();
        for (int i = 0; i < affinity_class_num; i++) { // init affinity_classes
            Affinity_Class ac = new Affinity_Class();
            ac.id = i;
            ac.partition_start = i * affinity_class_partition_num;
            ac.partition_num = affinity_class_partition_num;
            affinity_class.add(ac);
        }
    }

    private void generate_ycsb_key(int num, double cross_ratio) {
        ycsb_key.clear(); // clean the list
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        // 判断当次是否生成跨分区的key
        if (random.nextDouble() < cross_ratio) { // 产生跨亲和类的分区key
            // 跨亲和类的数量
            int cross_num = random.nextInt(affinity_class_num);
            List<Affinity_Class> cross_ac = new ArrayList<>();
            for (int i = 0; i < cross_num; i++) { // 添加跨亲和类
                int ac = random.nextInt(affinity_class_num);
                while(cross_ac.contains(affinity_class.get(ac))) {
                    ac = random.nextInt(affinity_class_num);
                }
                cross_ac.add(affinity_class.get(ac));
            }
            for (int i = 0; i < num; i++) { // 从跨亲和类中随机选取key
                int ac = random.nextInt(cross_num);
                int ac_start = cross_ac.get(ac).partition_start;
                int ac_partition_num = cross_ac.get(ac).partition_num;
                int key = random.nextInt(ac_partition_num) + ac_start;
                ycsb_key.add(key);
            }
        } else { // 产生同一亲和类的分区key
            int ac = random.nextInt(affinity_class_num);
            int ac_start = affinity_class.get(ac).partition_start;
            int ac_partition_num = affinity_class.get(ac).partition_num;
            for (int i = 0; i < num; i++) {
                int key = random.nextInt(ac_partition_num) + ac_start;
                ycsb_key.add(key);
            }
        }
    }

    public String generate_write(int num, double cross_ratio) {
        generate_ycsb_key(num, cross_ratio);
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append("benchbase.usertable\n");
        sb.append("SET FIELD1 = CASE \n");
        for (int i = 0; i < ycsb_key.size(); i++) {
            sb.append("\tWHEN YCSB_KEY = ");
            sb.append(ycsb_key.get(i));
            sb.append(" THEN ");
            sb.append("SUBSTRING(MD5(RAND()), 1, 10)\n");
        }
        sb.append("\tELSE FIELD1\n");
        sb.append("END\n");
        sb.append("WHERE YCSB_KEY IN (");
        for (int i = 0; i < ycsb_key.size(); i++) {
            sb.append(ycsb_key.get(i));
            if (i != ycsb_key.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        return sb.toString();
    }
}

class Affinity_Class{
    int id;
    int partition_start;
    int partition_num;
}
