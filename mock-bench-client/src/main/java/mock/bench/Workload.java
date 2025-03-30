package mock.bench;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Workload {
    private final List<Integer> ycsb_key;
    private final List<Affinity_Class> affinity_class;
    private final int affinity_class_num;
    private final Random random;
    public int cross_num = 0;

    public Workload(int affinity_class_num, int affinity_class_partition_num, int key_cnt_per_partition) {
        this.affinity_class_num = affinity_class_num;
        ycsb_key = new ArrayList<>();
        affinity_class = new ArrayList<>();
        random = new Random();
        for (int i = 0; i < affinity_class_num; i++) { // init affinity_classes
            Affinity_Class ac = new Affinity_Class();
            ac.id = i;
            ac.partition_num = affinity_class_partition_num;
            ac.key_num = key_cnt_per_partition;
            ac.key_start_list = new ArrayList<>();
            for (int j = 0; j < affinity_class_partition_num; j++) { // init key_start_list
                ac.key_start_list.add(( (i * affinity_class_partition_num + j) * key_cnt_per_partition));
            }
            affinity_class.add(ac);
        }
    }

    private void generate_ycsb_key(int num, double cross_ratio, int execution_count, Random random_double) {
        ycsb_key.clear(); // clean the list
        // Random random = new Random();
        random.setSeed(System.currentTimeMillis() + execution_count); // 设置随机数种子，避免重复
        // 判断当次是否生成跨分区的key
        if (random_double.nextDouble() % 1 < cross_ratio) { // 产生跨亲和类的分区key
            cross_num++;
            // 跨亲和类的数量
            int cross_num = random.nextInt(affinity_class_num - 2) + 2;
            List<Affinity_Class> cross_ac = new ArrayList<>();

            for (int i = 0; i < cross_num; i++) { // 添加跨亲和类
                int ac_id = random.nextInt(affinity_class_num);
                while(cross_ac.contains(affinity_class.get(ac_id ))) {
                    ac_id  = random.nextInt(affinity_class_num);
                }
                cross_ac.add(affinity_class.get(ac_id));
            }

            for (int i = 0; i < num; i++) { // 从跨亲和类中随机选取key
                int ac_id = random.nextInt(cross_num);
                // 随机选择分区中的key
                Affinity_Class ac = affinity_class.get(ac_id);
                int ac_start = ac.key_start_list.get(random.nextInt(ac.partition_num)).intValue();
                int ac_key_num = cross_ac.get(ac_id).key_num;
                int key = random.nextInt(ac_key_num) + ac_start;
                ycsb_key.add(key);
            }

            // // 打印跨亲和类的列表
            // synchronized (System.out) {
            //     System.out.print("[");
            //     for (Affinity_Class ac : cross_ac) {
            //         System.out.print(ac.id + ", ");   
            //     }
            //     System.out.print("] -> keys: ");
            //     System.out.print("[");
            //     for (Integer integer : ycsb_key) {
            //         System.out.print(integer + ", ");
            //     }
            //     System.out.println("]");
            // }

        } else { // 产生同一亲和类的分区key
            int ac_id = random.nextInt(affinity_class_num);
            Affinity_Class ac = affinity_class.get(ac_id);
            for (int i = 0; i < num; i++) {
                int ac_start = ac.key_start_list.get(random.nextInt(ac.partition_num)).intValue();
                int ac_key_num = affinity_class.get(ac_id).key_num;
                int key = random.nextInt(ac_key_num) + ac_start;
                ycsb_key.add(key);
            }

            // // 打印亲和类的id和key
            // synchronized (System.out) {
            //     System.out.print("[" + ac_id + "] -> keys: ");
            //     System.out.print("[");
            //     for (Integer integer : ycsb_key) {
            //         System.out.print(integer + ", ");
            //     }
            //     System.out.println("]");
            // }
        }
    }

    public String generate_write(int num, double cross_ratio, int execution_count, Random random_double) {
        generate_ycsb_key(num, cross_ratio, execution_count, random_double);
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append("benchbase.usertable\n");
        sb.append("SET FIELD1 = CASE \n");
        for (Integer integer : ycsb_key) {
            sb.append("\tWHEN YCSB_KEY = ");
            sb.append(integer);
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
    int partition_num;
    int key_num;
    List<Integer> key_start_list;
}
