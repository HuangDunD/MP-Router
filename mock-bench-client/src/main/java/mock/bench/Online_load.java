package mock.bench;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


import static mock.bench.Client.closeResources;

public class Online_load {
    private static ScheduledExecutorService scheduler;
    private static int executionCount = 0; // 记录当前执行次数
    private static int maxExecutionsTime; // 最大执行次数
    private static int threadPoolSize; // 线程池大小
    private static int taskIntervalTime; // 任务间隔时间（秒）
    private static int writeCount; // Write 操作的数量
    private static double crossRatio; // Cross Ratio 参数
    private static long startTime; // 任务开始时间
    private static Random random_global; // 随机数生成器
    private static double cross_num; // 统计跨亲和类和同一亲和类的数量
    private static HashMap<String, String> dbInformation = new HashMap<>(); // 数据库连接信息
    private static HashMap<String, Integer> ptInformation = new HashMap<>(); // 亲和性类和分区构建信息
    private static double joinRatio;

    public static void main(String[] args) {
        // // 加载配置文件 TODO
         loadConfig();

        // 获取Tpch DDL
        Workload_ddl.loadTables(dbInformation, ptInformation);
//        System.out.println("Tables: " + Workload_ddl.tables);


         // 初始化客户端
         Client.client_init();

         // 初始化调度器
         startTime = System.currentTimeMillis();
         scheduler = Executors.newScheduledThreadPool(threadPoolSize);
         random_global = new Random(startTime);
         cross_num = 0;

         // 提交多个任务到线程池
         for (int i = 0; i < threadPoolSize; i++) {
             scheduler.scheduleAtFixedRate(
                     Online_load::workloadSimulate,     // 任务
                     0,                                                  // 初始延迟
                     taskIntervalTime,                   // 任务间隔
                     TimeUnit.MILLISECONDS     // 时间单位
             );
         }

         if (dbInformation.get("connect_to_database").equals("true"))
             scheduler.scheduleAtFixedRate(
                        Online_load::throughputSatistic,     // 任务
                        0,                                                  // 初始延迟
                        10,                   // 任务间隔
                        TimeUnit.SECONDS     // 时间单位
             );

         // 添加关闭钩子
         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
             scheduler.shutdown();
         }));
    }

    private static AtomicBoolean isRunning = new AtomicBoolean(true); // 使用 AtomicBoolean

    public static void workloadSimulate() {
        // 检查是否达到最大执行时间
        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) / 1000 > maxExecutionsTime) {
            if (isRunning.compareAndSet(true, false)) // CAS 操作，确保只执行一次
                System.out.println("Total executed " + executionCount + " times, Cross Ratio: " + String.format("%.3f", cross_num / executionCount));
            stopScheduler(); // 停止调度器
            return;
        }

        // 执行任务逻辑
        String message = "";
        try {
            Workload workload = new Workload();
            message = workload.generate_write(writeCount, crossRatio, executionCount, random_global);
            Client.sendMessage(message);
            executionCount++;
            message = workload.generate_read(writeCount, crossRatio, executionCount, random_global, joinRatio);
            Client.sendMessage(message);
            cross_num += workload.cross_num;
            executionCount++;
            if (executionCount % 10000 == 0)
                System.out.println("Executed " + executionCount + " times, Cross Ratio: " + String.format("%.3f", (double) cross_num / executionCount));
        } catch (Exception e) {}
    }

    public static void throughputSatistic() {
        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) / 1000 > maxExecutionsTime) {
            stopScheduler(); // 停止调度器
            return;
        }
        // 执行任务逻辑
        Throughput throughput = new Throughput();
        throughput.throughputCalculate(dbInformation);
    }

    // 停止调度器
    private static void stopScheduler() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }
        closeResources();
    }

     //加载配置文件
    private static void loadConfig() {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream("config.properties")) {
            // 加载配置文件
            properties.load(input);
            // 读取参数
            maxExecutionsTime = Integer.parseInt(properties.getProperty("max_executions_time", "1200"));
            threadPoolSize = Integer.parseInt(properties.getProperty("thread_pool_size", "1"));
            taskIntervalTime = Integer.parseInt(properties.getProperty("task_interval_time", "5"));
            writeCount = Integer.parseInt(properties.getProperty("write_count", "5"));
            crossRatio = Double.parseDouble(properties.getProperty("cross_ratio", "0.2"));
            joinRatio = Double.parseDouble(properties.getProperty("join_ratio", "0.2"));
            // 亲和性类和分区构建信息
            ptInformation.put("affinity_class_num", Integer.parseInt(properties.getProperty("affinity_class_num", "8")));
            ptInformation.put("affinity_class_partition_num", Integer.parseInt(properties.getProperty("affinity_class_partition_num", "1000")));
            // 数据库连接信息
            dbInformation.put("connect_to_database", properties.getProperty("connect_to_database", "false"));
            dbInformation.put("workload_type", properties.getProperty("workload_type"));
            dbInformation.put("database_host", properties.getProperty("database_host"));
            dbInformation.put("database_port", properties.getProperty("database_port"));
            dbInformation.put("database_name", properties.getProperty("database_name"));
            dbInformation.put("database_user", properties.getProperty("database_user"));
            dbInformation.put("database_password", properties.getProperty("database_password"));
            System.out.println("Configuration loaded successfully.");
        } catch (IOException e) {
            System.err.println("Error loading configuration file: " + e.getMessage());
        }

        // JSON 文件路径
        String filePath = "../config/ycsb_config.json";
        try (FileReader fileReader = new FileReader(filePath)) {
            JsonObject jsonObject = JsonParser.parseReader(fileReader).getAsJsonObject();
            JsonObject ycsbObject = jsonObject.getAsJsonObject("ycsb");
            ptInformation.put("key_cnt_per_partition", ycsbObject.get("key_cnt_per_partition").getAsInt());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}