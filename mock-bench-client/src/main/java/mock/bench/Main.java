package mock.bench;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.FileReader;


import static mock.bench.Client.closeResources;

public class Main {
    private static ScheduledExecutorService scheduler;
    private static int executionCount = 0; // 记录当前执行次数
    private static int maxExecutionsTime; // 最大执行次数
    private static int threadPoolSize; // 线程池大小
    private static int taskIntervalTime; // 任务间隔时间（秒）
    private static int affinityClassNum; // 亲和类数
    private static int affinityClassPartitionNum; // 分区数
    private static int keyCntPerPartition; // 每个分区的 key 数量
    private static int writeCount; // Write 操作的数量
    private static double crossRatio; // Cross Ratio 参数
    private static long startTime; // 任务开始时间

    public static void main(String[] args) {
        // 加载配置文件
        loadConfig();

        // 初始化客户端
        Client.client_init();

        // 初始化调度器
        startTime = System.currentTimeMillis();
        scheduler = Executors.newScheduledThreadPool(threadPoolSize);
        scheduler.scheduleAtFixedRate(Main::workload_simulate, 0, taskIntervalTime, TimeUnit.MILLISECONDS);
        // 提交多个任务到线程池
        for (int i = 0; i < threadPoolSize; i++) {
            scheduler.scheduleAtFixedRate(
                    Main::workload_simulate,     // 任务
                    0,                                                  // 初始延迟
                    taskIntervalTime,                   // 任务间隔
                    TimeUnit.MILLISECONDS     // 时间单位
            );
        }

        // 关闭线程池（可选，通常在程序结束时调用）
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            System.out.println("Shutting down scheduler...");
            scheduler.shutdown();
        }));
    }

    public static void workload_simulate() {
        // 检查是否达到最大执行次数
        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) / 1000 > maxExecutionsTime) {
//            System.out.println(endTime - startTime);
            stopScheduler(); // 停止调度器
            return;
        }

        // 执行任务逻辑
        String ws = "";
        try {
            Workload workload = new Workload(affinityClassNum, affinityClassPartitionNum, keyCntPerPartition);
            ws = workload.generate_write(writeCount, crossRatio, executionCount);
            Client.sendMessage(ws);
            executionCount++;
            System.out.println("Executed " + executionCount + " times.");
        } catch (Exception e) {
            // 处理异常
//            System.err.println("Error during workload simulation: " + e.getMessage());
            // 停止调度器
//            System.err.println("Error during workload simulation: " + e.getMessage());
//            stopScheduler();
//            return;
        }
    }

    // 停止调度器
    private static void stopScheduler() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            System.out.println("Scheduler stopped.");
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
            affinityClassNum = Integer.parseInt(properties.getProperty("affinity_class_num", "8"));
            affinityClassPartitionNum = Integer.parseInt(properties.getProperty("affinity_class_partition_num", "1000"));
            writeCount = Integer.parseInt(properties.getProperty("write_count", "5"));
            crossRatio = Double.parseDouble(properties.getProperty("cross_ratio", "0.2"));
            System.out.println("Configuration loaded successfully.");
        } catch (IOException e) {
            System.err.println("Error loading configuration file: " + e.getMessage());
        }

        // JSON 文件路径
        String filePath = "../config/ycsb_config.json";
        try (FileReader fileReader = new FileReader(filePath)) {
            JsonObject jsonObject = JsonParser.parseReader(fileReader).getAsJsonObject();
            JsonObject ycsbObject = jsonObject.getAsJsonObject("ycsb");
            keyCntPerPartition = ycsbObject.get("key_cnt_per_partition").getAsInt();
            System.out.println("key_cnt_per_partition: " + keyCntPerPartition);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}