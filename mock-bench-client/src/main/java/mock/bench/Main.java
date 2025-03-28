package mock.bench;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static mock.bench.Client.closeResources;

public class Main {
    private static ScheduledExecutorService scheduler;
    private static int executionCount = 0; // 记录当前执行次数
    private static int maxExecutions; // 最大执行次数
    private static int threadPoolSize; // 线程池大小
    private static int taskIntervalSeconds; // 任务间隔时间（秒）
    private static int affinityClassNum; // Workload 线程数
    private static int affinityClassPartitionNum; // Workload 记录数
    private static int writeCount; // Write 操作的数量
    private static double crossRatio; // Cross Ratio 参数

    public static void main(String[] args) {
        // 加载配置文件
        loadConfig();

        // 初始化客户端
        Client.client_init();

        // 初始化调度器
        scheduler = Executors.newScheduledThreadPool(threadPoolSize);
        scheduler.scheduleAtFixedRate(Main::workload_simulate, 0, taskIntervalSeconds, TimeUnit.SECONDS);
    }

    public static void workload_simulate() {
        // 检查是否达到最大执行次数
        if (executionCount >= maxExecutions) {
            stopScheduler(); // 停止调度器
            return;
        }

        // 执行任务逻辑
        Workload workload = new Workload(affinityClassNum, affinityClassPartitionNum);
        String ws = workload.generate_write(writeCount, crossRatio);
        Client.sendMessage(ws);

        // 增加执行计数
        executionCount++;
        System.out.println("Executed " + executionCount + " times.");
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
            maxExecutions = Integer.parseInt(properties.getProperty("max_executions", "10"));
            threadPoolSize = Integer.parseInt(properties.getProperty("thread_pool_size", "1"));
            taskIntervalSeconds = Integer.parseInt(properties.getProperty("task_interval_seconds", "5"));
            affinityClassNum = Integer.parseInt(properties.getProperty("affinity_class_num", "8"));
            affinityClassPartitionNum = Integer.parseInt(properties.getProperty("affinity_class_partition_num", "1000"));
            writeCount = Integer.parseInt(properties.getProperty("write_count", "5"));
            crossRatio = Double.parseDouble(properties.getProperty("cross_ratio", "0.2"));

            System.out.println("Configuration loaded successfully.");
        } catch (IOException e) {
            System.err.println("Error loading configuration file: " + e.getMessage());
            System.exit(1); // 如果加载失败，退出程序
        }
    }
}