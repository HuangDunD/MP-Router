package mock.bench;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Offline_load {
    private static ScheduledExecutorService scheduler;
    private static int maxExecutionsTimes; // 最大执行次数
    private static int executionCount = 0; // 记录当前执行次数
    private static int threadPoolSize; // 线程池大小
    private static int writeCount; // Write 操作的数量
    private static double crossRatio; // Cross Ratio 参数
    private static Random random_global; // 随机数生成器
    private static double cross_num; // 统计跨亲和类和同一亲和类的数量
    private static HashMap<String, String> dbInformation = new HashMap<>(); // 数据库连接信息
    private static HashMap<String, Integer> ptInformation = new HashMap<>(); // 亲和性类和分区构建信息
    private static double joinRatio;
    private static String filePath;

    // 线程安全的消息队列
    private static final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        // 加载配置文件
        loadConfig();
        Workload_ddl.loadTables(dbInformation, ptInformation);

        // 初始化调度器
        scheduler = Executors.newScheduledThreadPool(threadPoolSize);
        random_global = new Random(System.currentTimeMillis());
        cross_num = 0;

        // 清空原文件内容
        clearFileContent();

        // 启动消息存储线程，用于写入文件
        startFileWriterThread();

        // 提交多个任务到线程池
        for (int i = 0; i < threadPoolSize; i++) {
            scheduler.scheduleAtFixedRate(
                    Offline_load::workloadSimulate, // 任务
                    0,                              // 初始延迟
                    5,               // 任务间隔
                    TimeUnit.MILLISECONDS           // 时间单位
            );
        }

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stopScheduler(); // 停止调度器
            stopFileWriterThread(); // 停止文件写入线程
        }));
    }

    private static AtomicBoolean isRunning = new AtomicBoolean(true); // 使用 AtomicBoolean

    public static void workloadSimulate() {
        // 检查是否达到最大执行次数
        if (executionCount >= maxExecutionsTimes) {
            if (isRunning.compareAndSet(true, false)) { // CAS 操作，确保只执行一次
                System.out.println("Total executed " + executionCount + " times, Cross Ratio: " +
                        String.format("%.3f", cross_num / executionCount));
            }
            stopScheduler(); // 停止调度器
            return;
        }

        // 执行任务逻辑
        try {
            Workload workload = new Workload();
            String writeMessage = workload.generate_write(writeCount, crossRatio, executionCount, random_global);
            saveMessageToFile(writeMessage); // 将写操作消息保存到队列中
            executionCount++;

            String readMessage = workload.generate_read(writeCount, crossRatio, executionCount, random_global, joinRatio);
            saveMessageToFile(readMessage); // 将读操作消息保存到队列中
            cross_num += workload.cross_num;
            executionCount++;

            if (executionCount % 10000 == 0) {
                System.out.println("Executed " + executionCount + " times, Cross Ratio: " +
                        String.format("%.3f", cross_num / executionCount));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 停止调度器
    private static void stopScheduler() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }

    /**
     * 将消息放入队列中，供消息存储线程写入文件
     */
    private static void saveMessageToFile(String message) {
        try {
            messageQueue.put(message); // 将消息放入队列
        } catch (InterruptedException e) {
            System.err.println("Failed to enqueue message: " + e.getMessage());
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
    }

    private static void startFileWriterThread() {
        Thread writerThread = new Thread(() -> {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                while (!Thread.currentThread().isInterrupted()) {
                    // 从队列中取出消息（如果队列为空，则阻塞等待）
                    String message = messageQueue.take();
                    writer.write(message);
                    writer.newLine(); // 每条消息占一行
                    writer.newLine();
                    writer.flush();   // 确保消息立即写入文件
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("File writing failed: " + e.getMessage());
            }
        });
        writerThread.setDaemon(true); // 设置为守护线程
        writerThread.start();
    }

    private static void stopFileWriterThread() {
        // 中断消息存储线程
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().equals("Thread")) { // 根据需要调整线程名称
                thread.interrupt();
            }
        }
    }

    private static void clearFileContent() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, false))) {
            writer.write(""); // 写入空字符串以清空文件
        } catch (IOException e) {
            System.err.println("Failed to clear file content: " + e.getMessage());
        }
    }

    // 加载配置文件
    private static void loadConfig() {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream("config.properties")) {
            // 加载配置文件
            properties.load(input);
            // 读取参数
            maxExecutionsTimes = Integer.parseInt(properties.getProperty("max_executions_times", "1000000"));
            filePath = properties.getProperty("offline_file_path", "../offline_workload.txt");
            threadPoolSize = Integer.parseInt(properties.getProperty("thread_pool_size", "1"));
            writeCount = Integer.parseInt(properties.getProperty("write_count", "5"));
            crossRatio = Double.parseDouble(properties.getProperty("cross_ratio", "0.2"));
            joinRatio = Double.parseDouble(properties.getProperty("join_ratio", "0.2"));
            // 亲和性类和分区构建信息
            ptInformation.put("affinity_class_num", Integer.parseInt(properties.getProperty("affinity_class_num", "8")));
            ptInformation.put("affinity_class_partition_num", Integer.parseInt(properties.getProperty("affinity_class_partition_num", "1000")));
            // 数据库连接信息
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