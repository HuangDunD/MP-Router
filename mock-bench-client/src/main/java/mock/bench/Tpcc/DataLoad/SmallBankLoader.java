package mock.bench.Tpcc.DataLoad;

import mock.bench.Tpcc.Tool.jTPCCRandom;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class SmallBankLoader {

    private static int NUM_ACCOUNTS = 10000; // Number of accounts to generate
    private static int NUM_THREADS = 8; // Number of threads to use
    private static String JDBC_URL = "";
    private static String JDBC_USER = "";
    private static String JDBC_PASSWORD = "";
    private static final Logger logger = Logger.getLogger(SmallBankLoader.class.getName());

    public static void main(String[] args) {
        // 加载配置文件
        loadConfig();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        int accountsPerThread = NUM_ACCOUNTS / NUM_THREADS;

        CountDownLatch latch = new CountDownLatch(NUM_THREADS); // 控制线程同步
        jTPCCRandom random = new jTPCCRandom();

        for (int t = 0; t < NUM_THREADS; t++) {
            final int startId = t * accountsPerThread + 1;
            final int endId = (t == NUM_THREADS - 1) ? NUM_ACCOUNTS : (t + 1) * accountsPerThread;

//            logger.info("Thread started for range: " + startId + " to " + endId);
            executor.submit(() -> {
                String threadName = Thread.currentThread().getName();
//                logger.info("[" + threadName + "] Thread started for range: " + startId + " to " + endId);
                try {
                    dataLoad(startId, endId, random);
                } catch (Exception e) {
                    logger.severe("[" + threadName + "] Error in thread for range: " + startId + " to " + endId + ", " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(); // 等待所有线程完成
            logger.info("Data loading complete into database.");
        } catch (InterruptedException e) {
            logger.severe("Main thread interrupted while waiting for completion: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    private static void loadConfig() {
        Properties ini = new Properties();
        try {
            ini.load(new FileInputStream(System.getProperty("prop")));
        } catch (IOException e) {
            logger.severe("Main, could not load properties file");
        }
        // 这里可以根据需要读取配置文件中的其他参数
        JDBC_URL = ini.getProperty("conn");
        JDBC_USER = ini.getProperty("user");
        JDBC_PASSWORD = ini.getProperty("password");
        NUM_ACCOUNTS = Integer.parseInt(ini.getProperty("smallBankNumAccounts", "10000"));
        NUM_THREADS = Integer.parseInt(ini.getProperty("smallBankLoadWorkers", "8"));
    }

    private static void dataLoad(int startId, int endId, jTPCCRandom random) {
        // This method is not used in the current implementation
        // It can be removed or implemented if needed

        try (Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
//            logger.info("Connection established for range: " + startId + " to " + endId);
            connection.setAutoCommit(false);

            try (PreparedStatement accountStmt = connection.prepareStatement("INSERT INTO ACCOUNTS (custid, name) VALUES (?, ?)");
                 PreparedStatement savingsStmt = connection.prepareStatement("INSERT INTO SAVINGS (custid, bal) VALUES (?, ?)");
                 PreparedStatement checkingStmt = connection.prepareStatement("INSERT INTO CHECKING (custid, bal) VALUES (?, ?)")) {

//                logger.info("Inserting accounts from " + startId + " to " + endId);
                for (int i = startId; i <= endId; i++) {
                    String name = random.getAString(5, 15);
                    double savingsBalance = random.nextDouble(100, 10000);
                    double checkingBalance = random.nextDouble(100, 5000);

                    accountStmt.setInt(1, i);
                    accountStmt.setString(2, name);
                    accountStmt.addBatch();

                    savingsStmt.setInt(1, i);
                    savingsStmt.setDouble(2, savingsBalance);
                    savingsStmt.addBatch();

                    checkingStmt.setInt(1, i);
                    checkingStmt.setDouble(2, checkingBalance);
                    checkingStmt.addBatch();

                    if (i % 500 == 0 || i == endId) {
                        accountStmt.executeBatch();
                        savingsStmt.executeBatch();
                        checkingStmt.executeBatch();
                        connection.commit();
//                        logger.info("Committed batch for range: " + startId + " to " + endId + ", up to custid: " + i);
                    }
                }
            } catch (SQLException e) {
                connection.rollback();
                logger.severe("Error during insertion for range: " + startId + " to " + endId + ", " + e.getMessage());
                throw e;
            }

        } catch (SQLException e) {
            logger.severe("Error in thread for range: " + startId + " to " + endId + ", " + e.getMessage());
        }
    }
}

