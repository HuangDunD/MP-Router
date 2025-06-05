package mock.bench.Tpcc.WorkLoad;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Formatter;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import mock.bench.Tpcc.Tool.OSCollector;
import mock.bench.Tpcc.Tool.jTPCCConfig;
import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.Application.AppGeneric;
import mock.bench.Tpcc.WorkLoad.Application.Tpcc.postgres.AppPostgreSQLStoredProc;



/**
 * jTPCC - BenchmarkSQL main class
 */
public class jTPCC {
  private static Logger log = LogManager.getLogger(jTPCC.class);

  private long now;

  public jTPCCRandom rnd;
  public String applicationName;
  // 修改为数组形式
  public static String[] iConn;
  public static String[] iUser;
  public static String[] iPassword;
  public static String loadType;
  public static String offlineLoadFilePath;

  public static String host;
  public static int port;

  public static int dbType;
  public static int numWarehouses;
  public static int useWarehouses;
  public static int useWarehouseFrom;
  public static int useWarehouseTo;
  public static int numMonkeys;
  public static int numSUTThreads;
  public static int maxDeliveryBGThreads;
  public static int maxDeliveryBGPerWH;
  public static int runMins;
  public static int rampupMins;
  public static int rampupSUTMins;
  public static int rampupTerminalMins;
  public static int reportIntervalSecs;
  public static int resultIntervalSecs;
  public static double restartSUTThreadProb;
  public static double transWaitTime;
  public static double keyingTimeMultiplier;
  public static double thinkTimeMultiplier;
  public static int terminalMultiplier;
  public static boolean traceTerminalIO = false;

  public static int crossWarehouseNewOrder;
  public static int crossWarehousePayment;

  public static int sutThreadDelay;
  public static int terminalDelay;
  public static int numTerms;
  public static String benchmarkType;

  public static double newOrderWeight;
  public static double paymentWeight;
  public static double orderStatusWeight;
  public static double deliveryWeight;
  public static double stockLevelWeight;
  public static double rollbackPercent;

    //smallbank
    public static long smallBankNumAccounts;
    public static double amalgamateWeight;
    public static double depositCheckingWeight;
    public static double sendPaymentWeight;
    public static double transactSavingsWeight;
    public static double writeCheckWeight;


  private OSCollector osCollector = null;
  private jTPCCTData terminal_data[];
  private Thread scheduler_thread;
  static public jTPCCScheduler scheduler;
  public jTPCCSUT systemUnderTest;
  public jTPCCMonkey monkeys;

  public static String resultDirectory = null;
  public static String osCollectorScript = null;
  public static String reportScript = null;
  private static String resultDirName = null;
  private static BufferedWriter summaryCSV = null;
  private static BufferedWriter histogramCSV = null;
  private static BufferedWriter resultCSV = null;
  private static BufferedWriter runInfoCSV = null;
  public static int runID = 0;
  public static long csv_begin;
  public static long result_begin;
  public static long result_end;
  private static Object result_lock;

  public static int next_o_id[][];
  public static int min_o_id[][];

  public static void main(String args[]) throws FileNotFoundException {
    new jTPCC();
  }

  private String getProp(Properties p, String pName) {
    String prop = p.getProperty(pName);
    log.info("Main, {}={}", pName, prop);
    return (prop);
  }

  private String getProp(Properties p, String pName, String defVal) {
    String prop = p.getProperty(pName);
    if (prop == null)
      prop = defVal;
    log.info("Main, {}={}", pName, prop);
    return (prop);
  }

  public jTPCC() throws FileNotFoundException {
    StringBuilder sb = new StringBuilder();
    Formatter fmt = new Formatter(sb);

    // load the ini file
    Properties ini = new Properties();
    try {
      ini.load(new FileInputStream(System.getProperty("prop")));
    } catch (IOException e) {
      log.error("Main, could not load properties file");
    }

    /*
     * Get all the configuration settings
     */
    log.info("Main, ");
    log.info("Main, +-------------------------------------------------------------+");
    log.warn("Main,      MockBenchSQL v{}", jTPCCConfig.JTPCCVERSION);
    log.info("Main, +-------------------------------------------------------------+");
    log.info("Main, ");
    String iDBType = getProp(ini, "db");
    String iDriver = getProp(ini, "driver");
    applicationName = getProp(ini, "application");
    
    // 读取数组形式的连接配置
    int connCount = 0;
    while (ini.containsKey("conn[" + connCount + "]")) {
      connCount++;
    }
    
    if (connCount == 0) {
      log.error("No database connections configured");
      return;
    }
    
    iConn = new String[connCount];
    iUser = new String[connCount];
    iPassword = new String[connCount];
    
    for (int i = 0; i < connCount; i++) {
      iConn[i] = getProp(ini, "conn[" + i + "]");
      iUser[i] = getProp(ini, "user[" + i + "]");
      iPassword[i] = getProp(ini, "password[" + i + "]");
    }

    benchmarkType = getProp(ini, "benchmarkType");

    crossWarehouseNewOrder = Integer.parseInt(getProp(ini, "crossWarehouseNewOrder", "1"));

    crossWarehousePayment = Integer.parseInt(getProp(ini, "crossWarehousePayment", "15"));
    
    loadType = getProp(ini, "loadType");
    offlineLoadFilePath = getProp(ini, "offlineLoadFilePath");
    host = getProp(ini, "host");
    port = Integer.parseInt(getProp(ini, "port"));

    log.info("Main, ");
    numWarehouses = Integer.parseInt(getProp(ini, "warehouses"));
    useWarehouseFrom = Integer.parseInt(getProp(ini, "useWarehouseFrom", "-1"));
    useWarehouseTo = Integer.parseInt(getProp(ini, "useWarehouseTo", "-1"));
    useWarehouses = numWarehouses;
    if (useWarehouseFrom > 0 && useWarehouseTo > 0) {
      useWarehouses = useWarehouseTo - useWarehouseFrom + 1;
    } else {
      useWarehouseFrom = 1;
      useWarehouseTo = useWarehouses;
    }

    numMonkeys = Integer.parseInt(getProp(ini, "monkeys"));
    numSUTThreads = Integer.parseInt(getProp(ini, "sutThreads"));
    maxDeliveryBGThreads = Integer.parseInt(getProp(ini, "maxDeliveryBGThreads"));
    maxDeliveryBGPerWH = Integer.parseInt(getProp(ini, "maxDeliveryBGPerWarehouse"));
    rampupMins = Integer.parseInt(getProp(ini, "rampupMins"));
    runMins = Integer.parseInt(getProp(ini, "runMins"));
    rampupSUTMins = Integer.parseInt(getProp(ini, "rampupSUTMins"));
    rampupTerminalMins = Integer.parseInt(getProp(ini, "rampupTerminalMins"));
    reportIntervalSecs = Integer.parseInt(getProp(ini, "reportIntervalSecs"));
    resultIntervalSecs = Integer.parseInt(getProp(ini, "resultIntervalSecs", "10"));
    restartSUTThreadProb = Double.parseDouble(getProp(ini, "restartSUTThreadProbability"));
    transWaitTime = Double.parseDouble(getProp(ini, "transWaitTime"));
//    keyingTimeMultiplier = Double.parseDouble(getProp(ini, "keyingTimeMultiplier"));
//    thinkTimeMultiplier = Double.parseDouble(getProp(ini, "thinkTimeMultiplier"));
    terminalMultiplier = Integer.parseInt(getProp(ini, "terminalMultiplier", "1"));
    traceTerminalIO = Boolean.parseBoolean(getProp(ini, "traceTerminalIO"));

    if (benchmarkType.equals("tpcc")){
        paymentWeight = Double.parseDouble(getProp(ini, "paymentWeight"));
        orderStatusWeight = Double.parseDouble(getProp(ini, "orderStatusWeight"));
        deliveryWeight = Double.parseDouble(getProp(ini, "deliveryWeight"));
        stockLevelWeight = Double.parseDouble(getProp(ini, "stockLevelWeight"));
        newOrderWeight = 100.0 - paymentWeight - orderStatusWeight - deliveryWeight - stockLevelWeight;
    } else if (benchmarkType.equals("smallbank")) {
        smallBankNumAccounts = Integer.parseInt(getProp(ini, "smallBankNumAccounts"));
        depositCheckingWeight = Double.parseDouble(getProp(ini, "depositCheckingWeight"));
        sendPaymentWeight = Double.parseDouble(getProp(ini, "sendPaymentWeight"));
        transactSavingsWeight = Double.parseDouble(getProp(ini, "transactSavingsWeight"));
        writeCheckWeight = Double.parseDouble(getProp(ini, "writeCheckWeight"));
        amalgamateWeight = 100.0 - depositCheckingWeight - sendPaymentWeight - transactSavingsWeight - writeCheckWeight;
    } else {
        log.error("Unknown benchmark type: " + benchmarkType);
        exit();
    }


      if (newOrderWeight < 0.0) {
      log.error("Main, newOrderWeight is below zero");
      return;
    }
    fmt.format("newOrderWeight=%.3f", newOrderWeight);
    log.info("Main, {}", sb.toString());
    log.info("Main, ");

    rollbackPercent = Double.parseDouble(getProp(ini, "rollbackPercent", "1.01"));
    log.info("Main, ");

    numTerms = 10 * terminalMultiplier; // 终端数
    sutThreadDelay = (rampupSUTMins * 60000) / numSUTThreads;
    terminalDelay = (rampupTerminalMins * 60000) / (useWarehouses * numTerms);

    if (iDBType.equals("postgres"))
      dbType = jTPCCConfig.DB_POSTGRES;
    else {
      log.error("Unknown database type '{}'", iDBType);
      return;
    }

    /*
     * Check that we support the requested application implementation
     * 检查应用名
     */
    if (!applicationName.equals("Generic") && !applicationName.equals("PostgreSQLStoredProc")) {
      log.error("Unknown application name '{}'", applicationName);
      return;
    }

    /*
      * Initialize global next_o_id, each warehouse contains 10 district, each district contains 3K orders
      * Initialize global min_o_id, starts from 1
     */
    next_o_id = new int[numWarehouses + 1][11];
    min_o_id = new int[numWarehouses + 1][11];
    for (int i = 1; i <= numWarehouses; i++) {
      for (int j = 1; j <= 10; j++) {
        next_o_id[i][j] = 3001;
        min_o_id[i][j] = 1;
      }
    }

    // delete the offlineLoad.txt
    if (loadType.equals("offline")){
      File file = new File(offlineLoadFilePath);
      if (file.exists()) {
        file.delete();
      }
      try {
        file.createNewFile();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /*
     * We need to have set the start time of the rampup (csv_begin)
     * from here on.
     * 结果表时间
     */
    now = System.currentTimeMillis();
    csv_begin = now;

    /*
     * Launch the OS metric collector if configured
     * OS性能监控
     */
    String resultDirectory = getProp(ini, "resultDirectory");
    String osCollectorScript = getProp(ini, "osCollectorScript");

    if (resultDirectory != null) {
      StringBuffer sbRes = new StringBuffer();
      Formatter fmtRes = new Formatter(sbRes);
      Pattern p = Pattern.compile("%t");
      Calendar cal = Calendar.getInstance();

      String iRunID;

      iRunID = System.getProperty("runID");
      if (iRunID != null) {
        runID = Integer.parseInt(iRunID);
      }

      /*
       * Split the resultDirectory into strings around patterns of %t and then insert date/time
       * formatting based on the current time. That way the resultDirectory in the properties file
       * can have date/time format elements like in result_%tY-%tm-%td to embed the current date in
       * the directory name.
       * 创建结果文件夹
       */
      String[] parts = p.split(resultDirectory, -1);
      sbRes.append(parts[0]);
      for (int i = 1; i < parts.length; i++) {
        fmtRes.format("%t" + parts[i].substring(0, 1), cal);
        sbRes.append(parts[i].substring(1));
      }
      resultDirName = sbRes.toString();
      File resultDir = new File(resultDirName);
      File resultDataDir = new File(resultDir, "data");

      // Create the output directory structure.
      if (!resultDir.mkdir()) {
        log.error("Failed to create directory '{}'", resultDir.getPath());
        System.exit(1);
      }
      if (!resultDataDir.mkdir()) {
        log.error("Failed to create directory '{}'", resultDataDir.getPath());
        System.exit(1);
      }

      // Copy the used properties file into the resultDirectory. 复制配置文件
      try {
        copyFile(new File(System.getProperty("prop")), new File(resultDir, "run.properties"));
      } catch (Exception e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("Main, copied {} to {}", System.getProperty("prop"),
          new File(resultDir, "run.properties").getPath());

      // Create the runInfo.csv file. 写入info文件
      String runInfoCSVName = new File(resultDataDir, "runInfo.csv").getPath();
      try {
        runInfoCSV = new BufferedWriter(new FileWriter(runInfoCSVName));
        runInfoCSV.write("runID,dbType,jTPCCVersion,application," + "rampupMins,runMins,"
            + "loadWarehouses,runWarehouses,numSUTThreads,"
            + "maxDeliveryBGThreads,maxDeliveryBGPerWarehouse," + "restartSUTThreadProbability,"
            + "thinkTimeMultiplier,keyingTimeMultiplier\n");
//        runInfoCSV.write(runID + "," + iDBType + "," + jTPCCConfig.JTPCCVERSION + ","
//            + applicationName + "," + rampupMins + "," + runMins + "," + numWarehouses + ","
//            + numWarehouses + "," + numSUTThreads + "," + maxDeliveryBGThreads + ","
//            + maxDeliveryBGPerWH + "," + restartSUTThreadProb + "," + thinkTimeMultiplier + ","
//            + keyingTimeMultiplier + "\n");
        runInfoCSV.write(runID + "," + iDBType + "," + jTPCCConfig.JTPCCVERSION + ","
            + applicationName + "," + rampupMins + "," + runMins + "," + numWarehouses + ","
            + numWarehouses + "," + numSUTThreads + "," + maxDeliveryBGThreads + ","
            + maxDeliveryBGPerWH + "," + restartSUTThreadProb + "," + transWaitTime + "\n");
        runInfoCSV.close();
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("Main, created {} for runID {}", runInfoCSVName, runID);

      // Open the aggregated transaction result.csv file. 事务执行结果文件，写入表头
      String resultCSVName = new File(resultDataDir, "result.csv").getPath();
      try {
        resultCSV = new BufferedWriter(new FileWriter(resultCSVName));
        resultCSV.write("ttype,second,numtrans," + "sumlatencyms,minlatencyms,maxlatencyms,"
            + "sumdelayms,mindelayms,maxdelayms\n");
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("Main, writing aggregated transaction results to {}", resultCSVName);
      result_lock = new Object();

      // Open the aggregated summary.csv file 总结表
      String summaryCSVName = new File(resultDataDir, "summary.csv").getPath();
      try {
        summaryCSV = new BufferedWriter(new FileWriter(summaryCSVName));
        summaryCSV.write("ttype,count,percent,mean,max,rollbacks,errors\n");
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("Main, writing transaction summary to " + summaryCSVName);

      // Open the histogram.csv file. 打开直方图文件
      String histogramCSVName = new File(resultDataDir, "histogram.csv").getPath();
      try {
        histogramCSV = new BufferedWriter(new FileWriter(histogramCSVName));
        histogramCSV.write("ttype,edge,numtrans\n");
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("Main, writing transaction histogram to " + histogramCSVName);

      // Launch the metric collector script if configured 加载os信息配置文件
      if (osCollectorScript != null) {
        try {
          osCollector = new OSCollector(getProp(ini, "osCollectorScript"),
              resultDataDir);
        } catch (IOException e) {
          log.error(e.getMessage());
          System.exit(1);
        }
      }

      log.info("Main,");
    }

    /*
     * Even though we don't deal with the report generator in the main
     * java code (the Flask UI does that and it is available on the
     * command line), we consume the property so that it is reported
     * in the logs.  尽管我们在主 Java 代码中不处理报告生成器（Flask UI 负责此功能，并且可以通过命令行使用），但我们会读取该属性以便将其记录到日志中
     */
    String reportScript = getProp(ini, "reportScript");

    /* Initialize the random number generator and report C values. 设置TPCC的随机种子，原本有load数据库中的随机种子，统计事务与数据加载种子的差值，但此处去掉*/
    rnd = new jTPCCRandom();
    log.info("Main, ");
    log.info("Main, C value for nURandCLast this run: {}", rnd.getNURandCLast());
    log.info("Main, ");

    terminal_data = new jTPCCTData[useWarehouses * numTerms];

    /* Create the scheduler. 创建调度器*/
    scheduler = new jTPCCScheduler(this);
    scheduler_thread = new Thread(this.scheduler);
    scheduler_thread.start();

    /*
     * Create the SUT and schedule the launch of the SUT threads. 创建被测系统
     */
    result_begin = now + rampupMins * 60000; // 系统预热时间
    result_end = result_begin + runMins * 60000; // 系统结束时间

    systemUnderTest = new jTPCCSUT(this); //创建终端
    for (int t = 0; t < numSUTThreads; t++) {
      jTPCCTData sut_launch_tdata; // tpcctdata是一个线程的数据结构
      sut_launch_tdata = new jTPCCTData();

      /*
       * We abuse the term_w_id to communicate which of the SUT threads to start.
       */
      sut_launch_tdata.term_w_id = t;
      scheduler.at(now + t * sutThreadDelay, jTPCCScheduler.SCHED_SUT_LAUNCH, sut_launch_tdata); // 依次开启被测系统以防负载激增
    }

    /*
     * Launch the threads that generate the terminal input data.
     * 猴子线程用于处理数据包并生成新的事务
     */
    monkeys = new jTPCCMonkey(this);

    /*
     * Create all the Terminal data sets and schedule their launch. We only assign their fixed
     * TERM_W_ID is TERM_D_ID (for stock level transactions) here. Once the scheduler is actually
     * launching them according to their delay, the trained monkeys will fill in real data, send
     * them back into the scheduler queue to the flow to the client threads performing the real DB
     * work.
     * 每个仓库有至少10个终端均匀分布在各个地点
     */
    for (int t = 0; t < useWarehouses * numTerms; t++) {
      terminal_data[t] = new jTPCCTData();
      terminal_data[t].term_w_id = (t / numTerms) + useWarehouseFrom;
      terminal_data[t].term_d_id = (t % 10) + 1;
      terminal_data[t].trans_type = jTPCCTData.TT_NONE;
      terminal_data[t].trans_due = now + t * terminalDelay;
      terminal_data[t].trans_start = terminal_data[t].trans_due;
      terminal_data[t].trans_end = terminal_data[t].trans_due;
      terminal_data[t].trans_error = false;

      scheduler.at(terminal_data[t].trans_due, jTPCCScheduler.SCHED_TERM_LAUNCH, terminal_data[t]);
    }

    /*
     * Schedule the special events to begin measurement (end of rampup time), to shut down the
     * system and to print messages when the terminals and SUT threads have all been started.
     * 功能线程
     */
    this.scheduler.at(result_begin, jTPCCScheduler.SCHED_BEGIN, new jTPCCTData());
    this.scheduler.at(result_end, jTPCCScheduler.SCHED_END, new jTPCCTData());
    this.scheduler.at(result_end + 10000, jTPCCScheduler.SCHED_DONE, new jTPCCTData());
    this.scheduler.at(now + (useWarehouses * numTerms) * terminalDelay,
        jTPCCScheduler.SCHED_TERM_LAUNCH_DONE, new jTPCCTData());
    this.scheduler.at(now + numSUTThreads * sutThreadDelay, jTPCCScheduler.SCHED_SUT_LAUNCH_DONE,
        new jTPCCTData());
    if (reportIntervalSecs > 0) {
      this.scheduler.at(now + reportIntervalSecs * 1000, jTPCCScheduler.SCHED_REPORT,
          new jTPCCTData());
    }
    this.scheduler.at(now + resultIntervalSecs * 1000, jTPCCScheduler.SCHED_DUMMY_RESULT, new jTPCCTData());

    try {
      scheduler_thread.join();
      log.info("Main, scheduler returned");
    } catch (InterruptedException e) {
      log.error("Main, InterruptedException: {}", e.getMessage());
    }

    /*
     * Time to stop input data generation.
     */
    monkeys.terminate();
    log.info("Main, all simulated terminals ended");

    /*
     * Stop the SUT.
     */
    systemUnderTest.terminate();
    log.info("Main, all SUT threads ended");

    /*
     * Report final transaction statistics.
     */
    monkeys.reportStatistics();

    /*
     * Close the aggregated transaction CSV result
     */
    if (resultCSV != null) {
      try {
        log.info("aggregated transaction result file finished");
        resultCSV.close();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }

    /*
     * Close the summary CSV
     */
    if (summaryCSV != null) {
      try {
        log.info("transaction summary file finished");
        summaryCSV.close();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }

    /*
     * Close the histogram CSV
     */
    if (histogramCSV != null) {
      try {
        log.info("transaction histogram file finished");
        histogramCSV.close();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }

    /*
     * Stop the OS stats collector
     */
    if (osCollector != null) {
      try {
        osCollector.stop();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
      osCollector = null;
      log.info("Main, OS Collector stopped");
    }
  }

  public jTPCCApplication getApplication() {
    if (applicationName.equals("Generic"))
      return new AppGeneric();
    if (applicationName.equals("PostgreSQLStoredProc"))
      return new AppPostgreSQLStoredProc();

    return new jTPCCApplication();
  }

  public static void csv_result_write(String line) {
    if (resultCSV != null) {
      synchronized (result_lock) {
        try {
          resultCSV.write(line);
          resultCSV.flush();
        } catch (Exception e) {
        }
      }
    }
  }

  public static void csv_summary_write(String line) {
    if (summaryCSV != null) {
      try {
        summaryCSV.write(line);
      } catch (Exception e) {
      }
    }
  }

  public static void csv_histogram_write(String line) {
    if (histogramCSV != null) {
      try {
        histogramCSV.write(line);
      } catch (Exception e) {
      }
    }
  }

  private void exit() {
    System.exit(0);
  }

  private String getCurrentTime() {
    return jTPCCConfig.dateFormat.format(new java.util.Date());
  }

  private String getFileNameSuffix() {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    return dateFormat.format(new java.util.Date());
  }

  private void copyFile(File in, File out) throws FileNotFoundException, IOException {
    FileInputStream strIn = new FileInputStream(in);
    FileOutputStream strOut = new FileOutputStream(out);
    byte buf[] = new byte[65536];

    int len = strIn.read(buf);
    while (len > 0) {
      strOut.write(buf, 0, len);
      len = strIn.read(buf);
    }

    strOut.close();
    strIn.close();
  }
}
