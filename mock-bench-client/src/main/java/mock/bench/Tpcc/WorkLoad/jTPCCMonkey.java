package mock.bench.Tpcc.WorkLoad;

import java.util.Formatter;
import java.util.Locale;
import java.util.Random;

import mock.bench.Tpcc.Tool.jTPCCRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * jTPCCMonkey - The terminal input data generator and output consumer.
 */
public class jTPCCMonkey {
  private static Logger log = LogManager.getLogger(jTPCCMonkey.class);
  private static Logger termlog = LogManager.getLogger(jTPCCMonkey.class.getName() + ".terminalIO");

  private jTPCC gdata;
  private int numMonkeys;
  private Monkey monkeys[];
  private Thread monkeyThreads[];

  private jTPCCTDataList queue;
  private Object queue_lock;

  private jTPCCRandom rnd;

  private String benchmarkType;
  private long smallBankNumAccounts;

  // for crossWarehouse
  private int crossWarehouseNewOrder; // 跨wh概率1
  private int crossWarehousePayment; // 跨wh概率2

  public jTPCCMonkey(jTPCC gdata) {
    this.gdata = gdata;
    this.numMonkeys = jTPCC.numMonkeys;
    this.monkeys = new Monkey[jTPCC.numMonkeys];
    this.monkeyThreads = new Thread[jTPCC.numMonkeys];

    this.benchmarkType = jTPCC.benchmarkType;
    this.smallBankNumAccounts = jTPCC.smallBankNumAccounts;

    this.crossWarehouseNewOrder = jTPCC.crossWarehouseNewOrder;
    this.crossWarehousePayment = 100 - jTPCC.crossWarehousePayment;

    this.queue = new jTPCCTDataList();
    this.queue_lock = new Object();

    this.rnd = gdata.rnd.newRandom();

    for (int m = 0; m < jTPCC.numMonkeys; m++) {
      monkeys[m] = new Monkey(m, this);
      monkeyThreads[m] = new Thread(monkeys[m]);
      monkeyThreads[m].start();
    }
  }

  public void reportStatistics() {
    jTPCCResult sumStats = new jTPCCResult();
    StringBuilder sb = new StringBuilder();
    Formatter fmt = new Formatter(sb, Locale.US);
    double total_count = 0.0;
    double statsDivider;

    for (int m = 0; m < jTPCC.numMonkeys; m++) {
      monkeys[m].result.aggregate(sumStats);
    }
    if (jTPCC.benchmarkType.equals("tpcc"))
        gdata.systemUnderTest.deliveryBg.result.aggregate(sumStats);

    for (int tt = jTPCCTData.TT_AMALGAMATE; tt <= jTPCCTData.TT_DELIVERY; tt++) {
      total_count += (double) (sumStats.histCounter[tt].numTrans);
    }

    log.info("result,                                           _____ latency (seconds) _____");
    log.info(
        "result,   TransType              count |   mix % |    mean       max     90th% |    rbk%          errors");
    log.info(
        "result, +--------------+---------------+---------+---------+---------+---------+---------+---------------+");
    for (int tt = jTPCCTData.TT_AMALGAMATE; tt <= jTPCCTData.TT_DELIVERY_BG; tt++) {
      double count;
      double percent;
      double mean;
      double max;
      double nth_pct;
      long nth_needed;
      long nth_have = 0;
      double rbk;
      double errors;
      int b;

      count = (double) (sumStats.histCounter[tt].numTrans);
      percent = count / total_count * 100.0;
      if (tt == jTPCCTData.TT_DELIVERY_BG)
        percent = 0.0;
      mean = (double) (sumStats.histCounter[tt].sumMS) / 1000.0 / count;
      max = (double) (sumStats.histCounter[tt].maxMS) / 1000.0;
      rbk = (double) (sumStats.histCounter[tt].numRbk) / count * 100.0;
      errors = (double) (sumStats.histCounter[tt].numError);

      nth_needed = (long) ((double) (sumStats.histCounter[tt].numTrans) * 0.9);
      for (b = 0; b < jTPCCResult.NUM_BUCKETS && nth_have < nth_needed; b++) {
        nth_have += sumStats.histCounter[tt].bucket[b];
      }
      nth_pct = Math.exp((double) b * sumStats.statsDivider) / 1000.0;
      if (sumStats.histCounter[tt].numTrans == 0) {
        count = max = nth_pct = rbk = errors = 0.0 / 0.0;
      }


      fmt.format("| %-12.12s | %,13d | %7.3f | %7.3f | %7.3f | %7.3f | %7.3f | %,13.0f |",
          jTPCCTData.trans_type_names[tt], sumStats.histCounter[tt].numTrans, percent, mean, max,
          nth_pct, rbk, errors);
      log.info("result, {}", sb.toString());
      sb.setLength(0);

      /*
       * Also collect this data in the summary.csv
       */
      jTPCC.csv_summary_write(
          jTPCCTData.trans_type_names[tt] + ","
              + sumStats.histCounter[tt].numTrans + ","
              + percent + ","
              + mean + ","
              + max + ","
              + sumStats.histCounter[tt].numRbk + ","
              + sumStats.histCounter[tt].numError + "\n");
    }
    log.info(
        "result, +--------------+---------------+---------+---------+---------+---------+---------+---------------+");

    log.info("result,");

    fmt.format("Overall NOPM: %,12.0f (%.2f%% of the theoretical maximum)",
        (double) (sumStats.histCounter[jTPCCTData.TT_NEW_ORDER].numTrans)
            / (double) (jTPCC.runMins),
        (double) (sumStats.histCounter[jTPCCTData.TT_NEW_ORDER].numTrans)
            / ((double) jTPCC.numWarehouses * 0.1286 * (double) (jTPCC.runMins)));
    log.info("result, {}", sb.toString());
    sb.setLength(0);

    fmt.format("Overall TPM:  %,12.0f", total_count / (double) (jTPCC.runMins));
    log.info("result, {}", sb.toString());
    sb.setLength(0);

    /*
     * Write out the transaction latency histogram
     */
    log.info("dumping result histogram");
    statsDivider = Math.log(jTPCCResult.STATS_CUTOFF * 1000.0) / (double) (jTPCCResult.NUM_BUCKETS);
    for (int tt = jTPCCTData.TT_NEW_ORDER; tt <= jTPCCTData.TT_DELIVERY_BG; tt++) {
      for (int b = 0; b < jTPCCResult.NUM_BUCKETS; b++) {
        double edge = Math.exp((double) (b + 1) * statsDivider) / jTPCCResult.NUM_BUCKETS;

        jTPCC.csv_histogram_write(jTPCCTData.trans_type_names[tt] + "," + edge + ","
            + sumStats.histCounter[tt].bucket[b] + "\n");
      }
    }
  }

  public void terminate() {
    synchronized (queue_lock) {
      queue.truncate();

      for (int m = 0; m < numMonkeys; m++) {
        jTPCCTData doneMsg = new jTPCCTData();
        doneMsg.trans_type = jTPCCTData.TT_DONE;
        queue.append(doneMsg);
      }

      queue_lock.notify();
    }

    for (int m = 0; m < numMonkeys; m++) {
      try {
        monkeyThreads[m].join();
      } catch (InterruptedException e) {
      }
    }
  }

  public void queueAppend(jTPCCTData tdata) {
    synchronized (queue_lock) {
      queue.append(tdata);
      queue_lock.notify();
    }
  }

  private class Monkey implements Runnable {

    private int m_id;
    private jTPCCMonkey parent;
    private Random random;

    public jTPCCResult result;

    public Monkey(int m_id, jTPCCMonkey parent) {

      this.m_id = m_id;
      this.parent = parent;
      this.random = new Random(System.currentTimeMillis());

      this.result = new jTPCCResult();
    }

    public void run() {
      jTPCCTData tdata;
      double think_time;
      double key_time;
      double key_mean;

      for (;;) {
        synchronized (queue_lock) {
          while ((tdata = queue.first()) == null) {
            try {
              queue_lock.wait();
            } catch (InterruptedException e) {
              log.error("monkey-{}, InterruptedException: {}", this.m_id, e.getMessage());
              return;
            }
          }
          queue.remove(tdata);

          /*
           * If there are more result, notify another input data generator (if there is an idle
           * one).
           */
          if (queue.first() != null)
            queue_lock.notify();
        }

        /*
         * Exit the loop (and terminate this thread) when we receive the DONE signal.
         */
        if (tdata.trans_type == jTPCCTData.TT_DONE)
          break;

        /*
        if trans_type is not done
         */

        /*
         * Process the last transactions result and determine the think time based on the previous
         * transaction type.
         */
        switch (tdata.trans_type) {
            case jTPCCTData.TT_NEW_ORDER:
            case jTPCCTData.TT_PAYMENT:
            case jTPCCTData.TT_ORDER_STATUS:
            case jTPCCTData.TT_STOCK_LEVEL:
            case jTPCCTData.TT_DELIVERY:

            case jTPCCTData.TT_AMALGAMATE:
            case jTPCCTData.TT_DEPOSIT_CHECKING:
            case jTPCCTData.TT_SEND_PAYMENT:
            case jTPCCTData.TT_TRANSACT_SAVINGS:
            case jTPCCTData.TT_WRITE_CHECK:
                result.collect(tdata);
                break;

            default:
                break;
        }

        /*
         * Initialize trans_rbk as false. The New Order input generator may set it to true.
         */
        tdata.trans_rbk = false;

        /*
         * Select the next transaction type.
         */
        tdata.trans_type = nextTransactionType();
        switch (tdata.trans_type) {

            case jTPCCTData.TT_NEW_ORDER:
                generateNewOrder(tdata);
                break;
            case jTPCCTData.TT_PAYMENT:
                generatePayment(tdata);
                break;
            case jTPCCTData.TT_ORDER_STATUS:
                generateOrderStatus(tdata);
                break;
            case jTPCCTData.TT_STOCK_LEVEL:
                generateStockLevel(tdata);
                break;
            case jTPCCTData.TT_DELIVERY:
                generateDelivery(tdata);
                break;

            case jTPCCTData.TT_AMALGAMATE:
                generateAmalgamate(tdata);
                break;
            case jTPCCTData.TT_DEPOSIT_CHECKING:
                generateDepositChecking(tdata);
                break;
            case jTPCCTData.TT_SEND_PAYMENT:
                generateSendPayment(tdata);
                break;
            case jTPCCTData.TT_TRANSACT_SAVINGS:
                generateTransactSavings(tdata);
                break;
            case jTPCCTData.TT_WRITE_CHECK:
                generateWriteCheck(tdata);
                break;

            default:
                break;
        }

        /*
         * Set up the terminal data header fields. The Transaction due time is based on the last
         * transactions end time. This eliminates delays caused bu the monkeys not reading or typing
         * at infinite speed.
         */
//        tdata.trans_due = tdata.trans_end + (long) ((think_time + key_time) * 1000.0);
        tdata.trans_due = tdata.trans_end + (long) jTPCC.transWaitTime;
        tdata.trans_start = 0;
        tdata.trans_end = 0;
        tdata.trans_error = false;

        gdata.scheduler.at(tdata.trans_due, jTPCCScheduler.SCHED_TERMINAL_DATA, tdata);
      }
    }

    private void generateNewOrder(jTPCCTData tdata) {
      jTPCCTData.NewOrderData screen = tdata.NewOrderData();
      int ol_count;
      int ol_idx = 0;

      // 2.4.1.1 - w_id = terminal's w_id
      screen.w_id = tdata.term_w_id;

      // 2.4.1.2 - random d_id and non uniform random c_id
      screen.d_id = rnd.nextInt(1, 10);
      screen.c_id = rnd.getCustomerID();

      // 2.4.1.3 - random [5..15] order lines
      ol_count = rnd.nextInt(5, 15);
      while (ol_idx < ol_count) {
        // 2.4.1.5 1) - non uniform ol_i_id
        screen.ol_i_id[ol_idx] = rnd.getItemID();

        // 2.4.1.5 2) - In 1% of order lines the supply warehouse
        // is different from the terminal's home warehouse.
        screen.ol_supply_w_id[ol_idx] = tdata.term_w_id;
        if (rnd.nextInt(1, 100) == crossWarehouseNewOrder) { // 跨wh概率1
          do {
            screen.ol_supply_w_id[ol_idx] = rnd.nextInt(1, jTPCC.numWarehouses);
          } while (screen.ol_supply_w_id[ol_idx] == tdata.term_w_id && jTPCC.numWarehouses > 1);
        }

        // 2.4.1.5 3) - random ol_quantity [1..10]
        screen.ol_quantity[ol_idx] = rnd.nextInt(1, 10);
        ol_idx++;
      }

      // 2.4.1.4 - 1% of orders must use an invalid ol_o_id in the last
      // order line generated.
      if (rnd.nextDouble(0.0, 100.0) <= jTPCC.rollbackPercent) {
        screen.ol_i_id[ol_idx - 1] = 999999;
        tdata.trans_rbk = true;
      }

      // Zero out the remaining order lines if they contain old data.
      while (ol_idx < 15) {
        screen.ol_supply_w_id[ol_idx] = 0;
        screen.ol_i_id[ol_idx] = 0;
        screen.ol_quantity[ol_idx] = 0;
        ol_idx++;
      }

      tdata.new_order = screen;
    }


    private void generatePayment(jTPCCTData tdata) {
      jTPCCTData.PaymentData screen = tdata.PaymentData();

      // 2.5.1.1 - w_id = terminal's w_id
      screen.w_id = tdata.term_w_id;

      // 2.5.1.2 - d_id = random [1..10]
      screen.d_id = rnd.nextInt(1, 10);

      // 2.5.1.2 - in 85% of cases (c_d_id, c_w_id) = (d_id, w_id)
      // in 15% of cases they are randomly chosen.
      if (rnd.nextInt(1, 100) <= crossWarehousePayment) { // 跨wh概率2
        screen.c_d_id = screen.d_id;
        screen.c_w_id = screen.w_id;
      } else {
        screen.c_d_id = rnd.nextInt(1, 10);
        do {
          screen.c_w_id = rnd.nextInt(1, jTPCC.numWarehouses);
        } while (screen.c_w_id == tdata.term_w_id && jTPCC.numWarehouses > 1);
      }

      // DONE: 修改为100%用c_id
      // 2.5.1.2 - in 60% of cases customer is selected by last name,
      // in 40% of cases by customer ID.
//      if (rnd.nextInt(1, 100) <= 60) {
//        screen.c_id = 0;
//        screen.c_last = rnd.getCLast();
//      } else {
      screen.c_id = rnd.getCustomerID();
      screen.c_last = null;
//      }

      // 2.5.1.3 - h_amount = random [1.00 .. 5,000.00] // 付款额度
      screen.h_amount = ((double) rnd.nextLong(100, 500000)) / 100.0;

      tdata.payment = screen;
    }

    /*
     * ORDER_STATUS
     */
    private void generateOrderStatus(jTPCCTData tdata) {
      jTPCCTData.OrderStatusData screen = tdata.OrderStatusData();

      // 2.6.1.1 - w_id = terminal's w_id
      screen.w_id = tdata.term_w_id;

      // 2.6.1.2 - d_id is random [1..10]
      screen.d_id = rnd.nextInt(1, 10);

      // 2.6.1.2 - in 0% of cases customer is selected by last name,
      // in 100% of cases by customer ID. 修改
//      if (rnd.nextInt(1, 100) <= 60) {
//        screen.c_id = 0;
//        screen.c_last = rnd.getCLast();
//      } else {
      screen.c_id = rnd.getCustomerID();
      screen.c_last = null;
//      }

      tdata.order_status = screen;
    }

    /*
     * DELIVERY
     */
    private void generateDelivery(jTPCCTData tdata) {
      jTPCCTData.DeliveryData screen = tdata.DeliveryData();

      // 2.7.1.1 - w_id = terminal's w_id
      screen.w_id = tdata.term_w_id;

      // 2.7.1.2 - o_carrier_id = random [1..10]
      screen.o_carrier_id = rnd.nextInt(1, 10);

      tdata.delivery = screen;
    }

    /*
     * STOCK_LEVEL
     */
    private void generateStockLevel(jTPCCTData tdata) {
      jTPCCTData.StockLevelData screen = tdata.StockLevelData();

      screen.w_id = tdata.term_w_id;
      screen.d_id = tdata.term_d_id;
      screen.threshold = rnd.nextInt(10, 20);

      tdata.stock_level = screen;
    }

    // smallbank transactions
    // Amalgamate
    private void generateAmalgamate(jTPCCTData tdata) {
        jTPCCTData.AmalgamateData screen = tdata.AmalgamateData();

        screen.acctID0 = randomInt(1, smallBankNumAccounts);
        do {
            screen.acctID1 = randomInt(1, smallBankNumAccounts);
        } while (screen.acctID0 == screen.acctID1);
        screen.updateValue = randomDouble() * 1000.0; // 随机金额
        tdata.amalgamate = screen;
    }

    // Deposit Checking
      private void generateDepositChecking(jTPCCTData tdata) {
          jTPCCTData.DepositCheckingData screen = tdata.DepositCheckingData();

          screen.acctID = randomInt(1, smallBankNumAccounts);
          screen.updateValue = randomDouble() * 1000.0; // 随机金额
          tdata.deposit_checking = screen;
      }

    // Send Payment
    private void generateSendPayment(jTPCCTData tdata) {
        jTPCCTData.SendPaymentData screen = tdata.SendPaymentData();

        screen.acctID0 = randomInt(1, smallBankNumAccounts);
        do {
            screen.acctID1 = randomInt(1, smallBankNumAccounts);
        } while (screen.acctID0 == screen.acctID1);
        screen.updateValue = randomDouble() * 1000.0; // 随机金额
        tdata.send_payment = screen;
    }

    // Transact Savings
    private void generateTransactSavings(jTPCCTData tdata) {
        jTPCCTData.TransactSavingsData screen = tdata.TransactSavingsData();
        screen.acctID = randomInt(1, smallBankNumAccounts);
        screen.updateValue = randomDouble() * 1000.0; // 随机金额
        tdata.transact_savings = screen;
    }

    // Write Check
    private void generateWriteCheck(jTPCCTData tdata) {
        jTPCCTData.WriteCheckData screen = tdata.WriteCheckData();
        screen.acctID = randomInt(1, smallBankNumAccounts);
        screen.updateValue = randomDouble() * 1000.0; // 随机金额
        tdata.write_check = screen;
    }


    private int nextTransactionType() {
      double chance = randomDouble() * 100.0;

      if (benchmarkType.equals("tpcc")){
          if (chance <= jTPCC.paymentWeight)
              return jTPCCTData.TT_PAYMENT;
          chance -= jTPCC.paymentWeight;

          if (chance <= jTPCC.orderStatusWeight)
              return jTPCCTData.TT_ORDER_STATUS;
          chance -= jTPCC.orderStatusWeight;

          if (chance <= jTPCC.stockLevelWeight)
              return jTPCCTData.TT_STOCK_LEVEL;
          chance -= jTPCC.stockLevelWeight;

          if (chance <= jTPCC.deliveryWeight)
              return jTPCCTData.TT_DELIVERY;

          return jTPCCTData.TT_NEW_ORDER;

      } else if (benchmarkType.equals("smallbank")) {
            if (chance <= jTPCC.depositCheckingWeight)
                return jTPCCTData.TT_DEPOSIT_CHECKING;
            chance -= jTPCC.depositCheckingWeight;

            if (chance <= jTPCC.sendPaymentWeight)
                return jTPCCTData.TT_SEND_PAYMENT;
            chance -= jTPCC.sendPaymentWeight;

            if (chance <= jTPCC.transactSavingsWeight)
                return jTPCCTData.TT_TRANSACT_SAVINGS;
            chance -= jTPCC.transactSavingsWeight;

            if (chance <= jTPCC.writeCheckWeight)
                return jTPCCTData.TT_WRITE_CHECK;

            return jTPCCTData.TT_AMALGAMATE;
        } else {
            log.error("Unknown benchmark type: {}", benchmarkType);
            return -1;
      }
    }

    private long randomInt(long min, long max) {
      return (long) (this.random.nextDouble() * (max - min + 1) + min);
    }

    private double randomDouble() {
      return this.random.nextDouble();
    }

  }
}
