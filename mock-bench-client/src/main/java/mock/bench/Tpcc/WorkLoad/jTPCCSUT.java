package mock.bench.Tpcc.WorkLoad;

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * jTPCCSUT - The application/DB-connection-pool side.
 */
public class jTPCCSUT {
  private static Logger log = LogManager.getLogger(jTPCCSUT.class);
  private jTPCC gdata;
  private Thread sutThreads[];

  private jTPCCTDataList queue;

  public DeliveryScheduler deliveryBg;
  private Thread deliveryBgThread;

  public jTPCCSUT(jTPCC gdata) {
    this.gdata = gdata;
    this.sutThreads = new Thread[jTPCC.numSUTThreads];

    this.queue = new jTPCCTDataList();

    if (jTPCC.benchmarkType.equals("tpcc")){
        this.deliveryBg = new DeliveryScheduler();
        this.deliveryBgThread = new Thread(this.deliveryBg);
        this.deliveryBgThread.start();
    }
  }

  public void launchSUTThread(jTPCCTData tdata) {

    int t_id = tdata.term_w_id;
    SUTThread sut;

    try {
      sut = new SUTThread(t_id);
      sutThreads[t_id] = new Thread(sut);
      sutThreads[t_id].start();
    } catch (Exception ex) {
      log.error(ex.getMessage());
      log.info(ex);
    }
  }

  public void terminate() {
      if (jTPCC.benchmarkType.equals("tpcc")){
          try {
              deliveryBgThread.interrupt();
              deliveryBgThread.join();
          } catch (InterruptedException e) {

          }
      }


    synchronized (queue) {
      queue.truncate();

      for (int m = 0; m < jTPCC.numSUTThreads; m++) {
        jTPCCTData doneMsg = new jTPCCTData();
        doneMsg.trans_type = jTPCCTData.TT_DONE;
        queue.append(doneMsg);
      }

      queue.notify();
    }

    for (int m = 0; m < jTPCC.numSUTThreads; m++) {
      try {
        sutThreads[m].join();
      } catch (InterruptedException e) {
      }
    }
  }

  public void queueAppend(jTPCCTData tdata) {
    synchronized (queue) {
      queue.append(tdata);
      queue.notify();
    }
  }

  public void queuePrepend(jTPCCTData tdata) {
    synchronized (queue) {
      queue.prepend(tdata);
      queue.notify();
    }
  }

  private class SUTThread implements Runnable {
    private int t_id;
    private Random random;
    private jTPCCApplication application;

    public SUTThread(int t_id) {
      this.t_id = t_id;
      this.random = new Random(System.currentTimeMillis());
    }

    public void run() {
      jTPCCTData tdata;
      boolean reconnect = false;

      /*
       * Create and initialize the Application class. This connects to the database and creates all
       * the prepared statements. There isn't much we can (or should) do if that fails.
       * 初始化Application类，连接数据库准备预处理语句
       * 
       */
      try {
        this.application = gdata.getApplication();
        this.application.init(gdata, t_id);
      } catch (Exception e) {
        log.error("sut-{} Exception: Error1: {}", this.t_id, e.getMessage());
        log.info("sut-{} restarting SUT thread in 5 seconds (initial connect)", this.t_id);

        long now = System.currentTimeMillis();
        jTPCCTData sut_launch_tdata = new jTPCCTData();
        sut_launch_tdata.term_w_id = this.t_id;
        jTPCC.scheduler.at(now + 5000, jTPCCScheduler.SCHED_SUT_LAUNCH, sut_launch_tdata);

        return;
      }

      for (;;) {
        /*
         * Fetch the next entry from the work queue or wait for there to be one. This is the SUT's
         * queue where the scheduler sends terminal input screens for immediate execution.
         * 从工作队列中获取下一个条目，或者等待直到有条目可用。这是调度器发送终端输入屏幕以供立即执行的 SUT 队列。
         */
        synchronized (queue) {
          while ((tdata = queue.first()) == null) {
            try {
              queue.wait();
            } catch (InterruptedException e) {
              log.error("sut-{} InterruptedException: Error2: {}", this.t_id, e.getMessage());
              return;
            }
          }
          queue.remove(tdata);

          /* If there is more in the queue, notify the next. */
          if (queue.first() != null)
            queue.notify();
        }

        /* Special message type signaling that the benchmark is over. */
        if (tdata.trans_type == jTPCCTData.TT_DONE)
          break;

        /* Stamp when the SUT started processing this transaction. */
        tdata.trans_start = System.currentTimeMillis();

        /* Process the requested transaction on the database. */
        try {
          switch (tdata.trans_type) { // 处理事务请求
            case jTPCCTData.TT_NEW_ORDER:
              processNewOrder(tdata);
              break;
            case jTPCCTData.TT_PAYMENT:
              processPayment(tdata);
              break;
            case jTPCCTData.TT_ORDER_STATUS:
              processOrderStatus(tdata);
              break;
            case jTPCCTData.TT_STOCK_LEVEL:
              processStockLevel(tdata);
              break;
            case jTPCCTData.TT_DELIVERY_BG:
              processDeliveryBG(tdata);
              break;
            case jTPCCTData.TT_DELIVERY: // 交付
              processDelivery(tdata);
              break;

              case jTPCCTData.TT_AMALGAMATE:
                  processAmalgamate(tdata);
                  break;
              case jTPCCTData.TT_DEPOSIT_CHECKING:
                  processDepositChecking(tdata);
                  break;
              case jTPCCTData.TT_SEND_PAYMENT:
                  processSendPayment(tdata);
                  break;
              case jTPCCTData.TT_TRANSACT_SAVINGS:
                  processTransactSavings(tdata);
                  break;
              case jTPCCTData.TT_WRITE_CHECK:
                  processWriteCheck(tdata);
                  break;

            default:
                log.error("sut-{} unhandled Transaction type code {} in SUT", this.t_id, tdata.trans_type);
                break;
          }
        } catch (Exception e) {
          log.error("sut-{} Exception: Error3: {} ttype={}", this.t_id, e.getMessage(), tdata.trans_type); //报错
          e.printStackTrace();
//          System.out.println("sut-" + e.printStackTrace());
          tdata.trans_error = true;
          log.info(e);
          reconnect = false; // 关闭重连接
        }

        /*
         * Stamp the transaction end time into the terminal data.
         */
        tdata.trans_end = System.currentTimeMillis();

        /*
         * Send the result of the transaction back. In the case of the background part of the
         * DELIVERY, that is no longer the terminal, that queued the process, but the delivery
         * background queue system.
         */
        if (tdata.trans_type == jTPCCTData.TT_DELIVERY_BG) // 后台交付任务
          deliveryBg.jobDone(tdata);
        else
          gdata.monkeys.queueAppend(tdata); // 其他任务返给monkey进行处理

        /*
         * Handle restartSUTThreadProb. If not 0.0 it specifies the probability in % at which the
         * SUTThread causes the Application (for this thread) to disconnect and reconnect to the
         * database.
         */
        if (jTPCC.restartSUTThreadProb > 0.0 || reconnect) {
          if (random.nextDouble() <= (jTPCC.restartSUTThreadProb / 100.0) || reconnect) {
            log.info("sut-{} Reconnecting", this.t_id);
            reconnect = false;
            try { // 尝试重启当前连接
              this.application.finish();
              this.application.init(gdata, t_id);
            } catch (Exception e) {
              log.error("sut-{} Exception: Error4: {}", this.t_id, e.getMessage());
              log.info("sut-{} restarting SUT thread in 5 seconds (failed transaction)", this.t_id);

              long now = System.currentTimeMillis();
              jTPCCTData sut_launch_tdata = new jTPCCTData();
              sut_launch_tdata.term_w_id = this.t_id;
              jTPCC.scheduler.at(now + 5000, jTPCCScheduler.SCHED_SUT_LAUNCH, sut_launch_tdata); //否则重启当前线程

              return;
            }
          }
        }
      }

      /*
       * Let the application implementation clean up and disconnect.
       */
      try {
        this.application.finish();
      } catch (Exception e) {
      }
    }

    private void processNewOrder(jTPCCTData tdata) throws Exception {
      application.executeNewOrder(tdata.new_order, tdata.trans_rbk);
    }

    private void processPayment(jTPCCTData tdata) throws Exception {
      application.executePayment(tdata.payment);
    }

    private void processOrderStatus(jTPCCTData tdata) throws Exception {
      application.executeOrderStatus(tdata.order_status);
    }

    private void processStockLevel(jTPCCTData tdata) throws Exception {
      application.executeStockLevel(tdata.stock_level);
    }

    private void processDelivery(jTPCCTData tdata) {
      jTPCCTData.DeliveryData screen = tdata.delivery;

      deliveryBg.queueAppend(tdata);
      screen.execution_status = new String("Delivery has been queued");
    }

    private void processDeliveryBG(jTPCCTData tdata) throws Exception {
      application.executeDeliveryBG(tdata.delivery_bg);
    }

    private void processAmalgamate(jTPCCTData tdata) throws Exception {
        application.executeAmalgamate(tdata.amalgamate);
    }

    private void processDepositChecking(jTPCCTData tdata) throws Exception {
        application.executeDepositChecking(tdata.deposit_checking);
    }

    private void processSendPayment(jTPCCTData tdata) throws Exception {
        application.executeSendPayment(tdata.send_payment);
    }

    private void processTransactSavings(jTPCCTData tdata) throws Exception {
        application.executeTransactSavings(tdata.transact_savings);
    }

    private void processWriteCheck(jTPCCTData tdata) throws Exception {
        application.executeWriteCheck(tdata.write_check);
    }

//    private long randomInt(long min, long max) {
//      return (long) (random.nextDouble() * (max - min + 1) + min);
//    }

  }

  public class DeliveryScheduler implements Runnable {
    private DeliveryEntry queueHead;
    private DeliveryEntry queueTail;
    private Object lock;
    public jTPCCResult result;
    private int warehouseBusy[];
    private int deliveriesBusy = 0;

    public DeliveryScheduler() {
      lock = new Object();
      result = new jTPCCResult();
      warehouseBusy = new int[jTPCC.numWarehouses];
      for (int i = 0; i < jTPCC.numWarehouses; i++)
        warehouseBusy[i] = 0;
    }

    public void run() {
      for (;;) {
        synchronized (lock) {
          DeliveryEntry ent;
          DeliveryEntry next = null;

          for (ent = queueHead; ent != null; ent = next) {
            /*
             * Bail out of here if the maximum allowed SUT theads are busy with deliviries.
             * But only do that if we actually reserve some SUT threads for foreground
             * transactions.
             */
            if (jTPCC.maxDeliveryBGThreads < jTPCC.numSUTThreads &&
                deliveriesBusy >= jTPCC.maxDeliveryBGThreads)
              break;

            next = ent.next;

            if (jTPCC.maxDeliveryBGPerWH <= 0 ||
                warehouseBusy[ent.w_id - 1] < jTPCC.maxDeliveryBGPerWH) {
              /*
               * This warehouse does not currently have the maximum allowed delivery transactions in
               * progress. Create a terminal data instance and fill it with the delivery background
               * information. Then send it directly into the SUT worker queue.
               */
              jTPCCTData tdata = new jTPCCTData();

              tdata.trans_type = jTPCCTData.TT_DELIVERY_BG;
              tdata.trans_due = ent.queue_time;
              tdata.trans_start = 0;
              tdata.trans_end = 0;
              tdata.trans_rbk = false;
              tdata.trans_error = false;

              tdata.term_w_id = ent.w_id;
              tdata.term_d_id = 0;

              tdata.delivery_bg = tdata.DeliveryBGData();

              tdata.delivery_bg.w_id = ent.w_id;
              tdata.delivery_bg.o_carrier_id = ent.o_carrier_id;
              tdata.delivery_bg.ol_delivery_d = ent.ol_delivery_d;

              gdata.systemUnderTest.queuePrepend(tdata);

              /* Remove this queue entry. */
              if (ent.prev == null)
                queueHead = ent.next;
              else
                ent.prev.next = ent.next;
              if (ent.next == null)
                queueTail = ent.prev;
              else
                ent.next.prev = ent.prev;

              /* Bump the busy counters */
              warehouseBusy[ent.w_id - 1]++;
              deliveriesBusy++;
            }
          }

          /*
           * We did a pass over the queue and pushed whatever was possible into the SUT workers. Now
           * we need to wait for something to change. Either a new request for delivery comes in, or
           * a previous one finishes and makes the warehouse idle.
           */
          try {
            lock.wait();
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }

    public void queueAppend(jTPCCTData tdata) {
      DeliveryEntry ent = new DeliveryEntry();

      ent.queue_time = tdata.trans_start; // This is when the
      // SUT started working
      // on the foreground
      // transaction.
      ent.w_id = tdata.delivery.w_id;
      ent.o_carrier_id = tdata.delivery.o_carrier_id;
      ent.ol_delivery_d = new java.sql.Timestamp(System.currentTimeMillis()).toString();

      synchronized (lock) {
        if (queueHead == null) {
          ent.prev = null;
          ent.next = null;
          queueHead = ent;
          queueTail = ent;
        } else {
          ent.prev = queueTail;
          ent.next = null;
          queueTail.next = ent;
          queueTail = ent;
        }

        lock.notify();
      }
    }

    public void jobDone(jTPCCTData tdata) {
      result.collect(tdata);

      synchronized (lock) {
        /* Decrease the busy counters */
        warehouseBusy[tdata.term_w_id - 1]--;
        deliveriesBusy--;
        lock.notify();
      }
    }
  }

  private class DeliveryEntry {
    public long queue_time;

    public int w_id;
    public int o_carrier_id;
    public String ol_delivery_d;

    public DeliveryEntry prev;
    public DeliveryEntry next;
  }
}


