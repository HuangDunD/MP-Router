package mock.bench.Tpcc.WorkLoad.Application;

import mock.bench.Tpcc.Tool.ExecClient;
import mock.bench.Tpcc.Tool.jTPCCRandom;
import mock.bench.Tpcc.WorkLoad.Application.SmallBank.*;
import mock.bench.Tpcc.WorkLoad.Application.Tpcc.*;
import mock.bench.Tpcc.WorkLoad.jTPCC;
import mock.bench.Tpcc.WorkLoad.jTPCCApplication;
import mock.bench.Tpcc.WorkLoad.jTPCCTData;

public class AppGeneric extends jTPCCApplication {
  private ExecClient execClient;
  private jTPCCRandom rnd;

  public void init(jTPCC gdata, int sut_id) throws Exception {
    this.execClient = new ExecClient(gdata.host, gdata.port);
    this.rnd = gdata.rnd.newRandom();
  }

  public void finish() throws Exception {
    this.execClient.close();
  }

  public void executeNewOrder(jTPCCTData.NewOrderData newOrder, boolean trans_rbk) throws Exception {
    execClient.sendTxn(NewOrder.executeNewOrder(newOrder, rnd));
  }

  public void executePayment(jTPCCTData.PaymentData payMent) throws Exception {
    execClient.sendTxn(Payment.executePayment(payMent, rnd));
  }

  public void executeOrderStatus(jTPCCTData.OrderStatusData orderStatus) throws Exception {
    execClient.sendTxn(OrderStatus.executeOrderStatus(orderStatus, rnd));
  }

  public void executeStockLevel(jTPCCTData.StockLevelData stockLevel) throws Exception {
    execClient.sendTxn(StockLevel.executeStockLevel(stockLevel));
  }

  public void executeDeliveryBG(jTPCCTData.DeliveryBGData deliveryBG) throws Exception {
    execClient.sendTxn(DeliverBG.executeDeliveryBG(deliveryBG, rnd));
  }

    public void executeAmalgamate(jTPCCTData.AmalgamateData amalgamate) throws Exception {
        execClient.sendTxn(Amalgamate.executeAmalgamate(amalgamate, rnd));
    }

    public void executeDepositChecking(jTPCCTData.DepositCheckingData transactSavings) throws Exception {
        execClient.sendTxn(DepositChecking.executeDepositChecking(transactSavings, rnd));
    }

    public void executeSendPayment(jTPCCTData.SendPaymentData sendPayment) throws Exception {
        execClient.sendTxn(SendPayment.executeSendPayment(sendPayment, rnd));
    }

    public void executeTransactSavings(jTPCCTData.TransactSavingsData transactSavings) throws Exception {
        execClient.sendTxn(TransactSavings.executeTransactSavings(transactSavings, rnd));
    }

    public void executeWriteCheck(jTPCCTData.WriteCheckData writeCheck) throws Exception {
        execClient.sendTxn(WriteCheck.executeWriteCheck(writeCheck, rnd));
    }

}
