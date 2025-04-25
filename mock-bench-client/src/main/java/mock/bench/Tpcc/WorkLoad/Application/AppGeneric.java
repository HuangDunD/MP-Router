package mock.bench.Tpcc.WorkLoad.Application;

import mock.bench.Tpcc.Tool.ExecClient;
import mock.bench.Tpcc.Tool.jTPCCRandom;
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
//    Payment payment = new Payment();
//    String payment_txn = payment.executePayment(payMent);
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
}
