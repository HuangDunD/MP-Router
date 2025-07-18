package mock.bench.Tpcc.WorkLoad;

import mock.bench.Tpcc.WorkLoad.Application.SmallBank.TransactSavings;

/*
 * jTPCCTData - The simulated terminal input/output data.
 */
public class jTPCCTData {
    public final static int TT_AMALGAMATE = 0, TT_DEPOSIT_CHECKING = 1, TT_SEND_PAYMENT = 2, TT_TRANSACT_SAVINGS = 3, TT_WRITE_CHECK = 4,
          TT_NEW_ORDER = 5, TT_PAYMENT = 6, TT_ORDER_STATUS = 7, TT_STOCK_LEVEL = 8, TT_DELIVERY = 9, TT_DELIVERY_BG = 10,
          TT_NONE = 11, TT_DONE = 12;

    public final static String trans_type_names[] = {"AMALGAMATE", "DEPOSIT_CHECKING", "SEND_PAYMENT", "TRANSACT_SAVINGS", "WRITE_CHECK",
          "NEW_ORDER", "PAYMENT", "ORDER_STATUS", "STOCK_LEVEL", "DELIVERY", "DELIVERY_BG",
          "NONE", "DONE"
    }; // 事务的类型

    public int sched_code;
    public long sched_fuzz;
    public jTPCCTData term_left;
    public jTPCCTData term_right;
    public int tree_height;

    public int trans_type;
    public long trans_due;
    public long trans_start;
    public long trans_end;
    public boolean trans_rbk;
    public boolean trans_error;
    public String trans_error_reason = null;

    public int term_w_id = 0;
    public int term_d_id = 0;

    public NewOrderData new_order = null;
    public PaymentData payment = null;
    public OrderStatusData order_status = null;
    public StockLevelData stock_level = null;
    public DeliveryData delivery = null;
    public DeliveryBGData delivery_bg = null;

    public AmalgamateData amalgamate = null;
    public DepositCheckingData deposit_checking = null;
    public SendPaymentData send_payment = null;
    public TransactSavingsData transact_savings = null;
    public WriteCheckData write_check = null;


    public String dumpHdr() {
    return new String("TData(" + "term_w_id=" + term_w_id + " term_d_id=" + term_d_id
        + " sched_code=" + sched_code + " trans_type=" + trans_type + " trans_due=" + trans_due
        + " trans_end=" + trans_end + " sched_fuzz=" + sched_fuzz + ")");
    }

    public NewOrderData NewOrderData() {
    return new NewOrderData();
    }

    public class NewOrderData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int c_id;

    public int ol_supply_w_id[] = new int[15];
    public int ol_i_id[] = new int[15];
    public int ol_quantity[] = new int[15];

    /* terminal output data */
    public String c_last;
    public String c_credit;
    public double c_discount;
    public double w_tax;
    public double d_tax;
    public int o_ol_cnt;
    public int o_id;
    public String o_entry_d;
    public double total_amount;
    public String execution_status;

    public String i_name[] = new String[15];
    public int s_quantity[] = new int[15];
    public String brand_generic[] = new String[15];
    public double i_price[] = new double[15];
    public double ol_amount[] = new double[15];
    }

    public PaymentData PaymentData() {
    return new PaymentData();
    }

    public class PaymentData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int c_id;
    public int c_d_id;
    public int c_w_id;
    public String c_last;
    public double h_amount;

    /* terminal output data */
    public String w_name;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public String w_zip;
    public String d_name;
    public String d_street_1;
    public String d_street_2;
    public String d_city;
    public String d_state;
    public String d_zip;
    public String c_first;
    public String c_middle;
    public String c_street_1;
    public String c_street_2;
    public String c_city;
    public String c_state;
    public String c_zip;
    public String c_phone;
    public String c_since;
    public String c_credit;
    public double c_credit_lim;
    public double c_discount;
    public double c_balance;
    public String c_data;
    public String h_date;
    }

    public OrderStatusData OrderStatusData() {
    return new OrderStatusData();
    }

    public class OrderStatusData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int c_id;
    public String c_last;

    /* terminal output data */
    public String c_first;
    public String c_middle;
    public double c_balance;
    public int o_id;
    public String o_entry_d;
    public int o_carrier_id;

    public int ol_supply_w_id[] = new int[15];
    public int ol_i_id[] = new int[15];
    public int ol_quantity[] = new int[15];
    public double ol_amount[] = new double[15];
    public String ol_delivery_d[] = new String[15];
    }

    public StockLevelData StockLevelData() {
    return new StockLevelData();
    }

    public class StockLevelData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int threshold;

    /* terminal output data */
    public int low_stock;
    }

    public DeliveryData DeliveryData() {
    return new DeliveryData();
    }

    public class DeliveryData {
    /* terminal input data */
    public int w_id;
    public int o_carrier_id;

    /* terminal output data */
    public String execution_status;
    }

    public DeliveryBGData DeliveryBGData() {
    return new DeliveryBGData();
    }

    public class DeliveryBGData {
    /* DELIVERY_BG data */
    public int w_id;
    public int o_carrier_id;
    public String ol_delivery_d;

    public int delivered_o_id[];
    }

    // smallbank transaction data
    public AmalgamateData AmalgamateData() {return new AmalgamateData();}

    public class AmalgamateData {
      public long acctID0;
      public long acctID1;
      public double updateValue;
    }

    public DepositCheckingData DepositCheckingData() {return new DepositCheckingData();}

    public class DepositCheckingData {
        public long acctID;
        public double updateValue;
    }

    public SendPaymentData SendPaymentData() {return new SendPaymentData();}

    public class SendPaymentData {
        public long acctID0;
        public long acctID1;
        public double updateValue;
    }

    public TransactSavingsData TransactSavingsData() {return new TransactSavingsData();}

    public class TransactSavingsData {
        public long acctID;
        public double updateValue;
    }

    public WriteCheckData WriteCheckData() {return new WriteCheckData();}

    public class WriteCheckData {
        public long acctID;
        public double updateValue;
    }
}
