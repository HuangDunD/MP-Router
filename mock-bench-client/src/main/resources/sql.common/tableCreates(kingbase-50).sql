create table bmsql_config (
                              cfg_name    varchar(30) primary key,
                              cfg_value   varchar(50)
);

create table bmsql_warehouse (
                                 w_id        integer   not null,
                                 w_ytd       decimal(12,2),
                                 w_tax       decimal(4,4),
                                 w_name      varchar(10),
                                 w_street_1  varchar(20),
                                 w_street_2  varchar(20),
                                 w_city      varchar(20),
                                 w_state     char(2),
                                 w_zip       char(9)
) PARTITION BY RANGE (w_id);



create table bmsql_district (
                                d_w_id       integer       not null,
                                d_id         integer       not null,
                                d_ytd        decimal(12,2),
                                d_tax        decimal(4,4),
                                d_next_o_id  integer,
                                d_name       varchar(10),
                                d_street_1   varchar(20),
                                d_street_2   varchar(20),
                                d_city       varchar(20),
                                d_state      char(2),
                                d_zip        char(9)
) PARTITION BY RANGE (d_w_id);



create table bmsql_customer (
                                c_w_id         integer        not null,
                                c_d_id         integer        not null,
                                c_id           integer        not null,
                                c_discount     decimal(4,4),
                                c_credit       char(2),
                                c_last         varchar(16),
                                c_first        varchar(16),
                                c_credit_lim   decimal(12,2),
                                c_balance      decimal(12,2),
                                c_ytd_payment  decimal(12,2),
                                c_payment_cnt  integer,
                                c_delivery_cnt integer,
                                c_street_1     varchar(20),
                                c_street_2     varchar(20),
                                c_city         varchar(20),
                                c_state        char(2),
                                c_zip          char(9),
                                c_phone        char(16),
                                c_since        timestamp,
                                c_middle       char(2),
                                c_data         varchar(500)
) PARTITION BY RANGE (c_w_id);

create sequence bmsql_hist_id_seq;

create table bmsql_history (
                               hist_id  integer,
                               h_c_id   integer,
                               h_c_d_id integer,
                               h_c_w_id integer,
                               h_d_id   integer,
                               h_w_id   integer,
                               h_date   timestamp,
                               h_amount decimal(6,2),
                               h_data   varchar(24)
) PARTITION BY RANGE (h_w_id);

create table bmsql_new_order (
                                 no_w_id  integer   not null,
                                 no_d_id  integer   not null,
                                 no_o_id  integer   not null
) PARTITION BY RANGE (no_w_id);

create table bmsql_oorder (
                              o_w_id       integer      not null,
                              o_d_id       integer      not null,
                              o_id         integer      not null,
                              o_c_id       integer,
                              o_carrier_id integer,
                              o_ol_cnt     integer,
                              o_all_local  integer,
                              o_entry_d    timestamp
) PARTITION BY RANGE (o_w_id);

create table bmsql_order_line (
                                  ol_w_id         integer   not null,
                                  ol_d_id         integer   not null,
                                  ol_o_id         integer   not null,
                                  ol_number       integer   not null,
                                  ol_i_id         integer   not null,
                                  ol_delivery_d   timestamp,
                                  ol_amount       decimal(6,2),
                                  ol_supply_w_id  integer,
                                  ol_quantity     integer,
                                  ol_dist_info    char(24)
) PARTITION BY RANGE (ol_w_id);

create table bmsql_item (
                            i_id     integer      not null,
                            i_name   varchar(24),
                            i_price  decimal(5,2),
                            i_data   varchar(50),
                            i_im_id  integer
);

create table bmsql_stock (
                             s_w_id       integer       not null,
                             s_i_id       integer       not null,
                             s_quantity   integer,
                             s_ytd        integer,
                             s_order_cnt  integer,
                             s_remote_cnt integer,
                             s_data       varchar(50),
                             s_dist_01    char(24),
                             s_dist_02    char(24),
                             s_dist_03    char(24),
                             s_dist_04    char(24),
                             s_dist_05    char(24),
                             s_dist_06    char(24),
                             s_dist_07    char(24),
                             s_dist_08    char(24),
                             s_dist_09    char(24),
                             s_dist_10    char(24)
) PARTITION BY RANGE (s_w_id);

-- Table: bmsql_customer
CREATE TABLE bmsql_customer_1 PARTITION OF bmsql_customer FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_customer_51 PARTITION OF bmsql_customer FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_customer_101 PARTITION OF bmsql_customer FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_customer_151 PARTITION OF bmsql_customer FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_customer_201 PARTITION OF bmsql_customer FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_customer_251 PARTITION OF bmsql_customer FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_customer_301 PARTITION OF bmsql_customer FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_customer_351 PARTITION OF bmsql_customer FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_customer_401 PARTITION OF bmsql_customer FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_customer_451 PARTITION OF bmsql_customer FOR VALUES FROM (451) TO (501);

-- Table: bmsql_district
CREATE TABLE bmsql_district_1 PARTITION OF bmsql_district FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_district_51 PARTITION OF bmsql_district FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_district_101 PARTITION OF bmsql_district FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_district_151 PARTITION OF bmsql_district FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_district_201 PARTITION OF bmsql_district FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_district_251 PARTITION OF bmsql_district FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_district_301 PARTITION OF bmsql_district FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_district_351 PARTITION OF bmsql_district FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_district_401 PARTITION OF bmsql_district FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_district_451 PARTITION OF bmsql_district FOR VALUES FROM (451) TO (501);

-- Table: bmsql_new_order
CREATE TABLE bmsql_new_order_1 PARTITION OF bmsql_new_order FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_new_order_51 PARTITION OF bmsql_new_order FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_new_order_101 PARTITION OF bmsql_new_order FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_new_order_151 PARTITION OF bmsql_new_order FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_new_order_201 PARTITION OF bmsql_new_order FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_new_order_251 PARTITION OF bmsql_new_order FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_new_order_301 PARTITION OF bmsql_new_order FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_new_order_351 PARTITION OF bmsql_new_order FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_new_order_401 PARTITION OF bmsql_new_order FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_new_order_451 PARTITION OF bmsql_new_order FOR VALUES FROM (451) TO (501);

-- Table: bmsql_oorder
CREATE TABLE bmsql_oorder_1 PARTITION OF bmsql_oorder FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_oorder_51 PARTITION OF bmsql_oorder FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_oorder_101 PARTITION OF bmsql_oorder FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_oorder_151 PARTITION OF bmsql_oorder FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_oorder_201 PARTITION OF bmsql_oorder FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_oorder_251 PARTITION OF bmsql_oorder FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_oorder_301 PARTITION OF bmsql_oorder FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_oorder_351 PARTITION OF bmsql_oorder FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_oorder_401 PARTITION OF bmsql_oorder FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_oorder_451 PARTITION OF bmsql_oorder FOR VALUES FROM (451) TO (501);

-- Table: bmsql_order_line
CREATE TABLE bmsql_order_line_1 PARTITION OF bmsql_order_line FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_order_line_51 PARTITION OF bmsql_order_line FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_order_line_101 PARTITION OF bmsql_order_line FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_order_line_151 PARTITION OF bmsql_order_line FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_order_line_201 PARTITION OF bmsql_order_line FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_order_line_251 PARTITION OF bmsql_order_line FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_order_line_301 PARTITION OF bmsql_order_line FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_order_line_351 PARTITION OF bmsql_order_line FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_order_line_401 PARTITION OF bmsql_order_line FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_order_line_451 PARTITION OF bmsql_order_line FOR VALUES FROM (451) TO (501);

-- Table: bmsql_stock
CREATE TABLE bmsql_stock_1 PARTITION OF bmsql_stock FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_stock_51 PARTITION OF bmsql_stock FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_stock_101 PARTITION OF bmsql_stock FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_stock_151 PARTITION OF bmsql_stock FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_stock_201 PARTITION OF bmsql_stock FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_stock_251 PARTITION OF bmsql_stock FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_stock_301 PARTITION OF bmsql_stock FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_stock_351 PARTITION OF bmsql_stock FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_stock_401 PARTITION OF bmsql_stock FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_stock_451 PARTITION OF bmsql_stock FOR VALUES FROM (451) TO (501);

-- Table: bmsql_warehouse
CREATE TABLE bmsql_warehouse_1 PARTITION OF bmsql_warehouse FOR VALUES FROM (1) TO (51);
CREATE TABLE bmsql_warehouse_51 PARTITION OF bmsql_warehouse FOR VALUES FROM (51) TO (101);
CREATE TABLE bmsql_warehouse_101 PARTITION OF bmsql_warehouse FOR VALUES FROM (101) TO (151);
CREATE TABLE bmsql_warehouse_151 PARTITION OF bmsql_warehouse FOR VALUES FROM (151) TO (201);
CREATE TABLE bmsql_warehouse_201 PARTITION OF bmsql_warehouse FOR VALUES FROM (201) TO (251);
CREATE TABLE bmsql_warehouse_251 PARTITION OF bmsql_warehouse FOR VALUES FROM (251) TO (301);
CREATE TABLE bmsql_warehouse_301 PARTITION OF bmsql_warehouse FOR VALUES FROM (301) TO (351);
CREATE TABLE bmsql_warehouse_351 PARTITION OF bmsql_warehouse FOR VALUES FROM (351) TO (401);
CREATE TABLE bmsql_warehouse_401 PARTITION OF bmsql_warehouse FOR VALUES FROM (401) TO (451);
CREATE TABLE bmsql_warehouse_451 PARTITION OF bmsql_warehouse FOR VALUES FROM (451) TO (501);

-- Table: bmsql_history
CREATE TABLE bmsql_history_1 PARTITION OF bmsql_history FOR VALUES FROM (0) TO (251);
CREATE TABLE bmsql_history_2 PARTITION OF bmsql_history FOR VALUES FROM (251) TO (501);