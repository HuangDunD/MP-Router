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
CREATE TABLE bmsql_customer_1 PARTITION OF bmsql_customer FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_customer_11 PARTITION OF bmsql_customer FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_customer_21 PARTITION OF bmsql_customer FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_customer_31 PARTITION OF bmsql_customer FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_customer_41 PARTITION OF bmsql_customer FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_customer_51 PARTITION OF bmsql_customer FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_customer_61 PARTITION OF bmsql_customer FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_customer_71 PARTITION OF bmsql_customer FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_customer_81 PARTITION OF bmsql_customer FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_customer_91 PARTITION OF bmsql_customer FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_customer_101 PARTITION OF bmsql_customer FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_customer_111 PARTITION OF bmsql_customer FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_customer_121 PARTITION OF bmsql_customer FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_customer_131 PARTITION OF bmsql_customer FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_customer_141 PARTITION OF bmsql_customer FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_customer_151 PARTITION OF bmsql_customer FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_customer_161 PARTITION OF bmsql_customer FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_customer_171 PARTITION OF bmsql_customer FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_customer_181 PARTITION OF bmsql_customer FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_customer_191 PARTITION OF bmsql_customer FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_customer_201 PARTITION OF bmsql_customer FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_customer_211 PARTITION OF bmsql_customer FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_customer_221 PARTITION OF bmsql_customer FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_customer_231 PARTITION OF bmsql_customer FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_customer_241 PARTITION OF bmsql_customer FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_customer_251 PARTITION OF bmsql_customer FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_customer_261 PARTITION OF bmsql_customer FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_customer_271 PARTITION OF bmsql_customer FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_customer_281 PARTITION OF bmsql_customer FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_customer_291 PARTITION OF bmsql_customer FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_customer_301 PARTITION OF bmsql_customer FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_customer_311 PARTITION OF bmsql_customer FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_customer_321 PARTITION OF bmsql_customer FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_customer_331 PARTITION OF bmsql_customer FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_customer_341 PARTITION OF bmsql_customer FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_customer_351 PARTITION OF bmsql_customer FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_customer_361 PARTITION OF bmsql_customer FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_customer_371 PARTITION OF bmsql_customer FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_customer_381 PARTITION OF bmsql_customer FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_customer_391 PARTITION OF bmsql_customer FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_customer_401 PARTITION OF bmsql_customer FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_customer_411 PARTITION OF bmsql_customer FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_customer_421 PARTITION OF bmsql_customer FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_customer_431 PARTITION OF bmsql_customer FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_customer_441 PARTITION OF bmsql_customer FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_customer_451 PARTITION OF bmsql_customer FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_customer_461 PARTITION OF bmsql_customer FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_customer_471 PARTITION OF bmsql_customer FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_customer_481 PARTITION OF bmsql_customer FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_customer_491 PARTITION OF bmsql_customer FOR VALUES FROM (491) TO (501);

-- Table: bmsql_district
CREATE TABLE bmsql_district_1 PARTITION OF bmsql_district FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_district_11 PARTITION OF bmsql_district FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_district_21 PARTITION OF bmsql_district FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_district_31 PARTITION OF bmsql_district FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_district_41 PARTITION OF bmsql_district FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_district_51 PARTITION OF bmsql_district FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_district_61 PARTITION OF bmsql_district FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_district_71 PARTITION OF bmsql_district FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_district_81 PARTITION OF bmsql_district FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_district_91 PARTITION OF bmsql_district FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_district_101 PARTITION OF bmsql_district FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_district_111 PARTITION OF bmsql_district FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_district_121 PARTITION OF bmsql_district FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_district_131 PARTITION OF bmsql_district FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_district_141 PARTITION OF bmsql_district FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_district_151 PARTITION OF bmsql_district FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_district_161 PARTITION OF bmsql_district FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_district_171 PARTITION OF bmsql_district FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_district_181 PARTITION OF bmsql_district FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_district_191 PARTITION OF bmsql_district FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_district_201 PARTITION OF bmsql_district FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_district_211 PARTITION OF bmsql_district FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_district_221 PARTITION OF bmsql_district FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_district_231 PARTITION OF bmsql_district FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_district_241 PARTITION OF bmsql_district FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_district_251 PARTITION OF bmsql_district FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_district_261 PARTITION OF bmsql_district FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_district_271 PARTITION OF bmsql_district FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_district_281 PARTITION OF bmsql_district FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_district_291 PARTITION OF bmsql_district FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_district_301 PARTITION OF bmsql_district FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_district_311 PARTITION OF bmsql_district FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_district_321 PARTITION OF bmsql_district FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_district_331 PARTITION OF bmsql_district FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_district_341 PARTITION OF bmsql_district FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_district_351 PARTITION OF bmsql_district FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_district_361 PARTITION OF bmsql_district FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_district_371 PARTITION OF bmsql_district FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_district_381 PARTITION OF bmsql_district FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_district_391 PARTITION OF bmsql_district FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_district_401 PARTITION OF bmsql_district FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_district_411 PARTITION OF bmsql_district FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_district_421 PARTITION OF bmsql_district FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_district_431 PARTITION OF bmsql_district FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_district_441 PARTITION OF bmsql_district FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_district_451 PARTITION OF bmsql_district FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_district_461 PARTITION OF bmsql_district FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_district_471 PARTITION OF bmsql_district FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_district_481 PARTITION OF bmsql_district FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_district_491 PARTITION OF bmsql_district FOR VALUES FROM (491) TO (501);

-- Table: bmsql_new_order
CREATE TABLE bmsql_new_order_1 PARTITION OF bmsql_new_order FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_new_order_11 PARTITION OF bmsql_new_order FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_new_order_21 PARTITION OF bmsql_new_order FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_new_order_31 PARTITION OF bmsql_new_order FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_new_order_41 PARTITION OF bmsql_new_order FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_new_order_51 PARTITION OF bmsql_new_order FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_new_order_61 PARTITION OF bmsql_new_order FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_new_order_71 PARTITION OF bmsql_new_order FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_new_order_81 PARTITION OF bmsql_new_order FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_new_order_91 PARTITION OF bmsql_new_order FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_new_order_101 PARTITION OF bmsql_new_order FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_new_order_111 PARTITION OF bmsql_new_order FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_new_order_121 PARTITION OF bmsql_new_order FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_new_order_131 PARTITION OF bmsql_new_order FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_new_order_141 PARTITION OF bmsql_new_order FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_new_order_151 PARTITION OF bmsql_new_order FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_new_order_161 PARTITION OF bmsql_new_order FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_new_order_171 PARTITION OF bmsql_new_order FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_new_order_181 PARTITION OF bmsql_new_order FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_new_order_191 PARTITION OF bmsql_new_order FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_new_order_201 PARTITION OF bmsql_new_order FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_new_order_211 PARTITION OF bmsql_new_order FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_new_order_221 PARTITION OF bmsql_new_order FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_new_order_231 PARTITION OF bmsql_new_order FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_new_order_241 PARTITION OF bmsql_new_order FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_new_order_251 PARTITION OF bmsql_new_order FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_new_order_261 PARTITION OF bmsql_new_order FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_new_order_271 PARTITION OF bmsql_new_order FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_new_order_281 PARTITION OF bmsql_new_order FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_new_order_291 PARTITION OF bmsql_new_order FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_new_order_301 PARTITION OF bmsql_new_order FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_new_order_311 PARTITION OF bmsql_new_order FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_new_order_321 PARTITION OF bmsql_new_order FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_new_order_331 PARTITION OF bmsql_new_order FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_new_order_341 PARTITION OF bmsql_new_order FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_new_order_351 PARTITION OF bmsql_new_order FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_new_order_361 PARTITION OF bmsql_new_order FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_new_order_371 PARTITION OF bmsql_new_order FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_new_order_381 PARTITION OF bmsql_new_order FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_new_order_391 PARTITION OF bmsql_new_order FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_new_order_401 PARTITION OF bmsql_new_order FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_new_order_411 PARTITION OF bmsql_new_order FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_new_order_421 PARTITION OF bmsql_new_order FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_new_order_431 PARTITION OF bmsql_new_order FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_new_order_441 PARTITION OF bmsql_new_order FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_new_order_451 PARTITION OF bmsql_new_order FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_new_order_461 PARTITION OF bmsql_new_order FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_new_order_471 PARTITION OF bmsql_new_order FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_new_order_481 PARTITION OF bmsql_new_order FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_new_order_491 PARTITION OF bmsql_new_order FOR VALUES FROM (491) TO (501);

-- Table: bmsql_oorder
CREATE TABLE bmsql_oorder_1 PARTITION OF bmsql_oorder FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_oorder_11 PARTITION OF bmsql_oorder FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_oorder_21 PARTITION OF bmsql_oorder FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_oorder_31 PARTITION OF bmsql_oorder FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_oorder_41 PARTITION OF bmsql_oorder FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_oorder_51 PARTITION OF bmsql_oorder FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_oorder_61 PARTITION OF bmsql_oorder FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_oorder_71 PARTITION OF bmsql_oorder FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_oorder_81 PARTITION OF bmsql_oorder FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_oorder_91 PARTITION OF bmsql_oorder FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_oorder_101 PARTITION OF bmsql_oorder FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_oorder_111 PARTITION OF bmsql_oorder FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_oorder_121 PARTITION OF bmsql_oorder FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_oorder_131 PARTITION OF bmsql_oorder FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_oorder_141 PARTITION OF bmsql_oorder FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_oorder_151 PARTITION OF bmsql_oorder FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_oorder_161 PARTITION OF bmsql_oorder FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_oorder_171 PARTITION OF bmsql_oorder FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_oorder_181 PARTITION OF bmsql_oorder FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_oorder_191 PARTITION OF bmsql_oorder FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_oorder_201 PARTITION OF bmsql_oorder FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_oorder_211 PARTITION OF bmsql_oorder FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_oorder_221 PARTITION OF bmsql_oorder FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_oorder_231 PARTITION OF bmsql_oorder FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_oorder_241 PARTITION OF bmsql_oorder FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_oorder_251 PARTITION OF bmsql_oorder FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_oorder_261 PARTITION OF bmsql_oorder FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_oorder_271 PARTITION OF bmsql_oorder FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_oorder_281 PARTITION OF bmsql_oorder FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_oorder_291 PARTITION OF bmsql_oorder FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_oorder_301 PARTITION OF bmsql_oorder FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_oorder_311 PARTITION OF bmsql_oorder FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_oorder_321 PARTITION OF bmsql_oorder FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_oorder_331 PARTITION OF bmsql_oorder FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_oorder_341 PARTITION OF bmsql_oorder FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_oorder_351 PARTITION OF bmsql_oorder FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_oorder_361 PARTITION OF bmsql_oorder FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_oorder_371 PARTITION OF bmsql_oorder FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_oorder_381 PARTITION OF bmsql_oorder FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_oorder_391 PARTITION OF bmsql_oorder FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_oorder_401 PARTITION OF bmsql_oorder FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_oorder_411 PARTITION OF bmsql_oorder FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_oorder_421 PARTITION OF bmsql_oorder FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_oorder_431 PARTITION OF bmsql_oorder FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_oorder_441 PARTITION OF bmsql_oorder FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_oorder_451 PARTITION OF bmsql_oorder FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_oorder_461 PARTITION OF bmsql_oorder FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_oorder_471 PARTITION OF bmsql_oorder FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_oorder_481 PARTITION OF bmsql_oorder FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_oorder_491 PARTITION OF bmsql_oorder FOR VALUES FROM (491) TO (501);

-- Table: bmsql_order_line
CREATE TABLE bmsql_order_line_1 PARTITION OF bmsql_order_line FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_order_line_11 PARTITION OF bmsql_order_line FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_order_line_21 PARTITION OF bmsql_order_line FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_order_line_31 PARTITION OF bmsql_order_line FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_order_line_41 PARTITION OF bmsql_order_line FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_order_line_51 PARTITION OF bmsql_order_line FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_order_line_61 PARTITION OF bmsql_order_line FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_order_line_71 PARTITION OF bmsql_order_line FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_order_line_81 PARTITION OF bmsql_order_line FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_order_line_91 PARTITION OF bmsql_order_line FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_order_line_101 PARTITION OF bmsql_order_line FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_order_line_111 PARTITION OF bmsql_order_line FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_order_line_121 PARTITION OF bmsql_order_line FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_order_line_131 PARTITION OF bmsql_order_line FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_order_line_141 PARTITION OF bmsql_order_line FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_order_line_151 PARTITION OF bmsql_order_line FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_order_line_161 PARTITION OF bmsql_order_line FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_order_line_171 PARTITION OF bmsql_order_line FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_order_line_181 PARTITION OF bmsql_order_line FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_order_line_191 PARTITION OF bmsql_order_line FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_order_line_201 PARTITION OF bmsql_order_line FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_order_line_211 PARTITION OF bmsql_order_line FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_order_line_221 PARTITION OF bmsql_order_line FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_order_line_231 PARTITION OF bmsql_order_line FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_order_line_241 PARTITION OF bmsql_order_line FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_order_line_251 PARTITION OF bmsql_order_line FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_order_line_261 PARTITION OF bmsql_order_line FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_order_line_271 PARTITION OF bmsql_order_line FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_order_line_281 PARTITION OF bmsql_order_line FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_order_line_291 PARTITION OF bmsql_order_line FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_order_line_301 PARTITION OF bmsql_order_line FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_order_line_311 PARTITION OF bmsql_order_line FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_order_line_321 PARTITION OF bmsql_order_line FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_order_line_331 PARTITION OF bmsql_order_line FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_order_line_341 PARTITION OF bmsql_order_line FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_order_line_351 PARTITION OF bmsql_order_line FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_order_line_361 PARTITION OF bmsql_order_line FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_order_line_371 PARTITION OF bmsql_order_line FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_order_line_381 PARTITION OF bmsql_order_line FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_order_line_391 PARTITION OF bmsql_order_line FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_order_line_401 PARTITION OF bmsql_order_line FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_order_line_411 PARTITION OF bmsql_order_line FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_order_line_421 PARTITION OF bmsql_order_line FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_order_line_431 PARTITION OF bmsql_order_line FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_order_line_441 PARTITION OF bmsql_order_line FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_order_line_451 PARTITION OF bmsql_order_line FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_order_line_461 PARTITION OF bmsql_order_line FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_order_line_471 PARTITION OF bmsql_order_line FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_order_line_481 PARTITION OF bmsql_order_line FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_order_line_491 PARTITION OF bmsql_order_line FOR VALUES FROM (491) TO (501);

-- Table: bmsql_stock
CREATE TABLE bmsql_stock_1 PARTITION OF bmsql_stock FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_stock_11 PARTITION OF bmsql_stock FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_stock_21 PARTITION OF bmsql_stock FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_stock_31 PARTITION OF bmsql_stock FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_stock_41 PARTITION OF bmsql_stock FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_stock_51 PARTITION OF bmsql_stock FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_stock_61 PARTITION OF bmsql_stock FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_stock_71 PARTITION OF bmsql_stock FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_stock_81 PARTITION OF bmsql_stock FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_stock_91 PARTITION OF bmsql_stock FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_stock_101 PARTITION OF bmsql_stock FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_stock_111 PARTITION OF bmsql_stock FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_stock_121 PARTITION OF bmsql_stock FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_stock_131 PARTITION OF bmsql_stock FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_stock_141 PARTITION OF bmsql_stock FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_stock_151 PARTITION OF bmsql_stock FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_stock_161 PARTITION OF bmsql_stock FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_stock_171 PARTITION OF bmsql_stock FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_stock_181 PARTITION OF bmsql_stock FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_stock_191 PARTITION OF bmsql_stock FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_stock_201 PARTITION OF bmsql_stock FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_stock_211 PARTITION OF bmsql_stock FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_stock_221 PARTITION OF bmsql_stock FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_stock_231 PARTITION OF bmsql_stock FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_stock_241 PARTITION OF bmsql_stock FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_stock_251 PARTITION OF bmsql_stock FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_stock_261 PARTITION OF bmsql_stock FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_stock_271 PARTITION OF bmsql_stock FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_stock_281 PARTITION OF bmsql_stock FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_stock_291 PARTITION OF bmsql_stock FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_stock_301 PARTITION OF bmsql_stock FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_stock_311 PARTITION OF bmsql_stock FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_stock_321 PARTITION OF bmsql_stock FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_stock_331 PARTITION OF bmsql_stock FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_stock_341 PARTITION OF bmsql_stock FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_stock_351 PARTITION OF bmsql_stock FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_stock_361 PARTITION OF bmsql_stock FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_stock_371 PARTITION OF bmsql_stock FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_stock_381 PARTITION OF bmsql_stock FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_stock_391 PARTITION OF bmsql_stock FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_stock_401 PARTITION OF bmsql_stock FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_stock_411 PARTITION OF bmsql_stock FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_stock_421 PARTITION OF bmsql_stock FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_stock_431 PARTITION OF bmsql_stock FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_stock_441 PARTITION OF bmsql_stock FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_stock_451 PARTITION OF bmsql_stock FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_stock_461 PARTITION OF bmsql_stock FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_stock_471 PARTITION OF bmsql_stock FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_stock_481 PARTITION OF bmsql_stock FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_stock_491 PARTITION OF bmsql_stock FOR VALUES FROM (491) TO (501);

-- Table: bmsql_warehouse
CREATE TABLE bmsql_warehouse_1 PARTITION OF bmsql_warehouse FOR VALUES FROM (1) TO (11);
CREATE TABLE bmsql_warehouse_11 PARTITION OF bmsql_warehouse FOR VALUES FROM (11) TO (21);
CREATE TABLE bmsql_warehouse_21 PARTITION OF bmsql_warehouse FOR VALUES FROM (21) TO (31);
CREATE TABLE bmsql_warehouse_31 PARTITION OF bmsql_warehouse FOR VALUES FROM (31) TO (41);
CREATE TABLE bmsql_warehouse_41 PARTITION OF bmsql_warehouse FOR VALUES FROM (41) TO (51);
CREATE TABLE bmsql_warehouse_51 PARTITION OF bmsql_warehouse FOR VALUES FROM (51) TO (61);
CREATE TABLE bmsql_warehouse_61 PARTITION OF bmsql_warehouse FOR VALUES FROM (61) TO (71);
CREATE TABLE bmsql_warehouse_71 PARTITION OF bmsql_warehouse FOR VALUES FROM (71) TO (81);
CREATE TABLE bmsql_warehouse_81 PARTITION OF bmsql_warehouse FOR VALUES FROM (81) TO (91);
CREATE TABLE bmsql_warehouse_91 PARTITION OF bmsql_warehouse FOR VALUES FROM (91) TO (101);
CREATE TABLE bmsql_warehouse_101 PARTITION OF bmsql_warehouse FOR VALUES FROM (101) TO (111);
CREATE TABLE bmsql_warehouse_111 PARTITION OF bmsql_warehouse FOR VALUES FROM (111) TO (121);
CREATE TABLE bmsql_warehouse_121 PARTITION OF bmsql_warehouse FOR VALUES FROM (121) TO (131);
CREATE TABLE bmsql_warehouse_131 PARTITION OF bmsql_warehouse FOR VALUES FROM (131) TO (141);
CREATE TABLE bmsql_warehouse_141 PARTITION OF bmsql_warehouse FOR VALUES FROM (141) TO (151);
CREATE TABLE bmsql_warehouse_151 PARTITION OF bmsql_warehouse FOR VALUES FROM (151) TO (161);
CREATE TABLE bmsql_warehouse_161 PARTITION OF bmsql_warehouse FOR VALUES FROM (161) TO (171);
CREATE TABLE bmsql_warehouse_171 PARTITION OF bmsql_warehouse FOR VALUES FROM (171) TO (181);
CREATE TABLE bmsql_warehouse_181 PARTITION OF bmsql_warehouse FOR VALUES FROM (181) TO (191);
CREATE TABLE bmsql_warehouse_191 PARTITION OF bmsql_warehouse FOR VALUES FROM (191) TO (201);
CREATE TABLE bmsql_warehouse_201 PARTITION OF bmsql_warehouse FOR VALUES FROM (201) TO (211);
CREATE TABLE bmsql_warehouse_211 PARTITION OF bmsql_warehouse FOR VALUES FROM (211) TO (221);
CREATE TABLE bmsql_warehouse_221 PARTITION OF bmsql_warehouse FOR VALUES FROM (221) TO (231);
CREATE TABLE bmsql_warehouse_231 PARTITION OF bmsql_warehouse FOR VALUES FROM (231) TO (241);
CREATE TABLE bmsql_warehouse_241 PARTITION OF bmsql_warehouse FOR VALUES FROM (241) TO (251);
CREATE TABLE bmsql_warehouse_251 PARTITION OF bmsql_warehouse FOR VALUES FROM (251) TO (261);
CREATE TABLE bmsql_warehouse_261 PARTITION OF bmsql_warehouse FOR VALUES FROM (261) TO (271);
CREATE TABLE bmsql_warehouse_271 PARTITION OF bmsql_warehouse FOR VALUES FROM (271) TO (281);
CREATE TABLE bmsql_warehouse_281 PARTITION OF bmsql_warehouse FOR VALUES FROM (281) TO (291);
CREATE TABLE bmsql_warehouse_291 PARTITION OF bmsql_warehouse FOR VALUES FROM (291) TO (301);
CREATE TABLE bmsql_warehouse_301 PARTITION OF bmsql_warehouse FOR VALUES FROM (301) TO (311);
CREATE TABLE bmsql_warehouse_311 PARTITION OF bmsql_warehouse FOR VALUES FROM (311) TO (321);
CREATE TABLE bmsql_warehouse_321 PARTITION OF bmsql_warehouse FOR VALUES FROM (321) TO (331);
CREATE TABLE bmsql_warehouse_331 PARTITION OF bmsql_warehouse FOR VALUES FROM (331) TO (341);
CREATE TABLE bmsql_warehouse_341 PARTITION OF bmsql_warehouse FOR VALUES FROM (341) TO (351);
CREATE TABLE bmsql_warehouse_351 PARTITION OF bmsql_warehouse FOR VALUES FROM (351) TO (361);
CREATE TABLE bmsql_warehouse_361 PARTITION OF bmsql_warehouse FOR VALUES FROM (361) TO (371);
CREATE TABLE bmsql_warehouse_371 PARTITION OF bmsql_warehouse FOR VALUES FROM (371) TO (381);
CREATE TABLE bmsql_warehouse_381 PARTITION OF bmsql_warehouse FOR VALUES FROM (381) TO (391);
CREATE TABLE bmsql_warehouse_391 PARTITION OF bmsql_warehouse FOR VALUES FROM (391) TO (401);
CREATE TABLE bmsql_warehouse_401 PARTITION OF bmsql_warehouse FOR VALUES FROM (401) TO (411);
CREATE TABLE bmsql_warehouse_411 PARTITION OF bmsql_warehouse FOR VALUES FROM (411) TO (421);
CREATE TABLE bmsql_warehouse_421 PARTITION OF bmsql_warehouse FOR VALUES FROM (421) TO (431);
CREATE TABLE bmsql_warehouse_431 PARTITION OF bmsql_warehouse FOR VALUES FROM (431) TO (441);
CREATE TABLE bmsql_warehouse_441 PARTITION OF bmsql_warehouse FOR VALUES FROM (441) TO (451);
CREATE TABLE bmsql_warehouse_451 PARTITION OF bmsql_warehouse FOR VALUES FROM (451) TO (461);
CREATE TABLE bmsql_warehouse_461 PARTITION OF bmsql_warehouse FOR VALUES FROM (461) TO (471);
CREATE TABLE bmsql_warehouse_471 PARTITION OF bmsql_warehouse FOR VALUES FROM (471) TO (481);
CREATE TABLE bmsql_warehouse_481 PARTITION OF bmsql_warehouse FOR VALUES FROM (481) TO (491);
CREATE TABLE bmsql_warehouse_491 PARTITION OF bmsql_warehouse FOR VALUES FROM (491) TO (501);

-- Table: bmsql_history
CREATE TABLE bmsql_history_1 PARTITION OF bmsql_history FOR VALUES FROM (0) TO (251);
CREATE TABLE bmsql_history_2 PARTITION OF bmsql_history FOR VALUES FROM (251) TO (501);