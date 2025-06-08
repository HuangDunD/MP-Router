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

