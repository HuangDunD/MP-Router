/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.ycsb;

import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.TABLE_NAME;
// import java.util.Random;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.*;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.distributions.UniformGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
/**
 * YCSBWorker Implementation I forget who really wrote this but I fixed it up in 2016...
 *
 * @author pavlo
 */
class YCSBWorker extends Worker<YCSBBenchmark> {

  private final ZipfianGenerator readRecord;
  private static CounterGenerator insertRecord;
  private final UniformGenerator randScan;

  private final char[] data;
  private final int totalTerminals;
  private final int totalRows;
  private final String[] params = new String[YCSBConstants.NUM_FIELDS];
  private final String[] results = new String[YCSBConstants.NUM_FIELDS];
  private final Random random;
  private final int totalStores;

  private final UpdateRecord procUpdateRecord;
  private final ScanRecord procScanRecord;
  private final ReadRecord procReadRecord;
  private final ReadModifyWriteRecord procReadModifyWriteRecord;
  private final MyUpdateRecord procMyUpdateRecord;
  private final InsertRecord procInsertRecord;
  private final DeleteRecord procDeleteRecord;

  public YCSBWorker(YCSBBenchmark benchmarkModule, int id, int init_record_count, int total_terminals) {
    super(benchmarkModule, id);
    this.totalTerminals = total_terminals;
    this.totalRows = init_record_count;
    this.data = new char[benchmarkModule.fieldSize];
    this.readRecord =
        new ZipfianGenerator(
            rng(), init_record_count, benchmarkModule.skewFactor); // pool for read keys
    this.randScan = new UniformGenerator(1, YCSBConstants.MAX_SCAN);
    this.random = new Random();
    this.totalStores = 5;

    synchronized (YCSBWorker.class) {
      // We must know where to start inserting
      if (insertRecord == null) {
        insertRecord = new CounterGenerator(init_record_count);
      }
    }

    // This is a minor speed-up to avoid having to invoke the hashmap look-up
    // everytime we want to execute a txn. This is important to do on
    // a client machine with not a lot of cores
    this.procUpdateRecord = this.getProcedure(UpdateRecord.class);
    this.procScanRecord = this.getProcedure(ScanRecord.class);
    this.procReadRecord = this.getProcedure(ReadRecord.class);
    this.procReadModifyWriteRecord = this.getProcedure(ReadModifyWriteRecord.class);
    this.procMyUpdateRecord = this.getProcedure(MyUpdateRecord.class);
    this.procInsertRecord = this.getProcedure(InsertRecord.class);
    this.procDeleteRecord = this.getProcedure(DeleteRecord.class);
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
      throws UserAbortException, SQLException {
    Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

    if (procClass.equals(DeleteRecord.class)) {
      deleteRecord(conn);
    } else if (procClass.equals(InsertRecord.class)) {
      insertRecord(conn);
    } else if (procClass.equals(ReadModifyWriteRecord.class)) {
      readModifyWriteRecord(conn);
    } else if (procClass.equals(MyUpdateRecord.class)) {
      myUpdateRecord(conn);
    } else if (procClass.equals(ReadRecord.class)) {
      readRecord(conn);
    } else if (procClass.equals(ScanRecord.class)) {
      scanRecord(conn);
    } else if (procClass.equals(UpdateRecord.class)) {
      updateRecord(conn);
    }
    return (TransactionStatus.SUCCESS);
  }

  private void updateRecord(Connection conn) throws SQLException {
    int keyname = readRecord.nextInt();
    this.buildParameters();
    this.procUpdateRecord.run(conn, keyname, this.params);
  }

  private void scanRecord(Connection conn) throws SQLException {
    int keyname = readRecord.nextInt();
    int count = randScan.nextInt();
    this.procScanRecord.run(conn, keyname, count, new ArrayList<>());
  }

  private void readRecord(Connection conn) throws SQLException {
    int keyname = readRecord.nextInt();
    this.procReadRecord.run(conn, keyname, this.results);
  }

  private void readModifyWriteRecord(Connection conn) throws SQLException {
    int keyname = readRecord.nextInt();
    this.buildParameters();
    this.procReadModifyWriteRecord.run(conn, keyname, this.params, this.results);
  }
  private int myGetNextInt() {
    return Math.abs(random.nextInt()) % this.totalRows;
  }
  private void myUpdateRecord(Connection conn) throws SQLException {
    final int NUM_KEYS = this.configuration.getYCSBkeys();
    int[] keys = new int[NUM_KEYS + 1];
    int[] isUpdate = new int[NUM_KEYS + 1];

    int totalRegions = this.totalRows / this.configuration.getKeysPerRegion();

    int randomNumber = myGetNextInt();
    boolean isCrossRegion = randomNumber % 100 < this.configuration.getCrossRatio();

    int baseRegionId = (randomNumber / this.configuration.getKeysPerRegion()) / NUM_KEYS * NUM_KEYS; // begin 

    int val = myGetNextInt();
    int baseRegionIdOffset = val % NUM_KEYS;

    // System.out.println("val: " + val % 100 + " " + 
    //                    "skew factor: " + this.configuration.getMySkewFactor());
    int idx = (baseRegionId / NUM_KEYS) % NUM_KEYS;

    boolean isSkew = val % 100 < this.configuration.getMySkewFactor();
    if(isSkew){
      // getMySkewFactor = 80 代表百分之80的负载落在某一个节点上
      baseRegionIdOffset = idx = 0;
    }

    int baseKeyOffset = myGetNextInt() % this.configuration.getKeysPerRegion() / NUM_KEYS * NUM_KEYS;
    int baseKey = (baseRegionId + baseRegionIdOffset) * this.configuration.getKeysPerRegion() + baseKeyOffset;

    
    // Generate keys and update flags
    for (int i = 0; i < NUM_KEYS + 1; i++) {
        if(isCrossRegion == true){
          if(i == NUM_KEYS){
            keys[i] = (baseKey + i + (idx - baseRegionIdOffset) * this.configuration.getKeysPerRegion()) % totalRows;
          } else {
            keys[i] = (baseKey + i + (i - baseRegionIdOffset) * this.configuration.getKeysPerRegion()) % totalRows;
            if(i >= 2 && isSkew){
              // getMySkewFactor = 80 代表百分之80的负载落在某一个节点上
              keys[i] = (baseKey + i) % totalRows;
            }
          }
        } else {
          keys[i] = baseKey + i;
        }
        isUpdate[i] = 1; // (readRecord.nextInt() % 2); // Randomly decide read or write
    }

    String strSQL = generateBatchUpdateSQL(keys);

    // Print keys and isCrossRegion
    // System.out.println(this.totalRows + " " + randomNumber + " " + totalRegions);
    // System.out.println("baseRegionId: " + baseRegionId + 
    //                   " baseRegionIdOffset: " + baseRegionIdOffset + 
    //                   " RegionId: " + (baseRegionId + baseRegionIdOffset) + 
    //                   " baseKeyOffset : " + baseKeyOffset + 
    //                   " baseKey: " + baseKey);
    // System.out.println("-Keys: " + java.util.Arrays.toString(keys));
    // System.out.println("isCrossRegion: " + isCrossRegion + "  " + this.configuration.getCrossRatio());
    // System.out.println(strSQL);

    this.procMyUpdateRecord.run(conn, strSQL);
  }

  /**
   * 生成批量更新的 SQL 语句
   *
   * @param keys   需要更新的 YCSB_KEY 数组
   * @param params 每个 YCSB_KEY 对应的字段值数组
   * @return 生成的 SQL 语句
   */
  public String generateBatchUpdateSQL(int[] keys) {
      StringBuilder sql = new StringBuilder();
      sql.append("UPDATE ").append(TABLE_NAME).append(" SET ");

      // 构建 SET 部分
      for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++) {
          this.buildParameters();
          sql.append("FIELD").append(i + 1).append(" = CASE ");
          for (int key : keys) {
              sql.append("WHEN YCSB_KEY = ").append(key).append(" THEN  '").append(this.params[i]).append("' \n");
          }
          sql.append("ELSE FIELD").append(i + 1).append(" END, ");
      }

      // 去掉最后一个逗号
      sql.delete(sql.length() - 2, sql.length());

      // 构建 WHERE 部分
      sql.append(" WHERE YCSB_KEY IN (");
      for (int i = 0; i < keys.length; i++) {
          sql.append(keys[i]);
          if (i < keys.length - 1) {
              sql.append(", ");
          }
      }
      sql.append(");");

      return sql.toString();
  }


  private void insertRecord(Connection conn) throws SQLException {
    int keyname = insertRecord.nextInt();
    this.buildParameters();
    this.procInsertRecord.run(conn, keyname, this.params);
  }

  private void deleteRecord(Connection conn) throws SQLException {
    int keyname = readRecord.nextInt();
    this.procDeleteRecord.run(conn, keyname);
  }

  private void buildParameters() {
    for (int i = 0; i < this.params.length; i++) {
      this.params[i] = new String(TextGenerator.randomFastChars(rng(), this.data));
    }
  }
}
