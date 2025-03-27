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

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.TABLE_NAME;
import com.oltpbenchmark.util.TextGenerator;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyUpdateRecord extends Procedure {
  private final String[] params = new String[YCSBConstants.NUM_FIELDS];
  // FIXME: The value in ysqb is a byteiterator
  public void run(Connection conn, String strSQL)
      throws SQLException {

    SQLStmt updateAllStmt = new SQLStmt(strSQL); 

    // Update that mofo
    try (PreparedStatement stmt = this.getPreparedStatement(conn, updateAllStmt)) {
      stmt.executeUpdate();
    }
  }

}
