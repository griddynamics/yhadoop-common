/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRecordReader {

  @Test
  public void testOracleDBRecordReader() throws Exception {
    DBInputSplit splitter = new DBInputSplit(1, 10);
    Configuration configuration = new Configuration();
    ConnectionForTest connect = new ConnectionForTest();

    DBConfiguration dbConfiguration = new DBConfiguration(configuration);
    dbConfiguration.setInputOrderBy("Order");
    String[] fields = { "f1", "f2" };
    
    OracleDBRecordReader<NullDBWritable> recorder = new OracleDBRecordReader<NullDBWritable>(
        splitter, NullDBWritable.class, configuration, connect,
        dbConfiguration, "condition", fields, "table");
    assertEquals("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( SELECT f1, f2 FROM table WHERE condition ORDER BY Order ) a WHERE rownum <= 1 + 9 ) WHERE dbif_rno >= 1",
        recorder.getSelectQuery());
    assertEquals("GMT", connect.getSessionTimeZone());

  }

 
  
}
