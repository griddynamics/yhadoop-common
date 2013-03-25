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
package org.apache.hadoop.mapred.lib.db;

import java.io.IOException;
import java.sql.DriverManager;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DriverForTest;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestConstructQuery {

  private String[] fieldNames = new String[] { "id", "name", "value" };
  private String[] nullFieldNames = new String[] { null, null, null };
  private String expected = "INSERT INTO hadoop_output (id,name,value) VALUES (?,?,?);";
  private String nullExpected = "INSERT INTO hadoop_output VALUES (?,?,?);";

  private DBOutputFormat<DBWritable, NullWritable> format = new DBOutputFormat<DBWritable, NullWritable>();

  @Test
  public void testConstructQuery() {
    String actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);

    actual = format.constructQuery("hadoop_output", nullFieldNames);
    assertEquals(nullExpected, actual);
  }

  @Test
  public void testSetOutput() throws IOException {
    JobConf job = new JobConf();
    DBOutputFormat.setOutput(job, "hadoop_output", fieldNames);

    DBConfiguration dbConf = new DBConfiguration(job);
    String actual = format.constructQuery(dbConf.getOutputTableName(),
        dbConf.getOutputFieldNames());

    assertEquals(expected, actual);

    job = new JobConf();
    dbConf = new DBConfiguration(job);
    DBOutputFormat.setOutput(job, "hadoop_output", nullFieldNames.length);
    assertNull(dbConf.getOutputFieldNames());
    assertEquals(nullFieldNames.length, dbConf.getOutputFieldCount());

    actual = format.constructQuery(dbConf.getOutputTableName(),
        new String[dbConf.getOutputFieldCount()]);

    assertEquals(nullExpected, actual);
  }

  @Test
  public void getgetRecordWriter() throws Exception {
    FileSystem fs = new RawLocalFileSystem();
    JobConf jobConf = new JobConf();
    Progressable progress = mock(Progressable.class);

    jobConf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_1_2_m3_4_5");
    jobConf.set(DBConfiguration.URL_PROPERTY, "testUrl");
    DriverManager.registerDriver(new DriverForTest());
    jobConf.set(DBConfiguration.DRIVER_CLASS_PROPERTY,
        "org.apache.hadoop.mapred.lib.db.DriverForTest");
    RecordWriter<DBWritable, NullWritable> recorder = format.getRecordWriter(
        fs, jobConf, "name", progress);
    assertEquals("org.apache.hadoop.mapred.lib.db.DBOutputFormat$DBRecordWriter", recorder.getClass().getName());
    recorder.close(null);
  }

}
