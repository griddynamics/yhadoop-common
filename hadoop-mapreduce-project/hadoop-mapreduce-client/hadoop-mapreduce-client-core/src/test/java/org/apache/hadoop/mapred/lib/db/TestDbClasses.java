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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestDbClasses {

  @Test
  public void testDBConfiguration() {
    JobConf jConfiguration = new JobConf();
    DBConfiguration.configureDB(jConfiguration, "driverClass", "dbUrl", "user",
        "password");
    assertEquals("driverClass",
        jConfiguration.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    assertEquals("dbUrl", jConfiguration.get(DBConfiguration.URL_PROPERTY));
    assertEquals("user", jConfiguration.get(DBConfiguration.USERNAME_PROPERTY));
    assertEquals("password",
        jConfiguration.get(DBConfiguration.PASSWORD_PROPERTY));
    jConfiguration = new JobConf();
    DBConfiguration.configureDB(jConfiguration, "driverClass", "dbUrl");
    assertEquals("driverClass",
        jConfiguration.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    assertEquals("dbUrl", jConfiguration.get(DBConfiguration.URL_PROPERTY));
    assertNull(jConfiguration.get(DBConfiguration.USERNAME_PROPERTY));
    assertNull(jConfiguration.get(DBConfiguration.PASSWORD_PROPERTY));
  }

  /*
  @Test
  public void testDBOutputFormat() throws Exception {
    JobConf jConfiguration = new JobConf();
    jConfiguration.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_1_2_r3_4_5");
    DBOutputFormat<DBWritable, Integer> format = new DBOutputFormat<DBWritable, Integer>();
    FileSystem filesystem = mock(FileSystem.class);
    Progressable progress = mock(Progressable.class);
    RecordWriter<DBWritable, Integer> recorder = format.getRecordWriter(
        filesystem, jConfiguration, "name", progress);
    System.out.println("OK");
  }
  */
}
