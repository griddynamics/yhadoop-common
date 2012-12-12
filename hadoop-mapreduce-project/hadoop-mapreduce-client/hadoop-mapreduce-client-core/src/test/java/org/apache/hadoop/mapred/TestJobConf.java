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

package org.apache.hadoop.mapred;

import java.util.regex.Pattern;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * test JobConf
 * @author polzovatel
 *
 */
public class TestJobConf {

  /**
   * test getters and setters of JobConf
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testJobConf() {
    JobConf conf = new JobConf();
    // test default value
    Pattern pattern = conf.getJarUnpackPattern();
    assertEquals(Pattern.compile("(?:classes/|lib/).*").toString(),
        pattern.toString());
    // conf.deleteLocalFiles();
    // default value
    assertFalse(conf.getKeepFailedTaskFiles());
    conf.setKeepFailedTaskFiles(true);
    assertTrue(conf.getKeepFailedTaskFiles());

    assertNull(conf.getKeepTaskFilesPattern());
    conf.setKeepTaskFilesPattern("123454");
    assertEquals("123454", conf.getKeepTaskFilesPattern());

    assertNotNull(conf.getWorkingDirectory());
    conf.setWorkingDirectory(new Path("test"));
    assertTrue(conf.getWorkingDirectory().toString().endsWith("test"));

    assertEquals(1, conf.getNumTasksToExecutePerJvm());

    assertNull(conf.getKeyFieldComparatorOption());
    conf.setKeyFieldComparatorOptions("keySpec");
    assertEquals("keySpec", conf.getKeyFieldComparatorOption());

    assertFalse(conf.getUseNewReducer());
    conf.setUseNewReducer(true);
    assertTrue(conf.getUseNewReducer());

    // default
    assertTrue(conf.getMapSpeculativeExecution());
    assertTrue(conf.getReduceSpeculativeExecution());
    assertTrue(conf.getSpeculativeExecution());
    conf.setReduceSpeculativeExecution(false);
    assertTrue(conf.getSpeculativeExecution());

    conf.setMapSpeculativeExecution(false);
    assertFalse(conf.getSpeculativeExecution());
    assertFalse(conf.getMapSpeculativeExecution());
    assertFalse(conf.getReduceSpeculativeExecution());

    conf.setSessionId("ses");
    assertEquals("ses", conf.getSessionId());

    assertEquals(3, conf.getMaxTaskFailuresPerTracker());
    conf.setMaxTaskFailuresPerTracker(2);
    assertEquals(2, conf.getMaxTaskFailuresPerTracker());

    assertEquals(0, conf.getMaxMapTaskFailuresPercent());
    conf.setMaxMapTaskFailuresPercent(50);
    assertEquals(50, conf.getMaxMapTaskFailuresPercent());

    assertEquals(0, conf.getMaxReduceTaskFailuresPercent());
    conf.setMaxReduceTaskFailuresPercent(70);
    assertEquals(70, conf.getMaxReduceTaskFailuresPercent());

    // by default
    assertEquals(JobPriority.NORMAL.name(), conf.getJobPriority().name());
    conf.setJobPriority(JobPriority.HIGH);
    assertEquals(JobPriority.HIGH.name(), conf.getJobPriority().name());

    assertNull(conf.getJobSubmitHostName());
    conf.setJobSubmitHostName("hostname");
    assertEquals("hostname", conf.getJobSubmitHostName());

    // default
    assertNull(conf.getJobSubmitHostAddress());
    conf.setJobSubmitHostAddress("ww");
    assertEquals("ww", conf.getJobSubmitHostAddress());

    assertFalse(conf.getProfileEnabled());
    conf.setProfileEnabled(true);
    assertTrue(conf.getProfileEnabled());

    assertEquals(conf.getProfileTaskRange(true).toString(), "0-2");
    assertEquals(conf.getProfileTaskRange(false).toString(), "0-2");
    conf.setProfileTaskRange(true, "0-3");
    assertEquals(conf.getProfileTaskRange(false).toString(), "0-2");
    assertEquals(conf.getProfileTaskRange(true).toString(), "0-3");

    assertNull(conf.getMapDebugScript());
    conf.setMapDebugScript("mDbgScript");
    assertEquals("mDbgScript", conf.getMapDebugScript());

    assertNull(conf.getReduceDebugScript());
    conf.setReduceDebugScript("rDbgScript");
    assertEquals("rDbgScript", conf.getReduceDebugScript());

    assertNull(conf.getJobLocalDir());

    assertEquals("default", conf.getQueueName());
    conf.setQueueName("qname");
    assertEquals("qname", conf.getQueueName());

    assertEquals(1, conf.computeNumSlotsPerMap(100L));
    assertEquals(1, conf.computeNumSlotsPerReduce(100L));
    
    conf.setMemoryForMapTask(100*1000);
    assertEquals(1000, conf.computeNumSlotsPerMap(100L));
    conf.setMemoryForReduceTask(1000*1000);
    assertEquals(1000, conf.computeNumSlotsPerReduce(1000L));

    assertEquals(-1, conf.getMaxPhysicalMemoryForTask());
    assertEquals("The variable key is no longer used.",
        JobConf.deprecatedString("key"));

  }
}
