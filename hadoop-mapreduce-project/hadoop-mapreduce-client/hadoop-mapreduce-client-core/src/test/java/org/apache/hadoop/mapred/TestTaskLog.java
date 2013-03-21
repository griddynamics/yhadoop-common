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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Test;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

/**
 * TestCounters checks the sanity and recoverability of Queue
 */
public class TestTaskLog {



  /**
   * test without TASK_LOG_DIR
   * 
   * @throws IOException
   */
  @Test
  public void testTaskLogWithoutTaskLogDir() throws IOException {
    // TaskLog tasklog= new TaskLog();
    System.clearProperty(MRJobConfig.TASK_LOG_DIR);

    // test TaskLog

    assertEquals(TaskLog.getMRv2LogDir(), null);
    TaskAttemptID taid = mock(TaskAttemptID.class);
    JobID jid = new JobID("job", 1);

    when(taid.getJobID()).thenReturn(jid);
    when(taid.toString()).thenReturn("JobId");

    File f = TaskLog.getTaskLogFile(taid, true, LogName.STDOUT);
    assertTrue(f.getAbsolutePath().endsWith("stdout"));

  }

}
