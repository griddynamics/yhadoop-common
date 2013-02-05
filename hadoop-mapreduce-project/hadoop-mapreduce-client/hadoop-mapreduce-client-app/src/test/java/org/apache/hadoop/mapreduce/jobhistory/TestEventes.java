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

package org.apache.hadoop.mapreduce.jobhistory;

import junit.framework.Assert;

import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;

public class TestEventes {

/**
 * test TaskAttemptFinishedEvent and TaskAttemptFinished
 * @throws Exception
 */
  @Test
  public void testTaskAttemptFinishedEvent() throws Exception {

    JobID jid = new JobID("001", 1);
    TaskID tid = new TaskID(jid, TaskType.REDUCE, 2);
    TaskAttemptID taid = new TaskAttemptID(tid, 3);
    Counters counters = new Counters();
    TaskAttemptFinishedEvent test = new TaskAttemptFinishedEvent(taid,
        TaskType.REDUCE, "TEST", 123L, "RAKNAME", "HOSTNAME", "STATUS",
        counters);
    Assert.assertEquals(test.getAttemptId().toString(), taid.toString());

    Assert.assertEquals(test.getCounters(), counters);
    Assert.assertEquals(test.getFinishTime(), 123L);
    Assert.assertEquals(test.getHostname(), "HOSTNAME");
    Assert.assertEquals(test.getRackName(), "RAKNAME");
    Assert.assertEquals(test.getState(), "STATUS");
    Assert.assertEquals(test.getTaskId(), tid);
    Assert.assertEquals(test.getTaskStatus(), "TEST");
    Assert.assertEquals(test.getTaskType(), TaskType.REDUCE);
 
  }
  /**
   * test JobPriorityChangeEvent and JobPriorityChange
   * @throws Exception
   */

  @Test
  public void testJobPriorityChange() throws Exception {
    org.apache.hadoop.mapreduce.JobID jid = new JobID("001", 1);
    JobPriorityChangeEvent test= new JobPriorityChangeEvent(jid,JobPriority.LOW);
    Assert.assertEquals(test.getJobId().toString(),jid.toString());
    Assert.assertEquals(test.getPriority(),JobPriority.LOW);
   
  }
  
 

  /**
   * test TaskUpdatedEvent and TaskUpdated
   * @throws Exception
   */
  @Test
  public void testTaskUpdated() throws Exception {
    JobID jid = new JobID("001", 1);
    TaskID tid = new TaskID(jid, TaskType.REDUCE, 2);
    TaskUpdatedEvent test= new TaskUpdatedEvent(tid, 1234L);
    Assert.assertEquals(test.getTaskId().toString(),tid.toString());
    Assert.assertEquals(test.getFinishTime(),1234L);
       
  }
   
}
