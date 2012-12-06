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

import org.apache.avro.generic.IndexedRecord;
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
    TaskAttemptFinished tsf = (TaskAttemptFinished) test.getDatum();
    Assert.assertEquals(test.getAttemptId().toString(), taid.toString());

    Assert.assertEquals(test.getCounters(), counters);
    test.getEventType();
    Assert.assertEquals(test.getFinishTime(), 123L);
    Assert.assertEquals(test.getHostname(), "HOSTNAME");
    Assert.assertEquals(test.getRackName(), "RAKNAME");
    Assert.assertEquals(test.getState(), "STATUS");
    Assert.assertEquals(test.getTaskId(), tid);
    Assert.assertEquals(test.getTaskStatus(), "TEST");
    Assert.assertEquals(test.getTaskType(), TaskType.REDUCE);
    // test TaskAttemptFinished
    // get method

    CharSequence taskid = (CharSequence) tsf.get(0);
    Assert.assertEquals(taskid.toString(), "task_001_0001_r_000002");
    CharSequence attemptId = (CharSequence) tsf.get(1);
    Assert.assertEquals(attemptId.toString(),taid.toString());
    CharSequence taskType = (CharSequence) tsf.get(2);
    Assert.assertEquals(taskType.toString(),TaskType.REDUCE.toString());
    CharSequence taskStatus = (CharSequence) tsf.get(3);
    Assert.assertEquals(taskStatus.toString(),"TEST");
    long finishTime = (Long) tsf.get(4);
    Assert.assertEquals(finishTime,123L);
    CharSequence rackname = (CharSequence) tsf.get(5);
    Assert.assertEquals(rackname.toString(),"RAKNAME");
    CharSequence hostname = (CharSequence) tsf.get(6);
    Assert.assertEquals(hostname.toString(),"HOSTNAME");
    CharSequence state = (CharSequence) tsf.get(7);
    Assert.assertEquals(state.toString(),"STATUS");
    JhCounters counterstest = (JhCounters) tsf.get(8);
    Assert.assertEquals(counterstest.name.toString(),"COUNTERS");
    
    // put method
    testCharSequence(tsf,0,taskid);
    testCharSequence(tsf,1,attemptId);
    testCharSequence(tsf,2,taskType);
    testCharSequence(tsf,3,taskStatus);
    
    tsf.put(4,567L);
    Assert.assertEquals(tsf.get(4),567L);
    
    testCharSequence(tsf,5,rackname);
    testCharSequence(tsf,6,hostname);
    testCharSequence(tsf,7,state);
  

    
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
    JobPriorityChange jpce= (JobPriorityChange)test.getDatum();
    
    // test get method
    
    CharSequence jobId = (CharSequence) jpce.get(0);
    Assert.assertEquals(jobId.toString(), "job_001_0001");

    CharSequence priorioty = (CharSequence) jpce.get(1);
    Assert.assertEquals(priorioty.toString(), "LOW");

    // test put method
    testCharSequence(jpce,0,jobId);
    testCharSequence(jpce,1,"LOW");

  }
  
 
  private void testCharSequence(IndexedRecord tsf ,int index, CharSequence template){
    tsf.put(index, template.subSequence(0, template.length()-1));
    Assert.assertEquals(tsf.get(index),template.subSequence(0, template.length()-1));
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
    TaskUpdated tu=(TaskUpdated)test.getDatum();
    //test get
    CharSequence taskId=(CharSequence)tu.get(0);
  
    Assert.assertEquals(tu.get(1),1234L);

    Assert.assertEquals(tu.get(0).toString(),"task_001_0001_r_000002");

    // test put
    tu.put(1, 12L);
    Assert.assertEquals(tu.get(1),12L);
    testCharSequence(tu,0,taskId);
    
       
  }
   
}
