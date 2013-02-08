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

package org.apache.hadoop.tools.rumen;

/**
 *  test ZombieJob class. 
 *  Test data read from json dump  file src/test/resources/data/19-jobs.topology
 */
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;
import org.apache.hadoop.mapred.TaskStatus.State;

import static org.junit.Assert.*;

public class TestZombieJob {
  private static File workSpace = new File("src" + File.separator + "test"
      + File.separator + "resources" + File.separator + "data");

  @Test
  public void testZombieProducer() throws Exception {
    File cluster = new File(workSpace.getAbsolutePath() + File.separator
        + "19-jobs.topology");

    // read cluster topology
    ZombieCluster zcl = new ZombieCluster(new FileInputStream(cluster), null);

    File f = new File(workSpace.getAbsolutePath() + File.separator
        + "19-jobs.trace");
    // read zombie job producer
    ZombieJobProducer zjp = new ZombieJobProducer(new FileInputStream(f), zcl);

    ZombieJob zj = zjp.getNextJob();
    assertEquals("(name unknown)", zj.getName());
    assertEquals(20, zj.getInputSplits().length);
    assertEquals("job_200904211745_0002", zj.getJobID().toString());
    assertEquals("JAVA", zj.getLoggedJob().getJobtype().toString());
    assertEquals(20, zj.getNumLoggedMaps());
    assertEquals(1, zj.getNumberReduces());
    assertEquals(20, zj.getNumberMaps());
    assertEquals("default", zj.getQueueName());
    assertEquals(1240335962848L, zj.getSubmissionTime());
    assertEquals("geek1", zj.getUser());
    assertNotNull(zj.getJobConf());

    assertEquals(1, zj.getNumLoggedReduces());
    assertEquals(20, zj.getInputSplits().length);
    // test MAP attempt record
    TaskAttemptInfo tai = zj.getTaskAttemptInfo(TaskType.MAP, 10, 0);
    assertEquals(State.SUCCEEDED, tai.getRunState());

    assertEquals(getList(2, 3),
        tai.getSplitVector(LoggedTaskAttempt.SplitVectorKind.CPU_USAGE));
    assertEquals(
        getList(7, 8),
        tai.getSplitVector(LoggedTaskAttempt.SplitVectorKind.PHYSICAL_MEMORY_KBYTES));
    assertEquals(getList(1, 2),
        tai.getSplitVector(LoggedTaskAttempt.SplitVectorKind.WALLCLOCK_TIME));

    TaskAttemptInfo rrtai = zj.getTaskAttemptInfo(TaskType.REDUCE, 10, 0);
    assertEquals(State.SUCCEEDED, rrtai.getRunState());
    // test REDUCE record
    TaskInfo ti = zj.getTaskInfo(TaskType.REDUCE, 0);
    assertEquals(106300, ti.getInputRecords());
    assertEquals(0, ti.getOutputRecords());
    assertEquals(705622, ti.getInputBytes());
    assertEquals(56630, ti.getOutputBytes());
    // test MAP record
    ti = zj.getTaskInfo(TaskType.MAP, 0);
    assertEquals(3601, ti.getInputRecords());
    assertEquals(26425, ti.getOutputRecords());
    assertEquals(148286, ti.getInputBytes());
    assertEquals(247925, ti.getOutputBytes());
    // test ResourceUsageMetrics record
    ResourceUsageMetrics rm = ti.getResourceUsageMetrics();
    assertEquals(0, rm.getCumulativeCpuUsage());
    assertEquals(0, rm.getPhysicalMemoryUsage());
    assertEquals(4, rm.size());

    // test task attempt info for MAP type
    tai = zj.getTaskAttemptInfo(TaskType.MAP, 10, 0);
    assertEquals(53639, tai.getTaskInfo().getInputBytes());
    assertEquals(3601, tai.getTaskInfo().getInputRecords());
    assertEquals(247925, tai.getTaskInfo().getOutputBytes());
    assertEquals(26425, tai.getTaskInfo().getOutputRecords());
    assertEquals(640, tai.getTaskInfo().getTaskMemory());

    ReduceTaskAttemptInfo rtai = (ReduceTaskAttemptInfo) zj.getTaskAttemptInfo(
        TaskType.REDUCE, 100, 30);
    assertEquals(83784, rtai.getRuntime());
    // rtai.getRuntime() should be = rtai.getReduceRuntime() +
    // rtai.getMergeRuntime() + rtai.getShuffleRuntime()
    assertEquals(
        83784,
        rtai.getReduceRuntime() + rtai.getMergeRuntime()
            + rtai.getShuffleRuntime());

    assertEquals(0, rtai.getTaskInfo().getInputBytes());
    assertEquals(0, rtai.getTaskInfo().getInputRecords());
    assertEquals(0, rtai.getTaskInfo().getOutputBytes());
    assertEquals(0, rtai.getTaskInfo().getOutputRecords());
    assertEquals(0, rtai.getTaskInfo().getTaskMemory());
    // test Task attempt info for MAP record
    tai = zj.getTaskAttemptInfo(TaskType.MAP, 100, 30);
    assertEquals(0, tai.getTaskInfo().getInputBytes());
    assertEquals(0, tai.getTaskInfo().getInputRecords());
    assertEquals(0, tai.getTaskInfo().getOutputBytes());
    assertEquals(0, tai.getTaskInfo().getOutputRecords());
    assertEquals(0, tai.getTaskInfo().getTaskMemory());
    tai = zj.getMapTaskAttemptInfoAdjusted(10, 0, 0);
    assertEquals(State.SUCCEEDED, tai.getRunState());
    assertEquals(3601, tai.getTaskInfo().getInputRecords());
    assertEquals(26425, tai.getTaskInfo().getOutputRecords());

    LoggedJob lg = zj.getLoggedJob();
    zjp.getNextJob();
    lg.deepCompare(lg, null);

    int counter = 2;
    while ((zjp.getNextJob()) != null) {
      counter++;
    }
    // only 12 jobs
    assertEquals(12, counter);
    zjp.close();

  }

  private List<Integer> getList(Integer... params) {
    List<Integer> result = new ArrayList<Integer>(params.length);
    Collections.addAll(result, params);
    return result;
  }
}
