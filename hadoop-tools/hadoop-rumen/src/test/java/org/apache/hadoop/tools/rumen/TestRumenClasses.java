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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test some small classes
 */
public class TestRumenClasses {
  private static File workSpace = new File("src" + File.separator + "test"
      + File.separator + "resources" + File.separator + "data");

  /**
   * test TreePath class
   */
  @Test
  public void testTreePath() {
    TreePath root = new TreePath(null, "Root");
    TreePath testTreePath = new TreePath(root, "Test", 10);
    assertEquals("Root-->Test[10]", testTreePath.toString());
  }

  /**
   * test ResourceUsageMetrics
   * 
   */
  @Test
  public void testResourceUsageMetrics() throws Exception {

    // create object for test
    ResourceUsageMetrics m1 = new ResourceUsageMetrics();
    m1.setCumulativeCpuUsage(1L);
    m1.setHeapUsage(2L);
    m1.setPhysicalMemoryUsage(3L);
    m1.setVirtualMemoryUsage(4L);

    // create copy of test object
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput doutput = new DataOutputStream(baos);
    m1.write(doutput);
    ResourceUsageMetrics m2 = new ResourceUsageMetrics();
    m2.readFields(new DataInputStream(new ByteArrayInputStream(baos
        .toByteArray())));
    // objects should be equals
    m1.deepCompare(m2, null);
    // change data
    m2.setVirtualMemoryUsage(5L);

    // object not equals
    try {
      m1.deepCompare(m2, null);
      fail();
    } catch (DeepInequalityException e) {
      assertEquals(e.getMessage(), "Value miscompared:virtualMemory");
    }
    // test contents
    assertEquals(m2.getCumulativeCpuUsage(), 1);
    assertEquals(m2.getHeapUsage(), 2);
    assertEquals(m2.getPhysicalMemoryUsage(), 3);
    assertEquals(m2.getVirtualMemoryUsage(), 5);
  }

  /**
   * test ZombieCluster
   */
  @Test
  public void testZombieCluster() throws Exception {
    File cluster = new File(workSpace.getAbsolutePath() + File.separator
        + "19-jobs.topology");

    // read topology data topology

    ZombieCluster zcl = new ZombieCluster(new FileInputStream(cluster), null);
    // check data
    Node root = zcl.getClusterTopology();
    Node chiNode = root.getChildren().iterator().next();
    assertEquals(1, zcl.distance(root, chiNode));

    // check getting random hosts from cluster
    Random r = new Random(System.currentTimeMillis());
    assertEquals(2, zcl.getRandomMachines(2, r).length);
    assertEquals(1545, zcl.getMachines().size());
    assertEquals(41, zcl.getRacks().size());

    // test data in MachineNode
    MachineNode mn = zcl.getMachines().iterator().next();
    assertEquals(2, mn.getLevel());
    assertEquals(1, mn.getMapSlots());

    assertNotNull(zcl.getRackByName("/192\\.30\\.116\\.128"));
    assertNull(zcl.getRackByName("/192\\.30\\.116\\.111"));

  }

  /**
   * test MachineNode
   */

  @Test
  public void testMachineNode() {
    // test builder
    MachineNode mn = new MachineNode.Builder("node", 1).setMapSlots(2)
        .setMemory(100).setMemoryPerMapSlot(50).setMemoryPerReduceSlot(50)
        .setNumCores(4).setReduceSlots(2).build();

    assertEquals(1, mn.getLevel());
    assertEquals(2, mn.getMapSlots());
    assertEquals(100, mn.getMemory());
    assertEquals(50, mn.getMemoryPerMapSlot());
    assertEquals(50, mn.getMemoryPerReduceSlot());
    assertEquals("node", mn.getName());
    assertEquals(4, mn.getNumCores());
    assertEquals(2, mn.getReduceSlots());
    assertNull(mn.getParent());
    assertNull(mn.getRackNode());
    // test clone method
    MachineNode copy = new MachineNode.Builder("node2", 2).cloneFrom(mn)
        .build();
    assertEquals(2, copy.getLevel());
    assertEquals(2, copy.getMapSlots());
    assertEquals(100, copy.getMemory());
    assertEquals(50, copy.getMemoryPerMapSlot());
    assertEquals(50, copy.getMemoryPerReduceSlot());
    assertEquals("node2", copy.getName());
    assertEquals(4, copy.getNumCores());
    assertEquals(2, copy.getReduceSlots());
    assertNull(copy.getParent());
    assertNull(copy.getRackNode());

    // test equals method different name and level
    assertFalse(mn.equals(copy));

    copy = new MachineNode.Builder("node", 1).cloneFrom(mn).build();
    // test equals method
    assertTrue(mn.equals(copy));
    assertTrue(mn.hashCode() > 0);

  }

  /**
   * test CurrentJHParser
   */
  @Test
  public void testCurrentJHParser() throws Exception {

    File trace = new File(
        workSpace.getAbsolutePath()
            + File.separator
            + "job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist");

    assertTrue(CurrentJHParser.canParse(new FileInputStream(trace)));
    CurrentJHParser parser = new CurrentJHParser(new FileInputStream(trace));
    HistoryEvent event = parser.nextEvent();
    assertNotNull(event.getDatum());
    int i = 1;
    while ((event = parser.nextEvent()) != null) {
      assertNotNull(event.getDatum());
      i++;
    }
    // should be 25 events
    assertEquals(25, i);
    parser.close();
    // parser works!
  }

  // test ParsedJob, ParsedTask , ParsedTaskAttempt
  @Test
  public void testParsedJob() {
    ParsedJob job = new ParsedJob();
    Map<String, Long> totalCounters = new HashMap<String, Long>();
    totalCounters.put("first", 1L);

    job.putTotalCounters(totalCounters);
    job.obtainTotalCounters().put("second", 2L);

    Map<String, Long> mapCounters = new HashMap<String, Long>();
    totalCounters.put("first", 1L);
    job.putMapCounters(mapCounters);
    job.obtainMapCounters().put("third", 3L);

    Map<String, Long> reduceCounters = new HashMap<String, Long>();
    reduceCounters.put("first", 1L);
    job.putReduceCounters(reduceCounters);
    job.obtainReduceCounters().put("second", 2L);

    job.putJobConfPath("JobConfPath");
    assertEquals("JobConfPath", job.obtainJobConfpath());

    List<LoggedTask> tasks = new ArrayList<LoggedTask>();
    tasks.add(getParsedTask("dianostic info for first task"));
    tasks.add(getParsedTask("dianostic info for second task"));
    job.setMapTasks(tasks);
    assertEquals(job.obtainMapTasks().size(), 2);

    job.setReduceTasks(tasks);
    assertEquals(job.obtainReduceTasks().size(), 2);

    job.setOtherTasks(tasks);
    assertEquals(job.obtainOtherTasks().size(), 2);

    job.dumpParsedJob();
  }

  private ParsedTask getParsedTask(String diagnosticInfo) {
    ParsedTask result = new ParsedTask();
    result.obtainCounters().put("counter first", 1L);
    result.putDiagnosticInfo(diagnosticInfo);
    result.putFailedDueToAttemptId("123456");
    List<LoggedTaskAttempt> attempts = new ArrayList<LoggedTaskAttempt>();
    attempts.add(getParsedTaskAttempt());
    attempts.add(getParsedTaskAttempt());
    result.setAttempts(attempts);

    assertEquals(result.obtainTaskAttempts().size(), 2);
    assertEquals(result.convertTaskAttempts(attempts).size(), 2);

    return result;
  }

  private LoggedTaskAttempt getParsedTaskAttempt() {
    LoggedTaskAttempt result = new ParsedTaskAttempt();
    result.setHostName("localhost");
    return result;
  }

}
