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

import java.io.File;
import java.io.FileInputStream;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestCommandLineUtilities {

  private static File workSpace = new File("target" + File.separator
      + "test-classes" + File.separator + "log");

  /**
   * test TestTraceBuilder utility
   */

  @Test
  public void testTraceBuilder() throws Exception {
    String[] args = new String[5];
    args[0] = "-demuxer";
    args[1] = "org.apache.hadoop.tools.rumen.DefaultInputDemuxer";
    args[2] = "file://" + workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "job-trace.json";
    args[3] = "file://" + workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "topology.output";
    args[4] = "file://" + workSpace.getAbsolutePath();
    File topologyFile = new File(workSpace.getAbsolutePath() + File.separator
        + "out" + File.separator + "topology.output");
    topologyFile.deleteOnExit();
    File jsonFile = new File(workSpace.getAbsolutePath() + File.separator
        + "out" + File.separator + "job-trace.json");
    jsonFile.deleteOnExit();

    TraceBuilder.main(args);
    // check job trace file and this file not empty
    assertTrue(topologyFile.exists());
    assertTrue(topologyFile.length() > 0);

    // check topology file and this file not empty
    assertTrue(jsonFile.exists());
    assertTrue(jsonFile.length() > 0);

    // test Folder
    args = new String[7];
    args[0] = "-output-duration";
    args[1] = "10h";
    args[2] = "-input-cycle";
    args[3] = "20m";
    args[4] = "-debug";
    args[5] = "file://" + workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "job-trace.json";
    args[6] = "file://" + workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "job-trace-10hr.json";
    Folder.main(args);

    assertTrue(new File(workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "job-trace-10hr.json").exists());

    // test data
    JobTraceReader reader = new JobTraceReader(new FileInputStream(new File(
        workSpace.getAbsolutePath() + File.separator + "out" + File.separator
            + "job-trace.json")));

    LoggedJob job = reader.getNext();
    assertEquals("job_201002080801_40864", job.getJobID().toString());
    assertEquals("[DOTS-PROD-job_name-DAILY/20100210] - data\\.transform", job
        .getJobName().getValue());
    assertEquals("JAVA", job.getJobtype().toString());
    assertEquals(5, job.getReduceTasks().size());

    int i = 1;
    while (reader.getNext() != null) {
      i++;
    }
    assertEquals(2, i);

  }

  /**
   * 
   * test Anonymizer utility
   */
  @Test
  public void testAnonymizer() throws Exception {
    String[] args = new String[3];
    args[0] = "file://" + workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "job-trace.json";
    args[1] = "file://" + workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "topology.output";
    args[2] = "file://" + workSpace.getAbsolutePath();
    File topologyFile = new File(workSpace.getAbsolutePath() + File.separator
        + "out" + File.separator + "topology.output");
    // topologyFile.deleteOnExit();
    File jsonFile = new File(workSpace.getAbsolutePath() + File.separator
        + "out" + File.separator + "job-trace.json");
    // jsonFile.deleteOnExit();

    TraceBuilder.main(args);

    assertTrue(topologyFile.exists());
    assertTrue(jsonFile.exists());

    args = new String[6];
    args[0] = "-trace";
    args[1] = jsonFile.getAbsolutePath();
    args[2] = workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "job-trace-out.json";

    args[3] = "-topology";
    args[4] = topologyFile.getAbsolutePath();
    args[5] = workSpace.getAbsolutePath() + File.separator + "out"
        + File.separator + "topology_out.output";

    Anonymizer.main(args);
    // test some fields in topology file

    ClusterTopologyReader ctp = new ClusterTopologyReader(new FileInputStream(
        new File(args[5])));
    LoggedNetworkTopology lnt = ctp.get();
    assertEquals(14, lnt.getChildren().size());
    assertEquals("<root>", lnt.getName().getValue());

    JobTraceReader reader = new JobTraceReader(new FileInputStream(new File(
        args[2])));
    // test some fields in job trace file
    LoggedJob lg = reader.getNext();
    assertEquals("job_201002080801_40864", lg.getJobID().toString());
    assertEquals("job0", lg.getJobName().getValue());
    assertEquals("JAVA", lg.getJobtype().toString());

    int i = 1;
    while (reader.getNext() != null) {
      i++;
    }
    assertEquals(2, i);

  }

}
