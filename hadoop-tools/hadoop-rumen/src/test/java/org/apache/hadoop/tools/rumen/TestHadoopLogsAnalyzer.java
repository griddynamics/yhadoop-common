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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/*
  Test Hadoop Logs Analyzer.  The HadoopLogsAnalyzer should print data from logs to std output
 */
public class TestHadoopLogsAnalyzer {
  // source path
  private static File workSpace = new File("src" + File.separator + "test"
      + File.separator + "resources" + File.separator + "log");
  // target path
  private static File outSpace = new File("target" + File.separator + "out");

  @SuppressWarnings("deprecation")
  @Test
  public void testTestHadoopLogsAnalyzer() throws Exception {

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    System.setOut(out);
    try {

      if (!outSpace.exists()) {
        Assert.assertTrue(outSpace.mkdirs());
      }
      String[] args = new String[14];
      // for jobs trace output
      args[0] = "-write-job-trace";
      args[1] = outSpace.getAbsolutePath() + File.separator + "job-trace";
      // for topology
      args[2] = "-write-topology";
      args[3] = outSpace.getAbsolutePath() + File.separator + "topology";
      args[4] = "-single-line-job-traces";

      args[5] = "-v1";
      args[6] = "-d";
      args[7] = "-delays";
      args[8] = "-job-digest-spectra";
      args[9] = "50";
      args[10] = "-spreads";
      args[11] = "1";
      args[12] = "999";

      args[13] = workSpace.getAbsolutePath();

      HadoopLogsAnalyzer.main(args);
      assertTrue(bytes.toString().contains("utcome = SUCCESS, and type = JAVA."));
      // we see the percents
      assertTrue(bytes.toString().contains("40%"));

      // trace file exists and not empty
      File trace = new File(outSpace.getAbsolutePath() + File.separator
          + "job-trace");
      assertTrue(trace.exists());
      assertTrue(trace.length() > 0);

      // topology file exists and not empty
      File topology = new File(outSpace.getAbsolutePath() + File.separator
          + "topology");
      assertTrue(topology.exists());
      assertTrue(topology.length() > 0);

      // check trace result
      JobTraceReader traceReader = new JobTraceReader(new FileInputStream(
          new File(args[1])));
      LoggedJob job = traceReader.getNext();
      assertEquals("job_201002080801_40864", job.getJobID().toString());
      assertEquals("JAVA", job.getJobtype().toString());
      assertEquals(5, job.getReduceTasks().size());
      assertEquals("geek1", job.getUser().getValue());

      int counter = 1;
      while (traceReader.getNext() != null) {
        counter++;
      }
      assertEquals(2, counter);

      // check topology
      ClusterTopologyReader topologyReader = new ClusterTopologyReader(
          new FileInputStream(new File(args[3])));
      assertEquals("<root>", topologyReader.get().getName().getValue());
      assertEquals(14, topologyReader.get().getChildren().size());

    } finally {
      System.setOut(oldOut);
    }
  }
}
