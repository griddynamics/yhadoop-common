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
package org.apache.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Ignore;
import org.junit.Test;

import com.sun.source.tree.AssertTree;

//@Ignore
public class TestMRJobClient extends ClusterMapReduceTestCase {

  private static final Log LOG = LogFactory.getLog(TestMRJobClient.class);

  private Job runJob(Configuration conf) throws Exception {
    String input = "hello1\nhello2\nhello3\n";

    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, input);
    job.setJobName("mr");
    job.setPriority(JobPriority.HIGH);
    job.waitForCompletion(true);
    return job;
  }

  public static int runTool(Configuration conf, Tool tool, String[] args,
      OutputStream out) throws Exception {
    PrintStream oldOut = System.out;
    PrintStream newOut = new PrintStream(out, true);
    try {
      System.setOut(newOut);
      return ToolRunner.run(conf, tool, args);
    } finally {
      System.setOut(oldOut);
    }
  }

  private static class BadOutputFormat extends TextOutputFormat {
    @Override
    public void checkOutputSpecs(JobContext job)
        throws FileAlreadyExistsException, IOException {
      throw new IOException();
    }
  }

  @Ignore
  public void testJobSubmissionSpecsAndFiles() throws Exception {
    Configuration conf = createJobConf();
    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1);
    job.setOutputFormatClass(BadOutputFormat.class);
    try {
      job.submit();
      fail("Should've thrown an exception while checking output specs.");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
    JobID jobId = job.getJobID();
    Cluster cluster = new Cluster(conf);
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster,
        job.getConfiguration());
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
    assertFalse("Shouldn't have created a job file if job specs failed.",
        FileSystem.get(conf).exists(submitJobFile));
  }

  @Test
  public void testJobClient() throws Exception {
    Configuration conf = createJobConf();
    Job job = runJob(conf);
    String jobId = job.getJobID().toString();
    testGetCounter(jobId, conf);
    testJobList(jobId, conf);
    testJobStatus(jobId, conf);
    testJobEvents(jobId, conf);
    testJobHistory(jobId, conf);
    testListTrackers(jobId, conf);
    testListAttemptIds(jobId, conf);
   testListBlackList(conf);
    
    testMain();
    // does not implemented !!!
    // testChangingJobPriority(jobId, conf);
  }

  private void testMain(){
    ByteArrayOutputStream data= new ByteArrayOutputStream();
    PrintStream error=System.err;
    System.setErr(new PrintStream(data));
    ExitUtil.disableSystemExit();
    try{
      CLI.main(new String[0]);
    }catch(ExitUtil.ExitException e){
      ExitUtil.resetFirstExitException();
      assertEquals(-1, e.status);
    }catch(Exception e){
      
    }finally{
      System.setErr(error);
    }
    String s= new String(data.toByteArray());
    assertTrue(s.contains("-submit"));
    assertTrue(s.contains("-status"));
    assertTrue(s.contains("-kill"));
    assertTrue(s.contains("-set-priority"));
    assertTrue(s.contains("-events"));
    assertTrue(s.contains("-history"));
    assertTrue(s.contains("-list"));
    assertTrue(s.contains("-list-active-trackers"));
    assertTrue(s.contains("-list-blacklisted-trackers"));
    assertTrue(s.contains("-list-attempt-ids"));
    assertTrue(s.contains("-kill-task"));
    assertTrue(s.contains("-fail-task"));
    assertTrue(s.contains("-logs"));
    
  } 
  private void testListBlackList(Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc,
        new String[] { "-list-blacklisted-trackers" }, out);
    assertEquals("Exit code", 0, exitCode);
    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      counter++;
    }
    assertEquals(0, counter);
  }

  private void testListAttemptIds(String jobId, Configuration conf)
      throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc, new String[] { "-list-attempt-ids", jobId,
        "MAP", "completed" }, out);
    assertEquals("Exit code", 0, exitCode);
    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      counter++;
    }
    assertEquals(1, counter);
  }

  private void testListTrackers(String jobId, Configuration conf)
      throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc, new String[] { "-list-active-trackers" },
        out);
    assertEquals("Exit code", 0, exitCode);
    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      counter++;
    }
    assertEquals(2, counter);
  }

  private void testJobHistory(String jobId, Configuration conf)
      throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    File f = new File("src/test/resources/job_1329348432655_0001-10.jhist");
    int exitCode = runTool(conf, jc, new String[] { "-history", "all",
        "file://" + f.getAbsolutePath() }, out);
    assertEquals("Exit code", 0, exitCode);
    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (line.startsWith("task_")) {
        counter++;
      }
    }
    assertEquals(23, counter);
  }

  private void testJobEvents(String jobId, Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    int exitCode = runTool(conf, jc, new String[] { "-events", jobId, "0",
        "100" }, out);
    assertEquals("Exit code", 0, exitCode);
    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    String attemptId = ("attempt" + jobId.substring(3));
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (line.contains(attemptId)) {
        counter++;
      }
    }
    assertEquals(2, counter);
  }

  private void testJobStatus(String jobId, Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    int exitCode = runTool(conf, jc, new String[] { "-status", jobId }, out);
    assertEquals("Exit code", 0, exitCode);
    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));

    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (!line.contains("Job state:")) {
        continue;
      }
      break;
    }
    assertTrue(line.contains("SUCCEEDED"));
  }

  public void testGetCounter(String jobId, Configuration conf) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, createJobClient(),
        new String[] { "-counter", jobId,
            "org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS" },
        out);
    assertEquals("Exit code", 0, exitCode);
    assertEquals("Counter", "3", out.toString().trim());
  }

  public void testJobList(String jobId, Configuration conf) throws Exception {
    verifyJobPriority(jobId, "NORMAL", conf, createJobClient());
  }

  protected void verifyJobPriority(String jobId, String priority,
      Configuration conf, CLI jc) throws Exception {
    PipedInputStream pis = new PipedInputStream();
    PipedOutputStream pos = new PipedOutputStream(pis);
    int exitCode = runTool(conf, jc, new String[] { "-list", "all" }, pos);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br = new BufferedReader(new InputStreamReader(pis));
    String line = null;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (!line.contains(jobId)) {
        continue;
      }
      assertTrue(line.contains(priority));
      break;
    }
    pis.close();
  }

  public void testChangingJobPriority(String jobId, Configuration conf)
      throws Exception {
    int exitCode = runTool(conf, createJobClient(), new String[] {
        "-set-priority", jobId, "VERY_LOW" }, new ByteArrayOutputStream());
    assertEquals("Exit code", 0, exitCode);
    verifyJobPriority(jobId, "VERY_LOW", conf, createJobClient());
  }

  @Ignore
  public void testMissingProfileOutput() throws Exception {
    Configuration conf = createJobConf();
    final String input = "hello1\n";

    // Set a job to be profiled with an empty agentlib parameter.
    // This will fail to create profile.out files for tasks.
    // This will succeed by skipping the HTTP fetch of the
    // profiler output.
    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, input);
    job.setJobName("disable-profile-fetch");
    job.setProfileEnabled(true);
    job.setProfileParams("-agentlib:,verbose=n,file=%s");
    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);
    job.setJobSetupCleanupNeeded(false);
    job.waitForCompletion(true);

    // Run another job with an hprof agentlib param; verify
    // that the HTTP fetch works here.
    Job job2 = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, input);
    job2.setJobName("enable-profile-fetch");
    job2.setProfileEnabled(true);
    job2.setProfileParams("-agentlib:hprof=cpu=samples,heap=sites,force=n,"
        + "thread=y,verbose=n,file=%s");
    job2.setProfileTaskRange(true, "0-1");
    job2.setProfileTaskRange(false, "");
    job2.setMaxMapAttempts(1);
    job2.setMaxReduceAttempts(1);
    job2.setJobSetupCleanupNeeded(false);
    job2.waitForCompletion(true);

    // Find the first map task, verify that we got its profile output file.
    TaskReport[] reports = job2.getTaskReports(TaskType.MAP);
    assertTrue("No task reports found!", reports.length > 0);
    TaskReport report = reports[0];
    TaskID id = report.getTaskId();
    assertTrue(TaskType.MAP == id.getTaskType());
    System.out.println("Using task id: " + id);
    TaskAttemptID attemptId = new TaskAttemptID(id, 0);

    File profileOutFile = new File(attemptId.toString() + ".profile");
    assertTrue("Couldn't find profiler output", profileOutFile.exists());
    assertTrue("Couldn't remove profiler output", profileOutFile.delete());
  }

  protected CLI createJobClient() throws IOException {
    return new CLI();
  }

}
