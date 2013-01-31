/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestLoadJob {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  {
    ((Log4JLogger) LogFactory.getLog("org.apache.hadoop.mapred.gridmix"))
        .getLogger().setLevel(Level.DEBUG);
    ((Log4JLogger) LogFactory.getLog(StressJobFactory.class)).getLogger()
        .setLevel(Level.DEBUG);
  }

  static GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.REPLAY;
  private static final int NJOBS = 2;
  private static final long GENDATA = 50; // in megabytes

  @Before
  public void init() throws IOException {
    GridmixTestUtils.initCluster(TestLoadJob.class);
  }

  @After
  public void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }

  static class TestMonitor extends JobMonitor {
    private final BlockingQueue<Job> retiredJobs;
    private final int expected;

    public TestMonitor(int expected, Statistics stats) {
      super(1, TimeUnit.SECONDS, stats, 1);
      this.expected = expected;
      retiredJobs = new LinkedBlockingQueue<Job>();
    }

    @Override
    protected void onSuccess(Job job) {
      LOG.info(" Job Sucess " + job);
      retiredJobs.add(job);
    }

    @Override
    protected void onFailure(Job job) {
      fail("Job failure: " + job);
    }

    public void verify(ArrayList<JobStory> submitted) throws Exception {
      assertEquals("Bad job count", expected, retiredJobs.size());
    }
  }

  static class DebugGridmix extends Gridmix {

    private JobFactory<?> factory;
    private TestMonitor monitor;

    @Override
    protected JobMonitor createJobMonitor(Statistics stats, Configuration conf)
        throws IOException {
      monitor = new TestMonitor(1, stats);
      return monitor;
    }

    @Override
    protected JobFactory<?> createJobFactory(JobSubmitter submitter,
        String traceIn, Path scratchDir, Configuration conf,
        CountDownLatch startFlag, UserResolver userResolver) throws IOException {
      factory = DebugJobFactory.getFactory(submitter, scratchDir, NJOBS, conf,
          startFlag, userResolver);
      return factory;
    }

    public void checkMonitor() throws Exception {
      monitor.verify(((DebugJobFactory.Debuggable) factory).getSubmitted());
    }
  }
/*
 * test serial Policy
 */
  @Test
  public void testSerialSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.SERIAL;
    LOG.info("Serial started at " + System.currentTimeMillis());
    doSubmission();
    LOG.info("Serial ended at " + System.currentTimeMillis());
  }
  /*
   * test reply Policy
   */
  @Test
  public void testReplaySubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.REPLAY;
    LOG.info(" Replay started at " + System.currentTimeMillis());
    doSubmission();
    LOG.info(" Replay ended at " + System.currentTimeMillis());
  }

  private void doSubmission(String... optional) throws Exception {

    final Path in = new Path("foo").makeQualified(
        GridmixTestUtils.dfs.getUri(),
        GridmixTestUtils.dfs.getWorkingDirectory());
    final Path out = GridmixTestUtils.DEST.makeQualified(
        GridmixTestUtils.dfs.getUri(),
        GridmixTestUtils.dfs.getWorkingDirectory());
    final Path root = new Path("/user");
    Configuration conf = null;
    try {
      // required options
      final String[] required = {
          "-D" + FilePool.GRIDMIX_MIN_FILE + "=0",
          "-D" + Gridmix.GRIDMIX_OUT_DIR + "=" + out,
          "-D" + Gridmix.GRIDMIX_USR_RSV + "="
              + EchoUserResolver.class.getName(),
          "-D" + JobCreator.GRIDMIX_JOB_TYPE + "=" + JobCreator.LOADJOB.name(),
          "-D" + SleepJob.GRIDMIX_SLEEP_INTERVAL + "=" + "10" };
      // mandatory arguments

      File fout = new File("src" + File.separator + "test" + File.separator
          + "resources" + File.separator + "data" + File.separator
          + "wordcount.json");

      final String[] mandatory = { "-generate", String.valueOf(GENDATA) + "m",
          in.toString(), "file:///" + fout.getAbsolutePath() 
      };

      ArrayList<String> argv = new ArrayList<String>(required.length
          + optional.length + mandatory.length);
      for (String s : required) {
        argv.add(s);
      }
      for (String s : optional) {
        argv.add(s);
      }
      for (String s : mandatory) {
        argv.add(s);
      }

      DebugGridmix client = new DebugGridmix();
      conf = new Configuration();
      conf.setEnum(GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, policy);
      conf = GridmixTestUtils.mrvl.getConfig();
      // set timestamps
      conf.setStrings("1", "2");
      // GridmixTestUtils.createHomeAndStagingDirectory((JobConf)conf);
      // allow synthetic users to create home directories
      GridmixTestUtils.dfs.mkdirs(root, new FsPermission((short) 0777));
      GridmixTestUtils.dfs.setPermission(root, new FsPermission((short) 0777));
      String[] args = argv.toArray(new String[argv.size()]);
      LOG.info("Command line arguments:");
      for (int i = 0; i < args.length; ++i) {
        System.out.printf("    [%d] %s\n", i, args[i]);
      }
      // run Gridmix
      int res = ToolRunner.run(conf, client, args);
      assertEquals("Client exited with nonzero status", 0, res);
      // check result
      client.checkMonitor();
      
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      in.getFileSystem(conf).delete(in, true);
      out.getFileSystem(conf).delete(out, true);
      root.getFileSystem(conf).delete(root, true);
    }
  }

}
