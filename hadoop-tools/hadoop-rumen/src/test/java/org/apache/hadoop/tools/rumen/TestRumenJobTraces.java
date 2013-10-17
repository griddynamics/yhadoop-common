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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TestNoJobSetupCleanup.MyOutputFormat;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.tools.rumen.TraceBuilder.MyOptions;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

public class TestRumenJobTraces {
  private static final Log LOG = LogFactory.getLog(TestRumenJobTraces.class);

  private final static String INPUT_DIR_ROOT = 
      System.getProperty("test.cache.data", "target/test-classes");

  private final Configuration conf = new Configuration();
  private FileSystem lfs;
  private Path rootInputDir;
  private Path rootTempDir;
  
  @Before
  public void setup() throws Exception {
    lfs = FileSystem.getLocal(conf);
    rootInputDir = new Path(INPUT_DIR_ROOT).makeQualified(
        lfs.getUri(), lfs.getWorkingDirectory());
    rootTempDir = PathUtils.getTestPath(getClass());
  }
  
  @Test(timeout=5000)
  public void testSmallTrace() throws Exception {
    performSingleTest("sample-job-tracker-logs.gz",
        "job-tracker-logs-topology-output", "job-tracker-logs-trace-output.gz");
  }

  @Test(timeout=5000)
  public void testTruncatedTask() throws Exception {
    performSingleTest("truncated-job-tracker-log", "truncated-topology-output",
        "truncated-trace-output");
  }

  private void performSingleTest(String jtLogName, String goldTopology,
      String goldTrace) throws Exception {
    
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputFile = new Path(rootInputDir, "small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    final Path topologyFile = new Path(tempDir, jtLogName + "-topology.json");
    final Path traceFile = new Path(tempDir, jtLogName + "-trace.json");

    final Path inputFile = new Path(rootInputFile, jtLogName);

    System.out.println("topology result file = " + topologyFile);
    System.out.println("trace result file = " + traceFile);

    String[] args = new String[6];

    args[0] = "-v1";

    args[1] = "-write-topology";
    args[2] = topologyFile.toString();

    args[3] = "-write-job-trace";
    args[4] = traceFile.toString();

    args[5] = inputFile.toString();

    final Path topologyGoldFile = new Path(rootInputFile, goldTopology);
    final Path traceGoldFile = new Path(rootInputFile, goldTrace);

    @SuppressWarnings("deprecation")
    HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces
        .<LoggedNetworkTopology> jsonFileMatchesGold(conf, topologyFile,
            topologyGoldFile, LoggedNetworkTopology.class, "topology");
    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(conf, traceFile,
        traceGoldFile, LoggedJob.class, "trace");
  }

  @Test(timeout=5000)
  public void testRumenViaDispatch() throws Exception {
    final Path rootInputPath = new Path(rootInputDir, "small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenViaDispatch");
    lfs.delete(tempDir, true);

    final Path topologyPath = new Path(tempDir, "dispatch-topology.json");
    final Path tracePath = new Path(tempDir, "dispatch-trace.json");

    final Path inputPath =
        new Path(rootInputPath, "dispatch-sample-v20-jt-log.gz");

    System.out.println("topology result file = " + topologyPath);
    System.out.println("testRumenViaDispatch() trace result file = " + tracePath);

    String demuxerClassName = ConcatenatedInputFilesDemuxer.class.getName();

    String[] args =
        { "-demuxer", demuxerClassName, tracePath.toString(),
            topologyPath.toString(), inputPath.toString() };

    final Path topologyGoldFile =
        new Path(rootInputPath, "dispatch-topology-output.json.gz");
    final Path traceGoldFile =
        new Path(rootInputPath, "dispatch-trace-output.json.gz");

    Tool analyzer = new TraceBuilder();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces
        .<LoggedNetworkTopology> jsonFileMatchesGold(conf, topologyPath,
            topologyGoldFile, LoggedNetworkTopology.class, "topology");
    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(conf, tracePath,
        traceGoldFile, LoggedJob.class, "trace");
  }

  @Test(timeout=5000)
  public void testBracketedCounters() throws Exception {
    final Path rootInputPath = new Path(rootInputDir, "small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestBracketedCounters");
    lfs.delete(tempDir, true);

    final Path topologyPath = new Path(tempDir, "dispatch-topology.json");
    final Path tracePath = new Path(tempDir, "dispatch-trace.json");

    final Path inputPath = new Path(rootInputPath, "counters-format-test-logs");

    System.out.println("topology result file = " + topologyPath);
    System.out.println("testBracketedCounters() trace result file = " + tracePath);

    final Path goldPath =
        new Path(rootInputPath, "counters-test-trace.json.gz");

    String[] args =
        { tracePath.toString(), topologyPath.toString(), inputPath.toString() };

    Tool analyzer = new TraceBuilder();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(conf, tracePath,
        goldPath, LoggedJob.class, "trace");
  }

  @Test(timeout=500)
  public void testHadoop20JHParser() throws Exception {
    final Path rootInputPath = new Path(rootInputDir, "small-trace-test");

    // history file to be parsed to get events
    final Path inputPath = new Path(rootInputPath, "v20-single-input-log.gz");

    RewindableInputStream ris = getRewindableInputStream(inputPath, conf);
    assertNotNull(ris);

    Hadoop20JHParser parser = null;

    try {
      assertEquals("Hadoop20JHParser can't parse the test file " +
          inputPath, true, Hadoop20JHParser.canParse(ris));

      ris.rewind();
      parser = new Hadoop20JHParser(ris);
      ArrayList<String> seenEvents = new ArrayList<String>(150);

      // this is same as the one in input history file
      String jobId = "job_200904211745_0002";
      JobBuilder builder = new JobBuilder(jobId);

      // get events into seenEvents
      getHistoryEvents(parser, seenEvents, builder);

      // Validate the events seen by history parser from
      // history file v20-single-input-log.gz
      validateSeenHistoryEvents(seenEvents, goldLines);

      ParsedJob parsedJob = builder.build();
      // validate the obtainXXX api of ParsedJob, ParsedTask and
      // ParsedTaskAttempt
      validateParsedJob(parsedJob, 20, 1, true);
    } finally {
      if (parser != null) {
        parser.close();
      }
      ris.close();
    }
  }

  /**
   * Check if processing of input arguments is as expected by passing globbed
   * input path
   * <li> without -recursive option and
   * <li> with -recursive option.
   */
  @Test(timeout=5000)
  public void testProcessInputArgument() throws Exception {
    // define the test's root input directory
    Path testRootInputDir = new Path(rootTempDir, "TestProcessInputArgument");
    // define the nested input directory
    Path nestedInputDir = new Path(testRootInputDir, "1/2/3/4");
    // define the globbed version of the nested input directory
    Path globbedInputNestedDir =
      lfs.makeQualified(new Path(testRootInputDir, "*/*/*/*/*"));
    try {
      lfs.delete(nestedInputDir, true);

      List<String> recursiveInputPaths = new ArrayList<String>();
      List<String> nonRecursiveInputPaths = new ArrayList<String>();
      // Create input files under the given path with multiple levels of
      // sub directories
      createHistoryLogsHierarchy(nestedInputDir, lfs, recursiveInputPaths,
          nonRecursiveInputPaths);

      // Check the case of globbed input path and without -recursive option
      List<Path> inputs = MyOptions.processInputArgument(
                              globbedInputNestedDir.toString(), conf, false);
      validateHistoryLogPaths(inputs, nonRecursiveInputPaths);

      // Check the case of globbed input path and with -recursive option
      inputs = MyOptions.processInputArgument(
                   globbedInputNestedDir.toString(), conf, true);
      validateHistoryLogPaths(inputs, recursiveInputPaths);

    } finally {
      lfs.delete(testRootInputDir, true);
    }
  }

  /**
   * Validate if the input history log paths are as expected.
   * @param inputs  the resultant input paths to be validated
   * @param expectedHistoryFileNames  the expected input history logs
   * @throws IOException
   */
  private void validateHistoryLogPaths(List<Path> inputs,
      List<String> expectedHistoryFileNames) throws IOException {

    System.out.println("\nExpected history files are:");
    for (String historyFile : expectedHistoryFileNames) {
      System.out.println(historyFile);
    }
    System.out.println("\nResultant history files are:");
    List<String> historyLogs = new ArrayList<String>();
    for (Path p : inputs) {
      historyLogs.add(p.toUri().getPath());
      System.out.println(p.toUri().getPath());
    }

    assertEquals("Number of history logs found is different from the expected.",
        expectedHistoryFileNames.size(), inputs.size());

    // Verify if all the history logs are expected ones and they are in the
    // expected order
    assertTrue("Some of the history log files do not match the expected.",
        historyLogs.equals(expectedHistoryFileNames));
  }

  /**
   * Create history logs under the given path with multiple levels of
   * sub directories as shown below.
   * <br>
   * Create a file, an empty subdirectory and a nonempty subdirectory
   * &lt;historyDir&gt; under the given input path.
   * <br>
   * The subdirectory &lt;historyDir&gt; contains the following dir structure:
   * <br>
   * <br>&lt;historyDir&gt;/historyFile1.txt
   * <br>&lt;historyDir&gt;/historyFile1.gz
   * <br>&lt;historyDir&gt;/subDir1/historyFile2.txt
   * <br>&lt;historyDir&gt;/subDir1/historyFile2.gz
   * <br>&lt;historyDir&gt;/subDir2/historyFile3.txt
   * <br>&lt;historyDir&gt;/subDir2/historyFile3.gz
   * <br>&lt;historyDir&gt;/subDir1/subDir11/historyFile4.txt
   * <br>&lt;historyDir&gt;/subDir1/subDir11/historyFile4.gz
   * <br>&lt;historyDir&gt;/subDir2/subDir21/
   * <br>
   * Create the lists of input paths that should be processed by TraceBuilder
   * for recursive case and non-recursive case.
   * @param nestedInputDir the input history logs directory where history files
   *                       with nested subdirectories are created
   * @param fs         FileSystem of the input paths
   * @param recursiveInputPaths input paths for recursive case
   * @param nonRecursiveInputPaths input paths for non-recursive case
   * @throws IOException
   */
  private void createHistoryLogsHierarchy(Path nestedInputDir, FileSystem fs,
      List<String> recursiveInputPaths, List<String> nonRecursiveInputPaths)
  throws IOException {
    List<Path> dirs = new ArrayList<Path>();
    // define a file in the nested test input directory
    Path inputPath1 = new Path(nestedInputDir, "historyFile.txt");
    // define an empty sub-folder in the nested test input directory
    Path emptyDir = new Path(nestedInputDir, "emptyDir");
    // define a nonempty sub-folder in the nested test input directory
    Path historyDir = new Path(nestedInputDir, "historyDir");

    fs.mkdirs(nestedInputDir);
    // Create an empty input file
    fs.createNewFile(inputPath1);
    // Create empty subdir
    fs.mkdirs(emptyDir);// let us not create any files under this dir

    fs.mkdirs(historyDir);
    dirs.add(historyDir);

    Path subDir1 = new Path(historyDir, "subDir1");
    fs.mkdirs(subDir1);
    dirs.add(subDir1);
    Path subDir2 = new Path(historyDir, "subDir2");
    fs.mkdirs(subDir2);
    dirs.add(subDir2);

    Path subDir11 = new Path(subDir1, "subDir11");
    fs.mkdirs(subDir11);
    dirs.add(subDir11);
    Path subDir21 = new Path(subDir2, "subDir21");
    fs.mkdirs(subDir21);// let us not create any files under this dir

    int i = 0;
    for (Path dir : dirs) {
      i++;
      Path gzPath = new Path(dir, "historyFile" + i + ".gz");
      Path txtPath = new Path(dir, "historyFile" + i + ".txt");
      fs.createNewFile(txtPath);
      fs.createNewFile(gzPath);
      recursiveInputPaths.add(gzPath.toUri().getPath());
      recursiveInputPaths.add(txtPath.toUri().getPath());
      if (i == 1) {
        nonRecursiveInputPaths.add(gzPath.toUri().getPath());
        nonRecursiveInputPaths.add(txtPath.toUri().getPath());
      }
    }
    recursiveInputPaths.add(inputPath1.toUri().getPath());
    nonRecursiveInputPaths.add(inputPath1.toUri().getPath());
  }

  /**
   * Test if {@link CurrentJHParser} can read events from current JH files.
   */
  @Test//(timeout=150000)
  public void testCurrentJHParser() throws Exception {
    final Path tempDir = new Path(rootTempDir, "TestCurrentJHParser");
    lfs.delete(tempDir, true);
    
    // Run a MR job
    // create a MR cluster
    conf.setInt(TTConfig.TT_MAP_SLOTS, 1);
    conf.setInt(TTConfig.TT_REDUCE_SLOTS, 1);
    MiniMRClientCluster mrCluster = MiniMRClientClusterFactory.create(getClass(), 1, conf);
    
    // run a job
    Path inDir = new Path(tempDir, "input");
    Path outDir = new Path(tempDir, "output");
    JobHistoryParser parser = null;
    RewindableInputStream ris = null;
    ArrayList<String> seenEvents = new ArrayList<String>(15);
    
    try {
      JobConf jConf = new JobConf(mrCluster.getConfig());
      // construct a job with 1 map and 1 reduce task.
      Job job = MapReduceTestUtil.createJob(jConf, inDir, outDir, 1, 1);
      // disable setup/cleanup
      job.setJobSetupCleanupNeeded(false);
      // set the output format to take care of the _temporary folder
      job.setOutputFormatClass(MyOutputFormat.class);
      // wait for the job to complete
      job.waitForCompletion(false);
      
      assertTrue("Job failed", job.isSuccessful());

      JobID id = job.getJobID();

      Path inputPath = null;
      
      // wait for 10 secs for the jobhistory file to move into the done folder
      for (int i = 0; i < 100; ++i) {
        // get the jobhistory filepath
        try {
          HistoryFileInfo fileInfo = mrCluster.getJobFileInfo(
              org.apache.hadoop.mapred.JobID.forName(id.toString()));
          if (fileInfo != null && !fileInfo.isMovePending()) {
            inputPath = fileInfo.getHistoryFile();
            if (lfs.exists(inputPath)) {
              break;
            }
          }
        } catch (IOException e) {
        }
        TimeUnit.MILLISECONDS.sleep(100);
      }
    
      assertTrue("Missing job history file", inputPath != null && 
          lfs.exists(inputPath));

      ris = getRewindableInputStream(inputPath, conf);

      // Test if the JobHistoryParserFactory can detect the parser correctly
      parser = JobHistoryParserFactory.getParser(ris);

      // create a job builder
      JobBuilder builder = new JobBuilder(id.toString());

      // get events into seenEvents and also process them using builder
      getHistoryEvents(parser, seenEvents, builder); 

      // Check against the gold standard
      System.out.println("testCurrentJHParser validating using gold std ");
      // The list of history events expected when parsing the above job's
      // history log file
      String[] goldLinesExpected = new String[] {
          AMSE, JSE, JIE, JICE, TSE, TSE, ASE, MFE, TFE, ASE, RFE, TFE, JFE
      };

      validateSeenHistoryEvents(seenEvents, goldLinesExpected);
      
      // validate resource usage metrics
      //  get the job counters
      Counters counters = job.getTaskReports(TaskType.MAP)[0].getTaskCounters();

      // get the parsed job
      ParsedJob parsedJob = builder.build();
      //  get the logged job
      LoggedJob loggedJob = parsedJob;
      //  get the logged attempts
      LoggedTaskAttempt attempt = 
        loggedJob.getMapTasks().get(0).getAttempts().get(0);
      //  get the resource usage metrics
      ResourceUsageMetrics metrics = attempt.getResourceUsageMetrics();
      
      //  check with the actual values
      testResourceUsageMetricViaDeepCompare(metrics, 
          counters.findCounter(TaskCounter.CPU_MILLISECONDS).getValue(), 
          counters.findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES).getValue(),
          counters.findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES).getValue(),
          counters.findCounter(TaskCounter.COMMITTED_HEAP_BYTES).getValue(),
          true);

      // validate the obtainXXX api of ParsedJob, ParsedTask and
      // ParsedTaskAttempt
      validateParsedJob(parsedJob, 1, 1, false);
    } finally {
      // stop the MR cluster
      mrCluster.stop();
      
      if (ris != null) {
          ris.close();
      }
      if (parser != null) {
        parser.close();
      }
      
      // cleanup the filesystem
      lfs.delete(tempDir, true);
    }
  }

  /**
   * Verify if the obtainXXX methods of {@link ParsedJob}, {@link ParsedTask}
   * and {@link ParsedTaskAttempt} give valid info
   */
  private void validateParsedJob(ParsedJob parsedJob, int numMaps,
      int numReduces, boolean pre21JobHistory) {
    validateParsedJobAPI(parsedJob, numMaps, numReduces, pre21JobHistory);

    List<ParsedTask> maps = parsedJob.obtainMapTasks();
    for (ParsedTask task : maps) {
      validateParsedTask(task);
    }
    List<ParsedTask> reduces = parsedJob.obtainReduceTasks();
    for (ParsedTask task : reduces) {
      validateParsedTask(task);
    }
    List<ParsedTask> others = parsedJob.obtainOtherTasks();
    for (ParsedTask task : others) {
      validateParsedTask(task);
    }
  }

  /** Verify if the obtainXXX methods of {@link ParsedJob} give valid info */
  private void validateParsedJobAPI(ParsedJob parsedJob, int numMaps,
      int numReduces, boolean pre21JobHistory) {
    LOG.info("Validating ParsedJob.obtainXXX api... for "
             + parsedJob.getJobID());
    assertNotNull("Job acls in ParsedJob is null",
                  parsedJob.obtainJobAcls());
    assertNotNull("Job conf path in ParsedJob is null",
                  parsedJob.obtainJobConfpath());

    assertNotNull("Map Counters in ParsedJob is null",
                  parsedJob.obtainMapCounters());
    assertNotNull("Reduce Counters in ParsedJob is null",
                  parsedJob.obtainReduceCounters());
    assertNotNull("Total Counters in ParsedJob is null",
                  parsedJob.obtainTotalCounters());

    assertNotNull("Map Tasks List in ParsedJob is null",
                  parsedJob.obtainMapTasks());
    assertNotNull("Reduce Tasks List in ParsedJob is null",
                  parsedJob.obtainReduceTasks());
    assertNotNull("Other Tasks List in ParsedJob is null",
                  parsedJob.obtainOtherTasks());

    // 1 map and 1 reduce task should be there
    assertEquals("Number of map tasks in ParsedJob is wrong",
                 numMaps, parsedJob.obtainMapTasks().size());
    assertEquals("Number of reduce tasks in ParsedJob is wrong",
                 numReduces, parsedJob.obtainReduceTasks().size(), 1);

    // old hadoop20 version history files don't have job-level-map-counters and
    // job-level-reduce-counters. Only total counters exist there.
    assertTrue("Total Counters in ParsedJob is empty",
               parsedJob.obtainTotalCounters().size() > 0);
    if (!pre21JobHistory) {
      assertTrue("Map Counters in ParsedJob is empty",
                 parsedJob.obtainMapCounters().size() > 0);
      assertTrue("Reduce Counters in ParsedJob is empty",
                 parsedJob.obtainReduceCounters().size() > 0);
    }
  }

  /**
   * Verify if the obtainXXX methods of {@link ParsedTask} and
   * {@link ParsedTaskAttempt} give valid info
   */
  private void validateParsedTask(ParsedTask parsedTask) {
    validateParsedTaskAPI(parsedTask);

    List<ParsedTaskAttempt> attempts = parsedTask.obtainTaskAttempts();
    for (ParsedTaskAttempt attempt : attempts) {
      validateParsedTaskAttemptAPI(attempt);
    }
  }

  /** Verify if the obtainXXX methods of {@link ParsedTask} give valid info */
  private void validateParsedTaskAPI(ParsedTask parsedTask) {
    LOG.info("Validating ParsedTask.obtainXXX api... for "
             + parsedTask.getTaskID());
    assertNotNull("Task counters in ParsedTask is null",
                  parsedTask.obtainCounters());

    if (parsedTask.getTaskStatus()
        == Pre21JobHistoryConstants.Values.SUCCESS) {
      // task counters should not be empty
      assertTrue("Task counters in ParsedTask is empty",
                 parsedTask.obtainCounters().size() > 0);
      assertNull("Diagnostic-info is non-null for a succeeded task",
                 parsedTask.obtainDiagnosticInfo());
      assertNull("Failed-due-to-attemptId is non-null for a succeeded task",
                 parsedTask.obtainFailedDueToAttemptId());
    } else {
      assertNotNull("Diagnostic-info is non-null for a succeeded task",
                    parsedTask.obtainDiagnosticInfo());
      assertNotNull("Failed-due-to-attemptId is non-null for a succeeded task",
                    parsedTask.obtainFailedDueToAttemptId());
    }

    List<ParsedTaskAttempt> attempts = parsedTask.obtainTaskAttempts();
    assertNotNull("TaskAttempts list in ParsedTask is null", attempts);
    assertTrue("TaskAttempts list in ParsedTask is empty",
               attempts.size() > 0);    
  }

  /**
   * Verify if the obtainXXX methods of {@link ParsedTaskAttempt} give
   * valid info
   */
  private void validateParsedTaskAttemptAPI(
      ParsedTaskAttempt parsedTaskAttempt) {
    LOG.info("Validating ParsedTaskAttempt.obtainXXX api... for "
             + parsedTaskAttempt.getAttemptID());
    assertNotNull("Counters in ParsedTaskAttempt is null",
                  parsedTaskAttempt.obtainCounters());

    if (parsedTaskAttempt.getResult()
        == Pre21JobHistoryConstants.Values.SUCCESS) { 
      assertTrue("Counters in ParsedTaskAttempt is empty",
               parsedTaskAttempt.obtainCounters().size() > 0);
      assertNull("Diagnostic-info is non-null for a succeeded taskAttempt",
                 parsedTaskAttempt.obtainDiagnosticInfo());
    } else {
      assertNotNull("Diagnostic-info is non-null for a succeeded taskAttempt",
                 parsedTaskAttempt.obtainDiagnosticInfo());
    }
    assertNotNull("TrackerName in ParsedTaskAttempt is null",
                  parsedTaskAttempt.obtainTrackerName());

    assertNotNull("http-port info in ParsedTaskAttempt is null",
        parsedTaskAttempt.obtainHttpPort());
    assertNotNull("Shuffle-port info in ParsedTaskAttempt is null",
        parsedTaskAttempt.obtainShufflePort());
  }

  @Test(timeout=500)
  public void testJobConfigurationParser() throws Exception {
    validateJobConfParser("small-trace-test/sample-conf.file.xml", false);
    validateJobConfParser("small-trace-test/sample-conf.file.new.xml", true);
  }

  private void validateJobConfParser(String confFile, boolean newConfig)
      throws Exception {
    final Path inputPath = new Path(rootInputDir, confFile);

    InputStream inputConfStream =
        new PossiblyDecompressedInputStream(inputPath, conf);

    try {
      Properties props = JobConfigurationParser.parse(inputConfStream);
      inputConfStream.close();

      String oldOrNew = newConfig ? "New" : "Old";
      assertEquals(oldOrNew + " config property for job queue name is not "
          + " extracted properly.", "TheQueue",
          JobBuilder.extract(props, JobConfPropertyNames.QUEUE_NAMES
          .getCandidates(), null));
      assertEquals(oldOrNew + " config property for job name is not "
          + " extracted properly.", "MyMRJob",
          JobBuilder.extract(props, JobConfPropertyNames.JOB_NAMES
          .getCandidates(), null));

      validateChildJavaOpts(newConfig, props);

    } finally {
      inputConfStream.close();
    }
  }

  // Validate child java opts in properties.
  // newConfigProperties: boolean that specifies if the config properties to be
  // validated are new OR old.
  private void validateChildJavaOpts(boolean newConfigProperties,
      Properties props) {
    if (newConfigProperties) {
      assertEquals("New config property " + MRJobConfig.MAP_JAVA_OPTS
          + " is not extracted properly.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.MAP_JAVA_OPTS_S
          .getCandidates(), null));
      assertEquals("New config property " + MRJobConfig.REDUCE_JAVA_OPTS
          + " is not extracted properly.",
          "-server -Xmx650m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.REDUCE_JAVA_OPTS_S
          .getCandidates(), null));
    }
    else {
      // if old property mapred.child.java.opts is set, then extraction of all
      // the following 3 properties should give that value.
      assertEquals("mapred.child.java.opts is not extracted properly.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.TASK_JAVA_OPTS_S
          .getCandidates(), null));
      assertEquals("New config property " + MRJobConfig.MAP_JAVA_OPTS
          + " is not extracted properly when the old config property "
          + "mapred.child.java.opts is set.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.MAP_JAVA_OPTS_S
          .getCandidates(), null));
      assertEquals("New config property " + MRJobConfig.REDUCE_JAVA_OPTS
              + " is not extracted properly when the old config property "
              + "mapred.child.java.opts is set.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.REDUCE_JAVA_OPTS_S
          .getCandidates(), null));
    }
  }
  
    /**
     * Test if the {@link JobConfigurationParser} can correctly extract out 
     * key-value pairs from the job configuration.
     */
    @Test(timeout=500)
    public void testJobConfigurationParsing() throws Exception {
      final Path tempDir = new Path(rootTempDir, "TestJobConfigurationParser");
      lfs.delete(tempDir, true);
  
      // Add some configuration parameters to the conf
      JobConf jConf = new JobConf(false);
      String key = "test.data";
      String value = "hello world";
      jConf.set(key, value);
      
      // create the job conf file
      Path jobConfPath = new Path(tempDir.toString(), "job.xml");
      lfs.delete(jobConfPath, false);
      DataOutputStream jobConfStream = lfs.create(jobConfPath);
      jConf.writeXml(jobConfStream);
      jobConfStream.close();
      
      // now read the job conf file using the job configuration parser
      Properties properties = 
        JobConfigurationParser.parse(lfs.open(jobConfPath));
      
      // check if the required parameter is loaded
      assertEquals("Total number of extracted properties (" + properties.size() 
                   + ") doesn't match the expected size of 1 ["
                   + "JobConfigurationParser]",
                   1, properties.size());
      // check if the key is present in the extracted configuration
      assertTrue("Key " + key + " is missing in the configuration extracted "
                 + "[JobConfigurationParser]",
                 properties.keySet().contains(key));
      // check if the desired property has the correct value
      assertEquals("JobConfigurationParser couldn't recover the parameters"
                   + " correctly",
                  value, properties.get(key));
      
      // Test ZombieJob
      LoggedJob job = new LoggedJob();
      job.setJobProperties(properties);
      
      ZombieJob zjob = new ZombieJob(job, null);
      Configuration zconf = zjob.getJobConf();
      // check if the required parameter is loaded
      assertEquals("ZombieJob couldn't recover the parameters correctly", 
                   value, zconf.get(key));
    }


  /**
   * Test {@link ResourceUsageMetrics}.
   */
  @Test(timeout=500)
  public void testResourceUsageMetrics() throws Exception {
    final long cpuUsage = 100;
    final long pMemUsage = 200;
    final long vMemUsage = 300;
    final long heapUsage = 400;
    
    // test ResourceUsageMetrics's setters
    ResourceUsageMetrics metrics = new ResourceUsageMetrics();
    metrics.setCumulativeCpuUsage(cpuUsage);
    metrics.setPhysicalMemoryUsage(pMemUsage);
    metrics.setVirtualMemoryUsage(vMemUsage);
    metrics.setHeapUsage(heapUsage);
    // test cpu usage value
    assertEquals("Cpu usage values mismatch via set", cpuUsage, 
                 metrics.getCumulativeCpuUsage());
    // test pMem usage value
    assertEquals("Physical memory usage values mismatch via set", pMemUsage, 
                 metrics.getPhysicalMemoryUsage());
    // test vMem usage value
    assertEquals("Virtual memory usage values mismatch via set", vMemUsage, 
                 metrics.getVirtualMemoryUsage());
    // test heap usage value
    assertEquals("Heap usage values mismatch via set", heapUsage, 
                 metrics.getHeapUsage());
    
    // test deepCompare() (pass case)
    testResourceUsageMetricViaDeepCompare(metrics, cpuUsage, vMemUsage, 
                                          pMemUsage, heapUsage, true);
    
    // test deepCompare (fail case)
    // test cpu usage mismatch
    testResourceUsageMetricViaDeepCompare(metrics, 0, vMemUsage, pMemUsage, 
                                          heapUsage, false);
    // test pMem usage mismatch
    testResourceUsageMetricViaDeepCompare(metrics, cpuUsage, vMemUsage, 0, 
                                          heapUsage, false);
    // test vMem usage mismatch
    testResourceUsageMetricViaDeepCompare(metrics, cpuUsage, 0, pMemUsage, 
                                          heapUsage, false);
    // test heap usage mismatch
    testResourceUsageMetricViaDeepCompare(metrics, cpuUsage, vMemUsage, 
                                          pMemUsage, 0, false);
    
    // define a metric with a fixed value of size()
    ResourceUsageMetrics metrics2 = new ResourceUsageMetrics() {
      @Override
      public int size() {
        return -1;
      }
    };
    metrics2.setCumulativeCpuUsage(cpuUsage);
    metrics2.setPhysicalMemoryUsage(pMemUsage);
    metrics2.setVirtualMemoryUsage(vMemUsage);
    metrics2.setHeapUsage(heapUsage);
    
    // test with size mismatch
    testResourceUsageMetricViaDeepCompare(metrics2, cpuUsage, vMemUsage, 
                                          pMemUsage, heapUsage, false);
  }
  
  // test ResourceUsageMetric's deepCompare() method
  private static void testResourceUsageMetricViaDeepCompare(
                        ResourceUsageMetrics metrics, long cpuUsage, 
                        long vMemUsage, long pMemUsage, long heapUsage,
                        boolean shouldPass) {
    ResourceUsageMetrics testMetrics = new ResourceUsageMetrics();
    testMetrics.setCumulativeCpuUsage(cpuUsage);
    testMetrics.setPhysicalMemoryUsage(pMemUsage);
    testMetrics.setVirtualMemoryUsage(vMemUsage);
    testMetrics.setHeapUsage(heapUsage);
    
    Boolean passed = null;
    try {
      metrics.deepCompare(testMetrics, new TreePath(null, "<root>"));
      passed = true;
    } catch (DeepInequalityException die) {
      passed = false;
    }
    
    assertEquals("ResourceUsageMetrics deepCompare() failed!", 
                 shouldPass, passed);
  }
  
  /**
   * Testing {@link ResourceUsageMetrics} using {@link HadoopLogsAnalyzer}.
   */
  @Test(timeout=500)
  @SuppressWarnings("deprecation")
  public void testResourceUsageMetricsWithHadoopLogsAnalyzer() 
  throws IOException {
    // get the input trace file
    Path rootInputSubFolder = new Path(rootInputDir, "small-trace-test");
    Path traceFile = new Path(rootInputSubFolder, "v20-resource-usage-log.gz");
    
    FileSystem lfs = FileSystem.getLocal(conf);
    
    // define the root test directory
    Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp"));

    // define output directory
    Path outputDir = 
      new Path(rootTempDir, "testResourceUsageMetricsWithHadoopLogsAnalyzer");
    lfs.delete(outputDir, true);
    lfs.deleteOnExit(outputDir);
    
    // run HadoopLogsAnalyzer
    HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();
    analyzer.setConf(conf);
    Path traceOutput = new Path(outputDir, "trace.json");
    analyzer.run(new String[] {"-write-job-trace", traceOutput.toString(), 
                               "-v1", traceFile.toString()});
    
    // test HadoopLogsAnalyzer's output w.r.t ResourceUsageMetrics
    //  get the logged job
    JsonObjectMapperParser<LoggedJob> traceParser =
      new JsonObjectMapperParser<LoggedJob>(traceOutput, LoggedJob.class, 
                                            conf);
    try {
      //  get the logged job from the output trace file
      LoggedJob job = traceParser.getNext();
      LoggedTaskAttempt attempt = job.getMapTasks().get(0).getAttempts().get(0);
      ResourceUsageMetrics metrics = attempt.getResourceUsageMetrics();
      
      //  test via deepCompare()
      testResourceUsageMetricViaDeepCompare(metrics, 200, 100, 75, 50, true);
    } finally {
      traceParser.close();
    }
  }
  
  @Test(timeout=100)
  public void testTopologyBuilder() throws Exception {
    final TopologyBuilder subject = new TopologyBuilder();

    // This 4 comes from 
    //   TaskInProgress.ProgressibleSplitsBlock.burst().size , which 
    //   is invisible here.

    int[][] splits = new int[4][];

    splits[0] = new int[12];
    splits[1] = new int[12];
    splits[2] = new int[12];
    splits[3] = new int[12];

    for (int j = 0; j < 4; ++j) {
      for (int i = 0; i < 12; ++i) {
        splits[j][i] = -1;
      }
    }

    for (int i = 0; i < 6; ++i) {
      splits[0][i] = 500000 * i;
      splits[1][i] = 300000 * i;
      splits[2][i] = 500000;
      splits[3][i] = 700000;
    }

    // currently we extract no host names from the Properties
    subject.process(new Properties());

    subject.process(new TaskAttemptFinishedEvent(TaskAttemptID
        .forName("attempt_200904211745_0003_m_000004_0"), TaskType
        .valueOf("MAP"), "STATUS", 1234567890L,
        "/194\\.6\\.134\\.64", "cluster50261\\.secondleveldomain\\.com",
        "SUCCESS", null));
    subject.process(new TaskAttemptUnsuccessfulCompletionEvent
                    (TaskAttemptID.forName("attempt_200904211745_0003_m_000004_1"),
                     TaskType.valueOf("MAP"), "STATUS", 1234567890L,
                     "cluster50262\\.secondleveldomain\\.com",
                     -1, "/194\\.6\\.134\\.80", "MACHINE_EXPLODED", splits));
    subject.process(new TaskAttemptUnsuccessfulCompletionEvent
                    (TaskAttemptID.forName("attempt_200904211745_0003_m_000004_2"),
                     TaskType.valueOf("MAP"), "STATUS", 1234567890L,
                     "cluster50263\\.secondleveldomain\\.com",
                     -1, "/194\\.6\\.134\\.80", "MACHINE_EXPLODED", splits));
    subject.process(new TaskStartedEvent(TaskID
        .forName("task_200904211745_0003_m_000004"), 1234567890L, TaskType
        .valueOf("MAP"),
        "/194\\.6\\.134\\.80/cluster50263\\.secondleveldomain\\.com"));

    final LoggedNetworkTopology topology = subject.build();

    List<LoggedNetworkTopology> racks = topology.getChildren();

    assertEquals("Wrong number of racks", 2, racks.size());

    boolean sawSingleton = false;
    boolean sawDoubleton = false;

    for (LoggedNetworkTopology rack : racks) {
      List<LoggedNetworkTopology> nodes = rack.getChildren();
      if (rack.getName().getValue().endsWith(".64")) {
        assertEquals("The singleton rack has the wrong number of elements", 1,
            nodes.size());
        sawSingleton = true;
      } else if (rack.getName().getValue().endsWith(".80")) {
        assertEquals("The doubleton rack has the wrong number of elements", 2,
            nodes.size());
        sawDoubleton = true;
      } else {
        assertTrue("Unrecognized rack name", false);
      }
    }

    assertTrue("Did not see singleton rack", sawSingleton);
    assertTrue("Did not see doubleton rack", sawDoubleton);
  }

  static private <T extends DeepCompare> void jsonFileMatchesGold(
      Configuration conf, Path result, Path gold, Class<? extends T> clazz,
      String fileDescription) throws IOException {
    JsonObjectMapperParser<T> goldParser =
        new JsonObjectMapperParser<T>(gold, clazz, conf);
    JsonObjectMapperParser<T> resultParser =
        new JsonObjectMapperParser<T>(result, clazz, conf);
    try {
      while (true) {
        DeepCompare goldJob = goldParser.getNext();
        DeepCompare resultJob = resultParser.getNext();
        if ((goldJob == null) || (resultJob == null)) {
          assertTrue(goldJob == resultJob);
          break;
        }

        try {
          resultJob.deepCompare(goldJob, new TreePath(null, "<root>"));
        } catch (DeepInequalityException e) {
          String error = e.path.toString();

          assertFalse(fileDescription + " mismatches: " + error, true);
        }
      }
    } finally {
      IOUtils.cleanup(null, goldParser, resultParser);
    }
  }

  /**
   * Creates {@link RewindableInputStream} for the given file path.
   * @param inputPath the input file path
   * @param conf configuration
   * @return {@link RewindableInputStream}
   * @throws IOException
   */
  private RewindableInputStream getRewindableInputStream(Path inputPath,
      Configuration conf) throws IOException {

    PossiblyDecompressedInputStream in =
        new PossiblyDecompressedInputStream(inputPath, conf);

    return new RewindableInputStream(in, BUFSIZE);
  }

  /**
   * Allows given history parser to parse the history events and places in
   * the given list
   * @param parser the job history parser
   * @param events the job history events seen while parsing
   * @throws IOException
   */
  private void getHistoryEvents(JobHistoryParser parser,
      ArrayList<String> events, JobBuilder builder) throws IOException {
    HistoryEvent e;
    while ((e = parser.nextEvent()) != null) {
      String eventString = e.getClass().getSimpleName();
      System.out.println(eventString);
      events.add(eventString);
      if (builder != null) {
        builder.process(e);
      }
    }
  }

  /**
   * Validate if history events seen are as expected
   * @param seenEvents the list of history events seen
   * @param goldLinesExpected  the expected history events
   */
  private void validateSeenHistoryEvents(ArrayList<String> seenEvents,
      String[] goldLinesExpected) {

    // Check the output with gold std
    assertEquals("Number of events expected is different from the events seen"
        + " by the history parser.",
        goldLinesExpected.length, seenEvents.size());

    int index = 0;
    for (String goldLine : goldLinesExpected) {
      assertEquals("History Event mismatch at line " + (index + 1),
          goldLine, seenEvents.get(index));
      index++;
    }
  }

  final static int BUFSIZE = 8192; // 8K

  // Any Map Reduce Job History Event should be 1 of the following 16
  final static String AMSE = "AMStartedEvent";
  final static String JSE = "JobSubmittedEvent";
  final static String JPCE = "JobPriorityChangeEvent";
  final static String JSCE = "JobStatusChangedEvent";
  final static String JIE = "JobInitedEvent";
  final static String JICE = "JobInfoChangeEvent";
  static String TSE = "TaskStartedEvent";
  static String ASE = "TaskAttemptStartedEvent";
  static String AFE = "TaskAttemptFinishedEvent";
  static String MFE = "MapAttemptFinishedEvent";
  static String TUE = "TaskUpdatedEvent";
  static String TFE = "TaskFinishedEvent";
  static String JUCE = "JobUnsuccessfulCompletionEvent";
  static String RFE = "ReduceAttemptFinishedEvent";
  static String AUCE = "TaskAttemptUnsuccessfulCompletionEvent";
  static String TFLE = "TaskFailedEvent";
  static String JFE = "JobFinishedEvent";

  // The expected job history events(in order) when parsing
  // the job history file v20-single-input-log.gz
  final static String[] goldLines = new String[] {
      JSE, JPCE, JSCE, JIE, JICE, TSE, ASE, AFE, MFE, TUE, TFE, JSCE, TSE,
      TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE,
      TSE, TSE, TSE, TSE, TSE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE,
      TFE, ASE, AFE, MFE, TUE, TFE, TSE, ASE, AFE, MFE, TUE, TFE, ASE, AFE,
      MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE,
      AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE,
      ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AUCE, ASE, AFE,
      MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE,
      AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE,
      ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, RFE, TUE,
      TFE, TSE, ASE, AFE, MFE, TUE, TFE, JSCE, JFE
  };

}
