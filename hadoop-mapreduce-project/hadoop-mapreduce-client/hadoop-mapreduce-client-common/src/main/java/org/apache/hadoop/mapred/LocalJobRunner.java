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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/** Implements MapReduce locally, in-process, for debugging. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LocalJobRunner implements ClientProtocol {
  public static final Log LOG =
    LogFactory.getLog(LocalJobRunner.class);

  /** The maximum number of map tasks to run in parallel in LocalJobRunner */
  public static final String LOCAL_MAX_MAPS =
    "mapreduce.local.map.tasks.maximum";

  private FileSystem fs;
  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
  private JobConf conf;
  private AtomicInteger map_tasks = new AtomicInteger(0);
  private int reduce_tasks = 0;
  final Random rand = new Random();
  
  private LocalJobRunnerMetrics myMetrics = null;

  private static final String jobDir =  "localRunner/";

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) {
    return ClientProtocol.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  private class Job extends Thread implements TaskUmbilicalProtocol {
    // The job directory on the system: JobClient places job configurations here.
    // This is analogous to JobTracker's system directory.
    private Path systemJobDir;
    private Path systemJobFile;
    
    // The job directory for the task.  Analagous to a task's job directory.
    private Path localJobDir;
    private Path localJobFile;

    private JobID id;
    private JobConf job;

    private int numMapTasks;
    private float [] partialMapProgress;
    private Counters [] mapCounters;
    private Counters reduceCounters;

    private JobStatus status;
    private List<TaskAttemptID> mapIds = Collections.synchronizedList(
        new ArrayList<TaskAttemptID>());

    private JobProfile profile;
    private FileSystem localFs;
    boolean killed = false;
    
    private LocalDistributedCacheManager localDistributedCacheManager;

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    public Job(JobID jobid, String jobSubmitDir) throws IOException {
      this.systemJobDir = new Path(jobSubmitDir);
      this.systemJobFile = new Path(systemJobDir, "job.xml");
      this.id = jobid;
      JobConf conf = new JobConf(systemJobFile);
      this.localFs = FileSystem.getLocal(conf);
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      this.localJobDir = localFs.makeQualified(new Path(
          new Path(conf.getLocalPath(jobDir), user), jobid.toString()));
      this.localJobFile = new Path(this.localJobDir, id + ".xml");

      // Manage the distributed cache.  If there are files to be copied,
      // this will trigger localFile to be re-written again.
      localDistributedCacheManager = new LocalDistributedCacheManager();
      localDistributedCacheManager.setup(conf);
      
      // Write out configuration file.  Instead of copying it from
      // systemJobFile, we re-write it, since setup(), above, may have
      // updated it.
      OutputStream out = localFs.create(localJobFile);
      try {
        conf.writeXml(out);
      } finally {
        out.close();
      }
      this.job = new JobConf(localJobFile);

      // Job (the current object) is a Thread, so we wrap its class loader.
      if (localDistributedCacheManager.hasLocalClasspaths()) {
        setContextClassLoader(localDistributedCacheManager.makeClassLoader(
                getContextClassLoader()));
      }
      
      profile = new JobProfile(job.getUser(), id, systemJobFile.toString(), 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING, 
          profile.getUser(), profile.getJobName(), profile.getJobFile(), 
          profile.getURL().toString());

      jobs.put(id, this);

      this.start();
    }

    /**
     * A Runnable instance that handles a map task to be run by an executor.
     */
    protected class MapTaskRunnable implements Runnable {
      private final int taskId;
      private final TaskSplitMetaInfo info;
      private final JobID jobId;
      private final JobConf localConf;

      // This is a reference to a shared object passed in by the
      // external context; this delivers state to the reducers regarding
      // where to fetch mapper outputs.
      private final Map<TaskAttemptID, MapOutputFile> mapOutputFiles;

      public volatile Throwable storedException;

      public MapTaskRunnable(TaskSplitMetaInfo info, int taskId, JobID jobId,
          Map<TaskAttemptID, MapOutputFile> mapOutputFiles) {
        this.info = info;
        this.taskId = taskId;
        this.mapOutputFiles = mapOutputFiles;
        this.jobId = jobId;
        this.localConf = new JobConf(job);
      }

      @Override
      public void run() {
        try {
          TaskAttemptID mapId = new TaskAttemptID(new TaskID(
              jobId, TaskType.MAP, taskId), 0);
          LOG.info("Starting task: " + mapId);
          mapIds.add(mapId);
          MapTask map = new MapTask(systemJobFile.toString(), mapId, taskId,
            info.getSplitIndex(), 1);
          map.setUser(UserGroupInformation.getCurrentUser().
              getShortUserName());
          setupChildMapredLocalDirs(localJobDir, map, localConf);

          MapOutputFile mapOutput = new MROutputFiles();
          mapOutput.setConf(localConf);
          mapOutputFiles.put(mapId, mapOutput);

          map.setJobFile(localJobFile.toString());
          localConf.setUser(map.getUser());
          map.localizeConfiguration(localConf);
          map.setConf(localConf);
          try {
            map_tasks.getAndIncrement();
            myMetrics.launchMap(mapId);
            map.run(localConf, Job.this);
            myMetrics.completeMap(mapId);
          } finally {
            map_tasks.getAndDecrement();
          }

          LOG.info("Finishing task: " + mapId);
        } catch (Throwable e) {
          this.storedException = e;
        }
      }
    }

    /**
     * Create Runnables to encapsulate map tasks for use by the executor
     * service.
     * @param taskInfo Info about the map task splits
     * @param jobId the job id
     * @param mapOutputFiles a mapping from task attempts to output files
     * @return a List of Runnables, one per map task.
     */
    protected List<MapTaskRunnable> getMapTaskRunnables(
        TaskSplitMetaInfo [] taskInfo, JobID jobId,
        Map<TaskAttemptID, MapOutputFile> mapOutputFiles) {

      int numTasks = 0;
      ArrayList<MapTaskRunnable> list = new ArrayList<MapTaskRunnable>();
      for (TaskSplitMetaInfo task : taskInfo) {
        list.add(new MapTaskRunnable(task, numTasks++, jobId,
            mapOutputFiles));
      }

      return list;
    }

    /**
     * Initialize the counters that will hold partial-progress from
     * the various task attempts.
     * @param numMaps the number of map tasks in this job.
     */
    private synchronized void initCounters(int numMaps) {
      // Initialize state trackers for all map tasks.
      this.partialMapProgress = new float[numMaps];
      this.mapCounters = new Counters[numMaps];
      for (int i = 0; i < numMaps; i++) {
        this.mapCounters[i] = new Counters();
      }

      this.reduceCounters = new Counters();
    }

    /**
     * Creates the executor service used to run map tasks.
     *
     * @param numMapTasks the total number of map tasks to be run
     * @return an ExecutorService instance that handles map tasks
     */
    protected ExecutorService createMapExecutor(int numMapTasks) {

      // Determine the size of the thread pool to use
      int maxMapThreads = job.getInt(LOCAL_MAX_MAPS, 1);
      if (maxMapThreads < 1) {
        throw new IllegalArgumentException(
            "Configured " + LOCAL_MAX_MAPS + " must be >= 1");
      }
      this.numMapTasks = numMapTasks;
      maxMapThreads = Math.min(maxMapThreads, this.numMapTasks);
      maxMapThreads = Math.max(maxMapThreads, 1); // In case of no tasks.

      initCounters(this.numMapTasks);

      LOG.debug("Starting thread pool executor.");
      LOG.debug("Max local threads: " + maxMapThreads);
      LOG.debug("Map tasks to process: " + this.numMapTasks);

      // Create a new executor service to drain the work queue.
      ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("LocalJobRunner Map Task Executor #%d")
        .build();
      ExecutorService executor = Executors.newFixedThreadPool(maxMapThreads, tf);

      return executor;
    }

    private org.apache.hadoop.mapreduce.OutputCommitter 
    createOutputCommitter(boolean newApiCommitter, JobID jobId, Configuration conf) throws Exception {
      org.apache.hadoop.mapreduce.OutputCommitter committer = null;

      LOG.info("OutputCommitter set in config "
          + conf.get("mapred.output.committer.class"));

      if (newApiCommitter) {
        org.apache.hadoop.mapreduce.TaskID taskId =
            new org.apache.hadoop.mapreduce.TaskID(jobId, TaskType.MAP, 0);
        org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID =
            new org.apache.hadoop.mapreduce.TaskAttemptID(taskId, 0);
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = 
            new TaskAttemptContextImpl(conf, taskAttemptID);
        OutputFormat outputFormat =
          ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), conf);
        committer = outputFormat.getOutputCommitter(taskContext);
      } else {
        committer = ReflectionUtils.newInstance(conf.getClass(
            "mapred.output.committer.class", FileOutputCommitter.class,
            org.apache.hadoop.mapred.OutputCommitter.class), conf);
      }
      LOG.info("OutputCommitter is " + committer.getClass().getName());
      return committer;
    }

    @Override
    public void run() {
      JobID jobId = profile.getJobID();
      JobContext jContext = new JobContextImpl(job, jobId);
      
      org.apache.hadoop.mapreduce.OutputCommitter outputCommitter = null;
      try {
        outputCommitter = createOutputCommitter(conf.getUseNewMapper(), jobId, conf);
      } catch (Exception e) {
        LOG.info("Failed to createOutputCommitter", e);
        return;
      }
      
      try {
        TaskSplitMetaInfo[] taskSplitMetaInfos = 
          SplitMetaInfoReader.readSplitMetaInfo(jobId, localFs, conf, systemJobDir);

        int numReduceTasks = job.getNumReduceTasks();
        if (numReduceTasks > 1 || numReduceTasks < 0) {
          // we only allow 0 or 1 reducer in local mode
          numReduceTasks = 1;
          job.setNumReduceTasks(1);
        }
        outputCommitter.setupJob(jContext);
        status.setSetupProgress(1.0f);

        Map<TaskAttemptID, MapOutputFile> mapOutputFiles =
            Collections.synchronizedMap(new HashMap<TaskAttemptID, MapOutputFile>());

        List<MapTaskRunnable> taskRunnables = getMapTaskRunnables(taskSplitMetaInfos,
            jobId, mapOutputFiles);
        ExecutorService mapService = createMapExecutor(taskRunnables.size());

        // Start populating the executor with work units.
        // They may begin running immediately (in other threads).
        for (Runnable r : taskRunnables) {
          mapService.submit(r);
        }

        try {
          mapService.shutdown(); // Instructs queue to drain.

          // Wait for tasks to finish; do not use a time-based timeout.
          // (See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179024)
          LOG.info("Waiting for map tasks");
          mapService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
          // Cancel all threads.
          mapService.shutdownNow();
          throw ie;
        }

        LOG.info("Map task executor complete.");

        // After waiting for the map tasks to complete, if any of these
        // have thrown an exception, rethrow it now in the main thread context.
        for (MapTaskRunnable r : taskRunnables) {
          if (r.storedException != null) {
            throw new Exception(r.storedException);
          }
        }

        TaskAttemptID reduceId =
          new TaskAttemptID(new TaskID(jobId, TaskType.REDUCE, 0), 0);
        try {
          if (numReduceTasks > 0) {
            ReduceTask reduce = new ReduceTask(systemJobFile.toString(), 
                reduceId, 0, mapIds.size(), 1);
            reduce.setUser(UserGroupInformation.getCurrentUser().
                getShortUserName());
            JobConf localConf = new JobConf(job);
            localConf.set("mapreduce.jobtracker.address", "local");
            setupChildMapredLocalDirs(localJobDir, reduce, localConf);
            // move map output to reduce input  
            for (int i = 0; i < mapIds.size(); i++) {
              if (!this.isInterrupted()) {
                TaskAttemptID mapId = mapIds.get(i);
                Path mapOut = mapOutputFiles.get(mapId).getOutputFile();
                MapOutputFile localOutputFile = new MROutputFiles();
                localOutputFile.setConf(localConf);
                Path reduceIn =
                  localOutputFile.getInputFileForWrite(mapId.getTaskID(),
                        localFs.getFileStatus(mapOut).getLen());
                if (!localFs.mkdirs(reduceIn.getParent())) {
                  throw new IOException("Mkdirs failed to create "
                      + reduceIn.getParent().toString());
                }
                if (!localFs.rename(mapOut, reduceIn))
                  throw new IOException("Couldn't rename " + mapOut);
              } else {
                throw new InterruptedException();
              }
            }
            if (!this.isInterrupted()) {
              reduce.setJobFile(localJobFile.toString());
              localConf.setUser(reduce.getUser());
              reduce.localizeConfiguration(localConf);
              reduce.setConf(localConf);
              reduce_tasks += 1;
              myMetrics.launchReduce(reduce.getTaskID());
              reduce.run(localConf, this);
              myMetrics.completeReduce(reduce.getTaskID());
              reduce_tasks -= 1;
            } else {
              throw new InterruptedException();
            }
          }
        } finally {
          for (MapOutputFile output : mapOutputFiles.values()) {
            output.removeAll();
          }
        }
        // delete the temporary directory in output directory
        outputCommitter.commitJob(jContext);
        status.setCleanupProgress(1.0f);

        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.SUCCEEDED);
        }

        JobEndNotifier.localRunnerNotification(job, status);

      } catch (Throwable t) {
        try {
          outputCommitter.abortJob(jContext, 
            org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        } catch (IOException ioe) {
          LOG.info("Error cleaning up job:" + id);
        }
        status.setCleanupProgress(1.0f);
        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.FAILED);
        }
        LOG.warn(id, t);

        JobEndNotifier.localRunnerNotification(job, status);

      } finally {
        try {
          fs.delete(systemJobFile.getParent(), true);  // delete submit dir
          localFs.delete(localJobFile, true);              // delete local copy
          // Cleanup distributed cache
          localDistributedCacheManager.close();
        } catch (IOException e) {
          LOG.warn("Error cleaning up "+id+": "+e);
        }
      }
    }

    // TaskUmbilicalProtocol methods

    @Override
    public JvmTask getTask(JvmContext context) { return null; }
    
    @Override
    public synchronized boolean statusUpdate(TaskAttemptID taskId,
        TaskStatus taskStatus) throws IOException, InterruptedException {
      // Serialize as we would if distributed in order to make deep copy
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      taskStatus.write(dos);
      dos.close();
      taskStatus = TaskStatus.createTaskStatus(taskStatus.getIsMap());
      taskStatus.readFields(new DataInputStream(
          new ByteArrayInputStream(baos.toByteArray())));
      
      LOG.info(taskStatus.getStateString());
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        float numTasks = (float) this.numMapTasks;

        partialMapProgress[taskIndex] = taskStatus.getProgress();
        mapCounters[taskIndex] = taskStatus.getCounters();

        float partialProgress = 0.0f;
        for (float f : partialMapProgress) {
          partialProgress += f;
        }
        status.setMapProgress(partialProgress / numTasks);
      } else {
        reduceCounters = taskStatus.getCounters();
        status.setReduceProgress(taskStatus.getProgress());
      }

      // ignore phase
      return true;
    }

    /** Return the current values of the counters for this job,
     * including tasks that are in progress.
     */
    public synchronized Counters getCurrentCounters() {
      if (null == mapCounters) {
        // Counters not yet initialized for job.
        return new Counters();
      }

      Counters current = new Counters();
      for (Counters c : mapCounters) {
        current = Counters.sum(current, c);
      }
      current = Counters.sum(current, reduceCounters);
      return current;
    }

    /**
     * Task is reporting that it is in commit_pending
     * and it is waiting for the commit Response
     */
    @Override
    public void commitPending(TaskAttemptID taskid,
                              TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      statusUpdate(taskid, taskStatus);
    }

    @Override
    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) {
      // Ignore for now
    }
    
    @Override
    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }

    @Override
    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }
    
    @Override
    public boolean canCommit(TaskAttemptID taskid) 
    throws IOException {
      return true;
    }
    
    @Override
    public void done(TaskAttemptID taskId) throws IOException {
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        status.setMapProgress(1.0f);
      } else {
        status.setReduceProgress(1.0f);
      }
    }

    @Override
    public synchronized void fsError(TaskAttemptID taskId, String message) 
    throws IOException {
      LOG.fatal("FSError: "+ message + "from task: " + taskId);
    }

    @Override
    public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
      LOG.fatal("shuffleError: "+ message + "from task: " + taskId);
    }
    
    @Override
    public synchronized void fatalError(TaskAttemptID taskId, String msg) 
    throws IOException {
      LOG.fatal("Fatal: "+ msg + "from task: " + taskId);
    }
    
    @Override
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
        int fromEventId, int maxLocs, TaskAttemptID id) throws IOException {
      return new MapTaskCompletionEventsUpdate(
        org.apache.hadoop.mapred.TaskCompletionEvent.EMPTY_ARRAY, false);
    }
    
  }

  public LocalJobRunner(Configuration conf) throws IOException {
    this(new JobConf(conf));
  }

  @Deprecated
  public LocalJobRunner(JobConf conf) throws IOException {
    this.fs = FileSystem.getLocal(conf);
    this.conf = conf;
    myMetrics = new LocalJobRunnerMetrics(new JobConf(conf));
  }

  // JobSubmissionProtocol methods

  private static int jobid = 0;
  // used for making sure that local jobs run in different jvms don't
  // collide on staging or job directories
  private int randid;
  
  public synchronized org.apache.hadoop.mapreduce.JobID getNewJobID() {
    return new org.apache.hadoop.mapreduce.JobID("local" + randid, ++jobid);
  }

  @Override
  public org.apache.hadoop.mapreduce.JobStatus submitJob(
      org.apache.hadoop.mapreduce.JobID jobid, String jobSubmitDir,
      Credentials credentials) throws IOException {
    Job job = new Job(JobID.downgrade(jobid), jobSubmitDir);
    job.job.setCredentials(credentials);
    return job.status;

  }

  @Override
  public void killJob(org.apache.hadoop.mapreduce.JobID id) {
    jobs.get(JobID.downgrade(id)).killed = true;
    jobs.get(JobID.downgrade(id)).interrupt();
  }

  @Override
  public void setJobPriority(org.apache.hadoop.mapreduce.JobID id,
      String jp) throws IOException {
    throw new UnsupportedOperationException("Changing job priority " +
                      "in LocalJobRunner is not supported.");
  }
  
  /** Throws {@link UnsupportedOperationException} */
  @Override
  public boolean killTask(org.apache.hadoop.mapreduce.TaskAttemptID taskId,
      boolean shouldFail) throws IOException {
    throw new UnsupportedOperationException("Killing tasks in " +
    "LocalJobRunner is not supported");
  }

  @Override
  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(
      org.apache.hadoop.mapreduce.JobID id, TaskType type) {
    return new org.apache.hadoop.mapreduce.TaskReport[0];
  }

  @Override
  public org.apache.hadoop.mapreduce.JobStatus getJobStatus(
      org.apache.hadoop.mapreduce.JobID id) {
    Job job = jobs.get(JobID.downgrade(id));
    if(job != null)
      return job.status;
    else 
      return null;
  }
  
  @Override
  public org.apache.hadoop.mapreduce.Counters getJobCounters(
      org.apache.hadoop.mapreduce.JobID id) {
    Job job = jobs.get(JobID.downgrade(id));

    return new org.apache.hadoop.mapreduce.Counters(job.getCurrentCounters());
  }

  @Override
  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }
  
  @Override
  public ClusterMetrics getClusterMetrics() {
    int numMapTasks = map_tasks.get();
    return new ClusterMetrics(numMapTasks, reduce_tasks, numMapTasks,
        reduce_tasks, 0, 0, 1, 1, jobs.size(), 1, 0, 0);
  }

  @Override
  public JobTrackerStatus getJobTrackerStatus() {
    return JobTrackerStatus.RUNNING;
  }

  @Override
  public long getTaskTrackerExpiryInterval() throws IOException, InterruptedException {
    return 0;
  }

  /** 
   * Get all active trackers in cluster. 
   * @return array of TaskTrackerInfo
   */
  @Override
  public TaskTrackerInfo[] getActiveTrackers() 
      throws IOException, InterruptedException {
    return new TaskTrackerInfo[0];
  }

  /** 
   * Get all blacklisted trackers in cluster. 
   * @return array of TaskTrackerInfo
   */
  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() 
      throws IOException, InterruptedException {
    return new TaskTrackerInfo[0];
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(
      org.apache.hadoop.mapreduce.JobID jobid
      , int fromEventId, int maxEvents) throws IOException {
    return TaskCompletionEvent.EMPTY_ARRAY;
  }
  
  @Override
  public org.apache.hadoop.mapreduce.JobStatus[] getAllJobs() {return null;}

  
  /**
   * Returns the diagnostic information for a particular task in the given job.
   * To be implemented
   */
  @Override
  public String[] getTaskDiagnostics(
      org.apache.hadoop.mapreduce.TaskAttemptID taskid) throws IOException{
	  return new String [0];
  }

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getSystemDir()
   */
  @Override
  public String getSystemDir() {
    Path sysDir = new Path(
      conf.get(JTConfig.JT_SYSTEM_DIR, "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getQueueAdmins(String)
   */
  @Override
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
	  return new AccessControlList(" ");// no queue admins for local job runner
  }

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getStagingAreaDir()
   */
  @Override
  public String getStagingAreaDir() throws IOException {
    Path stagingRootDir = new Path(conf.get(JTConfig.JT_STAGING_AREA_ROOT, 
        "/tmp/hadoop/mapred/staging"));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user;
    randid = rand.nextInt(Integer.MAX_VALUE);
    if (ugi != null) {
      user = ugi.getShortUserName() + randid;
    } else {
      user = "dummy" + randid;
    }
    return fs.makeQualified(new Path(stagingRootDir, user+"/.staging")).toString();
  }
  
  @Override
  public String getJobHistoryDir() {
    return null;
  }

  @Override
  public QueueInfo[] getChildQueues(String queueName) throws IOException {
    return null;
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException {
    return null;
  }

  @Override
  public QueueInfo[] getQueues() throws IOException {
    return null;
  }


  @Override
  public QueueInfo getQueue(String queue) throws IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.mapreduce.QueueAclsInfo[] 
      getQueueAclsForCurrentUser() throws IOException{
    return null;
  }

  /**
   * Set the max number of map tasks to run concurrently in the LocalJobRunner.
   * @param job the job to configure
   * @param maxMaps the maximum number of map tasks to allow.
   */
  public static void setLocalMaxRunningMaps(
      org.apache.hadoop.mapreduce.JobContext job,
      int maxMaps) {
    job.getConfiguration().setInt(LOCAL_MAX_MAPS, maxMaps);
  }

  /**
   * @return the max number of map tasks to run concurrently in the
   * LocalJobRunner.
   */
  public static int getLocalMaxRunningMaps(
      org.apache.hadoop.mapreduce.JobContext job) {
    return job.getConfiguration().getInt(LOCAL_MAX_MAPS, 1);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                       ) throws IOException,
                                                InterruptedException {
  }

  @Override
  public Token<DelegationTokenIdentifier> 
     getDelegationToken(Text renewer) throws IOException, InterruptedException {
    return null;
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                      ) throws IOException,InterruptedException{
    return 0;
  }

  @Override
  public LogParams getLogFileParams(org.apache.hadoop.mapreduce.JobID jobID,
      org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Not supported");
  }
  
  static void setupChildMapredLocalDirs(Path localJobDir, Task t, JobConf conf) {
    String[] localDirs = conf.getTrimmedStrings(MRConfig.LOCAL_DIR);
    String taskId = t.getTaskID().toString();
    boolean isCleanup = t.isTaskCleanupTask();
    StringBuffer childMapredLocalDir =
        new StringBuffer(localDirs[0] + Path.SEPARATOR
            + getLocalTaskDir(localJobDir, taskId, isCleanup));
    for (int i = 1; i < localDirs.length; i++) {
      childMapredLocalDir.append("," + localDirs[i] + Path.SEPARATOR
          + getLocalTaskDir(localJobDir, taskId, isCleanup));
    }
    LOG.debug(MRConfig.LOCAL_DIR + " for child : " + childMapredLocalDir);
    conf.set(MRConfig.LOCAL_DIR, childMapredLocalDir.toString());
  }
  
  static final String TASK_CLEANUP_SUFFIX = ".cleanup";
  static final String JOBCACHE = "jobcache";
  
  static String getLocalTaskDir(Path localJobDir, String taskid,
      boolean isCleanupAttempt) {
    String taskDir = localJobDir.toString() + Path.SEPARATOR + taskid;
    if (isCleanupAttempt) {
      taskDir = taskDir + TASK_CLEANUP_SUFFIX;
    }
    return taskDir;
  }
  
}
