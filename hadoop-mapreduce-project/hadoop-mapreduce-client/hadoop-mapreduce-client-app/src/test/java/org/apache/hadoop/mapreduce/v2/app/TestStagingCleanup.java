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

package org.apache.hadoop.mapreduce.v2.app;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnRuntimeException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.junit.Test;


/**
 * Make sure that the job staging directory clean up happens.
 */
 public class TestStagingCleanup extends TestCase {
   
   private Configuration conf = new Configuration();
   private FileSystem fs;
   private String stagingJobDir = "tmpJobDir";
   private Path stagingJobPath = new Path(stagingJobDir);
   private final static RecordFactory recordFactory = RecordFactoryProvider.
       getRecordFactory(null);
   
   @Test
   public void testDeletionofStaging() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
        0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 0);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
         JobStateInternal.RUNNING, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
     appMaster.init(conf);
     appMaster.start();
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     Assert.assertEquals(true, ((TestMRApp)appMaster).getTestIsLastAMRetry());
     verify(fs).delete(stagingJobPath, true);
   }

   @Test (timeout = 30000)
   public void testNoDeletionofStagingOnReboot() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class),anyBoolean())).thenReturn(true);
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 0);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
         JobStateInternal.REBOOT, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
     appMaster.init(conf);
     appMaster.start();
     //shutdown the job, not the lastRetry
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     Assert.assertEquals(false, ((TestMRApp)appMaster).getTestIsLastAMRetry());
     verify(fs, times(0)).delete(stagingJobPath, true);
   }

   @Test (timeout = 30000)
   public void testDeletionofStagingOnReboot() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class),anyBoolean())).thenReturn(true);
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
         JobStateInternal.REBOOT, 1); //no retry
     appMaster.init(conf);
     appMaster.start();
     //shutdown the job, is lastRetry
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     Assert.assertEquals(true, ((TestMRApp)appMaster).getTestIsLastAMRetry());
     verify(fs).delete(stagingJobPath, true);
   }
   
   @Test (timeout = 30000)
   public void testDeletionofStagingOnKill() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 0);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc, 4);
     appMaster.init(conf);
     //simulate the process being killed
     MRAppMaster.MRAppMasterShutdownHook hook = 
       new MRAppMaster.MRAppMasterShutdownHook(appMaster);
     hook.run();
     verify(fs, times(0)).delete(stagingJobPath, true);
   }
   
   @Test (timeout = 30000)
   public void testDeletionofStagingOnKillLastTry() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc, 1); //no retry
     appMaster.init(conf);
     //simulate the process being killed
     MRAppMaster.MRAppMasterShutdownHook hook = 
       new MRAppMaster.MRAppMasterShutdownHook(appMaster);
     hook.run();
     verify(fs).delete(stagingJobPath, true);
   }

   private class TestMRApp extends MRAppMaster {
     ContainerAllocator allocator;
     boolean testIsLastAMRetry = false;
     JobStateInternal jobStateInternal;

     public TestMRApp(ApplicationAttemptId applicationAttemptId, 
         ContainerAllocator allocator, int maxAppAttempts) {
       super(applicationAttemptId, ContainerId.newInstance(
           applicationAttemptId, 1), "testhost", 2222, 3333,
           System.currentTimeMillis(), maxAppAttempts);
       this.allocator = allocator;
     }

     public TestMRApp(ApplicationAttemptId applicationAttemptId,
         ContainerAllocator allocator, JobStateInternal jobStateInternal,
             int maxAppAttempts) {
       this(applicationAttemptId, allocator, maxAppAttempts);
       this.jobStateInternal = jobStateInternal;
     }

     @Override
     protected FileSystem getFileSystem(Configuration conf) {
       return fs;
     }

     @Override
     protected ContainerAllocator createContainerAllocator(
         final ClientService clientService, final AppContext context) {
       if(allocator == null) {
         return super.createContainerAllocator(clientService, context);
       }
       return allocator;
     }

     @Override
     protected Job createJob(Configuration conf, JobStateInternal forcedState,
         String diagnostic) {
       JobImpl jobImpl = mock(JobImpl.class);
       when(jobImpl.getInternalState()).thenReturn(this.jobStateInternal);
       JobID jobID = JobID.forName("job_1234567890000_0001");
       JobId jobId = TypeConverter.toYarn(jobID);
       when(jobImpl.getID()).thenReturn(jobId);
       ((AppContext) getContext())
           .getAllJobs().put(jobImpl.getID(), jobImpl);
       return jobImpl;
     }

     @Override
     public void start() {
       super.start();
       DefaultMetricsSystem.shutdown();
     }

     @Override
     public void notifyIsLastAMRetry(boolean isLastAMRetry){
       testIsLastAMRetry = isLastAMRetry;
       super.notifyIsLastAMRetry(isLastAMRetry);
     }

     @Override
     public RMHeartbeatHandler getRMHeartbeatHandler() {
       return getStubbedHeartbeatHandler(getContext());
     }

     @Override
     protected void sysexit() {      
     }

     @Override
     public Configuration getConfig() {
       return conf;
     }

     @Override
     protected void downloadTokensAndSetupUGI(Configuration conf) {
     }

     public boolean getTestIsLastAMRetry(){
       return testIsLastAMRetry;
     }
   }

  private final class MRAppTestCleanup extends MRApp {
    boolean stoppedContainerAllocator;
    boolean cleanedBeforeContainerAllocatorStopped;

    public MRAppTestCleanup(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
      stoppedContainerAllocator = false;
      cleanedBeforeContainerAllocatorStopped = false;
    }

    @Override
    protected Job createJob(Configuration conf, JobStateInternal forcedState, 
        String diagnostic) {
      UserGroupInformation currentUser = null;
      try {
        currentUser = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
      Job newJob = new TestJob(getJobId(), getAttemptID(), conf,
          getDispatcher().getEventHandler(),
          getTaskAttemptListener(), getContext().getClock(),
          getCommitter(), isNewApiCommitter(),
          currentUser.getUserName(), getContext(),
          forcedState, diagnostic);
      ((AppContext) getContext()).getAllJobs().put(newJob.getID(), newJob);

      getDispatcher().register(JobFinishEvent.Type.class,
          createJobFinishEventHandler());

      return newJob;
    }

    @Override
    protected ContainerAllocator createContainerAllocator(
        ClientService clientService, AppContext context) {
      return new TestCleanupContainerAllocator();
    }

    private class TestCleanupContainerAllocator extends AbstractService
        implements ContainerAllocator {
      private MRAppContainerAllocator allocator;

      TestCleanupContainerAllocator() {
        super(TestCleanupContainerAllocator.class.getName());
        allocator = new MRAppContainerAllocator();
      }

      @Override
      public void handle(ContainerAllocatorEvent event) {
        allocator.handle(event);
      }

      @Override
      public synchronized void stop() {
        stoppedContainerAllocator = true;
        super.stop();
      }
    }

    @Override
    public RMHeartbeatHandler getRMHeartbeatHandler() {
      return getStubbedHeartbeatHandler(getContext());
    }

    @Override
    public void cleanupStagingDir() throws IOException {
      cleanedBeforeContainerAllocatorStopped = !stoppedContainerAllocator;
    }

    @Override
    protected void sysexit() {
    }
  }

  private static RMHeartbeatHandler getStubbedHeartbeatHandler(
      final AppContext appContext) {
    return new RMHeartbeatHandler() {
      @Override
      public long getLastHeartbeatTime() {
        return appContext.getClock().getTime();
      }
      @Override
      public void runOnNextHeartbeat(Runnable callback) {
        callback.run();
      }
    };
  }

  @Test(timeout=20000)
  public void testStagingCleanupOrder() throws Exception {
    MRAppTestCleanup app = new MRAppTestCleanup(1, 1, true,
        this.getClass().getName(), true);
    JobImpl job = (JobImpl)app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    int waitTime = 20 * 1000;
    while (waitTime > 0 && !app.cleanedBeforeContainerAllocatorStopped) {
      Thread.sleep(100);
      waitTime -= 100;
    }
    Assert.assertTrue("Staging directory not cleaned before notifying RM",
        app.cleanedBeforeContainerAllocatorStopped);
  }
 }
