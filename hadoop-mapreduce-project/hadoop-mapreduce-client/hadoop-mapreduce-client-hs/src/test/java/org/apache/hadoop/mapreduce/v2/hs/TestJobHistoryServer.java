package org.apache.hadoop.mapreduce.v2.hs;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryEvents.MRAppWithHistory;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryParsing.MyResolver;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestJobHistoryServer {
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  @Test
  public void test1() throws Exception {

    JobHistoryServer server = new JobHistoryServer();
    Configuration cong = new Configuration();
    server.init(cong);
    assertEquals(STATE.INITED, server.getServiceState());
    assertEquals(3, server.getServices().size());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    assertNotNull(server.getClientService());
    HistoryClientService historyService = server.getClientService();
    assertNotNull(historyService.getClientHandler().getConnectAddress());

  }

 
@Test 
public void testPartialJob () throws Exception{
  JobId jobId= new JobIdPBImpl();
  jobId.setId(0);
  JobIndexInfo jii= new JobIndexInfo(0L, System.currentTimeMillis(), "user","jobName",  jobId, 3, 2, "JobStatus");
  PartialJob test = new PartialJob(jii,jobId);
  assertEquals(1.0f, test.getProgress(),0.001);
  assertNull(test.getAllCounters());
  assertNull(test.getTasks());
  assertNull(test.getTasks(TaskType.MAP));
  assertNull(test.getTask(new TaskIdPBImpl()));
  assertNull(test.getTaskAttemptCompletionEvents(0,100));
  assertNull(test.getMapAttemptCompletionEvents(0,100));
  assertTrue(test.checkAccess(UserGroupInformation.getCurrentUser(), null));
  assertNull(test.getAMInfos());
  
  
}
/*
  @Test
  public void test3() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    // conf.setLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS, 1400000);

    RackResolver.init(conf);
    MRApp app = new MRAppWithHistory(1, 1, true, this.getClass().getName(),
        true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    app.waitForState(job, JobState.SUCCEEDED);

    JobHistoryServer server = new JobHistoryServer();
    Configuration cong = new Configuration();
    server.init(cong);
    server.start();
    Task task = job.getTasks().values().iterator().next();
    TaskAttempt attempt = task.getAttempts().values().iterator().next();    
    
    HistoryClientService historyService = server.getClientService();
    MRClientProtocol protocol=  historyService.getClientHandler();
    
    spy(protocol).verifyAndGetJob(taskId.getJobId());
    GetTaskAttemptReportRequest gtarRequest =
        recordFactory.newRecordInstance(GetTaskAttemptReportRequest.class);
    TaskAttemptId taId=attempt.getID();
    taId.setTaskId(task.getID());
    taId.getTaskId().setJobId(job.getID());
    gtarRequest.setTaskAttemptId(taId);
    //GetTaskAttemptReportResponse responce= protocol.getTaskAttemptReport(gtarRequest);
    
    
    server.stop();
    HistoryFileManager historyFileManager= new HistoryFileManager();
    historyFileManager.init(conf);
    historyFileManager.scanIntermediateDirectory();
   // GetTaskAttemptReportResponse responce= protocol.getTaskAttemptReport(gtarRequest);
    GetTaskReportRequest request= recordFactory.newRecordInstance(GetTaskReportRequest.class); 
    TaskId taskId= task.getID();
    taskId.setJobId(job.getID());
    request.setTaskId(taskId);
    protocol.getTaskReport(request);
    System.out.println("OK");
  }
  */
  @Test
  public void testMainMethod() throws Exception {

    ExitUtil.disableSystemExit();
    try {
      JobHistoryServer.main(new String[0]);

    } catch (ExitUtil.ExitException e) {
      ExitUtil.resetFirstExitException();
      fail();
    }
  }
}
