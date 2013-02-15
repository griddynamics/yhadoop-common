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

package org.apache.hadoop.mapreduce.v2.hs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
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

/*
test JobHistoryServer protocols....
 */
public class TestJobHistoryServer {
  private static RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);

  
  /*
  simple test start/ stop    JobHistoryServer
   */
  @Test
  public void testStartStopServer() throws Exception {

    L
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

  /*
     Simple test PartialJob
  */
  @Test
  public void testPartialJob() throws Exception {
    JobId jobId = new JobIdPBImpl();
    jobId.setId(0);
    JobIndexInfo jii = new JobIndexInfo(0L, System.currentTimeMillis(), "user",
            "jobName", jobId, 3, 2, "JobStatus");
    PartialJob test = new PartialJob(jii, jobId);
    assertEquals(1.0f, test.getProgress(), 0.001);
    assertNull(test.getAllCounters());
    assertNull(test.getTasks());
    assertNull(test.getTasks(TaskType.MAP));
    assertNull(test.getTask(new TaskIdPBImpl()));
    assertNull(test.getTaskAttemptCompletionEvents(0, 100));
    assertNull(test.getMapAttemptCompletionEvents(0, 100));
    assertTrue(test.checkAccess(UserGroupInformation.getCurrentUser(), null));
    assertNull(test.getAMInfos());

  }

  /*
 Test reports of  JobHistoryServer. History server should gets log files from  MRApp and read them
  */
  @Test
  public void testReports() throws Exception {
    Configuration config = new Configuration();
    config
            .setClass(
                    CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
                    MyResolver.class, DNSToSwitchMapping.class);

    RackResolver.init(config);
    MRApp app = new MRAppWithHistory(1, 1, true, this.getClass().getName(),
            true);
    app.submit(config);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    app.waitForState(job, JobState.SUCCEEDED);

    JobHistoryServer historyServer = new JobHistoryServer();

    historyServer.init(config);
    historyServer.start();
    JobHistory jobHistory = (JobHistory) historyServer.getServices().iterator()
            .next();
    jobHistory.getAllJobs();

    Task task = job.getTasks().values().iterator().next();
    TaskAttempt attempt = task.getAttempts().values().iterator().next();

    HistoryClientService historyService = historyServer.getClientService();
    MRClientProtocol protocol = historyService.getClientHandler();

    GetTaskAttemptReportRequest gtarRequest = recordFactory
            .newRecordInstance(GetTaskAttemptReportRequest.class);
    // test getTaskAttemptReport
    TaskAttemptId taId = attempt.getID();
    taId.setTaskId(task.getID());
    taId.getTaskId().setJobId(job.getID());
    gtarRequest.setTaskAttemptId(taId);
    GetTaskAttemptReportResponse responce = protocol
            .getTaskAttemptReport(gtarRequest);
    assertEquals("container_0_0000_01_000000", responce.getTaskAttemptReport()
            .getContainerId().toString());
    assertTrue(responce.getTaskAttemptReport().getDiagnosticInfo().isEmpty());
    // counters
    assertNotNull(responce.getTaskAttemptReport().getCounters()
            .getCounter(TaskCounter.PHYSICAL_MEMORY_BYTES));
    assertEquals(taId.toString(), responce.getTaskAttemptReport()
            .getTaskAttemptId().toString());
    // test getTaskReport
    GetTaskReportRequest request = recordFactory
            .newRecordInstance(GetTaskReportRequest.class);
    TaskId taskId = task.getID();
    taskId.setJobId(job.getID());
    request.setTaskId(taskId);
    GetTaskReportResponse reportResponce = protocol.getTaskReport(request);
    assertEquals("", reportResponce.getTaskReport().getDiagnosticsList()
            .iterator().next());
    // progress
    assertEquals(1.0f, reportResponce.getTaskReport().getProgress(), 0.01);
    assertEquals(taskId.toString(), reportResponce.getTaskReport().getTaskId()
            .toString());
    assertEquals(TaskState.SUCCEEDED, reportResponce.getTaskReport()
            .getTaskState());
    // test getTaskAttemptCompletionEvents
    GetTaskAttemptCompletionEventsRequest taskAttemptRequest = recordFactory
            .newRecordInstance(GetTaskAttemptCompletionEventsRequest.class);
    taskAttemptRequest.setJobId(job.getID());
    GetTaskAttemptCompletionEventsResponse taskCompliteResponce = protocol
            .getTaskAttemptCompletionEvents(taskAttemptRequest);
    assertEquals(0, taskCompliteResponce.getCompletionEventCount());
    
    // test getDiagnostics
    GetDiagnosticsRequest diagnosticRequest = recordFactory
            .newRecordInstance(GetDiagnosticsRequest.class);
    diagnosticRequest.setTaskAttemptId(taId);
    GetDiagnosticsResponse diagnosticResponce = protocol
            .getDiagnostics(diagnosticRequest);
    // it is strange : why one empty string ?
    assertEquals(1, diagnosticResponce.getDiagnosticsCount());
    assertEquals("", diagnosticResponce.getDiagnostics(0));

    GetCountersRequest counterRequest=recordFactory
        .newRecordInstance(GetCountersRequest.class);
    counterRequest.setJobId(job.getID());
    GetCountersResponse counterResponce=protocol.getCounters(counterRequest);
    assertNotNull(counterResponce.getCounters().getCounterGroup("org.apache.hadoop.mapreduce.JobCounter"));
    // test getJobReport
    GetJobReportRequest  reportRequest=recordFactory
        .newRecordInstance(GetJobReportRequest.class);
    reportRequest.setJobId(job.getID());
    GetJobReportResponse jobReport=protocol.getJobReport(reportRequest);
    assertEquals(1, jobReport.getJobReport().getAMInfos().size());
    assertNotNull(jobReport.getJobReport().getJobFile());
    assertEquals(job.getID().toString(), jobReport.getJobReport().getJobId().toString());
    assertNotNull(jobReport.getJobReport().getTrackingUrl());
    
    
    //getTaskReports
    GetTaskReportsRequest taskReportRequest=recordFactory
        .newRecordInstance(GetTaskReportsRequest.class);
    taskReportRequest.setJobId(job.getID());
    taskReportRequest.setTaskType(TaskType.MAP);
    GetTaskReportsResponse taskReportResponce= protocol.getTaskReports(taskReportRequest);
    assertEquals(1,taskReportResponce.getTaskReportList().size());
    assertEquals(1,taskReportResponce.getTaskReportCount());
    assertEquals(task.getID(),taskReportResponce.getTaskReport(0).getTaskId());
    assertEquals(TaskState.SUCCEEDED, taskReportResponce.getTaskReport(0).getTaskState());
    
    //getDelegationToken
    GetDelegationTokenRequest  delegationTokenRequest= recordFactory
        .newRecordInstance(GetDelegationTokenRequest.class);
    String s=UserGroupInformation.getCurrentUser().getShortUserName();
    delegationTokenRequest.setRenewer(s);
    GetDelegationTokenResponse delegationTokenResponce= protocol.getDelegationToken(delegationTokenRequest);
   assertEquals("MR_DELEGATION_TOKEN", delegationTokenResponce.getDelegationToken().getKind());
   assertNotNull(delegationTokenResponce.getDelegationToken().getIdentifier());
   
   //renewDelegationToken
     
   RenewDelegationTokenRequest renewDelegationRequest= recordFactory
       .newRecordInstance(RenewDelegationTokenRequest.class);
   renewDelegationRequest.setDelegationToken(delegationTokenResponce.getDelegationToken());
   RenewDelegationTokenResponse  renewDelegationTokenResponce= protocol.renewDelegationToken(renewDelegationRequest);
   // should be about 1 day
  assertTrue( renewDelegationTokenResponce.getNextExpirationTime()>0);
  
  
// cancelDelegationToken   

  CancelDelegationTokenRequest  cancelDelegationTokenRequest=recordFactory
      .newRecordInstance(CancelDelegationTokenRequest.class);
  cancelDelegationTokenRequest.setDelegationToken(delegationTokenResponce.getDelegationToken());
  assertNotNull(protocol.cancelDelegationToken(cancelDelegationTokenRequest));
  
  
    historyServer.stop();
  }

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
