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
package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestAppController {

  private AppControllerForTest appController;
  private RequestContext ctx;
  private Job job;


  @Before
  public void setUp() throws IOException {
    AppContext context = mock(AppContext.class);
    when(context.getApplicationID()).thenReturn(
        Records.newRecord(ApplicationId.class));
    when(context.getApplicationName()).thenReturn("AppName");
    when(context.getUser()).thenReturn("User");
    when(context.getStartTime()).thenReturn(System.currentTimeMillis());
    job=mock(Job.class);
    Task task= mock(Task.class);
        
    
    when(job.getTask(any(TaskId.class))).thenReturn(task);
    
   // when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(true);
    
    JobId jobID = MRApps.toJobID("job_01_01");
    when(context.getJob(jobID)).thenReturn(job);
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(true);

    
    App app = new App(context);
    Configuration conf = new Configuration();
    ctx = mock(RequestContext.class);

    appController = new AppControllerForTest(app, conf, ctx);
    appController.getProperty().put(AMParams.JOB_ID, "job_01_01");
    appController.getProperty().put(AMParams.TASK_ID, "task_01_01_m01_01");
    
    
  }

  @Test
  public void testBadRequest() {
    String message = "test string";
    appController.badRequest(message);
    verifyExpectations(message);
  }

  @Test
  public void testBadRequestWithNullMessage() {
    // It should not throw NullPointerException
    appController.badRequest(null);
    verifyExpectations(StringUtils.EMPTY);
  }

  private void verifyExpectations(String message) {
    verify(ctx).setStatus(400);
    assertEquals("application_0_0000", appController.getProperty().get("app.id"));
    assertNotNull( appController.getProperty().get("rm.web"));
    assertEquals("Bad request: "+message, appController.getProperty().get("title"));
  }

  @Test
  public void testInfo() {
    
    appController.info();
    Iterator<ResponseInfo.Item> iterator = appController.getResponceInfo()
        .iterator();
    ResponseInfo.Item item = iterator.next();
    assertEquals("Application ID:", item.key);
    assertEquals("application_0_0000", item.value);
    item = iterator.next( );
    assertEquals("Application Name:", item.key);
    assertEquals("AppName", item.value);
    item = iterator.next( );
    assertEquals("User:", item.key);
    assertEquals("User", item.value);
    
    item = iterator.next( );
    assertEquals("Started on:", item.key);
    item = iterator.next( );
    assertEquals("Elasped: ", item.key);

  }
  @Test
  public void testGetJob() {
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(false);
    
    appController.job();
    verify( appController.response()).setContentType( MimeType.TEXT);
    assertEquals("Access denied: User user does not have permission to view job job_01_01", appController.getData());
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(true);
    
    appController.getProperty().remove(AMParams.JOB_ID);
    appController.job();
    assertEquals("Access denied: User user does not have permission to view job job_01_01Bad Request: Missing job ID", appController.getData());
    
    appController.getProperty().put(AMParams.JOB_ID, "job_01_01");
    appController.job();
    assertEquals(JobPage.class, appController.getClazz());
  }
  @Test
  public void testGetjobCounters() {
    
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(false);
    
    appController.jobCounters();
    verify( appController.response()).setContentType( MimeType.TEXT);
    assertEquals("Access denied: User user does not have permission to view job job_01_01", appController.getData());
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(true);
    
    appController.getProperty().remove(AMParams.JOB_ID);
    appController.jobCounters();
    assertEquals("Access denied: User user does not have permission to view job job_01_01Bad Request: Missing job ID", appController.getData());
    
    appController.getProperty().put(AMParams.JOB_ID, "job_01_01");
    appController.jobCounters();
    assertEquals(CountersPage.class, appController.getClazz());
  }
  
  
  @Test
  public void testGetTaskCounters() {
    
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(false);
    
    appController.taskCounters();
    verify( appController.response()).setContentType( MimeType.TEXT);
    assertEquals("Access denied: User user does not have permission to view job job_01_01", appController.getData());
    
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(true);
    
    appController.getProperty().remove(AMParams.TASK_ID);
    appController.taskCounters();
    assertEquals("Access denied: User user does not have permission to view job job_01_01missing task ID", appController.getData());
    
    appController.getProperty().put(AMParams.TASK_ID, "task_01_01_m01_01");
    appController.taskCounters();
    assertEquals(CountersPage.class, appController.getClazz());
  }
  
  @Test
  public void testGetSingleJobCounter() throws IOException {
    appController.singleJobCounter();
    assertEquals(SingleCounterPage.class, appController.getClazz());
  }
  
  @Test
  public void testGetSingleTaskCounter() throws IOException {
    appController.singleTaskCounter();
    assertEquals(SingleCounterPage.class, appController.getClazz());
    assertNotNull(appController.getProperty().get(AppController.COUNTER_GROUP));
    assertNotNull(appController.getProperty().get(AppController.COUNTER_NAME));

  }
  
}
