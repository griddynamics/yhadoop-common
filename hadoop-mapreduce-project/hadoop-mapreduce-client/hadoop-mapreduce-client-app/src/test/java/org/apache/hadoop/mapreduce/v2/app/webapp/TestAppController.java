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
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestAppController {

  private AppControllerForTest appController;
  private RequestContext ctx;

  @Before
  public void setUp() throws IOException {
    AppContext context = mock(AppContext.class);
    when(context.getApplicationID()).thenReturn(
        Records.newRecord(ApplicationId.class));
    when(context.getApplicationName()).thenReturn("AppName");
    when(context.getUser()).thenReturn("User");
    when(context.getStartTime()).thenReturn(System.currentTimeMillis());
    Job job=mock(Job.class);
    
    when(job.checkAccess(any(UserGroupInformation.class),any( JobACL.class))).thenReturn(true);
    
    JobId jobID = MRApps.toJobID("job_01_01");
    when(context.getJob(jobID)).thenReturn(job);
    
    
    App app = new App(context);
    Configuration conf = new Configuration();
    ctx = mock(RequestContext.class);

    appController = new AppControllerForTest(app, conf, ctx);
    appController.getProperty().put(AMParams.JOB_ID, "job_01_01");
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
    verify(ctx).set("app.id", "application_0_0000");
    verify(ctx).set(eq("rm.web"), anyString());
    verify(ctx).set("title", "Bad request: " + message);
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
    appController.job();
    assertEquals(JobPage.class, appController.getClazz());
  }
  @Test
  public void testGetjobCounters() {
    appController.jobCounters();
    assertEquals(CountersPage.class, appController.getClazz());
  }
  
}
