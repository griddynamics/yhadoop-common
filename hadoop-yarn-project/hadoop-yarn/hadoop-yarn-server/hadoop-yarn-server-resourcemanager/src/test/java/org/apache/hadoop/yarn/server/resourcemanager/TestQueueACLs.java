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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQueueACLs {

  private static final String COMMON_USER = "common_user";
  private static final String QUEUE_A_USER = "queueA_user";
  private static final String QUEUE_B_USER = "queueB_user";
  private static final String ROOT_ADMIN = "root_admin";
  private static final String QUEUE_A_ADMIN = "queueA_admin";
  private static final String QUEUE_B_ADMIN = "queueB_admin";

  private static final String QUEUEA = "queueA";
  private static final String QUEUEB = "queueB";

  private static final Log LOG = LogFactory.getLog(TestApplicationACLs.class);

  static MockRM resourceManager;
  static Configuration conf = createConfiguration();
  final static YarnRPC rpc = YarnRPC.create(conf);
  final static InetSocketAddress rmAddress = conf.getSocketAddr(
    YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS,
    YarnConfiguration.DEFAULT_RM_PORT);

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    AccessControlList adminACL = new AccessControlList("");
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACL.getAclString());

    resourceManager = new MockRM(conf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(getRMContext(), this.scheduler,
          this.rmAppManager, this.applicationACLsManager,
          this.queueACLsManager, this.rmDTSecretManager);
      };

      @Override
      protected void doSecureLogin() throws IOException {
      }
    };
    new Thread() {
      public void run() {
        resourceManager.start();
      };
    }.start();
    int waitCount = 0;
    while (resourceManager.getServiceState() == STATE.INITED
        && waitCount++ < 60) {
      LOG.info("Waiting for RM to start...");
      Thread.sleep(1500);
    }
    if (resourceManager.getServiceState() != STATE.STARTED) {
      // RM could have failed.
      throw new IOException("ResourceManager failed to start. Final state is "
          + resourceManager.getServiceState());
    }
  }

  @AfterClass
  public static void tearDown() {
    if (resourceManager != null) {
      resourceManager.stop();
    }
  }

  @Test
  public void testApplicationACLs() throws Exception {

    verifyKillAppSuccess(QUEUE_A_USER, QUEUE_A_USER, QUEUEA, true);
    verifyKillAppSuccess(QUEUE_A_USER, QUEUE_A_ADMIN, QUEUEA, true);
    verifyKillAppSuccess(QUEUE_A_USER, COMMON_USER, QUEUEA, true);
    verifyKillAppSuccess(QUEUE_A_USER, ROOT_ADMIN, QUEUEA, true);
    verifyKillAppFailure(QUEUE_A_USER, QUEUE_B_USER, QUEUEA, true);
    verifyKillAppFailure(QUEUE_A_USER, QUEUE_B_ADMIN, QUEUEA, true);

    verifyKillAppSuccess(QUEUE_B_USER, QUEUE_B_USER, QUEUEB, true);
    verifyKillAppSuccess(QUEUE_B_USER, QUEUE_B_ADMIN, QUEUEB, true);
    verifyKillAppSuccess(QUEUE_B_USER, COMMON_USER, QUEUEB, true);
    verifyKillAppSuccess(QUEUE_B_USER, ROOT_ADMIN, QUEUEB, true);

    verifyKillAppFailure(QUEUE_B_USER, QUEUE_A_USER, QUEUEB, true);
    verifyKillAppFailure(QUEUE_B_USER, QUEUE_A_ADMIN, QUEUEB, true);

    verifyKillAppSuccess(ROOT_ADMIN, ROOT_ADMIN, QUEUEA, false);
    verifyKillAppSuccess(ROOT_ADMIN, ROOT_ADMIN, QUEUEB, false);

    verifyGetClientAMToken(QUEUE_A_USER, ROOT_ADMIN, QUEUEA, true);

  }

  private void verifyGetClientAMToken(String submitter, String queueAdmin,
      String queueName, boolean setupACLs) throws Exception {
    ApplicationId applicationId =
        submitAppAndGetAppId(submitter, queueName, setupACLs);
    final GetApplicationReportRequest appReportRequest =
        GetApplicationReportRequest.newInstance(applicationId);

    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    ApplicationClientProtocol adMinUserClient = getRMClientForUser(queueAdmin);

    GetApplicationReportResponse submitterGetReport =
        submitterClient.getApplicationReport(appReportRequest);
    GetApplicationReportResponse adMinUserGetReport =
        adMinUserClient.getApplicationReport(appReportRequest);

    Assert.assertEquals(submitterGetReport.getApplicationReport()
      .getClientToAMToken(), adMinUserGetReport.getApplicationReport()
      .getClientToAMToken());
  }

  private void verifyKillAppFailure(String submitter, String killer,
      String queueName, boolean setupACLs) throws Exception {

    ApplicationId applicationId =
        submitAppAndGetAppId(submitter, queueName, setupACLs);

    final KillApplicationRequest finishAppRequest =
        KillApplicationRequest.newInstance(applicationId);

    ApplicationClientProtocol killerClient = getRMClientForUser(killer);

    // Kill app as the killer
    try {
      killerClient.forceKillApplication(finishAppRequest);
      Assert.fail("App killing by the enemy should fail!!");
    } catch (YarnException e) {
      LOG.info("Got exception while killing app as the enemy", e);
      Assert.assertTrue(e.getMessage().contains(
        "User " + killer + " cannot perform operation MODIFY_APP on "
            + applicationId));
    }

    getRMClientForUser(submitter).forceKillApplication(finishAppRequest);
  }

  private void verifyKillAppSuccess(String submitter, String killer,
      String queueName, boolean setupACLs) throws Exception {
    ApplicationId applicationId =
        submitAppAndGetAppId(submitter, queueName, setupACLs);

    final KillApplicationRequest finishAppRequest =
        KillApplicationRequest.newInstance(applicationId);

    ApplicationClientProtocol ownerClient = getRMClientForUser(killer);

    // Kill app as killer
    ownerClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private ApplicationId submitAppAndGetAppId(String submitter,
      String queueName, boolean setupACLs) throws Exception {

    GetNewApplicationRequest newAppRequest =
        GetNewApplicationRequest.newInstance();

    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    ApplicationId applicationId =
        submitterClient.getNewApplication(newAppRequest).getApplicationId();

    Resource resource = BuilderUtils.newResource(1024, 1);
    Map<ApplicationAccessType, String> acls = createACLs(submitter, setupACLs);
    ContainerLaunchContext amContainerSpec =
        ContainerLaunchContext.newInstance(null, null, null, null, null, acls);

    ApplicationSubmissionContext appSubmissionContext =
        ApplicationSubmissionContext.newInstance(applicationId,
          "applicationName", queueName, null, amContainerSpec, false, true, 1,
          resource, "applicationType");
    appSubmissionContext.setApplicationId(applicationId);
    appSubmissionContext.setQueue(queueName);

    SubmitApplicationRequest submitRequest =
        SubmitApplicationRequest.newInstance(appSubmissionContext);
    submitterClient.submitApplication(submitRequest);
    resourceManager.waitForState(applicationId, RMAppState.ACCEPTED);
    return applicationId;
  }

  private Map<ApplicationAccessType, String> createACLs(String submitter,
      boolean setupACLs) {
    AccessControlList viewACL = new AccessControlList("");
    AccessControlList modifyACL = new AccessControlList("");
    if (setupACLs) {
      viewACL.addUser(submitter);
      viewACL.addUser(COMMON_USER);
      modifyACL.addUser(submitter);
      modifyACL.addUser(COMMON_USER);
    }
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, viewACL.getAclString());
    acls.put(ApplicationAccessType.MODIFY_APP, modifyACL.getAclString());
    return acls;
  }

  private ApplicationClientProtocol getRMClientForUser(String user)
      throws IOException, InterruptedException {
    UserGroupInformation userUGI = UserGroupInformation.createRemoteUser(user);
    ApplicationClientProtocol userClient =
        userUGI
          .doAs(new PrivilegedExceptionAction<ApplicationClientProtocol>() {
            @Override
            public ApplicationClientProtocol run() throws Exception {
              return (ApplicationClientProtocol) rpc.getProxy(
                ApplicationClientProtocol.class, rmAddress, conf);
            }
          });
    return userClient;
  }

  private static YarnConfiguration createConfiguration() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {
        QUEUEA, QUEUEB });

    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA, 50f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB, 50f);

    Map<QueueACL, AccessControlList> aclsOnQueueA =
        new HashMap<QueueACL, AccessControlList>();
    AccessControlList submitACLonQueueA = new AccessControlList(QUEUE_A_USER);
    submitACLonQueueA.addUser(COMMON_USER);
    AccessControlList adminACLonQueueA = new AccessControlList(QUEUE_A_ADMIN);
    aclsOnQueueA.put(QueueACL.SUBMIT_APPLICATIONS, submitACLonQueueA);
    aclsOnQueueA.put(QueueACL.ADMINISTER_QUEUE, adminACLonQueueA);
    csConf.setAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
      aclsOnQueueA);

    Map<QueueACL, AccessControlList> aclsOnQueueB =
        new HashMap<QueueACL, AccessControlList>();
    AccessControlList submitACLonQueueB = new AccessControlList(QUEUE_B_USER);
    submitACLonQueueB.addUser(COMMON_USER);
    AccessControlList adminACLonQueueB = new AccessControlList(QUEUE_B_ADMIN);
    aclsOnQueueB.put(QueueACL.SUBMIT_APPLICATIONS, submitACLonQueueB);
    aclsOnQueueB.put(QueueACL.ADMINISTER_QUEUE, adminACLonQueueB);
    csConf.setAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB,
      aclsOnQueueB);

    Map<QueueACL, AccessControlList> aclsOnRootQueue =
        new HashMap<QueueACL, AccessControlList>();
    AccessControlList submitACLonRoot = new AccessControlList("");
    AccessControlList adminACLonRoot = new AccessControlList(ROOT_ADMIN);
    aclsOnRootQueue.put(QueueACL.SUBMIT_APPLICATIONS, submitACLonRoot);
    aclsOnRootQueue.put(QueueACL.ADMINISTER_QUEUE, adminACLonRoot);
    csConf.setAcls(CapacitySchedulerConfiguration.ROOT, aclsOnRootQueue);

    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    return conf;
  }
}
