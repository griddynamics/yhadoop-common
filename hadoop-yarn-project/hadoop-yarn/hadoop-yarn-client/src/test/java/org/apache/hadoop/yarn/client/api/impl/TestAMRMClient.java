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

package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mortbay.log.Log;

public class TestAMRMClient {
  static Configuration conf = null;
  static MiniYARNCluster yarnCluster = null;
  static YarnClient yarnClient = null;
  static List<NodeReport> nodeReports = null;
  static ApplicationAttemptId attemptId = null;
  static int nodeCount = 3;
  
  static Resource capability;
  static Priority priority;
  static Priority priority2;
  static String node;
  static String rack;
  static String[] nodes;
  static String[] racks;
  private final static int DEFAULT_ITERATION = 3;
  
  @BeforeClass
  public static void setup() throws Exception {
    // start minicluster
    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    yarnCluster = new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    
    priority = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    capability = Resource.newInstance(1024, 1);

    node = nodeReports.get(0).getNodeId().getHost();
    rack = nodeReports.get(0).getRackName();
    nodes = new String[]{ node };
    racks = new String[]{ rack };
  }
  
  @Before
  public void startApp() throws Exception {
    // submit new app
    ApplicationSubmissionContext appContext = 
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(
          Collections.<String, LocalResource> emptyMap(),
          new HashMap<String, String>(), Arrays.asList("sleep", "100"),
          new HashMap<String, ByteBuffer>(), null,
          new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    RMAppAttempt appAttempt = null;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt =
            yarnCluster.getResourceManager().getRMContext().getRMApps()
              .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
    }
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
      .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
  }
  
  @After
  public void cancelApp() throws YarnException, IOException {
    yarnClient.killApplication(attemptId.getApplicationId());
    attemptId = null;
  }
  
  @AfterClass
  public static void tearDown() {
    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
  }
  
  @Test (timeout=60000)
  public void testAMRMClientMatchingFit() throws YarnException, IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.<ContainerRequest>createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      Resource capability1 = Resource.newInstance(1024, 2);
      Resource capability2 = Resource.newInstance(1024, 1);
      Resource capability3 = Resource.newInstance(1000, 2);
      Resource capability4 = Resource.newInstance(2000, 1);
      Resource capability5 = Resource.newInstance(1000, 3);
      Resource capability6 = Resource.newInstance(2000, 1);
      Resource capability7 = Resource.newInstance(2000, 1);

      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability1, nodes, racks, priority);
      ContainerRequest storedContainer2 = 
          new ContainerRequest(capability2, nodes, racks, priority);
      ContainerRequest storedContainer3 = 
          new ContainerRequest(capability3, nodes, racks, priority);
      ContainerRequest storedContainer4 = 
          new ContainerRequest(capability4, nodes, racks, priority);
      ContainerRequest storedContainer5 = 
          new ContainerRequest(capability5, nodes, racks, priority);
      ContainerRequest storedContainer6 = 
          new ContainerRequest(capability6, nodes, racks, priority);
      ContainerRequest storedContainer7 = 
          new ContainerRequest(capability7, nodes, racks, priority2, false);
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer2);
      amClient.addContainerRequest(storedContainer3);
      amClient.addContainerRequest(storedContainer4);
      amClient.addContainerRequest(storedContainer5);
      amClient.addContainerRequest(storedContainer6);
      amClient.addContainerRequest(storedContainer7);
      
      // test matching of containers
      List<? extends Collection<ContainerRequest>> matches;
      ContainerRequest storedRequest;
      // exact match
      Resource testCapability1 = Resource.newInstance(1024,  2);
      matches = amClient.getMatchingRequests(priority, node, testCapability1);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertTrue(storedContainer1 == storedRequest);
      amClient.removeContainerRequest(storedContainer1);
      
      // exact matching with order maintained
      Resource testCapability2 = Resource.newInstance(2000, 1);
      matches = amClient.getMatchingRequests(priority, node, testCapability2);
      verifyMatches(matches, 2);
      // must be returned in the order they were made
      int i = 0;
      for(ContainerRequest storedRequest1 : matches.get(0)) {
        if(i++ == 0) {
          assertTrue(storedContainer4 == storedRequest1);
        } else {
          assertTrue(storedContainer6 == storedRequest1);
        }
      }
      amClient.removeContainerRequest(storedContainer6);
      
      // matching with larger container. all requests returned
      Resource testCapability3 = Resource.newInstance(4000, 4);
      matches = amClient.getMatchingRequests(priority, node, testCapability3);
      assert(matches.size() == 4);
      
      Resource testCapability4 = Resource.newInstance(1024, 2);
      matches = amClient.getMatchingRequests(priority, node, testCapability4);
      assert(matches.size() == 2);
      // verify non-fitting containers are not returned and fitting ones are
      for(Collection<ContainerRequest> testSet : matches) {
        assertTrue(testSet.size() == 1);
        ContainerRequest testRequest = testSet.iterator().next();
        assertTrue(testRequest != storedContainer4);
        assertTrue(testRequest != storedContainer5);
        assert(testRequest == storedContainer2 || 
                testRequest == storedContainer3);
      }
      
      Resource testCapability5 = Resource.newInstance(512, 4);
      matches = amClient.getMatchingRequests(priority, node, testCapability5);
      assert(matches.size() == 0);
      
      // verify requests without relaxed locality are only returned at specific
      // locations
      Resource testCapability7 = Resource.newInstance(2000, 1);
      matches = amClient.getMatchingRequests(priority2, ResourceRequest.ANY,
          testCapability7);
      assert(matches.size() == 0);
      matches = amClient.getMatchingRequests(priority2, node, testCapability7);
      assert(matches.size() == 1);
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  private void verifyMatches(
                  List<? extends Collection<ContainerRequest>> matches,
                  int matchSize) {
    assertTrue(matches.size() == 1);
    assertTrue(matches.get(0).size() == matchSize);    
  }
  
  @Test (timeout=60000)
  public void testAMRMClientMatchingFitInferredRack() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = new AMRMClientImpl<ContainerRequest>();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      Resource capability = Resource.newInstance(1024, 2);

      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability, nodes, null, priority);
      amClient.addContainerRequest(storedContainer1);

      // verify matching with original node and inferred rack
      List<? extends Collection<ContainerRequest>> matches;
      ContainerRequest storedRequest;
      // exact match node
      matches = amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertTrue(storedContainer1 == storedRequest);
      // inferred match rack
      matches = amClient.getMatchingRequests(priority, rack, capability);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertTrue(storedContainer1 == storedRequest);
      
      // inferred rack match no longer valid after request is removed
      amClient.removeContainerRequest(storedContainer1);
      matches = amClient.getMatchingRequests(priority, rack, capability);
      assertTrue(matches.isEmpty());
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  @Test //(timeout=60000)
  public void testAMRMClientMatchStorage() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      Priority priority1 = Records.newRecord(Priority.class);
      priority1.setPriority(2);
      
      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability, nodes, racks, priority);
      ContainerRequest storedContainer2 = 
          new ContainerRequest(capability, nodes, racks, priority);
      ContainerRequest storedContainer3 = 
          new ContainerRequest(capability, null, null, priority1);
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer2);
      amClient.addContainerRequest(storedContainer3);
      
      // test addition and storage
      int containersRequestedAny = amClient.remoteRequestsTable.get(priority)
       .get(ResourceRequest.ANY).get(capability).remoteRequest.getNumContainers();
      assertTrue(containersRequestedAny == 2);
      containersRequestedAny = amClient.remoteRequestsTable.get(priority1)
          .get(ResourceRequest.ANY).get(capability).remoteRequest.getNumContainers();
         assertTrue(containersRequestedAny == 1);
      List<? extends Collection<ContainerRequest>> matches = 
          amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 2);
      matches = amClient.getMatchingRequests(priority, rack, capability);
      verifyMatches(matches, 2);
      matches = 
          amClient.getMatchingRequests(priority, ResourceRequest.ANY, capability);
      verifyMatches(matches, 2);
      matches = amClient.getMatchingRequests(priority1, rack, capability);
      assertTrue(matches.isEmpty());
      matches = 
          amClient.getMatchingRequests(priority1, ResourceRequest.ANY, capability);
      verifyMatches(matches, 1);
      
      // test removal
      amClient.removeContainerRequest(storedContainer3);
      matches = amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 2);
      amClient.removeContainerRequest(storedContainer2);
      matches = amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 1);
      matches = amClient.getMatchingRequests(priority, rack, capability);
      verifyMatches(matches, 1);
      
      // test matching of containers
      ContainerRequest storedRequest = matches.get(0).iterator().next();
      assertTrue(storedContainer1 == storedRequest);
      amClient.removeContainerRequest(storedContainer1);
      matches = 
          amClient.getMatchingRequests(priority, ResourceRequest.ANY, capability);
      assertTrue(matches.isEmpty());
      matches = 
          amClient.getMatchingRequests(priority1, ResourceRequest.ANY, capability);
      assertTrue(matches.isEmpty());
      // 0 requests left. everything got cleaned up
      assertTrue(amClient.remoteRequestsTable.isEmpty());
      
      // go through an exemplary allocation, matching and release cycle
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer3);
      // RM should allocate container within 2 calls to allocate()
      int allocatedContainerCount = 0;
      int iterationsLeft = 3;
      while (allocatedContainerCount < 2
          && iterationsLeft-- > 0) {
        Log.info(" == alloc " + allocatedContainerCount + " it left " + iterationsLeft);
        AllocateResponse allocResponse = amClient.allocate(0.1f);
        assertTrue(amClient.ask.size() == 0);
        assertTrue(amClient.release.size() == 0);
        
        assertTrue(nodeCount == amClient.getClusterNodeCount());
        allocatedContainerCount += allocResponse.getAllocatedContainers().size();
        for(Container container : allocResponse.getAllocatedContainers()) {
          ContainerRequest expectedRequest = 
              container.getPriority().equals(storedContainer1.getPriority()) ?
                  storedContainer1 : storedContainer3;
          matches = amClient.getMatchingRequests(container.getPriority(), 
                                                 ResourceRequest.ANY, 
                                                 container.getResource());
          // test correct matched container is returned
          verifyMatches(matches, 1);
          ContainerRequest matchedRequest = matches.get(0).iterator().next();
          assertTrue(matchedRequest == expectedRequest);
          amClient.removeContainerRequest(matchedRequest);
          // assign this container, use it and release it
          amClient.releaseAssignedContainer(container.getId());
        }
        if(allocatedContainerCount < containersRequestedAny) {
          // sleep to let NM's heartbeat to RM and trigger allocations
          sleep(100);
        }
      }
      
      assertTrue(allocatedContainerCount == 2);
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertTrue(amClient.release.size() == 0);
      assertTrue(amClient.ask.size() == 0);
      assertTrue(allocResponse.getAllocatedContainers().size() == 0);
      // 0 requests left. everything got cleaned up
      assertTrue(amClient.remoteRequestsTable.isEmpty());      
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  @Test (timeout=60000)
  public void testAllocationWithBlacklist() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      assertTrue(amClient.ask.size() == 0);
      assertTrue(amClient.release.size() == 0);
      
      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability, nodes, racks, priority);
      amClient.addContainerRequest(storedContainer1);
      assertTrue(amClient.ask.size() == 3);
      assertTrue(amClient.release.size() == 0);
      
      List<String> localNodeBlacklist = new ArrayList<String>();
      localNodeBlacklist.add(node);
      
      // put node in black list, so no container assignment
      amClient.updateBlacklist(localNodeBlacklist, null);

      int allocatedContainerCount = getAllocatedContainersNumber(amClient,
        DEFAULT_ITERATION);
      // the only node is in blacklist, so no allocation
      assertTrue(allocatedContainerCount == 0);

      // Remove node from blacklist, so get assigned with 2
      amClient.updateBlacklist(null, localNodeBlacklist);
      ContainerRequest storedContainer2 = 
              new ContainerRequest(capability, nodes, racks, priority);
      amClient.addContainerRequest(storedContainer2);
      allocatedContainerCount = getAllocatedContainersNumber(amClient,
          DEFAULT_ITERATION);
      assertEquals(allocatedContainerCount, 2);
      
      // Test in case exception in allocate(), blacklist is kept
      assertTrue(amClient.blacklistAdditions.isEmpty());
      assertTrue(amClient.blacklistRemovals.isEmpty());
      
      // create a invalid ContainerRequest - memory value is minus
      ContainerRequest invalidContainerRequest = 
          new ContainerRequest(Resource.newInstance(-1024, 1),
              nodes, racks, priority);
      amClient.addContainerRequest(invalidContainerRequest);
      amClient.updateBlacklist(localNodeBlacklist, null);
      try {
        // allocate() should complain as ContainerRequest is invalid.
        amClient.allocate(0.1f);
        fail("there should be an exception here.");
      } catch (Exception e) {
        assertEquals(amClient.blacklistAdditions.size(), 1);
      }
    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  @Test (timeout=60000)
  public void testAMRMClientWithBlacklist() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      String[] nodes = {"node1", "node2", "node3"};
      
      // Add nodes[0] and nodes[1]
      List<String> nodeList01 = new ArrayList<String>();
      nodeList01.add(nodes[0]);
      nodeList01.add(nodes[1]);
      amClient.updateBlacklist(nodeList01, null);
      assertEquals(amClient.blacklistAdditions.size(),2);
      assertEquals(amClient.blacklistRemovals.size(),0);
      
      // Add nodes[0] again, verify it is not added duplicated.
      List<String> nodeList02 = new ArrayList<String>();
      nodeList02.add(nodes[0]);
      nodeList02.add(nodes[2]);
      amClient.updateBlacklist(nodeList02, null);
      assertEquals(amClient.blacklistAdditions.size(),3);
      assertEquals(amClient.blacklistRemovals.size(),0);
      
      // Add nodes[1] and nodes[2] to removal list, 
      // Verify addition list remove these two nodes.
      List<String> nodeList12 = new ArrayList<String>();
      nodeList12.add(nodes[1]);
      nodeList12.add(nodes[2]);
      amClient.updateBlacklist(null, nodeList12);
      assertEquals(amClient.blacklistAdditions.size(),1);
      assertEquals(amClient.blacklistRemovals.size(),2);
      
      // Add nodes[1] again to addition list, 
      // Verify removal list will remove this node.
      List<String> nodeList1 = new ArrayList<String>();
      nodeList1.add(nodes[1]);
      amClient.updateBlacklist(nodeList1, null);
      assertEquals(amClient.blacklistAdditions.size(),2);
      assertEquals(amClient.blacklistRemovals.size(),1);
    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  private int getAllocatedContainersNumber(
      AMRMClientImpl<ContainerRequest> amClient, int iterationsLeft)
      throws YarnException, IOException {
    int allocatedContainerCount = 0;
    while (iterationsLeft-- > 0) {
      Log.info(" == alloc " + allocatedContainerCount + " it left " + iterationsLeft);
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertTrue(amClient.ask.size() == 0);
      assertTrue(amClient.release.size() == 0);
        
      assertTrue(nodeCount == amClient.getClusterNodeCount());
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
        
      if(allocatedContainerCount == 0) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    return allocatedContainerCount;
  }

  @Test (timeout=60000)
  public void testAMRMClient() throws YarnException, IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.<ContainerRequest>createAMRMClient();

      //setting an instance NMTokenCache
      amClient.setNMTokenCache(new NMTokenCache());
      //asserting we are not using the singleton instance cache
      Assert.assertNotSame(NMTokenCache.getSingleton(), 
          amClient.getNMTokenCache());

      amClient.init(conf);
      amClient.start();

      amClient.registerApplicationMaster("Host", 10000, "");

      testAllocation((AMRMClientImpl<ContainerRequest>)amClient);

      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
    
  private void testAllocation(final AMRMClientImpl<ContainerRequest> amClient)  
      throws YarnException, IOException {
    // setup container request
    
    assertTrue(amClient.ask.size() == 0);
    assertTrue(amClient.release.size() == 0);
    
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    
    int containersRequestedNode = amClient.remoteRequestsTable.get(priority)
        .get(node).get(capability).remoteRequest.getNumContainers();
    int containersRequestedRack = amClient.remoteRequestsTable.get(priority)
        .get(rack).get(capability).remoteRequest.getNumContainers();
    int containersRequestedAny = amClient.remoteRequestsTable.get(priority)
    .get(ResourceRequest.ANY).get(capability).remoteRequest.getNumContainers();

    assertTrue(containersRequestedNode == 2);
    assertTrue(containersRequestedRack == 2);
    assertTrue(containersRequestedAny == 2);
    assertTrue(amClient.ask.size() == 3);
    assertTrue(amClient.release.size() == 0);

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 3;
    Set<ContainerId> releases = new TreeSet<ContainerId>();
    
    amClient.getNMTokenCache().clearCache();
    Assert.assertEquals(0, amClient.getNMTokenCache().numberOfTokensInCache());
    HashMap<String, Token> receivedNMTokens = new HashMap<String, Token>();
    
    while (allocatedContainerCount < containersRequestedAny
        && iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertTrue(amClient.ask.size() == 0);
      assertTrue(amClient.release.size() == 0);
      
      assertTrue(nodeCount == amClient.getClusterNodeCount());
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
      for(Container container : allocResponse.getAllocatedContainers()) {
        ContainerId rejectContainerId = container.getId();
        releases.add(rejectContainerId);
        amClient.releaseAssignedContainer(rejectContainerId);
      }
      
      for (NMToken token : allocResponse.getNMTokens()) {
        String nodeID = token.getNodeId().toString();
        if (receivedNMTokens.containsKey(nodeID)) {
          Assert.fail("Received token again for : " + nodeID);          
        }
        receivedNMTokens.put(nodeID, token.getToken());
      }
      
      if(allocatedContainerCount < containersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    
    // Should receive atleast 1 token
    Assert.assertTrue(receivedNMTokens.size() > 0
        && receivedNMTokens.size() <= nodeCount);
    
    assertTrue(allocatedContainerCount == containersRequestedAny);
    assertTrue(amClient.release.size() == 2);
    assertTrue(amClient.ask.size() == 0);
    
    // need to tell the AMRMClient that we dont need these resources anymore
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    assertTrue(amClient.ask.size() == 3);
    // send 0 container count request for resources that are no longer needed
    ResourceRequest snoopRequest = amClient.ask.iterator().next();
    assertTrue(snoopRequest.getNumContainers() == 0);
    
    // test RPC exception handling
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority));
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority));
    snoopRequest = amClient.ask.iterator().next();
    assertTrue(snoopRequest.getNumContainers() == 2);
    
    ApplicationMasterProtocol realRM = amClient.rmClient;
    try {
      ApplicationMasterProtocol mockRM = mock(ApplicationMasterProtocol.class);
      when(mockRM.allocate(any(AllocateRequest.class))).thenAnswer(
          new Answer<AllocateResponse>() {
            public AllocateResponse answer(InvocationOnMock invocation)
                throws Exception {
              amClient.removeContainerRequest(
                             new ContainerRequest(capability, nodes, 
                                                          racks, priority));
              amClient.removeContainerRequest(
                  new ContainerRequest(capability, nodes, racks, priority));
              throw new Exception();
            }
          });
      amClient.rmClient = mockRM;
      amClient.allocate(0.1f);
    }catch (Exception ioe) {}
    finally {
      amClient.rmClient = realRM;
    }

    assertTrue(amClient.release.size() == 2);
    assertTrue(amClient.ask.size() == 3);
    snoopRequest = amClient.ask.iterator().next();
    // verify that the remove request made in between makeRequest and allocate 
    // has not been lost
    assertTrue(snoopRequest.getNumContainers() == 0);
    
    iterationsLeft = 3;
    // do a few iterations to ensure RM is not going send new containers
    while(!releases.isEmpty() || iterationsLeft-- > 0) {
      // inform RM of rejection
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      // RM did not send new containers because AM does not need any
      assertTrue(allocResponse.getAllocatedContainers().size() == 0);
      if(allocResponse.getCompletedContainersStatuses().size() > 0) {
        for(ContainerStatus cStatus :allocResponse
            .getCompletedContainersStatuses()) {
          if(releases.contains(cStatus.getContainerId())) {
            assertTrue(cStatus.getState() == ContainerState.COMPLETE);
            assertTrue(cStatus.getExitStatus() == -100);
            releases.remove(cStatus.getContainerId());
          }
        }
      }
      if(iterationsLeft > 0) {
        // sleep to make sure NM's heartbeat
        sleep(100);
      }
    }
    assertTrue(amClient.ask.size() == 0);
    assertTrue(amClient.release.size() == 0);
  }
  
  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
