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

package org.apache.hadoop.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Simple test some classes from org.apache.hadoop.yarn.server.api
 */
public class TestYarnServerApiClasses {

  private final static org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  /**
   * Test RegisterNodeManagerResponsePBImpl. test getters and setters. The
   * RegisterNodeManagerResponsePBImpl should generate a prototype and restore a
   * data from prototype
   */
  @Test(timeout = 500)
  public void testRegisterNodeManagerResponsePBImpl() {
    RegisterNodeManagerResponsePBImpl original = new RegisterNodeManagerResponsePBImpl();
    original.setMasterKey(getMasterKey());
    original.setNodeAction(NodeAction.NORMAL);

    RegisterNodeManagerResponsePBImpl copy = new RegisterNodeManagerResponsePBImpl(
        original.getProto());
    assertEquals(1, copy.getMasterKey().getKeyId());
    assertEquals(NodeAction.NORMAL, copy.getNodeAction());

  }

  /**
   * Test NodeHeartbeatRequestPBImpl. test getters and setters. The
   * RegisterNodeManagerResponsePBImpl should generate a prototype and restore a
   * data from prototype
   */
  @Test(timeout = 500)
  public void testNodeHeartbeatRequestPBImpl() {
    NodeHeartbeatRequestPBImpl original = new NodeHeartbeatRequestPBImpl();
    original.setLastKnownMasterKey(getMasterKey());
    original.setNodeStatus(getNodeStatus());
    NodeHeartbeatRequestPBImpl copy = new NodeHeartbeatRequestPBImpl(
        original.getProto());
    assertEquals(1, copy.getLastKnownMasterKey().getKeyId());
    assertEquals("localhost", copy.getNodeStatus().getNodeId().getHost());
  }

 

  /**
   * Test RegisterNodeManagerRequestPBImpl. test getters and setters. The
   * RegisterNodeManagerResponsePBImpl should generate a prototype and restore a
   * data from prototype
   */

  @Test(timeout = 500)
  public void testRegisterNodeManagerRequestPBImpl() {
    RegisterNodeManagerRequestPBImpl original = new RegisterNodeManagerRequestPBImpl();
    original.setHttpPort(8080);
    original.setNodeId(getNodeId());
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemory(10000);
    resource.setVirtualCores(3);
    original.setResource(resource);
    RegisterNodeManagerRequestPBImpl copy = new RegisterNodeManagerRequestPBImpl(
        original.getProto());

    assertEquals(8080, copy.getHttpPort());
    assertEquals(9090, copy.getNodeId().getPort());
    assertEquals(10000, copy.getResource().getMemory());
  }

  /**
   * Test MasterKeyPBImpl. test getters and setters. The
   * RegisterNodeManagerResponsePBImpl should generate a prototype and restore a
   * data from prototype
   */

  @Test(timeout = 500)
  public void testMasterKeyPBImpl() {
    MasterKeyPBImpl original = new MasterKeyPBImpl();

    original.setBytes(ByteBuffer.allocate(0));
    original.setKeyId(1);
    MasterKeyPBImpl copy = new MasterKeyPBImpl(original.getProto());
    assertEquals(1, copy.getKeyId());

  }

  /**
   * Test NodeStatusPBImpl. test getters and setters. The
   * RegisterNodeManagerResponsePBImpl should generate a prototype and restore a
   * data from prototype
   */

  @Test(timeout = 500)
  public void testNodeStatusPBImpl() {
    NodeStatusPBImpl original = new NodeStatusPBImpl();

    original.setContainersStatuses(Arrays.asList(getContainerStatus(1, 2, 1),
        getContainerStatus(2, 3, 1)));
    original.setKeepAliveApplications(Arrays.asList(getApplicationId(3),
        getApplicationId(4)));
    original.setNodeHealthStatus(getNodeHealthStatus());
    original.setNodeId(getNodeId());
    original.setResponseId(1);

    NodeStatusPBImpl copy = new NodeStatusPBImpl(original.getProto());
    assertEquals(3, copy.getContainersStatuses().get(1).getContainerId()
        .getId());
    assertEquals(3, copy.getKeepAliveApplications().get(0).getId());
    assertEquals(1000, copy.getNodeHealthStatus().getLastHealthReportTime());
    assertEquals(9090, copy.getNodeId().getPort());
    assertEquals(1, copy.getResponseId());

  }

 
  

  private ContainerStatus getContainerStatus(int applicationId,
      int containerID, int appAttemptId) {
    ContainerStatus status = recordFactory
        .newRecordInstance(ContainerStatus.class);
    status.setContainerId(getContainerId(containerID, appAttemptId));
    return status;
  }

  private ApplicationAttemptId getApplicationAttemptId(int appAttemptId) {
    ApplicationAttemptIdPBImpl result = new ApplicationAttemptIdPBImpl();

    result.setApplicationId(getApplicationId(appAttemptId));
    result.setAttemptId(1);
    return result;
  }

  private ContainerId getContainerId(int containerID, int appAttemptId) {
    ContainerIdPBImpl containerId = new ContainerIdPBImpl();
    containerId.setId(containerID);
    containerId.setApplicationAttemptId(getApplicationAttemptId(appAttemptId));
    return containerId;
  }

  private ApplicationId getApplicationId(int applicationId) {
    ApplicationId appId = new ApplicationIdPBImpl();
    appId.setClusterTimestamp(1000);
    appId.setId(applicationId);
    return appId;
  }

  private NodeStatus getNodeStatus() {
    NodeStatus status = recordFactory.newRecordInstance(NodeStatus.class);
    status.setContainersStatuses(new ArrayList<ContainerStatus>());
    status.setKeepAliveApplications(new ArrayList<ApplicationId>());

    status.setNodeHealthStatus(getNodeHealthStatus());
    status.setNodeId(getNodeId());
    status.setResponseId(1);
    return status;
  }

  private NodeId getNodeId() {
    NodeId id = recordFactory.newRecordInstance(NodeId.class);
    id.setHost("localhost");
    id.setPort(9090);
    return id;
  }

  private NodeHealthStatus getNodeHealthStatus() {
    NodeHealthStatus healStatus = recordFactory
        .newRecordInstance(NodeHealthStatus.class);
    healStatus.setHealthReport("healthReport");
    healStatus.setIsNodeHealthy(true);
    healStatus.setLastHealthReportTime(1000);
    return healStatus;

  }

  private MasterKey getMasterKey() {
    MasterKey key = recordFactory.newRecordInstance(MasterKey.class);
    key.setBytes(ByteBuffer.allocate(0));
    key.setKeyId(1);
    return key;

  }
}
