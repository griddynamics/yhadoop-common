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

import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestRPC {

  private static final String EXCEPTION_MSG = "test error";
  private static final String EXCEPTION_CAUSE = "exception cause";
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  @Test
  public void testUnknownCall() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, HadoopYarnProtoRPC.class
        .getName());
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManagementProtocol.class,
        new DummyContainerManager(), addr, conf, null, 1);
    server.start();

    // Any unrelated protocol would do
    ApplicationClientProtocol proxy = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, NetUtils.getConnectAddress(server), conf);

    try {
      proxy.getNewApplication(Records
          .newRecord(GetNewApplicationRequest.class));
      Assert.fail("Excepted RPC call to fail with unknown method.");
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().matches(
          "Unknown method getNewApplication called on.*"
              + "org.apache.hadoop.yarn.proto.ApplicationClientProtocol"
              + "\\$ApplicationClientProtocolService\\$BlockingInterface protocol."));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testHadoopProtoRPC() throws Exception {
    test(HadoopYarnProtoRPC.class.getName());
  }
  
  private void test(String rpcClass) throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, rpcClass);
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManagementProtocol.class, 
            new DummyContainerManager(), addr, conf, null, 1);
    server.start();
    RPC.setProtocolEngine(conf, ContainerManagementProtocolPB.class, ProtobufRpcEngine.class);
    ContainerManagementProtocol proxy = (ContainerManagementProtocol) 
        rpc.getProxy(ContainerManagementProtocol.class, 
            NetUtils.getConnectAddress(server), conf);
    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    ApplicationId applicationId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    ContainerId containerId =
        ContainerId.newInstance(applicationAttemptId, 100);
    StartContainerRequest scRequest =
        recordFactory.newRecordInstance(StartContainerRequest.class);
    scRequest.setContainerLaunchContext(containerLaunchContext);
    NodeId nodeId = NodeId.newInstance("localhost", 1234);
    Resource resource = Resource.newInstance(1234, 2);
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(containerId, "localhost", "user",
          resource, System.currentTimeMillis() + 10000, 42, 42);
    Token containerToken = newContainerToken(nodeId, "password".getBytes(),
          containerTokenIdentifier);
    scRequest.setContainerToken(containerToken);
    proxy.startContainer(scRequest);
    
    GetContainerStatusRequest gcsRequest = 
        recordFactory.newRecordInstance(GetContainerStatusRequest.class);
    gcsRequest.setContainerId(containerId);
    GetContainerStatusResponse response =  proxy.getContainerStatus(gcsRequest);
    ContainerStatus status = response.getStatus();
    
    //test remote exception
    boolean exception = false;
    try {
      StopContainerRequest stopRequest = recordFactory.newRecordInstance(StopContainerRequest.class);
      stopRequest.setContainerId(containerId);
      proxy.stopContainer(stopRequest);
    } catch (YarnException e) {
      exception = true;
      Assert.assertTrue(e.getMessage().contains(EXCEPTION_MSG));
      Assert.assertTrue(e.getMessage().contains(EXCEPTION_CAUSE));
      System.out.println("Test Exception is " + e.getMessage());
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    Assert.assertTrue(exception);
    
    server.stop();
    Assert.assertNotNull(status);
    Assert.assertEquals(ContainerState.RUNNING, status.getState());
  }

  public class DummyContainerManager implements ContainerManagementProtocol {

    private ContainerStatus status = null;    
    
    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request)
    throws YarnException {
      GetContainerStatusResponse response = 
          recordFactory.newRecordInstance(GetContainerStatusResponse.class);
      response.setStatus(status);
      return response;
    }

    @Override
    public StartContainerResponse startContainer(StartContainerRequest request) 
        throws YarnException {
      Token containerToken = request.getContainerToken();
      ContainerTokenIdentifier tokenId = null;

      try {
        tokenId = newContainerTokenIdentifier(containerToken);
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
      StartContainerResponse response = 
          recordFactory.newRecordInstance(StartContainerResponse.class);
      status = recordFactory.newRecordInstance(ContainerStatus.class);
      status.setState(ContainerState.RUNNING);
      status.setContainerId(tokenId.getContainerID());
      status.setExitStatus(0);
      return response;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request) 
    throws YarnException {
      Exception e = new Exception(EXCEPTION_MSG, 
          new Exception(EXCEPTION_CAUSE));
      throw new YarnException(e);
    }
  }

  public static ContainerTokenIdentifier newContainerTokenIdentifier(
      Token containerToken) throws IOException {
    org.apache.hadoop.security.token.Token<ContainerTokenIdentifier> token =
        new org.apache.hadoop.security.token.Token<ContainerTokenIdentifier>(
            containerToken.getIdentifier()
                .array(), containerToken.getPassword().array(), new Text(
                containerToken.getKind()),
            new Text(containerToken.getService()));
    return token.decodeIdentifier();
  }

  public static Token newContainerToken(NodeId nodeId, byte[] password,
      ContainerTokenIdentifier tokenIdentifier) {
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr =
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    // NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
    Token containerToken =
        Token.newInstance(tokenIdentifier.getBytes(),
          ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }
}
