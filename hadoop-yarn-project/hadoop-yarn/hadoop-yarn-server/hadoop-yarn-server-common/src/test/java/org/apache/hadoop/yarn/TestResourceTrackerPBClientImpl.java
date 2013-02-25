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


import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test ResourceTrackerPBClientImpl
 */
public class TestResourceTrackerPBClientImpl {
  
  
  private static  ResourceTracker client ;
  private static Server server ;
  private final static org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  @BeforeClass
  public static  void start(){
    InetSocketAddress addr = new InetSocketAddress(0);
    Configuration conf = new Configuration();
    ResourceTracker instance = new ResourceTrackerTestImpl();
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            ResourceTracker.class, instance, addr, conf, null, 1);
      server.start();
      System.err.println(server.getListenerAddress());
      System.err.println(NetUtils.getConnectAddress(server));

      try {
        client = (ResourceTracker) RpcClientFactoryPBImpl.get().getClient(ResourceTracker.class, 1, NetUtils.getConnectAddress(server), conf);
      } catch (YarnException e) {
        e.printStackTrace();
        Assert.fail("Failed to create client");
      }
      
  }
  @AfterClass
  public static void stop(){
    if(server!=null){
      server.stop();
    }
  }
  @Test
  public void testResourceTrackerPBClientImpl() throws Exception {
    RegisterNodeManagerRequest request=recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    assertNotNull(client.registerNodeManager(request));
  }
  @Test
  public void testNodeHeartbeat() throws Exception {
    NodeHeartbeatRequest request=recordFactory.newRecordInstance(NodeHeartbeatRequest.class);
    assertNotNull(client.nodeHeartbeat(request));
  }
  
  public static class ResourceTrackerTestImpl implements ResourceTracker {

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      
      return recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    }
    
  }
}
