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
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.junit.Test;

public class TestRPCFactories {
  
  
  
  @Test
  public void test() {
    testPbServerFactory();
    
    testPbClientFactory();
  }
  
  
  
  private void testPbServerFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    Configuration conf = new Configuration();
    AMRMProtocol instance = new AMRMProtocolTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            AMRMProtocol.class, instance, addr, conf, null, 1);
      server.start();
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  
  private void testPbClientFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    System.err.println(addr.getHostName() + addr.getPort());
    Configuration conf = new Configuration();
    AMRMProtocol instance = new AMRMProtocolTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            AMRMProtocol.class, instance, addr, conf, null, 1);
      server.start();
      System.err.println(server.getListenerAddress());
      System.err.println(NetUtils.getConnectAddress(server));

      AMRMProtocol amrmClient = null;
      try {
        amrmClient = (AMRMProtocol) RpcClientFactoryPBImpl.get().getClient(AMRMProtocol.class, 1, NetUtils.getConnectAddress(server), conf);
      } catch (YarnRuntimeException e) {
        e.printStackTrace();
        Assert.fail("Failed to create client");
      }
      
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      if (server != null) {
        server.stop();
      }
    }     
  }

  public class AMRMProtocolTestImpl implements AMRMProtocol {

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnException,
        IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnException,
        IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnException, IOException {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
