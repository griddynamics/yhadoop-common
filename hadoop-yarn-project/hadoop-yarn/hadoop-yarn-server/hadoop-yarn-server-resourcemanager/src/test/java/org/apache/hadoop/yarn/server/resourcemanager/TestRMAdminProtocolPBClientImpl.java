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
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.RMAdminProtocol.RMAdminProtocolService.BlockingInterface;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.yarn.server.resourcemanager.api.impl.pb.client.RMAdminProtocolPBClientImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class TestRMAdminProtocolPBClientImpl {

  private static RMAdminProtocolPBClientImpl client;
  private static int resultFlag = 0;
  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @BeforeClass
  public static void setUpResourceManager() throws Exception {

    Configuration configuration = new Configuration();
    client = new RMAdminProtocolPBClientImpl(1L,
        getProtocolAddress(configuration), configuration);
    // change proxy
    Field field = client.getClass().getDeclaredField("proxy");
    field.setAccessible(true);
    BlockingInterface testStub = new RMAdminProtocolServiceProxyStub();
    field.set(client, testStub);

  }

  @Test(timeout = 500)
  public void testRefreshQueuesMethod() throws Exception {
    RefreshQueuesRequest request = recordFactory
        .newRecordInstance(RefreshQueuesRequest.class);
    resultFlag = 0;

    RefreshQueuesResponse response = client.refreshQueues(request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      client.refreshQueues(request);
      fail();
    } catch (YarnRemoteException e) {
      assertEquals("Yarn exception", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.refreshQueues(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertEquals("Undeclared exception", e.getMessage());
    }
    resultFlag = 3;
    try {
      client.refreshQueues(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertNull(e.getMessage());
    }

  }

  @Test(timeout = 500)
  public void testRefreshNodes() throws Exception {
    RefreshNodesRequest request = recordFactory
        .newRecordInstance(RefreshNodesRequest.class);
    resultFlag = 0;

    RefreshNodesResponse response = client.refreshNodes(request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      client.refreshNodes(request);
      fail();
    } catch (YarnRemoteException e) {
      assertEquals("Yarn exception", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.refreshNodes(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertEquals("Undeclared exception", e.getMessage());
    }
    resultFlag = 3;
    try {
      client.refreshNodes(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertNull(e.getMessage());
    }

  }

  
  @Test(timeout = 500)
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    RefreshSuperUserGroupsConfigurationRequest request = recordFactory
        .newRecordInstance(RefreshSuperUserGroupsConfigurationRequest.class);
    
    resultFlag = 0;
    
    RefreshSuperUserGroupsConfigurationResponse response = client.refreshSuperUserGroupsConfiguration(request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      client.refreshSuperUserGroupsConfiguration(request);
      fail();
    } catch (YarnRemoteException e) {
      assertEquals("Yarn exception", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.refreshSuperUserGroupsConfiguration(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertEquals("Undeclared exception", e.getMessage());
    }
    resultFlag = 3;
    try {
      client.refreshSuperUserGroupsConfiguration(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertNull(e.getMessage());
    }
  }

  
  @Test(timeout = 500)
  public void testRefreshUserToGroupsMappings() throws Exception {
    RefreshUserToGroupsMappingsRequest request = recordFactory
        .newRecordInstance(RefreshUserToGroupsMappingsRequest.class);
    resultFlag = 0;

    RefreshUserToGroupsMappingsResponse response = client.refreshUserToGroupsMappings(request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      client.refreshUserToGroupsMappings(request);
      fail();
    } catch (YarnRemoteException e) {
      assertEquals("Yarn exception", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.refreshUserToGroupsMappings(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertEquals("Undeclared exception", e.getMessage());
    }
    resultFlag = 3;
    try {
      client.refreshUserToGroupsMappings(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertNull(e.getMessage());
    }
  }
  
  @Test(timeout = 500)
  public void testRefreshAdminAcls() throws Exception {
    RefreshAdminAclsRequest request = recordFactory
        .newRecordInstance(RefreshAdminAclsRequest.class);
    resultFlag = 0;

    RefreshAdminAclsResponse response = client.refreshAdminAcls(request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      client.refreshAdminAcls(request);
      fail();
    } catch (YarnRemoteException e) {
      assertEquals("Yarn exception", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.refreshAdminAcls(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertEquals("Undeclared exception", e.getMessage());
    }
    resultFlag = 3;
    try {
      client.refreshAdminAcls(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertNull(e.getMessage());
    }
  }

  
  
  @Test(timeout = 500)
  public void testRefreshServiceAcls() throws Exception {
    RefreshServiceAclsRequest request = recordFactory
        .newRecordInstance(RefreshServiceAclsRequest.class);
    resultFlag = 0;

    RefreshServiceAclsResponse response = client.refreshServiceAcls(request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      client.refreshServiceAcls(request);
      fail();
    } catch (YarnRemoteException e) {
      assertEquals("Yarn exception", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.refreshServiceAcls(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertEquals("Undeclared exception", e.getMessage());
    }
    resultFlag = 3;
    try {
      client.refreshServiceAcls(request);
      fail();
    } catch (UndeclaredThrowableException e) {
      assertNull(e.getMessage());
    }
  }

  
  private static class RMAdminProtocolServiceProxyStub
      implements
      org.apache.hadoop.yarn.proto.RMAdminProtocol.RMAdminProtocolService.BlockingInterface {

    @Override
    public RefreshQueuesResponseProto refreshQueues(RpcController controller,
        RefreshQueuesRequestProto request) throws ServiceException {
      if (resultFlag == 0) {
        return RefreshQueuesResponseProto.getDefaultInstance();
      } else if (resultFlag == 1) {
        YarnRemoteException e = new YarnRemoteExceptionPBImpl("Yarn exception");
        throw new ServiceException("Exception", e);
      } else if (resultFlag == 2) {
        UndeclaredThrowableException e = new UndeclaredThrowableException(null,
            "Undeclared exception");
        throw new ServiceException("Exception", e);
      }
      throw new ServiceException("Exception");
    }

    @Override
    public RefreshNodesResponseProto refreshNodes(RpcController controller,
        RefreshNodesRequestProto request) throws ServiceException {
      if (resultFlag == 0) {
        return RefreshNodesResponseProto.getDefaultInstance();
      } else if (resultFlag == 1) {
        YarnRemoteException e = new YarnRemoteExceptionPBImpl("Yarn exception");
        throw new ServiceException("Exception", e);
      } else if (resultFlag == 2) {
        UndeclaredThrowableException e = new UndeclaredThrowableException(null,
            "Undeclared exception");
        throw new ServiceException("Exception", e);
      }
      throw new ServiceException("Exception");
    }

    @Override
    public RefreshSuperUserGroupsConfigurationResponseProto refreshSuperUserGroupsConfiguration(
        RpcController controller,
        RefreshSuperUserGroupsConfigurationRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return RefreshSuperUserGroupsConfigurationResponseProto.getDefaultInstance();
      } else if (resultFlag == 1) {
        YarnRemoteException e = new YarnRemoteExceptionPBImpl("Yarn exception");
        throw new ServiceException("Exception", e);
      } else if (resultFlag == 2) {
        UndeclaredThrowableException e = new UndeclaredThrowableException(null,
            "Undeclared exception");
        throw new ServiceException("Exception", e);
      }
      throw new ServiceException("Exception");
    }

    @Override
    public RefreshUserToGroupsMappingsResponseProto refreshUserToGroupsMappings(
        RpcController controller,
        RefreshUserToGroupsMappingsRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return RefreshUserToGroupsMappingsResponseProto.getDefaultInstance();
      } else if (resultFlag == 1) {
        YarnRemoteException e = new YarnRemoteExceptionPBImpl("Yarn exception");
        throw new ServiceException("Exception", e);
      } else if (resultFlag == 2) {
        UndeclaredThrowableException e = new UndeclaredThrowableException(null,
            "Undeclared exception");
        throw new ServiceException("Exception", e);
      }
      throw new ServiceException("Exception");
    }

    @Override
    public RefreshAdminAclsResponseProto refreshAdminAcls(
        RpcController controller, RefreshAdminAclsRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return RefreshAdminAclsResponseProto.getDefaultInstance();
      } else if (resultFlag == 1) {
        YarnRemoteException e = new YarnRemoteExceptionPBImpl("Yarn exception");
        throw new ServiceException("Exception", e);
      } else if (resultFlag == 2) {
        UndeclaredThrowableException e = new UndeclaredThrowableException(null,
            "Undeclared exception");
        throw new ServiceException("Exception", e);
      }
      throw new ServiceException("Exception");
    }

    @Override
    public RefreshServiceAclsResponseProto refreshServiceAcls(
        RpcController controller, RefreshServiceAclsRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return RefreshServiceAclsResponseProto.getDefaultInstance();
      } else if (resultFlag == 1) {
        YarnRemoteException e = new YarnRemoteExceptionPBImpl("Yarn exception");
        throw new ServiceException("Exception", e);
      } else if (resultFlag == 2) {
        UndeclaredThrowableException e = new UndeclaredThrowableException(null,
            "Undeclared exception");
        throw new ServiceException("Exception", e);
      }
      throw new ServiceException("Exception");
    }

  }

  private static InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return conf.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
  }
}
