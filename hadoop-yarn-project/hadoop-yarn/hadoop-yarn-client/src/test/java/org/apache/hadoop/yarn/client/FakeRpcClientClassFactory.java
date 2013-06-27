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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsResponsePBImpl;

import com.sun.jersey.api.container.ContainerException;

/**
 * Class for provide a fake implementation RMAdminProtocol
 */
public class FakeRpcClientClassFactory {

  public static RpcClientFactory get() {
    return new FakeRpcClientFactory();
  }

  private static class FakeRpcClientFactory implements RpcClientFactory {

    @Override
    public Object getClient(Class<?> protocol, long clientVersion,
        InetSocketAddress addr, Configuration conf)  {

      return new FakeRMAdminProtocol();

    }

    @Override
    public void stopClient(Object proxy) {

    }

  }

  public static class FakeRMAdminProtocol implements ResourceManagerAdministrationProtocol {
    public enum FunctionCall {
      refreshQueues, refreshNodes, refreshSuperUserGroupsConfiguration, 
      refreshUserToGroupsMappings,refreshAdminAcls, refreshServiceAcls,
      getGroupsForUser
    };

    public enum ResultCode {
      OK, RemoteException
    };

    // indicator called function
    public static FunctionCall functionCall;
    public static ResultCode resultCode = ResultCode.OK;

    @Override
    public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
        throws YarnException, IOException  {
      functionCall = FunctionCall.refreshQueues;
      if (ResultCode.OK.equals(resultCode)) {
        return new RefreshQueuesResponsePBImpl();
      } else {
        throw new ContainerException("test exception");
      }
    }

    @Override
    public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
        throws YarnException, IOException {
      functionCall = FunctionCall.refreshNodes;

      return new RefreshNodesResponsePBImpl();
    }

    @Override
    public RefreshSuperUserGroupsConfigurationResponse 
    refreshSuperUserGroupsConfiguration(
        RefreshSuperUserGroupsConfigurationRequest request)
            throws YarnException, IOException {
      functionCall = FunctionCall.refreshSuperUserGroupsConfiguration;
      return new RefreshSuperUserGroupsConfigurationResponsePBImpl();
    }

    @Override
    public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
        RefreshUserToGroupsMappingsRequest request) 
            throws YarnException, IOException {
      functionCall = FunctionCall.refreshUserToGroupsMappings;

      return new RefreshUserToGroupsMappingsResponsePBImpl();
    }

    @Override
    public RefreshAdminAclsResponse refreshAdminAcls(
        RefreshAdminAclsRequest request) throws YarnException, IOException {
      functionCall = FunctionCall.refreshAdminAcls;

      return new RefreshAdminAclsResponsePBImpl();
    }

    @Override
    public RefreshServiceAclsResponse refreshServiceAcls(
        RefreshServiceAclsRequest request) throws YarnException, IOException {
      functionCall = FunctionCall.refreshServiceAcls;

      return new RefreshServiceAclsResponsePBImpl();
    }

    @Override
    public String[] getGroupsForUser(String user) throws IOException {
      functionCall = FunctionCall.getGroupsForUser;

      return new String[] {"admin"};
    }

  }
}