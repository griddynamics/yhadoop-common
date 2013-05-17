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

package org.apache.hadoop.yarn.server.resourcemanager.tools;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.server.resourcemanager.api.RMAdminProtocol;
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
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshServiceAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsResponsePBImpl;

/**
 * Class for provide a fake implementation RMAdminProtocol
 */
public class FakeRpcClientClassFactory{
  
  public static  RpcClientFactory get(){
    return new FakeRpcClientFactory();
  }
  
  private static class FakeRpcClientFactory implements RpcClientFactory{

    @Override
    public Object getClient(Class<?> protocol, long clientVersion,
        InetSocketAddress addr, Configuration conf) throws YarnException {
      
      return new FakeRMAdminProtocol();
      
    }
    @Override
    public void stopClient(Object proxy) {
      
    }
    
  }
  
  public static class FakeRMAdminProtocol implements RMAdminProtocol{
    
    // indicator called function 
    public static int parameter=0;
    @Override
    public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
        throws YarnRemoteException {
      parameter=1;
      return new RefreshQueuesResponsePBImpl();
    }

    @Override
    public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
        throws YarnRemoteException {
      parameter=2;

      return new RefreshNodesResponsePBImpl();
    }

    @Override
    public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
        RefreshSuperUserGroupsConfigurationRequest request)
        throws YarnRemoteException {
      parameter=3;
      return new RefreshSuperUserGroupsConfigurationResponsePBImpl();
    }

    @Override
    public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
        RefreshUserToGroupsMappingsRequest request) throws YarnRemoteException {
      parameter=4;

      return new RefreshUserToGroupsMappingsResponsePBImpl();
    }

    @Override
    public RefreshAdminAclsResponse refreshAdminAcls(
        RefreshAdminAclsRequest request) throws YarnRemoteException {
      parameter=5;

      return new RefreshAdminAclsResponsePBImpl();
    }

    @Override
    public RefreshServiceAclsResponse refreshServiceAcls(
        RefreshServiceAclsRequest request) throws YarnRemoteException {
      parameter=6;

      return new RefreshServiceAclsResponsePBImpl();
    }
    
  }
}