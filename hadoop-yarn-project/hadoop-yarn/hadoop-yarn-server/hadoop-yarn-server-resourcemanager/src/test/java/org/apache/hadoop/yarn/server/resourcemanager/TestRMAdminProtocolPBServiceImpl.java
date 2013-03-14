package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
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
import org.apache.hadoop.yarn.server.resourcemanager.api.RMAdminProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.api.impl.pb.service.RMAdminProtocolPBServiceImpl;
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
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ServiceException;

public class TestRMAdminProtocolPBServiceImpl {
  private static RMAdminProtocolPBServiceImpl service;
  private static int resultFlag = 0;

  @BeforeClass
  public static void setUpResourceManager() throws Exception {

    service = new RMAdminProtocolPBServiceImpl(new RMAdminProtocolStub());

  }

  @Test(timeout = 500)
  public void testRefreshQueuesMethod() throws Exception {
    RefreshQueuesRequestProto request = RefreshQueuesRequestProto
        .getDefaultInstance();
    resultFlag = 0;
    RefreshQueuesResponseProto response = service.refreshQueues(null, request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      service.refreshQueues(null, request);
      fail();
    } catch (ServiceException e) {
      assertEquals("org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl: Yarn Exception", e.getMessage());
    }
  }

  @Test(timeout = 500)
  public void testRefreshNodesMethod() throws Exception {
    RefreshNodesRequestProto request = RefreshNodesRequestProto
        .getDefaultInstance();
    resultFlag = 0;
    RefreshNodesResponseProto response = service.refreshNodes(null, request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      service.refreshNodes(null, request);
      fail();
    } catch (ServiceException e) {
      assertEquals("org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl: Yarn Exception", e.getMessage());
    }
  }

  @Test(timeout = 500)
  public void testRefreshSuperUserGroupsConfigurationMethod() throws Exception {
    RefreshSuperUserGroupsConfigurationRequestProto request = RefreshSuperUserGroupsConfigurationRequestProto
        .getDefaultInstance();
    resultFlag = 0;
    RefreshSuperUserGroupsConfigurationResponseProto response = service
        .refreshSuperUserGroupsConfiguration(null, request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      service.refreshSuperUserGroupsConfiguration(null, request);
      fail();
    } catch (ServiceException e) {
      assertEquals("org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl: Yarn Exception", e.getMessage());
    }
  }

  
  @Test(timeout = 500)
  public void testRefreshUserToGroupsMappingsMethod() throws Exception {
    RefreshUserToGroupsMappingsRequestProto request = RefreshUserToGroupsMappingsRequestProto
        .getDefaultInstance();
    resultFlag = 0;
    RefreshUserToGroupsMappingsResponseProto response = service
        .refreshUserToGroupsMappings(null, request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      service.refreshUserToGroupsMappings(null, request);
      fail();
    } catch (ServiceException e) {
      assertEquals("org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl: Yarn Exception", e.getMessage());
    }
  }

  @Test(timeout = 500)
  public void testRefreshAdminAclsMethod() throws Exception {
    RefreshAdminAclsRequestProto request = RefreshAdminAclsRequestProto
        .getDefaultInstance();
    resultFlag = 0;
    RefreshAdminAclsResponseProto response = service
        .refreshAdminAcls(null, request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      service.refreshAdminAcls(null, request);
      fail();
    } catch (ServiceException e) {
      assertEquals("org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl: Yarn Exception", e.getMessage());
    }
  }

  @Test(timeout = 500)
  public void testRefreshServiceAclsMethod() throws Exception {
    RefreshServiceAclsRequestProto request = RefreshServiceAclsRequestProto
        .getDefaultInstance();
    resultFlag = 0;
    RefreshServiceAclsResponseProto response = service
        .refreshServiceAcls(null, request);
    assertNotNull(response);
    resultFlag = 1;
    try {
      service.refreshServiceAcls(null, request);
      fail();
    } catch (ServiceException e) {
      assertEquals("org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl: Yarn Exception", e.getMessage());
    }
  }
  
  
  private static class RMAdminProtocolStub implements RMAdminProtocol {

    @Override
    public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
        throws YarnRemoteException {
      if (resultFlag == 0) {
        return new RefreshQueuesResponsePBImpl();
      }
      throw new YarnRemoteExceptionPBImpl("Yarn Exception");
    }

    @Override
    public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
        throws YarnRemoteException {
      if (resultFlag == 0) {
        return new RefreshNodesResponsePBImpl();
      }
      throw new YarnRemoteExceptionPBImpl("Yarn Exception");
    }

    @Override
    public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
        RefreshSuperUserGroupsConfigurationRequest request)
        throws YarnRemoteException {
      if (resultFlag == 0) {
        return new RefreshSuperUserGroupsConfigurationResponsePBImpl();
      }
      throw new YarnRemoteExceptionPBImpl("Yarn Exception");
    }

    @Override
    public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
        RefreshUserToGroupsMappingsRequest request) throws YarnRemoteException {
      if (resultFlag == 0) {
        return new RefreshUserToGroupsMappingsResponsePBImpl();
      }
      throw new YarnRemoteExceptionPBImpl("Yarn Exception");
    }

    @Override
    public RefreshAdminAclsResponse refreshAdminAcls(
        RefreshAdminAclsRequest request) throws YarnRemoteException {
      if (resultFlag == 0) {
        return new RefreshAdminAclsResponsePBImpl();
      }
      throw new YarnRemoteExceptionPBImpl("Yarn Exception");
    }

    @Override
    public RefreshServiceAclsResponse refreshServiceAcls(
        RefreshServiceAclsRequest request) throws YarnRemoteException {
      if (resultFlag == 0) {
        return new RefreshServiceAclsResponsePBImpl();
      }
      throw new YarnRemoteExceptionPBImpl("Yarn Exception");
    }

  }
}
