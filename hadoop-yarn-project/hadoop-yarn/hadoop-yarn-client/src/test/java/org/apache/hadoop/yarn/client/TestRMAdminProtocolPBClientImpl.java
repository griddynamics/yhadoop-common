package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.impl.pb.client.RMAdminProtocolPBClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestRMAdminProtocolPBClientImpl {
  private static ResourceManager resourceManager;
  private static final Log LOG = LogFactory
      .getLog(TestRMAdminProtocolPBClientImpl.class);
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private static Configuration conf;
  private static RMAdminProtocolPBClientImpl client;

  @BeforeClass
  public static void setUpResourceManager() throws IOException,
      InterruptedException {
    conf = new YarnConfiguration();
    resourceManager = new ResourceManager() {
      @Override
      protected void doSecureLogin() throws IOException {
      };
    };
    resourceManager.init(conf);
    new Thread() {
      public void run() {
        resourceManager.start();
      };
    }.start();
    int waitCount = 0;
    while (resourceManager.getServiceState() == STATE.INITED
        && waitCount++ < 10) {
      LOG.info("Waiting for RM to start...");
      Thread.sleep(1000);
    }
    if (resourceManager.getServiceState() != STATE.STARTED) {
      throw new IOException("ResourceManager failed to start. Final state is "
          + resourceManager.getServiceState());
    }
    LOG.info("ResourceManager RMAdmin address: "
        + conf.get(YarnConfiguration.RM_ADMIN_ADDRESS));

    client = new RMAdminProtocolPBClientImpl(1L, getProtocolAddress(conf), conf);

  }

  @Test
  public void testRefreshQueues() throws Exception {

    RefreshQueuesRequest request = recordFactory
        .newRecordInstance(RefreshQueuesRequest.class);
    RefreshQueuesResponse responce = client.refreshQueues(request);
    assertNotNull(responce);
  }

  @Test
  public void testRefreshNodes() throws Exception {
    resourceManager.getClientRMService().
    RefreshNodesRequest request = recordFactory
        .newRecordInstance(RefreshNodesRequest.class);
    RefreshNodesResponse responce = client.refreshNodes(request);
    assertNotNull(responce);
  }

  @Test
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {

    RefreshSuperUserGroupsConfigurationRequest request = recordFactory
        .newRecordInstance(RefreshSuperUserGroupsConfigurationRequest.class);
    RefreshSuperUserGroupsConfigurationResponse responce = client
        .refreshSuperUserGroupsConfiguration(request);
    assertNotNull(responce);
  }

  @Test
  public void testRefreshUserToGroupsMappings() throws Exception {
    RefreshUserToGroupsMappingsRequest request = recordFactory
        .newRecordInstance(RefreshUserToGroupsMappingsRequest.class);
    RefreshUserToGroupsMappingsResponse responce = client
        .refreshUserToGroupsMappings(request);
    assertNotNull(responce);
  }

  @Test
  public void testRefreshAdminAcls() throws Exception {
    RefreshAdminAclsRequest request = recordFactory
        .newRecordInstance(RefreshAdminAclsRequest.class);
    RefreshAdminAclsResponse responce = client.refreshAdminAcls(request);
    assertNotNull(responce);
  }

  @Test
  public void testRefreshServiceAcls() throws Exception {
    RefreshServiceAclsRequest request = recordFactory
        .newRecordInstance(RefreshServiceAclsRequest.class);
    RefreshServiceAclsResponse responce = client.refreshServiceAcls(request);
    assertNotNull(responce);
    
  }

  @AfterClass
  public static void tearDownResourceManager() throws InterruptedException {
    if (resourceManager != null) {
      LOG.info("Stopping ResourceManager...");
      resourceManager.stop();
    }
  }

  private static InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return conf.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
  }

}
