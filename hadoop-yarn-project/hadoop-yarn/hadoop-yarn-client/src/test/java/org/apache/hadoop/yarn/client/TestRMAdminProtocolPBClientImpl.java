package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.impl.pb.client.ClientRMProtocolPBClientImpl;
import org.apache.hadoop.yarn.api.impl.pb.service.ClientRMProtocolPBServiceImpl;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.ClientRMProtocol.ClientRMProtocolService;
import org.apache.hadoop.yarn.proto.ClientRMProtocol.ClientRMProtocolService.BlockingInterface;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllApplicationsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationResponseProto;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import static org.junit.Assert.*;

/**
 * Test RMAdminProtocolPBClientImpl. Test a methods and the proxy without logic.
 * 
 */
public class TestRMAdminProtocolPBClientImpl {
  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private static ClientRMProtocol client;

  private static int resultFlag = 0;

  /**
   * Start resource manager server
   */

  @BeforeClass
  public static void setUpResourceManager() throws Exception,
      InterruptedException {

    Configuration.addDefaultResource("config-with-security.xml");
    Configuration conf = new Configuration();
    client = new ClientRMProtocolPBClientImpl(1L, getProtocolAddress(conf),
        conf);
    // change proxy
    Field field = client.getClass().getDeclaredField("proxy");
    field.setAccessible(true);
    ProtocolServiceStub stub = new ProtocolServiceStub();
    field.set(client, stub);
  }

  @Test(timeout = 100)
  public void testCancelDelegationToken() throws YarnRemoteException {
    CancelDelegationTokenRequest request = recordFactory
        .newRecordInstance(CancelDelegationTokenRequest.class);
    request.setDelegationToken(getDelegationToken());

    resultFlag = 0;
    assertNotNull(client.cancelDelegationToken(request));
    resultFlag = 1;
    try {
      client.cancelDelegationToken(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.cancelDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.cancelDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testRenewDelegationToken() throws YarnRemoteException {
    RenewDelegationTokenRequest request = recordFactory
        .newRecordInstance(RenewDelegationTokenRequest.class);
    request.setDelegationToken(getDelegationToken());

    resultFlag = 0;
    assertNotNull(client.renewDelegationToken(request));
    resultFlag = 1;
    try {
      client.renewDelegationToken(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.renewDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.renewDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetDelegationToken() throws YarnRemoteException {
    GetDelegationTokenRequest request = recordFactory
        .newRecordInstance(GetDelegationTokenRequest.class);

    resultFlag = 0;
    assertNotNull(client.getDelegationToken(request));
    resultFlag = 1;
    try {
      client.getDelegationToken(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetQueueUserAcls() throws YarnRemoteException {
    GetQueueUserAclsInfoRequest request = recordFactory
        .newRecordInstance(GetQueueUserAclsInfoRequest.class);

    resultFlag = 0;
    assertNotNull(client.getQueueUserAcls(request));
    resultFlag = 1;
    try {
      client.getQueueUserAcls(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getQueueUserAcls(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getQueueUserAcls(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetQueueInfo() throws YarnRemoteException {
    GetQueueInfoRequest request = recordFactory
        .newRecordInstance(GetQueueInfoRequest.class);

    resultFlag = 0;
    assertNotNull(client.getQueueInfo(request));
    resultFlag = 1;
    try {
      client.getQueueInfo(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getQueueInfo(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getQueueInfo(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetClusterNodes() throws YarnRemoteException {
    GetClusterNodesRequest request = recordFactory
        .newRecordInstance(GetClusterNodesRequest.class);

    resultFlag = 0;
    assertNotNull(client.getClusterNodes(request));
    resultFlag = 1;
    try {
      client.getClusterNodes(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getClusterNodes(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getClusterNodes(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetAllApplications() throws YarnRemoteException {
    GetAllApplicationsRequest request = recordFactory
        .newRecordInstance(GetAllApplicationsRequest.class);

    resultFlag = 0;
    assertNotNull(client.getAllApplications(request));
    resultFlag = 1;
    try {
      client.getAllApplications(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getAllApplications(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getAllApplications(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testSubmitApplication() throws YarnRemoteException {
    SubmitApplicationRequest request = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);

    resultFlag = 0;
    assertNotNull(client.submitApplication(request));
    resultFlag = 1;
    try {
      client.submitApplication(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.submitApplication(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.submitApplication(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetNewApplication() throws YarnRemoteException {
    GetNewApplicationRequest request = recordFactory
        .newRecordInstance(GetNewApplicationRequest.class);

    resultFlag = 0;
    assertNotNull(client.getNewApplication(request));
    resultFlag = 1;
    try {
      client.getNewApplication(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getNewApplication(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getNewApplication(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetClusterMetrics() throws YarnRemoteException {
    GetClusterMetricsRequest request = recordFactory
        .newRecordInstance(GetClusterMetricsRequest.class);

    resultFlag = 0;
    assertNotNull(client.getClusterMetrics(request));
    resultFlag = 1;
    try {
      client.getClusterMetrics(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getClusterMetrics(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getClusterMetrics(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testGetApplicationReport() throws YarnRemoteException {
    GetApplicationReportRequest request = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);

    resultFlag = 0;
    assertNotNull(client.getApplicationReport(request));
    resultFlag = 1;
    try {
      client.getApplicationReport(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.getApplicationReport(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.getApplicationReport(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testForceKillApplication() throws YarnRemoteException {
    KillApplicationRequest request = recordFactory
        .newRecordInstance(KillApplicationRequest.class);

    resultFlag = 0;
    assertNotNull(client.forceKillApplication(request));
    resultFlag = 1;
    try {
      client.forceKillApplication(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    resultFlag = 2;
    try {
      client.forceKillApplication(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
    resultFlag = 3;
    try {
      client.forceKillApplication(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {

    }
  }

  @Test(timeout = 100)
  public void testApplicationConstants() {
    ApplicationConstants.Environment env = ApplicationConstants.Environment.PATH;
    assertEquals("PATH", env.key());
  }

  private static InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return conf.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
  }

  private DelegationToken getDelegationToken() {
    DelegationToken token = recordFactory
        .newRecordInstance(DelegationToken.class);
    token.setKind("");
    token.setService("");
    token.setIdentifier(ByteBuffer.allocate(0));
    token.setPassword(ByteBuffer.allocate(0));
    return token;
  }

  private static class ClientRMProtocolImpl implements ClientRMProtocol {

    @Override
    public GetNewApplicationResponse getNewApplication(
        GetNewApplicationRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(GetNewApplicationResponse.class);
    }

    @Override
    public SubmitApplicationResponse submitApplication(
        SubmitApplicationRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(SubmitApplicationResponse.class);
    }

    @Override
    public KillApplicationResponse forceKillApplication(
        KillApplicationRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(KillApplicationResponse.class);
    }

    @Override
    public GetApplicationReportResponse getApplicationReport(
        GetApplicationReportRequest request) throws YarnRemoteException {
      return recordFactory
          .newRecordInstance(GetApplicationReportResponse.class);
    }

    @Override
    public GetClusterMetricsResponse getClusterMetrics(
        GetClusterMetricsRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(GetClusterMetricsResponse.class);
    }

    @Override
    public GetAllApplicationsResponse getAllApplications(
        GetAllApplicationsRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(GetAllApplicationsResponse.class);
    }

    @Override
    public GetClusterNodesResponse getClusterNodes(
        GetClusterNodesRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(GetClusterNodesResponse.class);
    }

    @Override
    public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
        throws YarnRemoteException {
      return recordFactory.newRecordInstance(GetQueueInfoResponse.class);
    }

    @Override
    public GetQueueUserAclsInfoResponse getQueueUserAcls(
        GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
      return recordFactory
          .newRecordInstance(GetQueueUserAclsInfoResponse.class);
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws YarnRemoteException {
      return recordFactory.newRecordInstance(GetDelegationTokenResponse.class);
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
        RenewDelegationTokenRequest request) throws YarnRemoteException {
      RenewDelegationTokenResponse result = recordFactory
          .newRecordInstance(RenewDelegationTokenResponse.class);
      result.setNextExpirationTime(System.currentTimeMillis());
      return result;
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
        CancelDelegationTokenRequest request) throws YarnRemoteException {
      return recordFactory
          .newRecordInstance(CancelDelegationTokenResponse.class);
    }

  }

  private static class ProtocolServiceStub implements
      ClientRMProtocolService.BlockingInterface {
    private BlockingInterface client = new ClientRMProtocolPBServiceImpl(
        new ClientRMProtocolImpl());

    public ProtocolServiceStub() {

    }

    @Override
    public GetNewApplicationResponseProto getNewApplication(
        RpcController controller, GetNewApplicationRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getNewApplication(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public GetApplicationReportResponseProto getApplicationReport(
        RpcController controller, GetApplicationReportRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getApplicationReport(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public SubmitApplicationResponseProto submitApplication(
        RpcController controller, SubmitApplicationRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.submitApplication(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public KillApplicationResponseProto forceKillApplication(
        RpcController controller, KillApplicationRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.forceKillApplication(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public GetClusterMetricsResponseProto getClusterMetrics(
        RpcController controller, GetClusterMetricsRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getClusterMetrics(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public GetAllApplicationsResponseProto getAllApplications(
        RpcController controller, GetAllApplicationsRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getAllApplications(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public GetClusterNodesResponseProto getClusterNodes(
        RpcController controller, GetClusterNodesRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getClusterNodes(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public GetQueueInfoResponseProto getQueueInfo(RpcController controller,
        GetQueueInfoRequestProto request) throws ServiceException {
      if (resultFlag == 0) {
        return client.getQueueInfo(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public GetQueueUserAclsInfoResponseProto getQueueUserAcls(
        RpcController controller, GetQueueUserAclsInfoRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getQueueUserAcls(null, request);
      }
      processMessage();
      return null;
    }

    @Override
    public GetDelegationTokenResponseProto getDelegationToken(
        RpcController controller, GetDelegationTokenRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.getDelegationToken(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public RenewDelegationTokenResponseProto renewDelegationToken(
        RpcController controller, RenewDelegationTokenRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.renewDelegationToken(null, request);
      }
      processMessage();
      return null;

    }

    @Override
    public CancelDelegationTokenResponseProto cancelDelegationToken(
        RpcController controller, CancelDelegationTokenRequestProto request)
        throws ServiceException {
      if (resultFlag == 0) {
        return client.cancelDelegationToken(null, request);
      }
      processMessage();
      return null;
    }

    private void processMessage() throws ServiceException {
      if (resultFlag == 1) {
        throw new ServiceException(new YarnRemoteExceptionPBImpl(
            "YarnRemoteException test message"));
      } else if (resultFlag == 2) {
        throw new ServiceException(new UndeclaredThrowableException(
            new Throwable()));

      }
      throw new ServiceException(new Throwable());
    }
  }

}
