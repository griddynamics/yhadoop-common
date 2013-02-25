package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.impl.pb.client.ClientRMProtocolPBClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.ClientRMProtocol.ClientRMProtocolService;
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

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import static org.junit.Assert.*;

/**
 * Test RMAdminProtocolPBClientImpl. Test a methods and the proxy without logic.
 * 
 */
public class TestRMAdminProtocolPBClientImpl {
  private final RecordFactory recordFactory = RecordFactoryProvider
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  private static class ProtocolServiceStub implements
      ClientRMProtocolService.BlockingInterface {

    public ProtocolServiceStub() {

    }

    @Override
    public GetNewApplicationResponseProto getNewApplication(
        RpcController controller, GetNewApplicationRequestProto request)
        throws ServiceException {
      return (GetNewApplicationResponseProto) processMessage(GetNewApplicationResponseProto.getDefaultInstance());

    }

    @Override
    public GetApplicationReportResponseProto getApplicationReport(
        RpcController controller, GetApplicationReportRequestProto request)
        throws ServiceException {
      return (GetApplicationReportResponseProto) processMessage(GetApplicationReportResponseProto.getDefaultInstance());

    }

    @Override
    public SubmitApplicationResponseProto submitApplication(
        RpcController controller, SubmitApplicationRequestProto request)
        throws ServiceException {
      return (SubmitApplicationResponseProto) processMessage(SubmitApplicationResponseProto.getDefaultInstance());

    }

    @Override
    public KillApplicationResponseProto forceKillApplication(
        RpcController controller, KillApplicationRequestProto request)
        throws ServiceException {
      return (KillApplicationResponseProto) processMessage(KillApplicationResponseProto.getDefaultInstance());

    }

    @Override
    public GetClusterMetricsResponseProto getClusterMetrics(
        RpcController controller, GetClusterMetricsRequestProto request)
        throws ServiceException {
      return (GetClusterMetricsResponseProto) processMessage(GetClusterMetricsResponseProto.getDefaultInstance());

    }

    @Override
    public GetAllApplicationsResponseProto getAllApplications(
        RpcController controller, GetAllApplicationsRequestProto request)
        throws ServiceException {
      return (GetAllApplicationsResponseProto) processMessage(GetAllApplicationsResponseProto.getDefaultInstance());

    }

    @Override
    public GetClusterNodesResponseProto getClusterNodes(
        RpcController controller, GetClusterNodesRequestProto request)
        throws ServiceException {
      return (GetClusterNodesResponseProto) processMessage(GetClusterNodesResponseProto.getDefaultInstance());

    }

    @Override
    public GetQueueInfoResponseProto getQueueInfo(RpcController controller,
        GetQueueInfoRequestProto request) throws ServiceException {
      return (GetQueueInfoResponseProto) processMessage(GetQueueInfoResponseProto.getDefaultInstance());

    }

    @Override
    public GetQueueUserAclsInfoResponseProto getQueueUserAcls(
        RpcController controller, GetQueueUserAclsInfoRequestProto request)
        throws ServiceException {
      return (GetQueueUserAclsInfoResponseProto) processMessage(GetQueueUserAclsInfoResponseProto.getDefaultInstance());

    }

    @Override
    public GetDelegationTokenResponseProto getDelegationToken(
        RpcController controller, GetDelegationTokenRequestProto request)
        throws ServiceException {
      return (GetDelegationTokenResponseProto) processMessage(GetDelegationTokenResponseProto.getDefaultInstance());

    }

    @Override
    public RenewDelegationTokenResponseProto renewDelegationToken(
        RpcController controller, RenewDelegationTokenRequestProto request)
        throws ServiceException {
      return (RenewDelegationTokenResponseProto) processMessage(RenewDelegationTokenResponseProto.getDefaultInstance());


    }

    @Override
    public CancelDelegationTokenResponseProto cancelDelegationToken(
        RpcController controller, CancelDelegationTokenRequestProto request)
        throws ServiceException {
        return (CancelDelegationTokenResponseProto) processMessage(CancelDelegationTokenResponseProto.getDefaultInstance());
    
    }
    
    private GeneratedMessage processMessage(GeneratedMessage result) throws ServiceException{
      if (resultFlag == 0) {
        return result;
      } else if (resultFlag == 1) {
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
