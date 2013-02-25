package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.impl.pb.client.ClientRMProtocolPBClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import static org.junit.Assert.*;

/**
 * Test RMAdminProtocolPBClientImpl. Test a methods and the proxy without logic.
 * 
 */
public class TestRMAdminProtocolPBClientImpl {
  private static final Log LOG = LogFactory
      .getLog(TestRMAdminProtocolPBClientImpl.class);
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private static ClientRMProtocol client;

  private static int responce = 0;

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

    responce = 0;
    assertNotNull(client.cancelDelegationToken(request));
    responce = 1;
    try {
      client.cancelDelegationToken(request);
      fail("should be ServiceException ");
    } catch (YarnRemoteException e) {
      assertEquals("YarnRemoteException test message", e.getMessage());
    }
    responce = 2;
    try {
      client.cancelDelegationToken(request);
      fail("should be UndeclaredThrowableException ");
    } catch (UndeclaredThrowableException e) {
      
    }
    responce = 3;
    try {
      client.cancelDelegationToken(request);
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
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetApplicationReportResponseProto getApplicationReport(
        RpcController controller, GetApplicationReportRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public SubmitApplicationResponseProto submitApplication(
        RpcController controller, SubmitApplicationRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public KillApplicationResponseProto forceKillApplication(
        RpcController controller, KillApplicationRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetClusterMetricsResponseProto getClusterMetrics(
        RpcController controller, GetClusterMetricsRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetAllApplicationsResponseProto getAllApplications(
        RpcController controller, GetAllApplicationsRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetClusterNodesResponseProto getClusterNodes(
        RpcController controller, GetClusterNodesRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetQueueInfoResponseProto getQueueInfo(RpcController controller,
        GetQueueInfoRequestProto request) throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetQueueUserAclsInfoResponseProto getQueueUserAcls(
        RpcController controller, GetQueueUserAclsInfoRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetDelegationTokenResponseProto getDelegationToken(
        RpcController controller, GetDelegationTokenRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RenewDelegationTokenResponseProto renewDelegationToken(
        RpcController controller, RenewDelegationTokenRequestProto request)
        throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public CancelDelegationTokenResponseProto cancelDelegationToken(
        RpcController controller, CancelDelegationTokenRequestProto request)
        throws ServiceException {
      if (responce == 0) {
        return CancelDelegationTokenResponseProto.getDefaultInstance();
      } else if (responce == 1) {
        throw new ServiceException(
            new YarnRemoteExceptionPBImpl("YarnRemoteException test message"));
      } else if (responce == 2) {
        throw new ServiceException(new UndeclaredThrowableException(
            new Throwable()));

      }
      throw new ServiceException(new Throwable());
    }
  }

}
