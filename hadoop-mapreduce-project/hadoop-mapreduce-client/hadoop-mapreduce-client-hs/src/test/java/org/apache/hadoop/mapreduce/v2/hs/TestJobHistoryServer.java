package org.apache.hadoop.mapreduce.v2.hs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestJobHistoryServer {

  @Test
  public void test1() throws Exception {

    JobHistoryServer server = new JobHistoryServer();
    Configuration cong = new Configuration();
    server.init(cong);
    assertEquals(STATE.INITED, server.getServiceState());
    assertEquals(3, server.getServices().size());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    assertNotNull(server.getClientService());
    HistoryClientService historyService= server.getClientService();
    assertNotNull(historyService.getClientHandler().getConnectAddress());
   
  }
  @Test
  public void testMainMethod() throws Exception {

    ExitUtil.disableSystemExit();
    try {
      JobHistoryServer.main(new String[0]);
      
    } catch (ExitUtil.ExitException e) {
      ExitUtil.resetFirstExitException();
      fail();
    }
  }
}
