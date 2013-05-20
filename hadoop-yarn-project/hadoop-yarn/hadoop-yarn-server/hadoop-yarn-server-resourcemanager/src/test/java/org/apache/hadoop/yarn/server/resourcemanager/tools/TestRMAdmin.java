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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class TestRMAdmin
 */
public class TestRMAdmin {

  private static ResourceManager resourceManager;

  @BeforeClass
  public static void setUp() {

    Configuration conf = new YarnConfiguration();
    Store store = StoreFactory.getStore(conf);
    conf.set(YarnConfiguration.IPC_CLIENT_FACTORY,
        "org.apache.hadoop.yarn.server.resourcemanager" +
        ".tools.FakeRpcClientClassFactory");
    resourceManager = new ResourceManager(store);
    resourceManager.init(conf);
    resourceManager.start();

  }

  /**
   * Test method refreshQueues
   */
  @Test
  public void testRefreshQueues() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshQueues" };
    assertEquals(0, test.run(args));

    assertEquals(
        FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.
        refreshQueues,
        FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
  }

  /**
   * Test method refreshUserToGroupsMappings
   */
  @Test
  public void testRefreshUserToGroupsMappings() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshUserToGroupsMappings" };
    assertEquals(0, test.run(args));
    assertEquals(
        FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.
        refreshUserToGroupsMappings,
        FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
  }

  /**
   * Test method refreshSuperUserGroupsConfiguration
   */
  @Test
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshSuperUserGroupsConfiguration" };
    assertEquals(0, test.run(args));
    assertEquals(
        FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.
        refreshSuperUserGroupsConfiguration,
        FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
  }

  /**
   * Test method refreshAdminAcls
   */
  @Test
  public void testRefreshAdminAcls() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshAdminAcls" };
    assertEquals(0, test.run(args));
    assertEquals(
        FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.
        refreshAdminAcls,
        FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
  }

  /**
   * Test method refreshServiceAcl
   */
  @Test
  public void testRefreshServiceAcl() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshServiceAcl" };
    assertEquals(0, test.run(args));
    assertEquals(
        FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.
        refreshServiceAcls,
        FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
  }

  /**
   * Test method refreshNodes
   */
  @Test
  public void testRefreshNodes() throws Exception {

    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshNodes" };
    assertEquals(0, test.run(args));
    assertEquals(
        FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.refreshNodes,
        FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
  }

  /**
   * Test print help messages
   */
  @Test
  public void testHelp() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));
    try {
      String[] args = { "-help" };
      assertEquals(0, new RMAdmin().run(args));
      assertTrue(dataOut
          .toString()
          .contains(
              "rmadmin is the command to execute Map-Reduce" +
              " administrative commands."));
      assertTrue(dataOut
          .toString()
          .contains(
              "hadoop rmadmin [-refreshQueues] [-refreshNodes] " +
              "[-refreshSuperUserGroupsConfiguration] " +
              "[-refreshUserToGroupsMappings] [-refreshAdminAcls] " +
              "[-refreshServiceAcl] [-help [cmd]]"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshQueues: Reload the queues' acls, states and scheduler " +
              "specific properties."));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshNodes: Refresh the hosts information at the " +
              "ResourceManager."));
      assertTrue(dataOut.toString().contains(
          "-refreshUserToGroupsMappings: Refresh user-to-groups mappings"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy" +
              " groups mappings"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshAdminAcls: Refresh acls for administration of " +
              "ResourceManager"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshServiceAcl: Reload the service-level authorization" +
              " policy file"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-help [cmd]: \tDisplays help for the given command or all " +
              "commands if none"));

      testError(new String[] { "-help", "-refreshQueues" },
          "Usage: java RMAdmin [-refreshQueues]", dataErr, 0);
      testError(new String[] { "-help", "-refreshNodes" },
          "Usage: java RMAdmin [-refreshNodes]", dataErr, 0);
      testError(new String[] { "-help", "-refreshUserToGroupsMappings" },
          "Usage: java RMAdmin [-refreshUserToGroupsMappings]", dataErr, 0);
      testError(
          new String[] { "-help", "-refreshSuperUserGroupsConfiguration" },
          "Usage: java RMAdmin [-refreshSuperUserGroupsConfiguration]",
          dataErr, 0);
      testError(new String[] { "-help", "-refreshAdminAcls" },
          "Usage: java RMAdmin [-refreshAdminAcls]", dataErr, 0);
      testError(new String[] { "-help", "-refreshServiceAcl" },
          "Usage: java RMAdmin [-refreshServiceAcl]", dataErr, 0);
      testError(new String[] { "-help", "-badParameter" },
          "Usage: java RMAdmin", dataErr, 0);
      testError(new String[] { "-badParameter" },
          "badParameter: Unknown command", dataErr, -1);

    } finally {
      System.setOut(oldOutPrintStream);
      System.setErr(oldErrPrintStream);

    }

  }

  /**
   * Test exceptions
   */
  @Test
  public void testException() throws Exception {
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(dataErr));
    try {

      FakeRpcClientClassFactory.FakeRMAdminProtocol.resultCode = 
          FakeRpcClientClassFactory.FakeRMAdminProtocol.ResultCode.
          RemoteException;
      String[] args = { "-refreshQueues" };
      RMAdmin test = new RMAdmin();
      Configuration configuration = resourceManager.getConfig();
      test.setConf(configuration);

      assertEquals(-1, test.run(args));
      assertEquals(
          FakeRpcClientClassFactory.FakeRMAdminProtocol.FunctionCall.
          refreshQueues,
          FakeRpcClientClassFactory.FakeRMAdminProtocol.functionCall);
      assertTrue(dataErr.toString().contains("refreshQueues: test exception"));
      configuration.set(YarnConfiguration.RM_ADMIN_ADDRESS, "fake address");
      // test IllegalArgumentException
      assertEquals(-1, test.run(args));
      assertTrue(dataErr.toString().contains(
          "refreshQueues: Does not contain a valid host:"
              + "port authority: fake adderss"));

    } finally {
      FakeRpcClientClassFactory.FakeRMAdminProtocol.resultCode = 
          FakeRpcClientClassFactory.FakeRMAdminProtocol.ResultCode.OK;
      System.setErr(oldErrPrintStream);

    }

  }

  private void testError(String[] args, String template,
      ByteArrayOutputStream data, int resultCode) throws Exception {
    assertEquals(resultCode, new RMAdmin().run(args));
    assertTrue(data.toString().contains(template));
    data.reset();

  }

  @AfterClass
  public static void tearDown() {
    resourceManager.stop();
  }

}
