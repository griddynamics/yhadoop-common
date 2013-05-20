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
import org.apache.hadoop.yarn.server.resourcemanager.tools.FakeRpcClientClassFactory.FakeRMAdminProtocol;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRMAdmin {
  private static ResourceManager resourceManager;

  @BeforeClass
  public static void setUp() {

    Configuration conf = new YarnConfiguration();
    Store store = StoreFactory.getStore(conf);
    conf.set(YarnConfiguration.IPC_CLIENT_FACTORY,
        "org.apache.hadoop.yarn.server.resourcemanager.tools.FakeRpcClientClassFactory");
    resourceManager = new ResourceManager(store);
    resourceManager.init(conf);
    resourceManager.start();

  }

  @Test
  public void testRefreshQueues() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshQueues" };
    assertEquals(0, test.run(args));

    assertEquals(1, FakeRMAdminProtocol.parameter);
  }

  @Test
  public void testRefreshUserToGroupsMappings() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshUserToGroupsMappings" };
    assertEquals(0, test.run(args));
    assertEquals(4, FakeRMAdminProtocol.parameter);
  }

  @Test
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshSuperUserGroupsConfiguration" };
    assertEquals(0, test.run(args));
    assertEquals(3, FakeRMAdminProtocol.parameter);
  }

  @Test
  public void testRefreshAdminAcls() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshAdminAcls" };
    assertEquals(0, test.run(args));
    assertEquals(5, FakeRMAdminProtocol.parameter);
  }

  @Test
  public void testRefreshServiceAcl() throws Exception {
    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshServiceAcl" };
    assertEquals(0, test.run(args));
    assertEquals(6, FakeRMAdminProtocol.parameter);
  }

  @Test
  public void testRefreshNodes() throws Exception {

    RMAdmin test = new RMAdmin();
    test.setConf(resourceManager.getConfig());
    String[] args = { "-refreshNodes" };
    assertEquals(0, test.run(args));
    assertEquals(2, FakeRMAdminProtocol.parameter);
  }
  @Test
  public void testHelp() throws Exception {
    PrintStream oldOutPrimtStream = System.out;
    PrintStream oldErrPrimtStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));
    try {
      String[] args = { "-help" };
      assertEquals(0,new RMAdmin().run(args));
      assertTrue(dataOut.toString().contains("rmadmin is the command to execute Map-Reduce administrative commands."));
      assertTrue(dataOut.toString().contains("hadoop rmadmin [-refreshQueues] [-refreshNodes] [-refreshSuperUserGroupsConfigurati" +
      		"on] [-refreshUserToGroupsMappings] [-refreshAdminAcls] [-refreshServiceAcl] [-help [cmd]]"));
      assertTrue(dataOut.toString().contains("-refreshQueues: Reload the queues' acls, states and scheduler specific properties."));
      assertTrue(dataOut.toString().contains("-refreshNodes: Refresh the hosts information at the ResourceManager."));
      assertTrue(dataOut.toString().contains("-refreshUserToGroupsMappings: Refresh user-to-groups mappings"));
      assertTrue(dataOut.toString().contains("-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings"));
      assertTrue(dataOut.toString().contains("-refreshAdminAcls: Refresh acls for administration of ResourceManager"));
      assertTrue(dataOut.toString().contains("-refreshServiceAcl: Reload the service-level authorization policy file"));
      assertTrue(dataOut.toString().contains("-help [cmd]: \tDisplays help for the given command or all commands if none"));
      
      testError(new String[]{ "-help" ,"-refreshQueues" },"Usage: java RMAdmin [-refreshQueues]",dataErr);
      testError(new String[]{ "-help" ,"-refreshNodes" },"Usage: java RMAdmin [-refreshNodes]",dataErr);
      testError(new String[]{ "-help" ,"-refreshUserToGroupsMappings" },"Usage: java RMAdmin [-refreshUserToGroupsMappings]",dataErr);
      testError(new String[]{ "-help" ,"-refreshSuperUserGroupsConfiguration" },"Usage: java RMAdmin [-refreshSuperUserGroupsConfiguration]",dataErr);
      testError(new String[]{ "-help" ,"-refreshAdminAcls" },"Usage: java RMAdmin [-refreshAdminAcls]",dataErr);
      testError(new String[]{ "-help" ,"-refreshServiceAcl" },"Usage: java RMAdmin [-refreshServiceAcl]",dataErr);


      
    } finally {
      System.setOut(oldOutPrimtStream);
      System.setErr(oldErrPrimtStream);

    }

  }

  private void testError(String[] args, String template, ByteArrayOutputStream dataErr) throws Exception{
    assertEquals(0,new RMAdmin().run(args));
    assertTrue(dataErr.toString().contains(template));
    dataErr.reset();
    
  } 
  @AfterClass
  public static void tearDown() {
    resourceManager.stop();
  }

}
