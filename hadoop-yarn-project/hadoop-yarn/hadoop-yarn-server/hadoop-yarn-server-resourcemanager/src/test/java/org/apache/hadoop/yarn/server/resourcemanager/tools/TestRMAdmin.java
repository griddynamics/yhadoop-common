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

  @AfterClass
  public static void tearDown() {
    resourceManager.stop();
  }

}
