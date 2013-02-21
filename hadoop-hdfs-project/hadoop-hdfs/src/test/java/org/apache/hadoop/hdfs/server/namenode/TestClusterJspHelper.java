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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.ClusterStatus;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.DecommissionStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterJspHelper {

  private static MiniDFSCluster cluster;  
  private static final int dataNodeNumber = 2;
  private static final int nameNodePort = 45541;
  private static final int nameNodeHttpPort = 50070;
  
  private static final class ConfigurationForTestClusterJspHelper extends Configuration {
    static {
      addDefaultResource("testClusterJspHelper.xml");
    }
  } 
  
  @BeforeClass
  public static void before() throws IOException {
    Configuration conf = new ConfigurationForTestClusterJspHelper();
    cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nameNodePort)
        .nameNodeHttpPort(nameNodeHttpPort)
        .numDataNodes(dataNodeNumber)
        .build();
    cluster.waitClusterUp();
  }

  @AfterClass
  public static void after() {
    cluster.shutdown();
  }

  @Test(timeout = 2000)
  public void testClusterJspHelperReports() {
    try {
      ClusterJspHelper clusterJspHelper = new ClusterJspHelper();
      ClusterStatus clusterStatus = clusterJspHelper
          .generateClusterHealthReport();
      assertNotNull("testClusterJspHelperReports ClusterStatus is null",
          clusterStatus);      
      DecommissionStatus decommissionStatus = clusterJspHelper
          .generateDecommissioningReport();
      assertNotNull("testClusterJspHelperReports DecommissionStatus is null",
          decommissionStatus);      
    } catch (Exception ex) {
      fail("testDefaultNamenode ex error !!!" + ex);
    }
  }
}
