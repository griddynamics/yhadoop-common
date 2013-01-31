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

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.junit.Test;

/**
 * Tests MiniDFS cluster setup/teardown and isolation.
 * Every instance is brought up with a new data dir, to ensure that
 * shutdown work in background threads don't interfere with bringing up
 * the new cluster.
 */
public class TestMiniDFSCluster {

  /**
   * Verify that without system properties the cluster still comes up, provided
   * the configuration is set
   *
   * @throws Throwable on a failure
   */
  @Test
  public void testClusterWithoutSystemProperties() throws Throwable {
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    cluster.shutdown();
  }

  /**
   * Bring up two clusters and assert that they are in different directories.
   * @throws Throwable on a failure
   */
  @Test
  public void testDualClusters() throws Throwable {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf).build();
    MiniDFSCluster cluster3 = null;
    try {
      String dataDir2 = cluster2.getDataDirectory();
      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
      cluster3 = builder.build();
      String dataDir3 = cluster3.getDataDirectory();
      assertTrue("Clusters are bound to the same directory: " + dataDir2,
                        !dataDir2.equals(dataDir3));
    } finally {
      MiniDFSCluster.shutdownCluster(cluster3);
      MiniDFSCluster.shutdownCluster(cluster2);
    }
  }

  @Test(timeout=100000)
  public void testIsClusterUpAfterShutdown() throws Throwable {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster4 = new MiniDFSCluster.Builder(conf).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster4.getFileSystem();
      dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_ENTER);
      cluster4.shutdown();
    } finally {
      while(cluster4.isClusterUp()){
        Thread.sleep(1000);
      }  
    }
  }

  /** MiniDFSCluster should not clobber dfs.datanode.hostname if requested */
  @Test(timeout=100000)
  public void testClusterSetDatanodeHostname() throws Throwable {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "MYHOST");
    MiniDFSCluster cluster5 = new MiniDFSCluster.Builder(conf)
      .numDataNodes(1)
      .checkDataNodeHostConfig(true)
      .build();
    try {
      assertEquals("DataNode hostname config not respected", "MYHOST",
          cluster5.getDataNodes().get(0).getDatanodeId().getHostName());
    } finally {
      MiniDFSCluster.shutdownCluster(cluster5);
    }
  }
}
