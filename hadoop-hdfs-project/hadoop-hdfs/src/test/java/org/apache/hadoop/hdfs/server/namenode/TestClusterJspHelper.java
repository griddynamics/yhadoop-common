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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestClusterJspHelper {

  private DistributedFileSystem dfs;
  private Configuration conf;
  private URI uri;
  private static final String SERVICE_VALUE = "localhost:2005";  
  
  @Before 
  public void init() throws URISyntaxException, IOException {
    dfs = new DistributedFileSystem();
    //mock(DistributedFileSystem.class);
    conf = new HdfsConfiguration();    
    uri = new URI("hdfs://" + SERVICE_VALUE);
    FileSystemTestHelper.addFileSystemForTesting(uri, conf, dfs);
  }
  
  @Ignore
  public void testDefaultNamenode() throws IOException {                                
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fs = cluster.getFileSystem();
      assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem);        
      ClusterJspHelper clusterJspHelper = new ClusterJspHelper();           
      //assertEquals(clusterJspHelper.generateClusterHealthReport(fs).clusterid, "testClusterID");      
      //clusterJspHelper.generateDecommissioningReport();
    } catch(Exception ex) {
      fail("" + ex);
    } finally {
      cluster.shutdown();
    }
  }
}
