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

import static org.junit.Assert.fail;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.ClusterStatus;
import org.junit.Test;

public class TestClusterJspHelper {    
  
  @Test
  public void testDefaultNamenode() {                                        
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {            
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleSingleNN(45541, 50070))
          .numDataNodes(2).build();     
      cluster.waitActive();                                                                    
      ClusterJspHelper clusterJspHelper = new ClusterJspHelper();      
      ClusterStatus clusterStatus = clusterJspHelper.generateClusterHealthReport();
      Assert.assertNotNull(clusterStatus);      
      clusterJspHelper.generateDecommissioningReport();
    } catch(Exception ex) {
      fail("testDefaultNamenode error !!!" + ex);
    } finally {
      cluster.shutdown();
    }
  }
}
