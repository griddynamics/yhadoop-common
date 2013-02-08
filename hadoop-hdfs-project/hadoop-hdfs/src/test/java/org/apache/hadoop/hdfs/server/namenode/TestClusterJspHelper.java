package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestClusterJspHelper {         
  

  /** Tests to ensure default namenode is used as fallback */
  @Test
  public void testDefaultNamenode() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    
    InetSocketAddress isa = NameNode.getAddress(conf);    
    
    final String hdfs_default = "hdfs://localhost:9999/";
    conf.set(FS_DEFAULT_NAME_KEY, hdfs_default);
    // If DFS_FEDERATION_NAMESERVICES is not set, verify that
    // default namenode address is returned.
    Map<String, Map<String, InetSocketAddress>> addrMap =
      DFSUtil.getNNServiceRpcAddresses(conf);
    assertEquals(1, addrMap.size());
    
    Map<String, InetSocketAddress> defaultNsMap = addrMap.get(null);
    assertEquals(1, defaultNsMap.size());
    
    assertEquals(9999, defaultNsMap.get(null).getPort());
    
    ClusterJspHelper clusterJspHelper = new ClusterJspHelper();
    clusterJspHelper.generateDecommissioningReport();
        
    
    
  }
}
    
