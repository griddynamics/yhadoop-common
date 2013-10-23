/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;

import org.junit.Test;

/**
 * This test starts a 1 NameNode 1 DataNode MiniDFSCluster with
 * kerberos authentication enabled.
 * The test uses MiniKdc for Kerberos authentication.
 */
public class TestStartSecureDataNode extends KerberosSecurityTestcase {
  
  final static private int NUM_OF_DATANODES = 1;

  private static final String NAME_NODE_PRINCIPAL = "a/localhost";
  private static final String DATA_NODE_PRINCIPAL = "a/localhost";
  private static final String SPNEGO_PRINCIPAL = "spnego/localhost";
  
  private String nnPrincipal, nnSpnegoPrincipal, dnPrincipal;
  private String nnKeyTab, dnKeyTab;

  private void composePrincipalNames() {
    final String realm = getKdc().getRealm();
    nnPrincipal = NAME_NODE_PRINCIPAL + "@" + realm;
    nnSpnegoPrincipal = SPNEGO_PRINCIPAL + "@" + realm;
    dnPrincipal = DATA_NODE_PRINCIPAL + "@" + realm;
  }

  // Create kaytabs and add the principals to them.
  private void createKeytabs() throws Exception {
    final File kdcWd = getWorkDir();
    
    File nnKtb = new File(kdcWd, "nn1.keytab");
    nnKeyTab = nnKtb.getAbsolutePath(); 
    getKdc().createPrincipal(nnKtb, NAME_NODE_PRINCIPAL, SPNEGO_PRINCIPAL);
    
    dnKeyTab = nnKeyTab;
  } 

  @Test
  public void testSecureNameNode() throws Exception {
    composePrincipalNames();
    createKeytabs();
    // ensure keytabs are created okay:
    assertTrue(new File(nnKeyTab).exists());
    assertTrue(new File(dnKeyTab).exists());
    
    MiniDFSCluster cluster = null;
    try {
      assertNotNull("NameNode principal was not specified", nnPrincipal);
      assertNotNull("NameNode SPNEGO principal was not specified",
                    nnSpnegoPrincipal);
      assertNotNull("NameNode keytab was not specified", nnKeyTab);
      assertNotNull("DataNode principal was not specified", dnPrincipal);
      assertNotNull("DataNode keytab was not specified", dnKeyTab);

      Configuration conf = new HdfsConfiguration();
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, nnPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY, nnSpnegoPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, nnKeyTab);
      conf.set(DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY, dnPrincipal);
      conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, dnKeyTab);
      // Normally Secure DataNode requires using ports lower than 1024,
      // but for the test we use higher values.
      conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:14004" /*1004*/ );
      conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:14006" /*1006*/ );
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, "700");
      
      conf.setBoolean("ignore.secure.ports.for.testing", true);

      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_OF_DATANODES)
        .checkDataNodeAddrConfig(true)
        .build();
      cluster.waitActive();
      assertTrue(cluster.isDataNodeUp());
    } catch (Exception ex) {
      ex.printStackTrace();
      throw ex;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
}
