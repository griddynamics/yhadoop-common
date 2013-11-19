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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Test;

public class TestSecureNameNode extends KerberosSecurityTestcase {
  
  private static final int NUM_OF_DATANODES = 0;
  
  private static final String NAME_NODE_PRINCIPAL = "nn1/localhost";
  private static final String USER_PRINCIPAL = "user1";
  
  private String nameNodePrincipalFull, userPrincipalFull;
  private String nn1KeytabPath, user1KeyTabPath;

  private void composePrincipalNames() {
    final String realm = getKdc().getRealm();
    nameNodePrincipalFull = NAME_NODE_PRINCIPAL + "@" + realm;
    userPrincipalFull = USER_PRINCIPAL + "@" + realm;
    assertFalse(nameNodePrincipalFull.equals(userPrincipalFull));
  }

  // Create kaytabs and add the principals to them.
  private void createKeytabs() throws Exception {
    final File kdcWd = getWorkDir();
    
    File userKtb = new File(kdcWd, "user1.keytab"); 
    user1KeyTabPath = userKtb.getAbsolutePath(); 
    getKdc().createPrincipal(userKtb, USER_PRINCIPAL);
    
    File nnKtb = new File(kdcWd, "nn1.keytab");
    nn1KeytabPath = nnKtb.getAbsolutePath(); 
    getKdc().createPrincipal(nnKtb, NAME_NODE_PRINCIPAL);
  } 
  
  @Test
  public void testName() throws Exception {
    composePrincipalNames();
    createKeytabs();
    // ensure keytabs are created okay:
    checkKeytab(nn1KeytabPath);
    checkKeytab(user1KeyTabPath);
    
    MiniDFSCluster cluster = null;
    try {
      final Configuration conf = new HdfsConfiguration();
      SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
      conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, nameNodePrincipalFull);
      conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, nn1KeytabPath);

      // Actually this config property is needed when the test is running on Java6. 
      // Surprisingly, on Java7 it passes even if this property is not set, 
      // because Krb5LoginModule successfully passes the #login() method even if
      // the principal "${dfs.web.authentication.kerberos.principal}" is not 
      // defined in the keytab ".../nn1.keytab".
      conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, nameNodePrincipalFull);
      
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
          .build();
      final MiniDFSCluster clusterRef = cluster;
      cluster.waitActive();
      
      FileSystem fsForCurrentUser = cluster.getFileSystem();
      fsForCurrentUser.mkdirs(new Path("/tmp"));
      fsForCurrentUser.setPermission(new Path("/tmp"), new FsPermission(
          (short) 511));

      UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(userPrincipalFull, user1KeyTabPath);
      FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return clusterRef.getFileSystem();
        }
      });
      try {
        Path p = new Path("/users");
        fs.mkdirs(p);
        fail("user1 must not be allowed to write in /");
      } catch (IOException expected) {
      }

      Path p = new Path("/tmp/alpha");
      fs.mkdirs(p);
      assertNotNull(fs.listStatus(p));
      assertEquals(AuthenticationMethod.KERBEROS,
          ugi.getAuthenticationMethod());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Override
  public void createTestDir() {
    super.createTestDir();
    File wd = getWorkDir();
    wd.mkdirs();
    assertTrue(wd.exists());
  }
   
  @Override
  public void createMiniKdcConf() {
    super.createMiniKdcConf();
    Properties conf = getConf();
    conf.put(MiniKdc.DEBUG, "true");
  }
  
  public static void checkKeytab(String keytabPath) {
    final File keytabFile = new File(keytabPath);
    assertTrue("Keytab file ["+keytabFile.getAbsolutePath()
        + "] not found or is not readable.", keytabFile.exists() 
        && keytabFile.canRead());
  }

}
