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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import static org.apache.hadoop.security.SecurityUtilTestHelper.isExternalKdcRunning;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * This test brings up a MiniDFSCluster with 1 NameNode and 0
 * DataNodes with kerberos authentication enabled using user-specified
 * KDC, principals, and keytabs.
 *
 * To run, users must specify the following system properties:
 *   externalKdc=true
 *   java.security.krb5.conf
 *   dfs.namenode.kerberos.principal
 *   dfs.namenode.kerberos.internal.spnego.principal
 *   dfs.namenode.keytab.file
 *   user.principal (do not specify superuser!)
 *   user.keytab
 */
public class TestSecureNameNodeWithExternalKdc {
  final static private int NUM_OF_DATANODES = 0;

  @Before
  public void testExternalKdcRunning() {
    // Tests are skipped if external KDC is not running.
    Assume.assumeTrue(isExternalKdcRunning());
  }

  @Test
  public void testSecureNameNode() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      final String nnPrincipal =
        System.getProperty(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);
      final String nnSpnegoPrincipal =
        System.getProperty(DFSConfigKeys.DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY);
      final String nnKeyTab = System.getProperty(
          DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY);
      assertNotNull("NameNode principal was not specified", nnPrincipal);
      assertNotNull("NameNode SPNEGO principal was not specified",
        nnSpnegoPrincipal);
      assertNotNull("NameNode keytab was not specified", nnKeyTab);
      checkKeytab(nnKeyTab);

      final Configuration conf = new HdfsConfiguration();
      SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
      conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, nnPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY,
          nnSpnegoPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, nnKeyTab);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
          .build();
      final MiniDFSCluster clusterRef = cluster;
      cluster.waitActive();
      FileSystem fsForCurrentUser = cluster.getFileSystem();
      fsForCurrentUser.mkdirs(new Path("/tmp"));
      fsForCurrentUser.setPermission(new Path("/tmp"), new FsPermission(
          (short) 511));

      // The user specified should not be a superuser
      final String userPrincipal = System.getProperty("user.principal");
      final String userKeyTab = System.getProperty("user.keytab");
      assertNotNull("User principal was not specified", userPrincipal);
      assertTrue("Incorrect configuration: user principal ["+userPrincipal
          +"] is the same as DFS Name Node principal ["+nnPrincipal+"]", 
          !userPrincipal.equals(nnPrincipal));
      assertNotNull("User keytab was not specified", userKeyTab);
      checkKeytab(userKeyTab);

      final UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(userPrincipal, userKeyTab);
      
      assertEquals(AuthenticationMethod.KERBEROS,
          ugi.getAuthenticationMethod());
      
      FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return clusterRef.getFileSystem();
        }
      });
      try {
        Path p = new Path("/users");
        fs.mkdirs(p);
        fail("User must not be allowed to write in /");
      } catch (IOException expected) {
      }

      Path p = new Path("/tmp/alpha");
      fs.mkdirs(p);
      assertNotNull(fs.listStatus(p));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  public static void checkKeytab(String keytabPath) {
    final File keytabFile = new File(keytabPath);
    assertTrue("Keytab file ["+keytabFile.getAbsolutePath()
        + "] not found or is not readable.", keytabFile.exists() 
        && keytabFile.canRead());
  }
}
