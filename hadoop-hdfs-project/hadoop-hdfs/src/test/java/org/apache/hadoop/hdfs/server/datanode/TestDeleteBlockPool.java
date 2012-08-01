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

package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.utils.ThreadUtils;
import org.junit.Test;

/**
 * Tests deleteBlockPool functionality.
 */
public class TestDeleteBlockPool {
  
  @Test
  public void testDeleteBlockPool() throws Exception {
    // Start cluster with a 2 NN and 2 DN
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES,
          "namesServerId1,namesServerId2");
      cluster = new MiniDFSCluster.Builder(conf).federation(true).numNameNodes(
          2).numDataNodes(2).build();

      cluster.waitActive();

      FileSystem fs1 = cluster.getFileSystem(0);
      FileSystem fs2 = cluster.getFileSystem(1);

      DFSTestUtil.createFile(fs1, new Path("/alpha"), 1024, (short) 2, 54);
      DFSTestUtil.createFile(fs2, new Path("/beta"), 1024, (short) 2, 54);

      DataNode dn1 = cluster.getDataNodes().get(0);
      DataNode dn2 = cluster.getDataNodes().get(1);

      String bpid1 = cluster.getNamesystem(0).getBlockPoolId();
      String bpid2 = cluster.getNamesystem(1).getBlockPoolId();

      File dn1StorageDir1 = cluster.getInstanceStorageDir(0, 0);
      File dn1StorageDir2 = cluster.getInstanceStorageDir(0, 1);
      File dn2StorageDir1 = cluster.getInstanceStorageDir(1, 0);
      File dn2StorageDir2 = cluster.getInstanceStorageDir(1, 1);

      // Although namenode is shutdown, the bp offerservice is still running
      try {
        dn1.deleteBlockPool(bpid1, true);
        Assert.fail("Must not delete a running block pool");
      } catch (IOException expected) {
      }

      Configuration nn1Conf = cluster.getConfiguration(1);
      nn1Conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES, "namesServerId2");
      dn1.refreshNamenodes(nn1Conf);
      assertEquals(1, dn1.getAllBpOs().length);

      try {
        dn1.deleteBlockPool(bpid1, false);
        Assert.fail("Must not delete if any block files exist unless "
            + "force is true");
      } catch (IOException expected) {
      }

      verifyBlockPoolDirectories(true, dn1StorageDir1, bpid1);
      verifyBlockPoolDirectories(true, dn1StorageDir2, bpid1);

      dn1.deleteBlockPool(bpid1, true);

      verifyBlockPoolDirectories(false, dn1StorageDir1, bpid1);
      verifyBlockPoolDirectories(false, dn1StorageDir2, bpid1);
     
      fs1.delete(new Path("/alpha"), true);
      
      // Wait till all blocks are deleted from the dn2 for bpid1.
      while ((MiniDFSCluster.getFinalizedDir(dn2StorageDir1, 
          bpid1).list().length != 0) || (MiniDFSCluster.getFinalizedDir(
              dn2StorageDir2, bpid1).list().length != 0)) {
        try {
          ThreadUtils.sleep(3000);
        } catch (Exception ignored) {
        }
      }
      cluster.shutdownNameNode(0);
      
      // Although namenode is shutdown, the bp offerservice is still running 
      // on dn2
      try {
        dn2.deleteBlockPool(bpid1, true);
        Assert.fail("Must not delete a running block pool");
      } catch (IOException expected) {
      }
      
      dn2.refreshNamenodes(nn1Conf);
      assertEquals(1, dn2.getAllBpOs().length);
      
      verifyBlockPoolDirectories(true, dn2StorageDir1, bpid1);
      verifyBlockPoolDirectories(true, dn2StorageDir2, bpid1);
      
      // Now deleteBlockPool must succeed with force as false, because no 
      // blocks exist for bpid1 and bpOfferService is also stopped for bpid1.
      dn2.deleteBlockPool(bpid1, false);
      
      verifyBlockPoolDirectories(false, dn2StorageDir1, bpid1);
      verifyBlockPoolDirectories(false, dn2StorageDir2, bpid1);
      
      //bpid2 must not be impacted
      verifyBlockPoolDirectories(true, dn1StorageDir1, bpid2);
      verifyBlockPoolDirectories(true, dn1StorageDir2, bpid2);
      verifyBlockPoolDirectories(true, dn2StorageDir1, bpid2);
      verifyBlockPoolDirectories(true, dn2StorageDir2, bpid2);
      //make sure second block pool is running all fine
      Path gammaFile = new Path("/gamma");
      DFSTestUtil.createFile(fs2, gammaFile, 1024, (short) 1, 55);
      fs2.setReplication(gammaFile, (short)2);
      DFSTestUtil.waitReplication(fs2, gammaFile, (short) 2);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testDfsAdminDeleteBlockPool() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES,
          "namesServerId1,namesServerId2");
      cluster = new MiniDFSCluster.Builder(conf).federation(true).numNameNodes(
          2).numDataNodes(1).build();

      cluster.waitActive();

      FileSystem fs1 = cluster.getFileSystem(0);
      FileSystem fs2 = cluster.getFileSystem(1);

      DFSTestUtil.createFile(fs1, new Path("/alpha"), 1024, (short) 1, 54);
      DFSTestUtil.createFile(fs2, new Path("/beta"), 1024, (short) 1, 54);

      DataNode dn1 = cluster.getDataNodes().get(0);

      String bpid1 = cluster.getNamesystem(0).getBlockPoolId();
      String bpid2 = cluster.getNamesystem(1).getBlockPoolId();
      
      File dn1StorageDir1 = cluster.getInstanceStorageDir(0, 0);
      File dn1StorageDir2 = cluster.getInstanceStorageDir(0, 1);
      
      Configuration nn1Conf = cluster.getConfiguration(0);
      nn1Conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES, "namesServerId1");
      dn1.refreshNamenodes(nn1Conf);
      Assert.assertEquals(1, dn1.getAllBpOs().length);
      
      DFSAdmin admin = new DFSAdmin(nn1Conf);
      String dn1Address = dn1.getSelfAddr().getHostName()+":"+dn1.getIpcPort();
      String[] args = { "-deleteBlockPool", dn1Address, bpid2 };
      
      int ret = admin.run(args);
      Assert.assertFalse(0 == ret);

      verifyBlockPoolDirectories(true, dn1StorageDir1, bpid2);
      verifyBlockPoolDirectories(true, dn1StorageDir2, bpid2);
      
      String[] forceArgs = { "-deleteBlockPool", dn1Address, bpid2, "force" };
      ret = admin.run(forceArgs);
      Assert.assertEquals(0, ret);
      
      verifyBlockPoolDirectories(false, dn1StorageDir1, bpid2);
      verifyBlockPoolDirectories(false, dn1StorageDir2, bpid2);
      
      //bpid1 remains good
      verifyBlockPoolDirectories(true, dn1StorageDir1, bpid1);
      verifyBlockPoolDirectories(true, dn1StorageDir2, bpid1);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void verifyBlockPoolDirectories(boolean shouldExist,
      File storageDir, String bpid) throws IOException {
    File bpDir = new File(storageDir, DataStorage.STORAGE_DIR_CURRENT + "/"
        + bpid);

    if (shouldExist == false) {
      Assert.assertFalse(bpDir.exists());
    } else {
      File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
      File finalizedDir = new File(bpCurrentDir,
          DataStorage.STORAGE_DIR_FINALIZED);
      File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
      File versionFile = new File(bpCurrentDir, "VERSION");

      Assert.assertTrue(finalizedDir.isDirectory());
      Assert.assertTrue(rbwDir.isDirectory());
      Assert.assertTrue(versionFile.exists());
    }
  }
}
