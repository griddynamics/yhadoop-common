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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.DATA_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.apache.hadoop.test.GenericTestUtils.assertExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.TestParallelImageWrite;
import org.apache.hadoop.util.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is upgraded under various storage state and
* version conditions.
*/
public class TestDFSUpgrade extends UpgradeUtilities {
 
  private static final int EXPECTED_TXID = 49;
  private static final Log LOG = LogFactory.getLog(TestDFSUpgrade.class.getName());
  private Configuration conf;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
    
  public TestDFSUpgrade() throws Exception {
      super(MiniDFSCluster.newDfsBaseDir());
  }
  
  /**
   * Writes an INFO log message containing the parameters.
   */
  void log(String label, int numDirs) {
    LOG.info("============================================================");
    LOG.info("***TEST " + (testCounter++) + "*** " 
             + label + ":"
             + " numDirs="+numDirs);
  }
  
  /**
   * For namenode, Verify that the current and previous directories exist.
   * Verify that previous hasn't been modified by comparing the checksum of all
   * its files with their original checksum. It is assumed that the
   * server has recovered and upgraded.
   */
  void checkNameNode(String[] baseDirs, long imageTxId) throws IOException {
    for (String baseDir : baseDirs) {
      LOG.info("Checking namenode directory " + baseDir);
      LOG.info("==== Contents ====:\n  " +
          Joiner.on("  \n").join(new File(baseDir, "current").list()));
      LOG.info("==================");
      
      assertExists(new File(baseDir,"current"));
      assertExists(new File(baseDir,"current/VERSION"));
      assertExists(new File(baseDir,"current/" 
                          + getInProgressEditsFileName(imageTxId + 1)));
      assertExists(new File(baseDir,"current/" 
                          + getImageFileName(imageTxId)));
      assertExists(new File(baseDir,"current/seen_txid"));
      
      File previous = new File(baseDir, "previous");
      assertExists(previous);
      assertEquals(checksumContents(NAME_NODE, previous),
          checksumMasterNameNodeContents());
    }
  }
 
  /**
   * For datanode, for a block pool, verify that the current and previous
   * directories exist. Verify that previous hasn't been modified by comparing
   * the checksum of all its files with their original checksum. It
   * is assumed that the server has recovered and upgraded.
   */
  void checkDataNode(String[] baseDirs, String bpid) throws IOException {
    for (int i = 0; i < baseDirs.length; i++) {
      File current = new File(baseDirs[i], "current/" + bpid + "/current");
      assertEquals(checksumContents(DATA_NODE, current),
          checksumMasterDataNodeContents());
      
      // block files are placed under <sd>/current/<bpid>/current/finalized
      File currentFinalized = 
        MiniDFSCluster.getFinalizedDir(new File(baseDirs[i]), bpid);
      assertEquals(checksumContents(DATA_NODE, currentFinalized),
          checksumMasterBlockPoolFinalizedContents());
      
      File previous = new File(baseDirs[i], "current/" + bpid + "/previous");
      assertTrue(previous.isDirectory());
      assertEquals(checksumContents(DATA_NODE, previous),
          checksumMasterDataNodeContents());
      
      File previousFinalized = 
        new File(baseDirs[i], "current/" + bpid + "/previous"+"/finalized");
      assertEquals(checksumContents(DATA_NODE, previousFinalized),
          checksumMasterBlockPoolFinalizedContents());
      
    }
  }

  /**
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   */
  void startNameNodeShouldFail(StartupOption operation) {
    startNameNodeShouldFail(operation, null, null);
  }

  /**
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   * @param operation - NameNode startup operation
   * @param exceptionClass - if non-null, will check that the caught exception
   *     is assignment-compatible with exceptionClass
   * @param messagePattern - if non-null, will check that a substring of the 
   *     message from the caught exception matches this pattern, via the
   *     {@link Matcher#find()} method.
   */
  void startNameNodeShouldFail(StartupOption operation,
      Class<? extends Exception> exceptionClass, Pattern messagePattern) {
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .dfsBaseDir(dfsBaseDir)
                                                .startupOption(operation)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .build(); // should fail
      fail("NameNode should have failed to start");
      
    } catch (Exception e) {
      // expect exception
      if (exceptionClass != null) {
        assertTrue("Caught exception is not of expected class "
            + exceptionClass.getSimpleName() + ": "
            + StringUtils.stringifyException(e), 
            exceptionClass.isInstance(e));
      }
      if (messagePattern != null) {
        assertTrue("Caught exception message string does not match expected pattern \""
            + messagePattern.pattern() + "\" : "
            + StringUtils.stringifyException(e), 
            messagePattern.matcher(e.getMessage()).find());
      }
      LOG.info("Successfully detected expected NameNode startup failure.");
    }
  }
  
  /**
   * Attempts to start a DataNode with the given operation. Starting
   * the given block pool should fail.
   * @param operation startup option
   * @param bpid block pool Id that should fail to start
   * @throws IOException 
   */
  void startBlockPoolShouldFail(StartupOption operation, String bpid) throws IOException {
    cluster.startDataNodes(conf, 1, false, operation, null); // should fail
    assertFalse("Block pool " + bpid + " should have failed to start",
        cluster.getDataNodes().get(0).isBPServiceAlive(bpid));
  }
 
  /**
   * Create an instance of a newly configured cluster for testing that does
   * not manage its own directories or files
   */
  private MiniDFSCluster createCluster() throws IOException {
    return new MiniDFSCluster.Builder(conf).dfsBaseDir(dfsBaseDir)
                                           .numDataNodes(0)
                                           .format(false)
                                           .manageDataDfsDirs(false)
                                           .manageNameDfsDirs(false)
                                           .startupOption(StartupOption.UPGRADE)
                                           .build();
  }
  
  /**
   * This test attempts to upgrade the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  @Test
  public void testUpgrade() throws Exception {
    File[] baseDirs;
    StorageInfo storageInfo = null;
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf = initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      String[] dataNodeDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
      
      log("Normal NameNode upgrade", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      checkNameNode(nameNodeDirs, EXPECTED_TXID);
      if (numDirs > 1)
        TestParallelImageWrite.checkImages(cluster.getNamesystem(), numDirs);
      cluster.shutdown();
      createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode upgrade", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      createDataNodeStorageDirs(dataNodeDirs, "current");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkDataNode(dataNodeDirs, getCurrentBlockPoolID(null));
      cluster.shutdown();
      createEmptyDirs(nameNodeDirs);
      createEmptyDirs(dataNodeDirs);
      
      log("NameNode upgrade with existing previous dir", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      createNameNodeStorageDirs(nameNodeDirs, "previous");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      createEmptyDirs(nameNodeDirs);
      
      log("DataNode upgrade with existing previous dir", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      createDataNodeStorageDirs(dataNodeDirs, "current");
      createDataNodeStorageDirs(dataNodeDirs, "previous");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkDataNode(dataNodeDirs, getCurrentBlockPoolID(null));
      cluster.shutdown();
      createEmptyDirs(nameNodeDirs);
      createEmptyDirs(dataNodeDirs);

      log("DataNode upgrade with future stored layout version in current", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = createDataNodeStorageDirs(dataNodeDirs, "current");
      storageInfo = new StorageInfo(Integer.MIN_VALUE, 
          getCurrentNamespaceID(cluster),
          getCurrentClusterID(cluster),
          getCurrentFsscTime(cluster));
      
      createDataNodeVersionFile(baseDirs, storageInfo,
          getCurrentBlockPoolID(cluster));
      
      startBlockPoolShouldFail(StartupOption.REGULAR, getCurrentBlockPoolID(null));
      cluster.shutdown();
      createEmptyDirs(nameNodeDirs);
      createEmptyDirs(dataNodeDirs);
      
      log("DataNode upgrade with newer fsscTime in current", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = createDataNodeStorageDirs(dataNodeDirs, "current");
      storageInfo = new StorageInfo(getCurrentLayoutVersion(), 
          getCurrentNamespaceID(cluster),
          getCurrentClusterID(cluster), Long.MAX_VALUE);
          
      createDataNodeVersionFile(baseDirs, storageInfo, 
          getCurrentBlockPoolID(cluster));
      // Ensure corresponding block pool failed to initialized
      startBlockPoolShouldFail(StartupOption.REGULAR, getCurrentBlockPoolID(null));
      cluster.shutdown();
      createEmptyDirs(nameNodeDirs);
      createEmptyDirs(dataNodeDirs);

      log("NameNode upgrade with no edits file", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      deleteStorageFilesWithPrefix(nameNodeDirs, "edits_");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with no image file", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      deleteStorageFilesWithPrefix(nameNodeDirs, "fsimage_");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with corrupt version file", numDirs);
      baseDirs = createNameNodeStorageDirs(nameNodeDirs, "current");
      for (File f : baseDirs) { 
        corruptFile(
            new File(f,"VERSION"),
            "layoutVersion".getBytes(Charsets.UTF_8),
            "xxxxxxxxxxxxx".getBytes(Charsets.UTF_8));
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with old layout version in current", numDirs);
      baseDirs = createNameNodeStorageDirs(nameNodeDirs, "current");
      storageInfo = new StorageInfo(Storage.LAST_UPGRADABLE_LAYOUT_VERSION + 1, 
          getCurrentNamespaceID(null),
          getCurrentClusterID(null),
          getCurrentFsscTime(null));
      
      createNameNodeVersionFile(conf, baseDirs, storageInfo,
          getCurrentBlockPoolID(cluster));
      
      startNameNodeShouldFail(StartupOption.UPGRADE);
      createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with future layout version in current", numDirs);
      baseDirs = createNameNodeStorageDirs(nameNodeDirs, "current");
      storageInfo = new StorageInfo(Integer.MIN_VALUE, 
          getCurrentNamespaceID(null),
          getCurrentClusterID(null),
          getCurrentFsscTime(null));
      
      createNameNodeVersionFile(conf, baseDirs, storageInfo,
          getCurrentBlockPoolID(cluster));
      
      startNameNodeShouldFail(StartupOption.UPGRADE);
      createEmptyDirs(nameNodeDirs);
    } // end numDir loop
    
    // One more check: normal NN upgrade with 4 directories, concurrent write
    int numDirs = 4;
    {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      
      log("Normal NameNode upgrade", numDirs);
      createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      checkNameNode(nameNodeDirs, EXPECTED_TXID);
      TestParallelImageWrite.checkImages(cluster.getNamesystem(), numDirs);
      cluster.shutdown();
      createEmptyDirs(nameNodeDirs);
    }
  }
  
  /*
   * Stand-alone test to detect failure of one SD during parallel upgrade.
   * At this time, can only be done with manual hack of {@link FSImage.doUpgrade()}
   */
  @Ignore
  public void testUpgrade4() throws Exception {
    int numDirs = 4;
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
    conf = initializeStorageStateConf(numDirs, conf);
    String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);

    log("NameNode upgrade with one bad storage dir", numDirs);
    createNameNodeStorageDirs(nameNodeDirs, "current");
    try {
      // assert("storage dir has been prepared for failure before reaching this point");
      startNameNodeShouldFail(StartupOption.UPGRADE, IOException.class,
          Pattern.compile("failed in 1 storage"));
    } finally {
      // assert("storage dir shall be returned to normal state before exiting");
      createEmptyDirs(nameNodeDirs);
    }
  }
 
  private void deleteStorageFilesWithPrefix(String[] nameNodeDirs, String prefix)
  throws Exception {
    for (String baseDirStr : nameNodeDirs) {
      File baseDir = new File(baseDirStr);
      File currentDir = new File(baseDir, "current");
      for (File f : currentDir.listFiles()) {
        if (f.getName().startsWith(prefix)) {
          assertTrue("Deleting " + f, f.delete());
        }
      }
    }
  }

  @Test(expected=IOException.class)
  public void testUpgradeFromPreUpgradeLVFails() throws IOException {
    // Upgrade from versions prior to Storage#LAST_UPGRADABLE_LAYOUT_VERSION
    // is not allowed
    Storage.checkVersionUpgradable(Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION + 1);
    fail("Expected IOException is not thrown");
  }
  
  @Ignore
  public void test203LayoutVersion() {
    for (int lv : Storage.LAYOUT_VERSIONS_203) {
      assertTrue(Storage.is203LayoutVersion(lv));
    }
  }
  
  public static void main(String[] args) throws Exception {
    TestDFSUpgrade t = new TestDFSUpgrade();
    t.testUpgrade();
  }
}


