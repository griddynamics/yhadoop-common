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

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.utils.ThreadUtils;


/**
 * This class tests that a file system adheres to the limit of
 * maximum number of files that is configured.
 */
public class TestFileLimit extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  boolean simulatedStorage = false;

  // creates a zero file.
  private void createFile(FileSystem fileSys, Path name)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)1, (long)blockSize);
    byte[] buffer = new byte[1024];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }

  private void waitForLimit(FSNamesystem namesys, long num)
  {
    // wait for number of blocks to decrease
    while (true) {
      long total = namesys.getBlocksTotal() + namesys.dir.totalInodes();
      System.out.println("Comparing current nodes " + total +
                         " to become " + num);
      if (total == num) {
        break;
      }
      try {
        ThreadUtils.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Test that file data becomes available before file is closed.
   */
  public void testFileLimit() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int maxObjects = 5;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY, maxObjects);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000L);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    int currentNodes = 0;
    
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();
    FSNamesystem namesys = cluster.getNamesystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDirectory());
      currentNodes = 1;          // root inode

      // verify that we can create the specified number of files. We leave
      // one for the "/". Each file takes an inode and a block.
      //
      for (int i = 0; i < maxObjects/2; i++) {
        Path file = new Path("/filestatus" + i);
        createFile(fs, file);
        System.out.println("Created file " + file);
        currentNodes += 2;      // two more objects for this creation.
      }

      // verify that creating another file fails
      boolean hitException = false;
      try {
        Path file = new Path("/filestatus");
        createFile(fs, file);
        System.out.println("Created file " + file);
      } catch (IOException e) {
        hitException = true;
      }
      assertTrue("Was able to exceed file limit", hitException);

      // delete one file
      Path file0 = new Path("/filestatus0");
      fs.delete(file0, true);
      System.out.println("Deleted file " + file0);
      currentNodes -= 2;

      // wait for number of blocks to decrease
      waitForLimit(namesys, currentNodes);

      // now, we shud be able to create a new file
      createFile(fs, file0);
      System.out.println("Created file " + file0 + " again.");
      currentNodes += 2;

      // delete the file again
      file0 = new Path("/filestatus0");
      fs.delete(file0, true);
      System.out.println("Deleted file " + file0 + " again.");
      currentNodes -= 2;

      // wait for number of blocks to decrease
      waitForLimit(namesys, currentNodes);

      // create two directories in place of the file that we deleted
      Path dir = new Path("/dir0/dir1");
      fs.mkdirs(dir);
      System.out.println("Created directories " + dir);
      currentNodes += 2;
      waitForLimit(namesys, currentNodes);

      // verify that creating another directory fails
      hitException = false;
      try {
        fs.mkdirs(new Path("dir.fail"));
        System.out.println("Created directory should not have succeeded.");
      } catch (IOException e) {
        hitException = true;
      }
      assertTrue("Was able to exceed dir limit", hitException);

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  public void testFileLimitSimulated() throws IOException {
    simulatedStorage = true;
    testFileLimit();
    simulatedStorage = false;
  }
}
