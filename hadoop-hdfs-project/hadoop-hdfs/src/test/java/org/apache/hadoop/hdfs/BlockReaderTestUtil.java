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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetUtils;

/**
 * A helper class to setup the cluster, and get to BlockReader and DataNode for a block.
 */
public class BlockReaderTestUtil {

  private HdfsConfiguration conf = null;
  private MiniDFSCluster cluster = null;

  /**
   * Setup the cluster
   */
  public BlockReaderTestUtil(int replicationFactor) throws Exception {
    this(replicationFactor, new HdfsConfiguration());
  }

  public BlockReaderTestUtil(int replicationFactor, HdfsConfiguration config) throws Exception {
    this.conf = config;
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    cluster = new MiniDFSCluster.Builder(conf).format(true).build();
    cluster.waitActive();
  }

  /**
   * Shutdown cluster
   */
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  public HdfsConfiguration getConf() {
    return conf;
  }

  /**
   * Create a file of the given size filled with random data.
   * @return  File data.
   */
  public byte[] writeFile(Path filepath, int sizeKB)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();

    // Write a file with the specified amount of data
    DataOutputStream os = fs.create(filepath);
    byte data[] = new byte[1024 * sizeKB];
    new Random().nextBytes(data);
    os.write(data);
    os.close();
    return data;
  }

  /**
   * Get the list of Blocks for a file.
   */
  public List<LocatedBlock> getFileBlocks(Path filepath, int sizeKB)
      throws IOException {
    // Return the blocks we just wrote
    DFSClient dfsclient = getDFSClient();
    return dfsclient.getNamenode().getBlockLocations(
      filepath.toString(), 0, sizeKB * 1024).getLocatedBlocks();
  }

  /**
   * Get the DFSClient.
   */
  public DFSClient getDFSClient() throws IOException {
    InetSocketAddress nnAddr = new InetSocketAddress("localhost", cluster.getNameNodePort());
    return new DFSClient(nnAddr, conf);
  }

  /**
   * Exercise the BlockReader and read length bytes.
   *
   * It does not verify the bytes read.
   */
  public void readAndCheckEOS(BlockReader reader, int length, boolean expectEof)
      throws IOException {
    byte buf[] = new byte[1024];
    int nRead = 0;
    while (nRead < length) {
      DFSClient.LOG.info("So far read " + nRead + " - going to read more.");
      int n = reader.read(buf, 0, buf.length);
      assertTrue(n > 0);
      nRead += n;
    }

    if (expectEof) {
      DFSClient.LOG.info("Done reading, expect EOF for next read.");
      assertEquals(-1, reader.read(buf, 0, buf.length));
    }
  }

  /**
   * Get a BlockReader for the given block.
   */
  public BlockReader getBlockReader(LocatedBlock testBlock, int offset, int lenToRead)
      throws IOException {
    InetSocketAddress targetAddr = null;
    Socket sock = null;
    ExtendedBlock block = testBlock.getBlock();
    DatanodeInfo[] nodes = testBlock.getLocations();
    targetAddr = NetUtils.createSocketAddr(nodes[0].getXferAddr());
    sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
    sock.connect(targetAddr, HdfsServerConstants.READ_TIMEOUT);
    sock.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);

    return BlockReaderFactory.newBlockReader(
        new BlockReaderFactory.Params(new Conf(conf)).
          setPeer(TcpPeerServer.peerFromSocket(sock)).
          setFile(targetAddr.toString() + ":" + block.getBlockId()).
          setBlock(block).setBlockToken(testBlock.getBlockToken()).
          setStartOffset(offset).setLen(lenToRead).
          setBufferSize(conf.getInt(
              CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096)).
          setVerifyChecksum(true).setDatanodeID(nodes[0]));
  }

  /**
   * Get a DataNode that serves our testBlock.
   */
  public DataNode getDataNode(LocatedBlock testBlock) {
    DatanodeInfo[] nodes = testBlock.getLocations();
    int ipcport = nodes[0].getIpcPort();
    return cluster.getDataNode(ipcport);
  }

}
