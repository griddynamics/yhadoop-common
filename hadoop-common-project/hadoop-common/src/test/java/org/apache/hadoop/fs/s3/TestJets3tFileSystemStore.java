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

package org.apache.hadoop.fs.s3;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.security.AWSCredentials;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Jets3tFileSystemStore methods and exceptions. The S3ServerStub uses as a
 * S3 service back end .
 */
public class TestJets3tFileSystemStore {

  private static Jets3tFileSystemStore store = null;
  private static File workspace = new File("target" + File.separator
      + "s3FileSystem");
  private static S3ServerStub stub;

  @BeforeClass
  public static void start() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "s3://abc:xyz@hostname/");
    URI fakeUri = new URI("s3://abc:xyz@hostname/");
    store = new Jets3tFileSystemStore();
    store.initialize(fakeUri, configuration);

    Field internalService = store.getClass().getDeclaredField("s3Service");
    internalService.setAccessible(true);

    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(fakeUri, configuration);
    AWSCredentials awsCredentials = new AWSCredentials(
        s3Credentials.getAccessKey(), s3Credentials.getSecretAccessKey());
    stub = new S3ServerStub(awsCredentials, workspace.getAbsolutePath());
    internalService.set(store, stub);
  }

  @AfterClass
  public static void stop() throws Exception {
    store.purge();
  }

  /**
   * test operations with Block
   */
  @Test(timeout = 500)
  public void testBlock() throws Exception {

    File f = getDummiTextFile("block file");
    Block block = new Block(1, f.length());
    // save block
    store.storeBlock(block, f);
    // test saved block
    assertTrue(store.blockExists(1));
    File result = new File(workspace.getAbsolutePath() + "hostname"
        + File.separator + "block_" + block.getId());
    assertTrue(result.exists());
    assertEquals(10, result.length());
    // get block
    File newFile = store.retrieveBlock(block, 0);
    assertNotNull(newFile);
    assertEquals(f.length(), newFile.length());
    // test exceptions
    stub.setThrowException(true);
    // get block
    try {
      store.retrieveBlock(block, 0);
      fail();
    } catch (IOException e) {

      S3ServiceException s3e = (S3ServiceException) e.getCause();
      assertEquals("12345", s3e.getS3ErrorCode());
    }
    // delete block
    try {
      store.deleteBlock(block);
      fail();
    } catch (IOException e) {
      S3ServiceException s3e = (S3ServiceException) e.getCause();
      assertEquals("12345", s3e.getS3ErrorCode());
    } finally {
      stub.setThrowException(false);
    }
    // clean data
    store.deleteBlock(block);
    assertFalse(result.exists());
    assertFalse(store.blockExists(1));

  }

  /**
   * Test operations with node
   */
  @Test(timeout = 500)
  public void testNode() throws Exception {

    store.purge();
    stub.setThrowException(false);
    File f = getDummiTextFile("node file");
    Path path = new Path("/testNode");
    Block[] blocks = new Block[2];
    blocks[0] = new Block(0, f.length());
    blocks[1] = new Block(1, f.length());

    INode node = new INode(FileType.FILE, blocks);
    // node path should be absolute
    try {
      store.storeINode(new Path("testNode"), node);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Path must be absolute: testNode", e.getMessage());
    }
    // test exceptions
    try {
      stub.setThrowException(true);
      store.storeINode(path, node);
      fail();
    } catch (IOException e) {
      S3ServiceException parent = (S3ServiceException) e.getCause();
      assertEquals("12345", parent.getS3ErrorCode());
    } finally {
      stub.setThrowException(false);
    }
    assertFalse(store.inodeExists(path));
    // store node
    store.storeINode(path, node);

    // test stored node
    assertTrue(store.inodeExists(path));
    File result = new File(workspace.getAbsolutePath() + "hostname"
        + File.separator + "testNode");
    assertTrue(result.exists());
    assertTrue(result.length() > 0);
    node = store.retrieveINode(path);
    assertEquals(2, node.getBlocks().length);
    assertEquals(FileType.FILE, node.getFileType());

    // delete node
    store.deleteINode(path);
    assertFalse(store.inodeExists(path));
    assertFalse(result.exists());

    try {
      store.deleteINode(path);
    } catch (IOException e) {
      S3ServiceException parent = (S3ServiceException) e.getCause();
      assertEquals("12345", parent.getS3ErrorCode());
    }
    stub.setThrowException(true);

    // test exceptions
    try {
      store.inodeExists(path);
      fail();
    } catch (IOException e) {
      S3ServiceException parent = (S3ServiceException) e.getCause();
      assertEquals("12345", parent.getS3ErrorCode());
    }

    try {
      store.retrieveINode(path);
      fail();
    } catch (IOException e) {
      S3ServiceException parent = (S3ServiceException) e.getCause();
      assertEquals("12345", parent.getS3ErrorCode());
    } finally {
      stub.setThrowException(false);

    }
  }

  /**
   * Test List operations
   */
  @Test(timeout = 500)
  public void testListSubPaths() throws Exception {
    // clean
    store.purge();

    File f = getDummiTextFile("node file");
    Path path = new Path("/testNode");
    Block[] blocks = new Block[2];
    blocks[0] = new Block(0, f.length());
    blocks[1] = new Block(1, f.length());

    INode node = new INode(FileType.FILE, blocks);
    store.storeINode(path, node);

    Set<Path> subPaths = store.listSubPaths(new Path("/"));
    assertEquals(1, subPaths.size());

    subPaths = store.listDeepSubPaths(new Path("/"));
    assertEquals(1, subPaths.size());

    store.dump();
    assertEquals(1, subPaths.size());
    // test exceptions
    stub.setThrowException(true);
    try {
      store.listSubPaths(new Path("/"));
      fail();
    } catch (Exception e) {
      S3ServiceException ex = (S3ServiceException) e.getCause();
      assertEquals("12345", ex.getS3ErrorCode());
    }
    try {
      store.listDeepSubPaths(new Path("/"));
      fail();
    } catch (Exception e) {
      S3ServiceException ex = (S3ServiceException) e.getCause();
      assertEquals("12345", ex.getS3ErrorCode());
    } finally {
      stub.setThrowException(false);

    }
    store.purge();
  }

  /**
   * test versions
   */

  @Test(timeout = 500)
  public void testGetVersion() throws Exception {
    assertEquals("1", store.getVersion());
  }

  private File getDummiTextFile(String text) throws Exception {
    File result = new File(workspace.getParent() + File.separator
        + "tmpFile.txt");
    Writer writer = new FileWriter(result);
    writer.write(text);
    writer.flush();
    writer.close();
    return result;

  }

  /**
   * Test operations with S3InputStream
   * 
   * @throws Exception
   */
  @Test(timeout = 500)
  public void testS3InputStream() throws Exception {
    File f = getDummiTextFile("node file");
    Path path = new Path("/testNode");
    Block[] blocks = new Block[2];
    blocks[0] = new Block(0, f.length());
    blocks[1] = new Block(1, f.length());
    store.storeBlock(blocks[0], f);
    store.storeBlock(blocks[1], f);

    INode node = new INode(FileType.FILE, blocks);
    store.storeINode(path, node);

    FileSystem.Statistics stats = new FileSystem.Statistics("statistic");
    S3InputStream input = new S3InputStream(new Configuration(), store, node,
        stats);
    int counter = 0;
    // test read method
    while (input.read() >= 0) {
      counter++;
    }

    assertEquals(18, stats.getBytesRead());
    assertEquals(18, counter);
    store.purge();
    assertFalse(input.markSupported());
    assertFalse(input.seekToNewSource(0));
    // finish
    assertEquals(0, input.available());
    input.close();
  }

  /**
   * Test Meta data
   */
  @Test(timeout = 500)
  public void testMetaData() throws Exception {
    store.purge();
    stub.setThrowException(false);
    File f = getDummiTextFile("node file");
    Path path = new Path("/testNode");
    Block[] blocks = new Block[2];
    blocks[0] = new Block(0, f.length());
    blocks[1] = new Block(1, f.length());

    INode node = new INode(FileType.FILE, blocks);
    store.storeINode(path, node);
    // test exceptions
    Map<String, String> metaData = new HashMap<String, String>();
    stub.setMetaData(metaData);
    try {
      store.inodeExists(path);
      fail();
    } catch (S3FileSystemException e) {
      assertEquals("Not a Hadoop S3 file.", e.getMessage());
    }
    metaData.put("fs", "Hadoop");
    try {
      store.inodeExists(path);
      fail();
    } catch (S3FileSystemException e) {
      assertEquals("Not a block file.", e.getMessage());
    }
    metaData.put("fs-type", "block");
    try {
      store.inodeExists(path);
      fail();
    } catch (VersionMismatchException e) {
      assertEquals(
          "Version mismatch: client expects version 1, but data has version [unversioned]",
          e.getMessage());
    }
    metaData.put("fs-version", "1");

    assertTrue(store.inodeExists(path));
    stub.setMetaData(null);
  }

  /**
   * Test S3Credentials class
   */
  @Test(timeout = 500)
  public void testS3Credentials() throws Exception {
    S3Credentials credentials = new S3Credentials();
    Configuration conf = new Configuration();
    try {
      credentials.initialize(new URI("s3://abc"), conf);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "AWS Access Key ID and Secret Access Key must be specified as the "
              + "username or password (respectively) of a s3 URL, or by setting the"
              + " fs.s3.awsAccessKeyId or fs.s3.awsSecretAccessKey properties (respectively).",
          e.getMessage());
    }
    conf.set("fs.s3.awsAccessKeyId", "xyz");
    try {
      credentials.initialize(new URI("s3://abc"), conf);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "AWS Secret Access Key must be specified as the password of a s3 URL,"
              + " or by setting the fs.s3.awsSecretAccessKey property.",
          e.getMessage());
    }
    conf.set("fs.s3.awsSecretAccessKey", "secret");
    credentials.initialize(new URI("s3://abc"), conf);
  }
}
