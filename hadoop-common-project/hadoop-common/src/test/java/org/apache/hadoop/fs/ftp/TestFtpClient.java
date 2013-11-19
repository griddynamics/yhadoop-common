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
package org.apache.hadoop.fs.ftp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.listener.nio.NioListener;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test  FTPFileSystem. Create, copy, delete files and directories
 */
public class TestFtpClient {
  private static final File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.data", "build/test/data"), "ftpclient");
  private static final File LOCAL_DIR = new File(TEST_ROOT_DIR, "local");
  private static final File FTP_DIR = new File(TEST_ROOT_DIR, "ftp");
  
  private static FtpServer server;
  private static int port;
  private static String tmpFile;
  private static FTPFileSystem ftpFs;

  /**
   * Start Ftp server
   */
  @BeforeClass
  public static void setUp() throws Exception {
    assertTrue(FileUtil.fullyDelete(TEST_ROOT_DIR, true));
    assertTrue(LOCAL_DIR.mkdirs() && FTP_DIR.mkdirs());
    
    tmpFile = createFile("temporary file");

    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory factory = new ListenerFactory();

    // set the port of the listener
    factory.setPort(0);
    factory.setImplicitSsl(false);
    // replace the default listener
    serverFactory.addListener("default", factory.createListener());
    PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
    UserManager um = userManagerFactory.createUserManager();
  // set user and password
    BaseUser user = new BaseUser();
    user.setName("user");
    user.setPassword("secret");
    user.setAuthorities(Arrays.asList(new WritePermission(),
        new ConcurrentLoginPermission(100, 100), new TransferRatePermission(
            100, 100)));
    user.setHomeDirectory(FTP_DIR.getAbsolutePath());
    um.save(user);
    serverFactory.setUserManager(um);
    // start the server
    server = serverFactory.createServer();
    server.start();
    NioListener listener = (NioListener) serverFactory.getListeners().values()
        .iterator().next();
    port = listener.getPort();
    ftpFs = new FTPFileSystem();
    ftpFs.initialize(getURI(), getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    try {
      ftpFs.close();
    } finally {
      server.stop();
    }
  }

  /**
   * Test a create file functions
   *
   */
  @Test(timeout = 3000)
  public void testCreateFile() throws Exception {
    assertEquals("/", ftpFs.getHomeDirectory().toString());
    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.create(testFile);
    assertTrue(ftpFs.exists(testFile));
    // test create file with prohibited overwrite
    try {
      ftpFs.create(testFile, new FsPermission((short) 777), false, 512,
          (short) 0, 256L, new Progressable() {
            @Override
            public void progress() {
            }
          });
    } catch (IOException e) {
      assertEquals("File already exists: test1/test2/testFile.txt",
          e.getMessage());
    }
    
    // try to remove a no empty folder with prohibited recursion
    try {
      ftpFs.delete(new Path("test1"), false);
      fail();
    } catch (IOException e) {
      assertEquals("Directory: test1 is not empty.", e.getMessage());
    }

    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));

    ftpFs.close();
  }

  /**
   * Test a create directory functions
   * @throws Exception
   */
  @Test(timeout = 3000)
  public void testCreateDirectories() throws Exception {
    FsPermission permission = new FsPermission((short) 777);

    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.create(testFile);
    try {
      ftpFs.mkdirs(testFile, permission);
      fail();
    } catch (IOException e) {
      assertEquals(
          "Can't make directory for path /test1/test2/testFile.txt since it is a file.",
          e.getMessage());
    }
    testFile = new Path("test1/test2");

    ftpFs.mkdirs(testFile, permission);
    assertTrue(ftpFs.exists(testFile));
    FileStatus status = ftpFs.getFileStatus(testFile);
    assertTrue(status.isDirectory());
    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));
  }

  /**
   * Test a copy files functions
   *
   */
  @Test(timeout = 5000)
  public void testTransferFile() throws Exception {
    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.create(testFile);
    assertTrue(ftpFs.exists(testFile));
    // copy and overwrite
    ftpFs.copyFromLocalFile(new Path(tmpFile), testFile);
    FileStatus status = ftpFs.getFileStatus(testFile);
    assertEquals(14, status.getLen());

    File localFile = new File(TEST_ROOT_DIR, "localFile");

    ftpFs.copyToLocalFile(testFile, new Path(localFile.getAbsolutePath()));
    assertTrue(localFile.exists());
    assertEquals(14, localFile.length());

    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));
  }

  /**
   * Test a get  list functions
   *
   */
  @Test(timeout = 5000)
  public void testListFile() throws Exception {
    Path testFile = new Path("test1/test2/testFile.txt");
    Path testFile2 = new Path("test1/testFile2.txt");
    Path testFile3 = new Path("testFile3.txt");
    ftpFs.copyFromLocalFile(new Path(tmpFile), testFile);
    assertTrue(ftpFs.exists(testFile));
    ftpFs.copyFromLocalFile(new Path(tmpFile), testFile2);
    assertTrue(ftpFs.exists(testFile2));
    ftpFs.copyFromLocalFile(new Path(tmpFile), testFile3);
    assertTrue(ftpFs.exists(testFile3));

    int counter = 0;
    RemoteIterator<LocatedFileStatus> it = ftpFs.listFiles(new Path("test1"),
        true);
    while (it.hasNext()) {
      counter++;
      LocatedFileStatus status = it.next();
      assertEquals(14, status.getLen());
    }
    assertEquals(2, counter);

    counter = 0;
    it = ftpFs.listFiles(new Path("/"), true);
    while (it.hasNext()) {
      counter++;
      LocatedFileStatus status = it.next();
      assertEquals(14, status.getLen());
    }
    assertEquals(3, counter);

    FileStatus[] statuses = ftpFs.listStatus(new Path("test1"));
    assertEquals(2, statuses.length);

    statuses = ftpFs.listStatus(testFile);
    assertEquals(1, statuses.length);
    assertTrue(statuses[0].isFile());

    try {
       ftpFs.listStatus(new Path("test2"));
    } catch (IOException e) {
      assertEquals("File /test2 does not exist.", e.getMessage());
    }

    ftpFs.delete(new Path("test1"), true);
    ftpFs.delete(new Path("testFile3.txt"), false);
    assertFalse(ftpFs.exists(new Path("test1")));
  }

  /**
   * test rename files functions
   */
  @Test(timeout = 3000)
  public void testRenameFile() throws Exception {
    Path testFile = new Path("test1/test2/testFile.txt");
    Path copyFile = new Path("test1/test2/copyFile.txt");
    ftpFs.copyFromLocalFile(new Path(tmpFile), testFile);
    assertTrue(ftpFs.exists(testFile));

    try {
      ftpFs.rename(new Path("test1/sss"), copyFile);
      fail();
    } catch (IOException e) {
      assertEquals("Source path test1/sss does not exist", e.getMessage());
    }

    try {
      ftpFs.rename(testFile, testFile);
      fail();
    } catch (IOException e) {
      assertEquals(
          "Destination path test1/test2/testFile.txt already exist, cannot rename!",
          e.getMessage());
    }
    try {
      ftpFs.rename(testFile, new Path("test1/copyFile.txt"));
      fail();
    } catch (IOException e) {
      assertEquals(
          "Cannot rename parent(source): /test1/test2, parent(destination):  /test1",
          e.getMessage());
    }
    ftpFs.rename(testFile, copyFile);
    assertFalse(ftpFs.exists(testFile));
    assertTrue(ftpFs.exists(copyFile));

    FileStatus status = ftpFs.getFileStatus(copyFile);
    assertEquals(14, status.getLen());

    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));
  }

  /**
   * Test a FTPInputStream class
   */
  @SuppressWarnings("resource")
  @Test(timeout = 3000)
  public void testFTPInputStream() throws Exception {
    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.copyFromLocalFile(new Path(tmpFile), testFile);
    assertTrue(ftpFs.exists(testFile));

    Configuration configuration = getConfiguration();
    FTPClient client = new FTPClient();
    client.connect(configuration.get("fs.ftp.host"), port);

    InputStream is = new FileInputStream(new File(tmpFile));
    try {
      new FTPInputStream(null, client, FileSystem.getStatistics(getURI()
          .getScheme(), FTPFileSystem.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Null InputStream", e.getMessage());
    }

    try {
      new FTPInputStream(is, null, FileSystem.getStatistics(getURI()
          .getScheme(), FTPFileSystem.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("FTP client null or not connected", e.getMessage());
    }

    FTPInputStream ftpInputstream = new FTPInputStream(is, client,
        FileSystem.getStatistics(getURI().getScheme(), FTPFileSystem.class));

    int counter = 0;
    while (ftpInputstream.read() >= 0) {
      counter++;
      assertEquals(counter, ftpInputstream.getPos());
    }
    assertEquals(14, counter);

    assertFalse(ftpInputstream.markSupported());

    try {
      ftpInputstream.seek(0);
      fail();
    } catch (IOException e) {
      assertEquals("Seek not supported", e.getMessage());
    }
    try {
      ftpInputstream.seekToNewSource(0);
      fail();
    } catch (IOException e) {
      assertEquals("Seek not supported", e.getMessage());
    }
    try {
      ftpInputstream.reset();
      fail();
    } catch (IOException e) {
      assertEquals("Mark not supported", e.getMessage());
    }

    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));
  }

  /**
   * test FtpFs
   */
  @Test(timeout = 3000)
  public void testFtpFs() throws Exception {
    FtpFs test = new FtpFs(getURI(), getConfiguration());
    assertEquals(21, test.getUriDefaultPort());
    assertNotNull(test.getServerDefaults());
  }

  private static Configuration getConfiguration() {
    Configuration configuration = new Configuration();
    configuration
        .set(FileSystem.FS_DEFAULT_NAME_KEY, "ftp://localhost:" + port);
    configuration.set("fs.ftp.host", "localhost");
    configuration.setInt("fs.ftp.host.port", port);
    configuration.set("fs.ftp.user.localhost", "user");
    configuration.set("fs.ftp.password.localhost", "secret");
    return configuration;
  }

  private static URI getURI() throws URISyntaxException {
    return new URI("ftp://localhost:" + port);
  }

  private static String createFile(String content) throws Exception {
    File tmpFile = new File(LOCAL_DIR, "tmp.txt");
    BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile));
    writer.write(content);
    writer.flush();
    writer.close();
    return tmpFile.getAbsolutePath();
  }

}
