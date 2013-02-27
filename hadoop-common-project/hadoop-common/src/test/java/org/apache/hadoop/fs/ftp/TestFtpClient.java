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
import java.io.FileWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import junit.framework.TestCase;

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
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFtpClient extends TestCase {
  private static FtpServer server;
  private static int port;
  private static File workspace = new File("target" + File.separator + "ftp");

  protected void setUp() throws Exception {
    if (!workspace.exists()) {
      workspace.mkdirs();
    }
    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory factory = new ListenerFactory();

    // set the port of the listener
    factory.setPort(0);
    factory.setImplicitSsl(false);
    // replace the default listener
    serverFactory.addListener("default", factory.createListener());
    PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
    UserManager um = userManagerFactory.createUserManager();
    BaseUser user = new BaseUser();
    user.setName("user");
    user.setPassword("secret");
    user.setAuthorities(Arrays.asList(new WritePermission(),
        new ConcurrentLoginPermission(100, 100), new TransferRatePermission(
            100, 100)));
    user.setHomeDirectory(workspace.getAbsolutePath());
    // user.setHomeDirectory("/");
    um.save(user);
    serverFactory.setUserManager(um);
    // start the server
    server = serverFactory.createServer();
    server.start();
    NioListener listener = (NioListener) serverFactory.getListeners().values()
        .iterator().next();
    port = listener.getPort();
    System.out.println("port:" + port);

  }

  @Test(timeout = 10000)
  public void testCreateFile() throws Exception {


    FTPFileSystem ftpFs = new FTPFileSystem();

    ftpFs.initialize(getURI(), getConfiguration());
    assertEquals("/", ftpFs.getHomeDirectory().toString());
    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.create(testFile);
    assertTrue(ftpFs.exists(testFile));
    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));

    ftpFs.close();
  }

  @Test(timeout = 10000)
  public void testTransferFile() throws Exception {

    String tmpfile = createFile("temproary file");

    FTPFileSystem ftpFs = new FTPFileSystem();

    ftpFs.initialize(getURI(), getConfiguration());
    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.create(testFile);
    assertTrue(ftpFs.exists(testFile));

    ftpFs.copyFromLocalFile(new Path(tmpfile), testFile);
    FileStatus status = ftpFs.getFileStatus(testFile);
    assertEquals(14, status.getLen());
    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));

    ftpFs.close();
  }

  @Test(timeout = 10000)
  public void test2() throws Exception {

    String tmpfile = createFile("temproary file");
    FTPFileSystem ftpFs = new FTPFileSystem();

    ftpFs.initialize(getURI(), getConfiguration());
    
    Path testFile = new Path("test1/test2/testFile.txt");
    ftpFs.copyFromLocalFile(new Path(tmpfile), testFile);
    assertTrue(ftpFs.exists(testFile));
    File localfile=new File(workspace.getParent()+File.separator+"locafile");
    
    ftpFs.copyToLocalFile(testFile, new Path(localfile.getAbsolutePath()));
    assertTrue(localfile.exists());
    assertEquals(14, localfile.length());
    
    ftpFs.delete(new Path("test1"), true);
    assertFalse(ftpFs.exists(new Path("test1")));

    ftpFs.close();
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.ftp.host", "localhost");
    conf.setInt("fs.ftp.host.port", port);
    conf.set("fs.ftp.user.localhost", "user");
    conf.set("fs.ftp.password.localhost", "secret");
    return conf;
  }

  private URI getURI() throws URISyntaxException{
    return new URI("ftp://localhost:" + port);
  }
  private String createFile(String content) throws Exception {
    File tmpfile = new File(workspace.getParent() + File.separator + "tmp.txt");
    BufferedWriter writer = new BufferedWriter(new FileWriter(tmpfile));
    writer.write(content);
    writer.flush();
    writer.close();
    return tmpfile.getAbsolutePath();
  }

  protected void tearDown() {
    server.stop();
  }
}
