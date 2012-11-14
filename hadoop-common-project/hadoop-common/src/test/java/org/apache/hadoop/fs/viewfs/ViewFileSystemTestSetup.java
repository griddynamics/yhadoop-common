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
package org.apache.hadoop.fs.viewfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.junit.Assert;
import org.mortbay.log.Log;


/**
 * This class is for  setup and teardown for viewFileSystem so that
 * it can be tested via the standard FileSystem tests.
 * 
 * If tests launched via ant (build.xml) the test root is absolute path
 * If tests launched via eclipse, the test root is 
 * is a test dir below the working directory. (see FileSystemTestHelper).
 * Since viewFs has no built-in wd, its wd is /user/<username> 
 *          (or /User/<username> on mac)
 * 
 * We set a viewFileSystems with mount point for 
 * /<firstComponent>" pointing to the target fs's  testdir 
 */
public class ViewFileSystemTestSetup {

  /**
   * A LocalFileSystem which places home dir under the tests root dir, to avoid modifications of the real user home 
   * while running tests.
   */
  static class TestLFS extends LocalFileSystem {
    Path home;
    TestLFS() throws IOException {
      this(new LocalFileSystem());
    }
    TestLFS(FileSystem fs) throws IOException {
      this(fs, new Path(new File(FileSystemTestHelper.TEST_ROOT_DIR).getCanonicalPath(), System.getProperty("user.name")));
    }
    TestLFS(FileSystem fs, Path home) throws IOException {
      super(fs);
      this.home = home;
    }
    public FileSystem getRawFileSystem() {
      return ((LocalFileSystem)fs).getRawFileSystem();
    }
    public Path getHomeDirectory() {
      return home;
    }
    public boolean delete(Path f, boolean recursive) throws IOException {
      Path testRootDir;
      Path path;
      try {
        testRootDir = new Path(new URI("file:///").resolve(new File(FileSystemTestHelper.TEST_ROOT_DIR).getCanonicalPath()));
        path = new Path(new URI("file:///").resolve(f.toUri()));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      assertParent(String.format("Attempt to delete dir %s which is not under the test dir %s", f, testRootDir), testRootDir, path);
      return super.delete(f, recursive);
    }
    private void assertParent(String message, Path parent, Path f) {
      if (f == null)
        Assert.fail(message);
      if (f.equals(parent))
        return;
      assertParent(message, parent, f.getParent());
    }
  }

  /**
   * 
   * @param fsTarget - the target fs of the view fs.
   * @return return the ViewFS File context to be used for tests
   * @throws Exception
   */
  static public FileSystem setupForViewFileSystem(Configuration conf, FileSystem fsTarget) throws Exception {
    /**
     * create the test root on local_fs - the  mount table will point here
     */
    Path targetOfTests = FileSystemTestHelper.getTestRootPath(fsTarget);
    // In case previous test was killed before cleanup
    Log.info("Clean up " + targetOfTests);
    fsTarget.delete(targetOfTests, true);
    fsTarget.mkdirs(targetOfTests);

    // Setup a link from viewfs to targetfs for the first component of
    // path of testdir.
    String testDir = FileSystemTestHelper.getTestRootPath(fsTarget).toUri()
        .getPath();
    String testDirFirstComponent = getDirFirstComponent(testDir);
    Log.info("add link " + testDirFirstComponent + " -- " + fsTarget.makeQualified(
        new Path(testDirFirstComponent)).toUri());
    ConfigUtil.addLink(conf, testDirFirstComponent, fsTarget.makeQualified(
        new Path(testDirFirstComponent)).toUri());

    // viewFs://home => fsTarget://home
    String homeDirRoot = fsTarget.getHomeDirectory()
        .getParent().toUri().getPath();
    ConfigUtil.setHomeDirConf(conf, homeDirRoot);
    Log.info("Home dir base " + homeDirRoot);

    FileSystem fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    return fsView;
  }

  /**
   * 
   * delete the test directory in the target  fs
   */
  static public void tearDown(FileSystem fsTarget) throws Exception {
    Path targetOfTests = FileSystemTestHelper.getTestRootPath(fsTarget);
    fsTarget.delete(targetOfTests, true);
  }
  
  public static Configuration createConfig() {
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", TestLFS.class.getName());
    conf.set("fs.viewfs.impl", ViewFileSystem.class.getName());
    return conf; 
  }

  private static String getDirFirstComponent(String dir) {
	System.out.println("Get first component of dir " + dir);
    int indexOf2ndSlash = dir.indexOf('/', 1);
    if (indexOf2ndSlash == -1)
      return dir;
    String testDirFirstComponent = dir.substring(0, indexOf2ndSlash);
    return testDirFirstComponent;
  }

}
