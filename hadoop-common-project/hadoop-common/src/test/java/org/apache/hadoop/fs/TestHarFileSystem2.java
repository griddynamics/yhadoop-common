package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * NB: to run this test from an IDE make sure the 
 * folder "hadoop-common-project/hadoop-common/src/main/resources/" is added as a source path.
 * This will allow the system to pick up the "core-default.xml" and "META-INF/services/..." resources 
 * from the class-path in the runtime.
 */
public class TestHarFileSystem2 {

  private static final String ROOT_PATH = System.getProperty("test.build.data", "build/test/data");
  private static final Path rootPath = new Path(new File(ROOT_PATH).getAbsolutePath() + "/localfs");
  private static final Path harPath = new Path(rootPath, "path1/path2/my.har"); // NB: .har suffix is necessary

  private static HarFileSystem harFileSystem;
  private static Configuration conf;
  
  /*
   * creates and returns fully initialized HarFileSystem  
   */
  private static HarFileSystem createHarFileSysten(final Configuration conf) throws Exception {
    final FileSystem localFileSystem = FileSystem.getLocal(conf);
    localFileSystem.initialize(new URI("file:///"), conf);
    localFileSystem.mkdirs(rootPath);
    localFileSystem.mkdirs(harPath);
    final Path indexPath = new Path(harPath, "_index");
    final Path masterIndexPath = new Path(harPath, "_masterindex");
    localFileSystem.createNewFile(indexPath);
      assertTrue(localFileSystem.exists(indexPath));
    localFileSystem.createNewFile(masterIndexPath);
      assertTrue(localFileSystem.exists(masterIndexPath));
    
    // write Har version into the master index:
    final FSDataOutputStream fsdos = localFileSystem.create(masterIndexPath);
    try {
      String versionString = HarFileSystem.VERSION + "\n";
      fsdos.write(versionString.getBytes("UTF-8"));
      fsdos.flush();
    } finally {
      fsdos.close();
    }
    
    final HarFileSystem harFileSystem = new HarFileSystem(localFileSystem);
    final URI uri = new URI("har://" + harPath.toString()); 
    harFileSystem.initialize(uri, conf);
    return harFileSystem;
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    final File rootDirIoFile = new File(rootPath.toUri().getPath());
    rootDirIoFile.mkdirs();
    if (!rootDirIoFile.exists()) {
      throw new IOException("Failed to create temp directory ["+rootDirIoFile.getAbsolutePath()+"]");
    }
    // create Har to test:
    conf = new Configuration();
    harFileSystem = createHarFileSysten(conf);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    // close Har FS:
    final FileSystem harFS = harFileSystem;
    if (harFS != null) {
      harFS.close();
      harFileSystem = null;
    }
    // cleanup: delete all the temporary files: 
    final File rootDirIoFile = new File(rootPath.toUri().getPath());
    if (rootDirIoFile.exists()) {
      FileUtil.fullyDelete(rootDirIoFile);
    }
    if (rootDirIoFile.exists()) {
      throw new IOException("Failed to delete temp directory ["+rootDirIoFile.getAbsolutePath()+"]");
    }
  }
  
  @Test
  public void testHarFileSystemBasics() throws Exception {
    // check Har version:
    assertEquals(HarFileSystem.VERSION, harFileSystem.getHarVersion());

    // check Har URI:
    final URI harUri = harFileSystem.getUri();
    assertEquals(harPath.toUri().getPath(), harUri.getPath());
    assertEquals("har", harUri.getScheme());
    
    // check Har home path:
    final Path homePath = harFileSystem.getHomeDirectory();
    assertEquals(harPath.toUri().getPath(), homePath.toUri().getPath());

    // check working directory:
    final Path workDirPath0 = harFileSystem.getWorkingDirectory();
    assertEquals(homePath, workDirPath0);
    
    // check that its impossible to reset the working directory (#setWorkingDirectory should have no effect): 
    harFileSystem.setWorkingDirectory(new Path("/foo/bar"));
    assertEquals(workDirPath0, harFileSystem.getWorkingDirectory());
  }
  
  // TODO: 
  // 1) measure the coverage again with org.apache.hadoop.tools.TestHadoopArchives (hadoop-tools/hadoop-archives/ module) test;
  // 2) after that, if the coverage is still not enough, test the following methods: 
  // #listStatus()
  // #makeQualified()
  // #open(2)
  // #copyToLocalFile()
  // #getFileBlockLocations()
//  @Test
//  public void testRealHarFileSystemReadOperations() throws Exception {
//  }
  
}
