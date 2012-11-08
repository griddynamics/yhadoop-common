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

package org.apache.hadoop.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * test {@link HadoopArchives}
 */
public class TestHadoopArchives {

  public static final String HADOOP_ARCHIVES_JAR = JarFinder.getJar(HadoopArchives.class);

  {
    ((Log4JLogger)LogFactory.getLog(org.apache.hadoop.security.Groups.class)
        ).getLogger().setLevel(Level.ERROR);
    ((Log4JLogger)org.apache.hadoop.ipc.Server.LOG
        ).getLogger().setLevel(Level.ERROR);
    ((Log4JLogger)org.apache.hadoop.util.AsyncDiskService.LOG
        ).getLogger().setLevel(Level.ERROR);
  }

  private static final String inputDir = "input";

  private Path inputPath;
  private Path archivePath;
  private final List<String> fileList = new ArrayList<String>();
  private MiniDFSCluster dfscluster;
  private FileSystem fs;
  private MiniMRClientCluster miniMRClientCluster;
  
  static private String createFile(Path root, FileSystem fs, String... dirsAndFile 
      ) throws IOException {
    String fileBaseName = dirsAndFile[dirsAndFile.length - 1]; 
    return createFile(root, fs, fileBaseName.getBytes("UTF-8"), dirsAndFile);
  }
  
  static private String createFile(Path root, FileSystem fs, byte[] fileContent, String... dirsAndFile
      ) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (String segment: dirsAndFile) {
      if (sb.length() > 0) {
        sb.append(Path.SEPARATOR);  
      }
      sb.append(segment);
    }
    final Path f = new Path(root, sb.toString());
    final FSDataOutputStream out = fs.create(f);
    try {
      out.write(fileContent);
    } finally {
      out.close();
    }
    return sb.toString();
  }
    
  @Before
  public void setUp() throws Exception {
    fileList.clear();
    
    final Configuration configuration = new Configuration(); 
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    builder.numDataNodes(2);
    builder.format(true);
    builder.racks(null);
    dfscluster = builder.build();
    
    fs = dfscluster.getFileSystem();
    
    miniMRClientCluster 
      = MiniMRClientClusterFactory.create(getClass(), 1, configuration);
    
    inputPath = new Path(fs.getHomeDirectory(), inputDir); 
    archivePath = new Path(fs.getHomeDirectory(), "archive");
    fs.delete(inputPath, true);
    fs.mkdirs(inputPath);
    fileList.add(createFile(inputPath, fs, "a"));
    fileList.add(createFile(inputPath, fs, "b"));
    fileList.add(createFile(inputPath, fs, "c"));
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (miniMRClientCluster != null) {
        miniMRClientCluster.stop();
      }
      if (dfscluster != null) {
        dfscluster.shutdown();
      }
    } catch(Exception e) {
      System.err.println(e);
      throw e;
    }
  }
   
  @Test
  public void testRelativePath() throws Exception {
    fs.delete(archivePath, true);

    final Path sub1 = new Path(inputPath, "dir1");
    fs.mkdirs(sub1);
    createFile(inputPath, fs, sub1.getName(), "a");
    final Configuration conf = miniMRClientCluster.getConfig();
    final FsShell shell = new FsShell(conf);

    final List<String> originalPaths = lsr(shell, "input");
    System.out.println("originalPath: " + originalPaths);
    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() +":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    {
      final String harName = "foo.har";
      final String[] args = {
          "-archiveName",
          harName,
          "-p",
          "input",
          "*",
          "archive"
      };
      System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH, HADOOP_ARCHIVES_JAR);
      final Configuration conf2 = miniMRClientCluster.getConfig();
      final HadoopArchives har = new HadoopArchives(conf2);
      assertEquals(0, ToolRunner.run(har, args));

      //compare results
      final List<String> harPaths = lsr(shell, prefix + harName);
      assertEquals(originalPaths, harPaths);
    }
  }

  @Test
  public void testPathWithSpaces() throws Exception {
    fs.delete(archivePath, true);

    //create files/directories with spaces
    createFile(inputPath, fs, "c c");
    final Path sub1 = new Path(inputPath, "sub 1");
    fs.mkdirs(sub1);
    createFile(sub1, fs, "file x y z");
    createFile(sub1, fs, "file");
    createFile(sub1, fs, "x");
    createFile(sub1, fs, "y");
    createFile(sub1, fs, "z");
    final Path sub2 = new Path(inputPath, "sub 1 with suffix");
    fs.mkdirs(sub2);
    createFile(sub2, fs, "z");
    
    final Configuration conf = miniMRClientCluster.getConfig();
    final FsShell shell = new FsShell(conf);

    final String inputPathStr = inputPath.toUri().getPath();
    System.out.println("inputPathStr = " + inputPathStr);

    final List<String> originalPaths = lsr(shell, inputPathStr);
    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() +":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    {//Enable space replacement
      final String harName = "foo.har";
      final String[] args = {
          "-archiveName",
          harName,
          "-p",
          inputPathStr,
          "*",
          archivePath.toString()
      };
      System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH, HADOOP_ARCHIVES_JAR);
      final Configuration conf2 = miniMRClientCluster.getConfig();
      final HadoopArchives har = new HadoopArchives(conf2);
      assertEquals(0, ToolRunner.run(har, args));

      //compare results
      final List<String> harPaths = lsr(shell, prefix + harName);
      assertEquals(originalPaths, harPaths);
    }
  }

  private static List<String> lsr(final FsShell shell, String dir
      ) throws Exception {
    System.out.println("lsr root=" + dir);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      assertEquals(0, shell.run(new String[]{"-lsr", dir}));
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("lsr results:\n" + results);
    String dirname = dir;
    if (dir.lastIndexOf(Path.SEPARATOR) != -1 ) {
      dirname = dir.substring(dir.lastIndexOf(Path.SEPARATOR));
    }

    final List<String> paths = new ArrayList<String>();
    for(StringTokenizer t = new StringTokenizer(results, "\n");
        t.hasMoreTokens(); ) {
      final String s = t.nextToken();
      final int i = s.indexOf(dirname);
      if (i >= 0) {
        paths.add(s.substring(i + dirname.length()));
      }
    }
    Collections.sort(paths);
    System.out.println("lsr paths = " + paths.toString().replace(", ", ",\n  "));
    return paths;
  }
  
  @Test
  public void testReadFileContent() throws Exception {
    fs.delete(archivePath, true);

    fileList.add(createFile(inputPath, fs, "c c"));
    final Path sub1 = new Path(inputPath, "sub 1");
    fs.mkdirs(sub1);
    fileList.add(createFile(inputPath, fs, sub1.getName(), "file x y z"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "file"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "x"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "y"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "z"));
    final Path sub2 = new Path(inputPath, "sub 1 with suffix");
    fs.mkdirs(sub2);
    fileList.add(createFile(inputPath, fs, sub2.getName(), "z"));
    // Generate a big binary file content:
    final byte[] binContent = prepareBin();
    fileList.add(createFile(inputPath, fs, binContent, sub2.getName(), "bin"));
    fileList.add(createFile(inputPath, fs, new byte[0], sub2.getName(), "zero-length"));

    final String inputPathStr = inputPath.toUri().getPath();
    System.out.println("inputPathStr = " + inputPathStr);

    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    final String harName = "foo.har";
    final String fullHarPathStr = prefix + harName;
    final String[] args = { "-archiveName", harName, "-p", inputPathStr, "*",
        archivePath.toString() };
    System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
        HADOOP_ARCHIVES_JAR);
    final Configuration conf = miniMRClientCluster.getConfig();
    final HadoopArchives har = new HadoopArchives(conf);
    assertEquals(0, ToolRunner.run(har, args));

    // Create fresh HarFs:
    final HarFileSystem harFileSystem = new HarFileSystem(fs);
    try {
      final URI harUri = new URI(fullHarPathStr);
      harFileSystem.initialize(harUri, fs.getConf());
      // now read the file content and compare it against the expected:
      int readFileCount = 0;
      for (final String pathStr0 : fileList) {
        final Path path = new Path(fullHarPathStr + Path.SEPARATOR + pathStr0);
        final String baseName = path.getName();
        final FileStatus status = harFileSystem.getFileStatus(path);
        if (status.isFile()) {
          // read the file:
          final FSDataInputStream fsdis1 = harFileSystem.open(path);
          final byte[] actualContent1 = readAllSimple(fsdis1, true/* close */);
          final FSDataInputStream fsdis2 = harFileSystem.open(path);
          final byte[] actualContent2 = readAllWithBuffer(fsdis2, true/* close */);
          assertArrayEquals(actualContent1, actualContent2);
          if ("bin".equals(baseName)) {
            assertArrayEquals(binContent, actualContent1);
          } else if ("zero-length".equals(baseName)) {
            assertEquals(0, actualContent1.length);
          } else {
            String actual = new String(actualContent1, "UTF-8");
            assertEquals(baseName, actual);
          }
          readFileCount++;
        }
      }
      assertEquals(fileList.size(), readFileCount);
    } finally {
      harFileSystem.close();
    }
  }
  
  private static byte[] readAllSimple(FSDataInputStream fsdis, boolean close) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      int b;
      while (true) {
        b = fsdis.read();
        if (b < 0) {
          break;
        } else {
          baos.write(b);
        }
      }
      baos.close();
      return baos.toByteArray();
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }

  private static byte[] readAllWithBuffer(FSDataInputStream fsdis, boolean close)
      throws IOException {
    try {
      final int available = fsdis.available();
      final byte[] buffer;
      final ByteArrayOutputStream baos;
      if (available < 0) {
        buffer = new byte[1024];
        baos = new ByteArrayOutputStream(buffer.length * 2);
      } else {
        buffer = new byte[available + 1];
        baos = new ByteArrayOutputStream(available);
      }
      int readIntoBuffer = 0;
      int read; 
      while (true) {
        read = fsdis.read(buffer, readIntoBuffer, buffer.length - readIntoBuffer);
        if (read < 0) {
          // end of stream:
          if (readIntoBuffer > 0) {
            baos.write(buffer, 0, readIntoBuffer);
          }
          return baos.toByteArray();
        } else {
          readIntoBuffer += read;
          if (readIntoBuffer == buffer.length) {
            // buffer is full, need to clean the buffer.
            // drop the buffered data to baos:
            baos.write(buffer);
            // reset the counter to start reading to the buffer beginning:
            readIntoBuffer = 0;
          } else if (readIntoBuffer > buffer.length) {
            throw new IOException("Read more than the buffer length: "
                + readIntoBuffer + ", bugger length = " + buffer.length);
          }
        }
      }
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }
  
  private static byte[] prepareBin() {
    byte[] bb = new byte[77777];
    for (int i=0; i<bb.length; i++) {
      // Generate unique values, as possible:
      double d = Math.log(i + 2);
      long bits = Double.doubleToLongBits(d);
      bb[i] = (byte)bits;
    }
    return bb;
  }
}
