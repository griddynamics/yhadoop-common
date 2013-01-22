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

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.Progressable;

import junit.framework.TestCase;

public class TestBloomMapFile extends TestCase {
  private static Configuration conf = new Configuration();

  @SuppressWarnings("deprecation")
  public void testMembershipTest() throws Exception {
    // write the file
    Path dirName = new Path(System.getProperty("test.build.data", ".")
        + getName() + ".bloommapfile");
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
    conf.setInt("io.mapfile.bloom.size", 2048);
    BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, fs,
        qualifiedDirName.toString(), IntWritable.class, Text.class);
    IntWritable key = new IntWritable();
    Text value = new Text();
    for (int i = 0; i < 2000; i += 2) {
      key.set(i);
      value.set("00" + i);
      writer.append(key, value);
    }
    writer.close();

    BloomMapFile.Reader reader = new BloomMapFile.Reader(fs,
        qualifiedDirName.toString(), conf);
    // check false positives rate
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      key.set(i);
      boolean exists = reader.probablyHasKey(key);
      if (i % 2 == 0) {
        if (!exists)
          falseNeg++;
      } else {
        if (exists)
          falsePos++;
      }
    }
    reader.close();
    fs.delete(qualifiedDirName, true);
    System.out.println("False negatives: " + falseNeg);
    assertEquals(0, falseNeg);
    System.out.println("False positives: " + falsePos);
    assertTrue(falsePos < 2);
  }

  @SuppressWarnings("deprecation")
  private void checkMembershipVaryingSizedKeys(String name, List<Text> keys)
      throws Exception {
    Path dirName = new Path(System.getProperty("test.build.data", ".") + name
        + ".bloommapfile");
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
    BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, fs,
        qualifiedDirName.toString(), Text.class, NullWritable.class);
    for (Text key : keys) {
      writer.append(key, NullWritable.get());
    }
    writer.close();

    // will check for membership in the opposite order of how keys were inserted
    BloomMapFile.Reader reader = new BloomMapFile.Reader(fs,
        qualifiedDirName.toString(), conf);
    Collections.reverse(keys);
    for (Text key : keys) {
      assertTrue("False negative for existing key " + key,
          reader.probablyHasKey(key));
    }
    reader.close();
    fs.delete(qualifiedDirName, true);
  }

  public void testMembershipVaryingSizedKeysTest1() throws Exception {
    ArrayList<Text> list = new ArrayList<Text>();
    list.add(new Text("A"));
    list.add(new Text("BB"));
    checkMembershipVaryingSizedKeys(getName(), list);
  }

  public void testMembershipVaryingSizedKeysTest2() throws Exception {
    ArrayList<Text> list = new ArrayList<Text>();
    list.add(new Text("AA"));
    list.add(new Text("B"));
    checkMembershipVaryingSizedKeys(getName(), list);
  }

  /**
   * test {@code BloomMapFile.delete()} method
   */
  public void testDeleteFile() {
    try {
      String DELETABLE_FILE_NAME = "deletableFile.bloommapfile";
      FileSystem fs = FileSystem.getLocal(conf);
      Path dirName = new Path(System.getProperty("test.build.data", ".")
          + DELETABLE_FILE_NAME);
      BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, dirName,
          MapFile.Writer.keyClass(IntWritable.class),
          MapFile.Writer.valueClass(Text.class));
      assertNotNull("testDeleteFile error !!!", writer);
      BloomMapFile.delete(fs, "." + DELETABLE_FILE_NAME);
    } catch (Exception ex) {
      fail("unexpect ex in testDeleteFile !!!");
    }
  }
  
  /**
   * test {@link BloomMapFile.Reader} constructor with 
   * IOException
   */
  public void testIOExceptionInWriterConstructor() {
    String TEST_FILE_NAME = "testFile.bloommapfile";
    Path dirName = new Path(System.getProperty("test.build.data", ".")
        + TEST_FILE_NAME);
    Path dirNameSpy = org.mockito.Mockito.spy(dirName);
    try {
      BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, dirName,
          MapFile.Writer.keyClass(IntWritable.class),
          MapFile.Writer.valueClass(Text.class));
      writer.append(new IntWritable(1), new Text("123124142"));
      writer.close();

      org.mockito.Mockito.when(dirNameSpy.getFileSystem(conf)).thenThrow(
          new IOException());
      BloomMapFile.Reader reader = new BloomMapFile.Reader(dirNameSpy, conf,
          MapFile.Reader.comparator(new WritableComparator(IntWritable.class)));

      assertNull("testIOExceptionInWriterConstructor error !!!",
          reader.getBloomFilter());
      reader.close();
    } catch (Exception ex) {
      fail("unexpect ex in testIOExceptionInWriterConstructor !!!");
    }
  }

  /**
   *  test {@link BloomMapFile.Reader.get()} method 
   */
  public void testGetBloomMapFile() {
    String TEST_FILE_NAME = "getTestFile.bloommapfile";
    int SIZE = 10;
    Path dirName = new Path(System.getProperty("test.build.data", ".")
        + TEST_FILE_NAME);
    try {
      BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, dirName,
          MapFile.Writer.keyClass(IntWritable.class),
          MapFile.Writer.valueClass(Text.class));

      for (int i = 0; i < SIZE; i++) {
        writer.append(new IntWritable(i), new Text());
      }
      writer.close();

      BloomMapFile.Reader reader = new BloomMapFile.Reader(dirName, conf,
          MapFile.Reader.comparator(new WritableComparator(IntWritable.class)));

      for (int i = 0; i < SIZE; i++) {
        assertNotNull("testGetBloomMapFile error !!!",
            reader.get(new IntWritable(i), new Text()));
      }
            
      assertNull("testGetBloomMapFile error !!!",
          reader.get(new IntWritable(SIZE + 5), new Text()));
      reader.close();
    } catch (Exception ex) {
      fail("unexpect ex in testGetBloomMapFile !!!");
    }
  }

  /**
   * test {@code BloomMapFile.Writer} constructors
   */
  @SuppressWarnings("deprecation")
  public void testBloomMapFileConstructors() {
    final String TEST_PATH = System.getProperty("test.build.data", ".")
        + ".bloommapfile";
    try {
      FileSystem ts = FileSystem.get(conf);
      BloomMapFile.Writer writer1 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, IntWritable.class, Text.class, CompressionType.BLOCK,
          defaultCodec, defaultProgress);
      assertNotNull("testBloomMapFileConstructors error !!!", writer1);
      BloomMapFile.Writer writer2 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, IntWritable.class, Text.class, CompressionType.BLOCK,
          defaultProgress);
      assertNotNull("testBloomMapFileConstructors error !!!", writer2);
      BloomMapFile.Writer writer3 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, IntWritable.class, Text.class, CompressionType.BLOCK);
      assertNotNull("testBloomMapFileConstructors error !!!", writer3);
      BloomMapFile.Writer writer4 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, IntWritable.class, Text.class, CompressionType.RECORD,
          defaultCodec, defaultProgress);
      assertNotNull("testBloomMapFileConstructors error !!!", writer4);
      BloomMapFile.Writer writer5 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, IntWritable.class, Text.class, CompressionType.RECORD,
          defaultProgress);
      assertNotNull("testBloomMapFileConstructors error !!!", writer5);
      BloomMapFile.Writer writer6 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, IntWritable.class, Text.class, CompressionType.RECORD);
      assertNotNull("testBloomMapFileConstructors error !!!", writer6);
      BloomMapFile.Writer writer7 = new BloomMapFile.Writer(conf, ts,
          TEST_PATH, WritableComparator.get(Text.class), Text.class);
      assertNotNull("testBloomMapFileConstructors error !!!", writer7);
    } catch (Exception ex) {
      fail("testBloomMapFileConstructors error !!!");
    }
  }

  static final Progressable defaultProgress = new Progressable() {
    @Override
    public void progress() {
    }
  };

  static final CompressionCodec defaultCodec = new CompressionCodec() {
    @Override
    public String getDefaultExtension() {
      return null;
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
      return null;
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
      return null;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
        Compressor compressor) throws IOException {
      return null;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
        throws IOException {
      return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in,
        Decompressor decompressor) throws IOException {
      return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
        throws IOException {
      return null;
    }

    @Override
    public Decompressor createDecompressor() {
      return null;
    }

    @Override
    public Compressor createCompressor() {
      return null;
    }
  };
}
