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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

public class TestMapFile {
  
  private static final Path TEST_DIR = new Path(
      System.getProperty("test.build.data", "/tmp"),
      TestMapFile.class.getSimpleName());
  
  private static Configuration conf = new Configuration();

  @Before
  public void setup() throws Exception {
    LocalFileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(TEST_DIR) && !fs.delete(TEST_DIR, true)) {
      Assert.fail("Can't clean up test root dir");
    }
    fs.mkdirs(TEST_DIR);
  }
  
  private static final Progressable defaultProgressable = new Progressable() {
    @Override
    public void progress() {
    }
  };

  private static final CompressionCodec defaultCodec = new CompressionCodec() {
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
        throws IOException {
      return null;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
        Compressor compressor) throws IOException {
      return null;
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
      return null;
    }

    @Override
    public Compressor createCompressor() {
      return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
        throws IOException {
      return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in,
        Decompressor decompressor) throws IOException {
      return null;
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
      return null;
    }

    @Override
    public Decompressor createDecompressor() {
      return null;
    }

    @Override
    public String getDefaultExtension() {
      return null;
    }
  };

  private MapFile.Writer createWriter(String fileName,
      Class<? extends WritableComparable<?>> keyClass,
      Class<? extends Writable> valueClass) throws IOException {
    Path dirName = new Path(TEST_DIR, fileName);
    MapFile.Writer.setIndexInterval(conf, 4);
    return new MapFile.Writer(conf, dirName, MapFile.Writer.keyClass(keyClass),
        MapFile.Writer.valueClass(valueClass));
  }

  private MapFile.Reader createReader(String fileName,
      Class<? extends WritableComparable<?>> keyClass) throws IOException {
    Path dirName = new Path(TEST_DIR, fileName);
    return new MapFile.Reader(dirName, conf,
        MapFile.Reader.comparator(new WritableComparator(keyClass)));
  }
  
  /**
   * test {@code MapFile.Reader.getClosest()} method 
   *
   */
  @Test
  public void testGetClosestOnCurrentApi() throws Exception {
    final String TEST_PREFIX = "testGetClosestOnCurrentApi.mapfile";
    MapFile.Writer writer = createWriter(TEST_PREFIX, Text.class, Text.class);
    int FIRST_KEY = 1;
    // Test keys: 11,21,31,...,91
    for (int i = FIRST_KEY; i < 100; i += 10) {      
      Text t = new Text(Integer.toString(i));
      writer.append(t, t);
    }
    writer.close();

    MapFile.Reader reader = createReader(TEST_PREFIX, Text.class);
    Text key = new Text("55");
    Text value = new Text();

    // Test get closest with step forward
    Text closest = (Text) reader.getClosest(key, value);
    assertEquals(new Text("61"), closest);

    // Test get closest with step back
    closest = (Text) reader.getClosest(key, value, true);
    assertEquals(new Text("51"), closest);

    // Test get closest when we pass explicit key
    final Text explicitKey = new Text("21");
    closest = (Text) reader.getClosest(explicitKey, value);
    assertEquals(new Text("21"), explicitKey);

    // Test what happens at boundaries. Assert if searching a key that is
    // less than first key in the mapfile, that the first key is returned.
    key = new Text("00");
    closest = (Text) reader.getClosest(key, value);
    assertEquals(FIRST_KEY, Integer.parseInt(closest.toString()));

    // Assert that null is returned if key is > last entry in mapfile.
    key = new Text("92");
    closest = (Text) reader.getClosest(key, value);
    assertNull("Not null key in testGetClosestWithNewCode", closest);

    // If we were looking for the key before, we should get the last key
    closest = (Text) reader.getClosest(key, value, true);
    assertEquals(new Text("91"), closest);
  }
  
  /**
   * test {@code MapFile.Reader.midKey() } method 
   */
  @Test
  public void testMidKeyOnCurrentApi() throws Exception {
    // Write a mapfile of simple data: keys are
    final String TEST_PREFIX = "testMidKeyOnCurrentApi.mapfile";
    MapFile.Writer writer = createWriter(TEST_PREFIX, IntWritable.class,
        IntWritable.class);
    // 0,1,....9
    int SIZE = 10;
    for (int i = 0; i < SIZE; i++)
      writer.append(new IntWritable(i), new IntWritable(i));
    writer.close();

    MapFile.Reader reader = createReader(TEST_PREFIX, IntWritable.class);
    assertEquals(new IntWritable((SIZE - 1) / 2), reader.midKey());
  }
  
  /**
   * test  {@code MapFile.Writer.rename()} method 
   */
  @Test
  public void testRename() {
    final String NEW_FILE_NAME = "test-new.mapfile";
    final String OLD_FILE_NAME = "test-old.mapfile";
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      MapFile.Writer writer = createWriter(OLD_FILE_NAME, IntWritable.class,
          IntWritable.class);
      writer.close();
      MapFile.rename(fs, new Path(TEST_DIR, OLD_FILE_NAME).toString(), 
          new Path(TEST_DIR, NEW_FILE_NAME).toString());
      MapFile.delete(fs, new Path(TEST_DIR, NEW_FILE_NAME).toString());
    } catch (IOException ex) {
      fail("testRename error " + ex);
    }
  }
  
  /**
   * test {@code MapFile.rename()} 
   *  method with throwing {@code IOException}  
   */
  @Test
  public void testRenameWithException() {
    final String ERROR_MESSAGE = "Can't rename file";
    final String NEW_FILE_NAME = "test-new.mapfile";
    final String OLD_FILE_NAME = "test-old.mapfile";
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      FileSystem spyFs = spy(fs);

      MapFile.Writer writer = createWriter(OLD_FILE_NAME, IntWritable.class,
          IntWritable.class);
      writer.close();

      Path oldDir = new Path(TEST_DIR, OLD_FILE_NAME);
      Path newDir = new Path(TEST_DIR, NEW_FILE_NAME);
      when(spyFs.rename(oldDir, newDir)).thenThrow(
          new IOException(ERROR_MESSAGE));

      MapFile.rename(spyFs, oldDir.toString(), newDir.toString());
      fail("testRenameWithException no exception error !!!");
    } catch (IOException ex) {
      assertEquals("testRenameWithException invalid IOExceptionMessage !!!",
          ex.getMessage(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testRenameWithFalse() {
    final String ERROR_MESSAGE = "Could not rename";
    final String NEW_FILE_NAME = "test-new.mapfile";
    final String OLD_FILE_NAME = "test-old.mapfile";
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      FileSystem spyFs = spy(fs);

      MapFile.Writer writer = createWriter(OLD_FILE_NAME, IntWritable.class,
          IntWritable.class);
      writer.close();

      Path oldDir = new Path(TEST_DIR, OLD_FILE_NAME);
      Path newDir = new Path(TEST_DIR, NEW_FILE_NAME);
      when(spyFs.rename(oldDir, newDir)).thenReturn(false);

      MapFile.rename(spyFs, oldDir.toString(), newDir.toString());
      fail("testRenameWithException no exception error !!!");
    } catch (IOException ex) {
      assertTrue("testRenameWithFalse invalid IOExceptionMessage error !!!", ex
          .getMessage().startsWith(ERROR_MESSAGE));
    }
  }
  
  /**
   * test throwing {@code IOException} in {@code MapFile.Writer} constructor    
   */
  @Test
  public void testWriteWithFailDirCreation() {
    String ERROR_MESSAGE = "Mkdirs failed to create directory";
    Path dirName = new Path(TEST_DIR, "fail.mapfile");
    MapFile.Writer writer = null;
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      FileSystem spyFs = spy(fs);
      Path pathSpy = spy(dirName);
      when(pathSpy.getFileSystem(conf)).thenReturn(spyFs);
      when(spyFs.mkdirs(dirName)).thenReturn(false);

      writer = new MapFile.Writer(conf, pathSpy,
          MapFile.Writer.keyClass(IntWritable.class),
          MapFile.Writer.valueClass(Text.class));
      fail("testWriteWithFailDirCreation error !!!");
    } catch (IOException ex) {
      assertTrue("testWriteWithFailDirCreation ex error !!!", ex.getMessage()
          .startsWith(ERROR_MESSAGE));
    } finally {
      if (writer != null)
        try {
          writer.close();
        } catch (IOException e) {
        }
    }
  }

  /**
   * test {@code MapFile.Reader.finalKey()} method
   */
  @Test
  public void testOnFinalKey() {
    final String TEST_METHOD_KEY = "testOnFinalKey.mapfile";
    int SIZE = 10;
    try {
      MapFile.Writer writer = createWriter(TEST_METHOD_KEY, IntWritable.class,
          IntWritable.class);
      for (int i = 0; i < SIZE; i++)
        writer.append(new IntWritable(i), new IntWritable(i));
      writer.close();

      MapFile.Reader reader = createReader(TEST_METHOD_KEY, IntWritable.class);
      IntWritable expectedKey = new IntWritable(0);
      reader.finalKey(expectedKey);
      assertEquals("testOnFinalKey not same !!!", expectedKey, new IntWritable(
          9));
    } catch (IOException ex) {
      fail("testOnFinalKey error !!!");
    }
  }
  
  /**
   * test {@code MapFile.Writer} constructor with key, value
   * and validate it with {@code keyClass(), valueClass()} methods 
   */
  @Test
  public void testKeyValueClasses() {
    Class<? extends WritableComparable<?>> keyClass = IntWritable.class;
    Class<?> valueClass = Text.class;
    try {
      createWriter("testKeyValueClasses.mapfile", IntWritable.class, Text.class);
      assertNotNull("writer key class null error !!!",
          MapFile.Writer.keyClass(keyClass));
      assertNotNull("writer value class null error !!!",
          MapFile.Writer.valueClass(valueClass));
    } catch (IOException ex) {
      fail(ex.getMessage());
    }
  }
  
  /**
   * test {@code MapFile.Reader.getClosest() } with wrong class key
   */
  @Test
  public void testReaderGetClosest() throws Exception {
    final String TEST_METHOD_KEY = "testReaderWithWrongKeyClass.mapfile";
    try {
      MapFile.Writer writer = createWriter(TEST_METHOD_KEY, IntWritable.class,
          Text.class);

      for (int i = 0; i < 10; i++)
        writer.append(new IntWritable(i), new Text("value" + i));
      writer.close();

      MapFile.Reader reader = createReader(TEST_METHOD_KEY, Text.class);
      reader.getClosest(new Text("2"), new Text(""));
      fail("no excepted exception in testReaderWithWrongKeyClass !!!");
    } catch (IOException ex) {
      /* Should be thrown to pass the test */
    }
  }
  
  /**
   * test {@code MapFile.Writer.append() } with wrong key class
   */
  @Test
  public void testReaderWithWrongValueClass() {
    final String TEST_METHOD_KEY = "testReaderWithWrongValueClass.mapfile";
    try {
      MapFile.Writer writer = createWriter(TEST_METHOD_KEY, IntWritable.class,
          Text.class);
      writer.append(new IntWritable(0), new IntWritable(0));
      fail("no excepted exception in testReaderWithWrongKeyClass !!!");
    } catch (IOException ex) {
      /* Should be thrown to pass the test */
    }
  }
  
  /**
   * test {@code MapFile.Reader.next(key, value)} for iteration.
   */
  @Test
  public void testReaderKeyIteration() {
    final String TEST_METHOD_KEY = "testReaderKeyIteration.mapfile";
    int SIZE = 10;
    int ITERATIONS = 5;
    try {
      MapFile.Writer writer = createWriter(TEST_METHOD_KEY, IntWritable.class,
          Text.class);
      int start = 0;
      for (int i = 0; i < SIZE; i++)
        writer.append(new IntWritable(i), new Text("Value:" + i));
      writer.close();

      MapFile.Reader reader = createReader(TEST_METHOD_KEY, IntWritable.class);
      // test iteration
      Writable startValue = new Text("Value:" + start);
      int i = 0;
      while (i++ < ITERATIONS) {
        IntWritable key = new IntWritable(start);
        Writable value = startValue;
        while (reader.next(key, value)) {
          assertNotNull(key);
          assertNotNull(value);
        }
        reader.reset();
      }
      assertTrue("reader seek error !!!",
          reader.seek(new IntWritable(SIZE / 2)));
      assertFalse("reader seek error !!!",
          reader.seek(new IntWritable(SIZE * 2)));
    } catch (IOException ex) {
      fail("reader seek error !!!");
    }
  }

  /**
   * test {@code MapFile.Writer.testFix} method
   */
  @Test
  public void testFix() {
    final String INDEX_LESS_MAP_FILE = "testFix.mapfile";
    int PAIR_SIZE = 20;
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      Path dir = new Path(TEST_DIR, INDEX_LESS_MAP_FILE);
      MapFile.Writer writer = createWriter(INDEX_LESS_MAP_FILE,
          IntWritable.class, Text.class);
      for (int i = 0; i < PAIR_SIZE; i++)
        writer.append(new IntWritable(0), new Text("value"));
      writer.close();

      File indexFile = new File(".", "." + INDEX_LESS_MAP_FILE + "/index");
      boolean isDeleted = false;
      if (indexFile.exists())
        isDeleted = indexFile.delete();

      if (isDeleted)
        assertTrue("testFix error !!!",
            MapFile.fix(fs, dir, IntWritable.class, Text.class, true, conf) == PAIR_SIZE);
    } catch (Exception ex) {
      fail("testFix error !!!");
    }
  }
  /**
   * test all available constructor for {@code MapFile.Writer}
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedConstructors() {
    String path = new Path(TEST_DIR, "writes.mapfile").toString();
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      MapFile.Writer writer = new MapFile.Writer(conf, fs, path,
          IntWritable.class, Text.class, CompressionType.RECORD);
      assertNotNull(writer);
      writer = new MapFile.Writer(conf, fs, path, IntWritable.class,
          Text.class, CompressionType.RECORD, defaultProgressable);
      assertNotNull(writer);
      writer = new MapFile.Writer(conf, fs, path, IntWritable.class,
          Text.class, CompressionType.RECORD, defaultCodec, defaultProgressable);
      assertNotNull(writer);
      writer = new MapFile.Writer(conf, fs, path,
          WritableComparator.get(Text.class), Text.class);
      assertNotNull(writer);
      writer = new MapFile.Writer(conf, fs, path,
          WritableComparator.get(Text.class), Text.class,
          SequenceFile.CompressionType.RECORD);
      assertNotNull(writer);
      writer = new MapFile.Writer(conf, fs, path,
          WritableComparator.get(Text.class), Text.class,
          CompressionType.RECORD, defaultProgressable);
      assertNotNull(writer);
      writer.close();

      MapFile.Reader reader = new MapFile.Reader(fs, path,
          WritableComparator.get(IntWritable.class), conf);
      assertNotNull(reader);
      assertNotNull("reader key is null !!!", reader.getKeyClass());
      assertNotNull("reader value in null", reader.getValueClass());

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
  
  /**
   * test {@code MapFile.Writer} constructor 
   * with IllegalArgumentException  
   *  
   */
  @Test
  public void testKeyLessWriterCreation() {
    MapFile.Writer writer = null;
    try {
      writer = new MapFile.Writer(conf, TEST_DIR);
      fail("fail in testKeyLessWriterCreation !!!");
    } catch (IllegalArgumentException ex) {
    } catch (Exception e) {
      fail("fail in testKeyLessWriterCreation. Other ex !!!");
    } finally {
      if (writer != null)
        try {
          writer.close();
        } catch (IOException e) {
        }
    }
  }
  /**
   * test {@code MapFile.Writer} constructor with IOException
   */
  @Test
  public void testPathExplosionWriterCreation() {
    Path path = new Path(TEST_DIR, "testPathExplosionWriterCreation.mapfile");
    String TEST_ERROR_MESSAGE = "Mkdirs failed to create directory "
        + path.getName();
    MapFile.Writer writer = null;
    try {
      FileSystem fsSpy = spy(FileSystem.get(conf));
      Path pathSpy = spy(path);
      when(fsSpy.mkdirs(path)).thenThrow(new IOException(TEST_ERROR_MESSAGE));

      when(pathSpy.getFileSystem(conf)).thenReturn(fsSpy);

      writer = new MapFile.Writer(conf, pathSpy,
          MapFile.Writer.keyClass(IntWritable.class),
          MapFile.Writer.valueClass(IntWritable.class));
      fail("fail in testPathExplosionWriterCreation !!!");
    } catch (IOException ex) {
      assertEquals("testPathExplosionWriterCreation ex message error !!!",
          ex.getMessage(), TEST_ERROR_MESSAGE);
    } catch (Exception e) {
      fail("fail in testPathExplosionWriterCreation. Other ex !!!");
    } finally {
      if (writer != null)
        try {
          writer.close();
        } catch (IOException e) {
        }
    }
  }

  /**
   * test {@code MapFile.Writer.append} method with desc order  
   */
  @Test
  public void testDescOrderWithThrowExceptionWriterAppend() {
    try {
      MapFile.Writer writer = createWriter(".mapfile", IntWritable.class,
          Text.class);
      writer.append(new IntWritable(2), new Text("value: " + 1));
      writer.append(new IntWritable(2), new Text("value: " + 2));
      writer.append(new IntWritable(2), new Text("value: " + 4));
      writer.append(new IntWritable(1), new Text("value: " + 3));
      fail("testDescOrderWithThrowExceptionWriterAppend not expected exception error !!!");
    } catch (IOException ex) {
    } catch (Exception e) {
      fail("testDescOrderWithThrowExceptionWriterAppend other ex throw !!!");
    }
  }

  @Test
  public void testMainMethodMapFile() {
    String path = new Path(TEST_DIR, "mainMethodMapFile.mapfile").toString();
    String inFile = "mainMethodMapFile.mapfile";
    String outFile = "mainMethodMapFile.mapfile";
    String[] args = { path, outFile };
    try {
      MapFile.Writer writer = createWriter(inFile, IntWritable.class,
          Text.class);
      writer.append(new IntWritable(1), new Text("test_text1"));
      writer.append(new IntWritable(2), new Text("test_text2"));
      writer.close();
      MapFile.main(args);
    } catch (Exception ex) {
      fail("testMainMethodMapFile error !!!");
    }
  }

  /**
   * Test getClosest feature.
   * 
   * @throws Exception
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testGetClosest() throws Exception {
    // Write a mapfile of simple data: keys are
    Path dirName = new Path(TEST_DIR, "testGetClosest.mapfile");
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
    // Make an index entry for every third insertion.
    MapFile.Writer.setIndexInterval(conf, 3);
    MapFile.Writer writer = new MapFile.Writer(conf, fs,
        qualifiedDirName.toString(), Text.class, Text.class);
    // Assert that the index interval is 1
    assertEquals(3, writer.getIndexInterval());
    // Add entries up to 100 in intervals of ten.
    final int FIRST_KEY = 10;
    for (int i = FIRST_KEY; i < 100; i += 10) {
      String iStr = Integer.toString(i);
      Text t = new Text("00".substring(iStr.length()) + iStr);
      writer.append(t, t);
    }
    writer.close();
    // Now do getClosest on created mapfile.
    MapFile.Reader reader = new MapFile.Reader(qualifiedDirName, conf);
    try {
    Text key = new Text("55");
    Text value = new Text();
    Text closest = (Text) reader.getClosest(key, value);
    // Assert that closest after 55 is 60
    assertEquals(new Text("60"), closest);
    // Get closest that falls before the passed key: 50
    closest = (Text) reader.getClosest(key, value, true);
    assertEquals(new Text("50"), closest);
    // Test get closest when we pass explicit key
    final Text TWENTY = new Text("20");
    closest = (Text) reader.getClosest(TWENTY, value);
    assertEquals(TWENTY, closest);
    closest = (Text) reader.getClosest(TWENTY, value, true);
    assertEquals(TWENTY, closest);
    // Test what happens at boundaries. Assert if searching a key that is
    // less than first key in the mapfile, that the first key is returned.
    key = new Text("00");
    closest = (Text) reader.getClosest(key, value);
    assertEquals(FIRST_KEY, Integer.parseInt(closest.toString()));

    // If we're looking for the first key before, and we pass in a key before
    // the first key in the file, we should get null
    closest = (Text) reader.getClosest(key, value, true);
    assertNull(closest);

    // Assert that null is returned if key is > last entry in mapfile.
    key = new Text("99");
    closest = (Text) reader.getClosest(key, value);
    assertNull(closest);

    // If we were looking for the key before, we should get the last key
    closest = (Text) reader.getClosest(key, value, true);
    assertEquals(new Text("90"), closest);
    } finally {
      reader.close();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMidKey() throws Exception {
    // Write a mapfile of simple data: keys are
    Path dirName = new Path(TEST_DIR, "testMidKey.mapfile");
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);

    MapFile.Writer writer = new MapFile.Writer(conf, fs,
        qualifiedDirName.toString(), IntWritable.class, IntWritable.class);
    writer.append(new IntWritable(1), new IntWritable(1));
    writer.close();
    // Now do getClosest on created mapfile.
    MapFile.Reader reader = new MapFile.Reader(qualifiedDirName, conf);
    try {
      assertEquals(new IntWritable(1), reader.midKey());
    } finally {
      reader.close();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMidKeyEmpty() throws Exception {
    // Write a mapfile of simple data: keys are
    Path dirName = new Path(TEST_DIR, "testMidKeyEmpty.mapfile");
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);

    MapFile.Writer writer = new MapFile.Writer(conf, fs,
        qualifiedDirName.toString(), IntWritable.class, IntWritable.class);
    writer.close();
    // Now do getClosest on created mapfile.
    MapFile.Reader reader = new MapFile.Reader(qualifiedDirName, conf);
    try {
      assertEquals(null, reader.midKey()); 
    } finally {
      reader.close();
    }
  }
}
