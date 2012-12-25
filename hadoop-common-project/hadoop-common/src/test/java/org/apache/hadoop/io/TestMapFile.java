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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import junit.framework.TestCase;

public class TestMapFile extends TestCase {
  private static Configuration conf = new Configuration();           
    
  private MapFile.Writer createWriter(String fileName, 
		  Class<? extends WritableComparable<?>> keyClass, Class<? extends Writable> valueClass) throws IOException {
	Path dirName = new Path(System.getProperty("test.build.data",".") + fileName);
	MapFile.Writer.setIndexInterval(conf, 4);
	return new MapFile.Writer(conf, dirName,
		MapFile.Writer.keyClass(keyClass), MapFile.Writer.valueClass(valueClass));	  
  }
  
  private MapFile.Reader createReader(String fileName, Class<? extends WritableComparable<?>> keyClass) throws IOException {
    Path dirName = new Path(System.getProperty("test.build.data",".") + fileName);
	return new MapFile.Reader(dirName, conf, MapFile.Reader.comparator(new WritableComparator(keyClass)));
  }
  
  public void testGetClosestOnCurrentApi() throws Exception {    
    final String TEST_PREFIX = "testGetClosestOnCurrentApi.mapfile";
    deleteFileIfExists(TEST_PREFIX);
    MapFile.Writer writer = createWriter(TEST_PREFIX, Text.class, Text.class);        								
	int FIRST_KEY = 1;
	//Test keys: 11,21,31,...,91
	for (int i = FIRST_KEY; i < 100; i += 10) {
      String iStr = Integer.toString(i);
      Text t = new Text(iStr);            
      writer.append(t, t);
    }
	writer.close();
	
	MapFile.Reader reader = createReader(TEST_PREFIX, Text.class);		
	Text key = new Text("55");
    Text value = new Text();
    
    // Test get closest with step forward
    Text closest = (Text)reader.getClosest(key, value);
    assertEquals(new Text("61"), closest);
    
    // Test get closest with step back
    closest = (Text)reader.getClosest(key, value, true);
    assertEquals(new Text("51"), closest);
        
    // Test get closest when we pass explicit key
    final Text explicitKey = new Text("21");
    closest = (Text)reader.getClosest(explicitKey, value);
    assertEquals(new Text("21"), explicitKey);   
            
    // Test what happens at boundaries.  Assert if searching a key that is
    // less than first key in the mapfile, that the first key is returned.
    key = new Text("00");
    closest = (Text)reader.getClosest(key, value);
    assertEquals(FIRST_KEY, Integer.parseInt(closest.toString()));
    
    // Assert that null is returned if key is > last entry in mapfile.
    key = new Text("92");
    closest = (Text)reader.getClosest(key, value);
    assertNull("Not null key in testGetClosestWithNewCode", closest);
    
    // If we were looking for the key before, we should get the last key
    closest = (Text)reader.getClosest(key, value, true);
    assertEquals(new Text("91"), closest);    
  }
  
  public void testMidKeyOnCurrentApi() throws Exception {
    // Write a mapfile of simple data: keys are 
	final String TEST_PREFIX = "testMidKeyOnCurrentApi.mapfile";
    MapFile.Writer writer = createWriter(TEST_PREFIX, IntWritable.class, IntWritable.class);           
    //0,1,....9
    int SIZE = 10;
    for (int i = 0; i < SIZE; i++) 
	  writer.append(new IntWritable(i), new IntWritable(i));
    writer.close();
    
    MapFile.Reader reader = createReader(TEST_PREFIX, IntWritable.class);    
    assertEquals(new IntWritable((SIZE-1)/2), reader.midKey());            
  }
  
  public void testRename() {	 
    final String NEW_FILE_NAME = "test-new.mapfile"; 
    final String OLD_FILE_NAME = "test-old.mapfile";    
    final String PATH_PREFIX = System.getProperty("test.build.data",".");	
	try {
	    FileSystem fs = FileSystem.getLocal(conf);  
	    MapFile.Writer writer = createWriter(OLD_FILE_NAME, IntWritable.class, IntWritable.class); 	
	    writer.close();
		MapFile.rename(fs, PATH_PREFIX + OLD_FILE_NAME, PATH_PREFIX + NEW_FILE_NAME);
		MapFile.delete(fs, PATH_PREFIX + NEW_FILE_NAME);		
	} catch(IOException ex) {
	  fail(ex.getMessage());	
	}
  } 
  
  public void testKeyValueClasses() {	
	Class<? extends WritableComparable<?>> keyClass = IntWritable.class;
	Class<?> valueClass = Text.class;
	try {
	  createWriter("testKeyValueClasses.mapfile", IntWritable.class, Text.class); 			  			 
	  assertNotNull("writer key class null error !!!", MapFile.Writer.keyClass(keyClass));
	  assertNotNull("writer value class null error !!!", MapFile.Writer.valueClass(valueClass));	  
	} catch(IOException ex) {
		  fail(ex.getMessage());	
	}
  }
    
  public void testReaderWithWrongKeyClass() {    
	try {	  	 
	  final String TEST_METHOD_KEY = "testReaderWithWrongKeyClass.mapfile";
	  deleteFileIfExists(TEST_METHOD_KEY);
	  MapFile.Writer writer = createWriter(TEST_METHOD_KEY, IntWritable.class, Text.class);	  
	  
	  for (int i = 0; i < 10; i++)
	    writer.append(new IntWritable(i), new Text("value" + i));
	  writer.close();
	  
	  MapFile.Reader reader = createReader(TEST_METHOD_KEY, Text.class);	  	  
	  reader.getClosest(new Text("2"), new Text(""));
	  fail("no excepted exception in testReaderWithWrongKeyClass !!!");
	} catch (IOException ex) { 
	  /*for pass test this should be throw*/	
	}	
  }     
  
  public void testReaderWithWrongValueClass() {
    MapFile.Writer writer = null;
	try {
     final String TEST_METHOD_KEY = "testReaderWithWrongValueClass.mapfile";
     deleteFileIfExists(TEST_METHOD_KEY);
     writer = createWriter(TEST_METHOD_KEY, IntWritable.class, Text.class);
     writer.append(new IntWritable(0), new IntWritable(0));     
     fail("no excepted exception in testReaderWithWrongKeyClass !!!");
   } catch (IOException ex) {	   
	 /*for pass test this should be throw*/	   
   }	  
  }
  
  public void testReaderKeyIteration() {	  
    final String TEST_METHOD_KEY = "testReaderKeyIteration.mapfile";
    int SIZE = 10;
    int ITERATIONS = 5;
    try {
      deleteFileIfExists(TEST_METHOD_KEY);
	  MapFile.Writer writer = createWriter(TEST_METHOD_KEY, IntWritable.class, Text.class);
	  int start = 0;
	  for (int i = 0; i < SIZE; i++)
	    writer.append(new IntWritable(i), new Text("Value:" + i));    	
	  writer.close();    	    	
	
	  MapFile.Reader reader = createReader(TEST_METHOD_KEY, IntWritable.class);
	  //test iteration	  
	  Writable startValue = new Text("Value:" + start);    	
	  int i = 0;
	  while(i++ < ITERATIONS) {
		IntWritable key = new IntWritable(start);
		Writable value = startValue;
	    while(reader.next(key, value)) {
	      assertNotNull(key);
	      assertNotNull(value);
	    }
	    reader.reset();
	  }	  
      assertTrue("reader seek error !!!", reader.seek(new IntWritable(SIZE/2)));      
      assertFalse("reader seek error !!!", reader.seek(new IntWritable(SIZE*2)));
    } catch(IOException ex) {
	  fail("reader seek error !!!");  
    }
  }

  private boolean deleteFileIfExists(String fileName) {
	File file = new File(".","." + fileName);
    if (file.exists())
      return file.delete();
    return false;
  }
  
  public void testFix() {
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      final String INDEX_LESS_MAP_FILE = "testFix.mapfile";
      int PAIR_SIZE = 20;
      Path dir = new Path(System.getProperty("test.build.data",".") + INDEX_LESS_MAP_FILE);
	  MapFile.Writer writer = createWriter(INDEX_LESS_MAP_FILE, IntWritable.class, Text.class);
	  for (int i = 0; i < PAIR_SIZE; i ++)
		  writer.append(new IntWritable(0), new Text("value"));	  
	  writer.close();
	  
	  File indexFile = new File(".", "." + INDEX_LESS_MAP_FILE);
	  boolean tryToFix = false;
	  if (indexFile.exists())
		  tryToFix = indexFile.delete();
	  
	  if (tryToFix)
	    assertTrue("testFix error !!!", MapFile.fix(fs, dir, IntWritable.class, Text.class, true, conf) == PAIR_SIZE);	  
    } catch (Exception ex) {
      fail("testFix error !!!");
    }
  } 
  /**
   * Test getClosest feature.
   * @throws Exception
   */    
  public void testGetClosest() throws Exception {
    // Write a mapfile of simple data: keys are 
    Path dirName = new Path(System.getProperty("test.build.data",".") +
      getName() + ".mapfile"); 
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
    MapFile.Reader reader = new MapFile.Reader(fs, qualifiedDirName.toString(),
      conf);
    Text key = new Text("55");
    Text value = new Text();
    Text closest = (Text)reader.getClosest(key, value);
    // Assert that closest after 55 is 60
    assertEquals(new Text("60"), closest);
    // Get closest that falls before the passed key: 50
    closest = (Text)reader.getClosest(key, value, true);
    assertEquals(new Text("50"), closest);
    // Test get closest when we pass explicit key
    final Text TWENTY = new Text("20");
    closest = (Text)reader.getClosest(TWENTY, value);
    assertEquals(TWENTY, closest);
    closest = (Text)reader.getClosest(TWENTY, value, true);
    assertEquals(TWENTY, closest);
    // Test what happens at boundaries.  Assert if searching a key that is
    // less than first key in the mapfile, that the first key is returned.
    key = new Text("00");
    closest = (Text)reader.getClosest(key, value);
    assertEquals(FIRST_KEY, Integer.parseInt(closest.toString()));
    
    // If we're looking for the first key before, and we pass in a key before 
    // the first key in the file, we should get null
    closest = (Text)reader.getClosest(key, value, true);
    assertNull(closest);
    
    // Assert that null is returned if key is > last entry in mapfile.
    key = new Text("99");
    closest = (Text)reader.getClosest(key, value);
    assertNull(closest);

    // If we were looking for the key before, we should get the last key
    closest = (Text)reader.getClosest(key, value, true);
    assertEquals(new Text("90"), closest);
  }
    
  public void testMidKey() throws Exception {
    // Write a mapfile of simple data: keys are 
    Path dirName = new Path(System.getProperty("test.build.data",".") +
      getName() + ".mapfile"); 
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
 
    MapFile.Writer writer = new MapFile.Writer(conf, fs,
      qualifiedDirName.toString(), IntWritable.class, IntWritable.class);
    writer.append(new IntWritable(1), new IntWritable(1));
    writer.close();
    // Now do getClosest on created mapfile.
    MapFile.Reader reader = new MapFile.Reader(fs, qualifiedDirName.toString(),
      conf);
    assertEquals(new IntWritable(1), reader.midKey());
  }
  
  public void testMidKeyEmpty() throws Exception {
    // Write a mapfile of simple data: keys are 
    Path dirName = new Path(System.getProperty("test.build.data",".") +
      getName() + ".mapfile"); 
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
 
    MapFile.Writer writer = new MapFile.Writer(conf, fs,
      qualifiedDirName.toString(), IntWritable.class, IntWritable.class);
    writer.close();
    // Now do getClosest on created mapfile.
    MapFile.Reader reader = new MapFile.Reader(fs, qualifiedDirName.toString(),
      conf);
    assertEquals(null, reader.midKey());
  }
}
