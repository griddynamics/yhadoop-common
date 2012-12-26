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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestBloomMapFile extends TestCase {
  private static Configuration conf = new Configuration();
  
  public void testMembershipTest() throws Exception {
    // write the file
    Path dirName = new Path(System.getProperty("test.build.data",".") +
        getName() + ".bloommapfile"); 
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
        if (!exists) falseNeg++;
      } else {
        if (exists) falsePos++;
      }
    }
    reader.close();
    fs.delete(qualifiedDirName, true);
    System.out.println("False negatives: " + falseNeg);
    assertEquals(0, falseNeg);
    System.out.println("False positives: " + falsePos);
    assertTrue(falsePos < 2);
  }

  private void checkMembershipVaryingSizedKeys(String name, List<Text> keys) throws Exception {
    Path dirName = new Path(System.getProperty("test.build.data",".") +
        name + ".bloommapfile"); 
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
      assertTrue("False negative for existing key " + key, reader.probablyHasKey(key));
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
  
  public void testDeleteFile() {	
    try { 
      String DELETABLE_FILE_NAME = "deletableFile.bloommapfile"; 	
	  FileSystem fs = FileSystem.getLocal(conf);
      Path dirName = new Path(System.getProperty("test.build.data",".") + DELETABLE_FILE_NAME);	  
	  BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, dirName, 
		MapFile.Writer.keyClass(IntWritable.class), MapFile.Writer.valueClass(Text.class));
	  assertNotNull("testDeleteFile error !!!", writer);
	  BloomMapFile.delete(fs, "." + DELETABLE_FILE_NAME);
	} catch(Exception ex) {
	  fail("unexpect ez in testDeleteFile !!!");	
	}
  }
  
  public void testIOExceptionInWriterConstructor() {
	String TEST_FILE_NAME = "testFile.bloommapfile";
	Path dirName = new Path(System.getProperty("test.build.data",".") + TEST_FILE_NAME);
	Path dirNameSpy = org.mockito.Mockito.spy(dirName);
	try {
	  BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, dirName, 
		MapFile.Writer.keyClass(IntWritable.class), MapFile.Writer.valueClass(Text.class));
	  writer.append(new IntWritable(1), new Text("123124142"));
	  writer.close();
		
	  org.mockito.Mockito.when(dirNameSpy.getFileSystem(conf)).thenThrow(new IOException());      
      BloomMapFile.Reader reader = new BloomMapFile.Reader(dirNameSpy, conf, 
    	  MapFile.Reader.comparator(new WritableComparator(IntWritable.class)));                        
      
      assertNull("testIOExceptionInWriterConstructor error !!!", reader.getBloomFilter());      		  	 
	} catch (Exception ex) {
	  fail("unexpect ex in testIOExceptionInWriterConstructor !!!");
	}
  }
  
  public void testGetBloomMapFile() {
	String TEST_FILE_NAME = "getTestFile.bloommapfile";
	int SIZE = 10;
	Path dirName = new Path(System.getProperty("test.build.data",".") + TEST_FILE_NAME);
	try {
      BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, dirName, 
		MapFile.Writer.keyClass(IntWritable.class), MapFile.Writer.valueClass(Text.class));
      
      for (int i = 0; i < SIZE; i++)
    	  writer.append(new IntWritable(i), new Text() );
      writer.close();
      
      BloomMapFile.Reader reader = new BloomMapFile.Reader(dirName, conf, 
        	  MapFile.Reader.comparator(new WritableComparator(IntWritable.class)));
            
      assertNotNull("testGetBloomMapFile error !!!", reader.get(new IntWritable(6), new Text()));
      assertNull("testGetBloomMapFile error !!!", reader.get(new IntWritable(16), new Text()));      
	} catch (Exception ex) {
	  fail("unexpect ex in testGetBloomMapFile !!!");
	}
  }
  
}
