/*
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
import junit.framework.TestCase;

public class TestVLongWritable extends TestCase {
  
  public void testSetGetVLong() {
    VLongWritable vLongWritable = new VLongWritable(1l);
    assertEquals("testSetGetVLong error !!!", 1l , vLongWritable.get());
    vLongWritable.set(2l);
    assertEquals("testSetGetVLong error !!!", 2l , vLongWritable.get());
    assertTrue("testSetGetVLong equals error !!!", new VLongWritable(545242l).equals(new VLongWritable(545242l)));
    assertTrue("testSetGetVLong compareTo error !!!", new VLongWritable(1l).compareTo(new VLongWritable(2l)) < 0 );
    assertTrue("testSetGetVLong compareTo error !!!", new VLongWritable(2l).compareTo(new VLongWritable(1l)) > 0 );
    assertTrue("testSetGetVLong compareTo error !!!", new VLongWritable(2l).compareTo(new VLongWritable(2l)) == 0 );
  }
  
  public void testReadFields() {
    try {
      long longValue = 10l;	            
      DataInputBuffer in = new DataInputBuffer();            
      in.reset(new byte[] {0,0,0,0,0,0,0,0xA}, 8);
      
      VLongWritable vLongWritable = new VLongWritable(longValue);
      vLongWritable.readFields(in);  
      byte[] b = in.getData();
      assertEquals("testReadFields error !!!", b.length , 8);
      assertEquals("testReadFields error !!!", longValue , byteToLong(b));
    } catch (IOException ex) {
      fail("ex testReadFields error !!!");
    }
  } 
  
  private static long byteToLong(byte[] buf) {
    if (buf.length == 8) {
	return ((buf[0] & 0xFFL) << 56) |
	         ((buf[1] & 0xFFL) << 48) |
	         ((buf[2] & 0xFFL) << 40) |
	         ((buf[3] & 0xFFL) << 32) |
	         ((buf[4] & 0xFFL) << 24) |
	         ((buf[5] & 0xFFL) << 16) |
	         ((buf[6] & 0xFFL) <<  8) |
	         ((buf[7] & 0xFFL) <<  0) ;
    } else { 
    	return 0;
    }
  }
}
