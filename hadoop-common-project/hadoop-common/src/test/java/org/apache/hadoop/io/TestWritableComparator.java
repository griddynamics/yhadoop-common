package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import org.junit.Test;

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
public class TestWritableComparator {
	
	@Test
	public void testShouldThrowsException() {
	  //1 byte - size, other - body	  
	  // valid
	  byte[] bytes1 = new byte[] {0x1, 0x2};
	  // invalid
	  byte[] bytes2 = new byte[] {0x10, 0x2, 0x3};	  	  	 
	  try {		 
	    WritableComparator wCmp = new WritableComparator(Text.class, true);	    
	    wCmp.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length);
	    fail("testThrowsException error !!!");
	  } catch(Exception ex) {}
	}
	
	/**
	 * test {@code WritableComparator.compare()}
	 * method 
	 */
	@Test
	public void testWritableComparator() {
	  byte[] tbyte1 = new Text("111111").copyBytes();
	  byte[] tbyte2 = new Text("222222").copyBytes();
	  WritableComparator wCmp = WritableComparator.get(Text.class);
	  assertTrue("testCompareTest less error", wCmp.compare(tbyte1, 0, tbyte1.length, tbyte2, 0, tbyte2.length) < 0);
	  assertTrue("testCompareTest more error", wCmp.compare(tbyte2, 0, tbyte2.length, tbyte1, 0, tbyte1.length) > 0);
	  assertTrue("testCompareTest equal error", wCmp.compare(tbyte1, 0, tbyte1.length, tbyte1, 0, tbyte1.length) == 0);	  
	}
	
	/**
	 *  test static methods {@code WritableComparator readUnsignedShort()/readVInt()/readVLong}
	 *  with different parameters
	 */
	@Test
	public void testReadTypes() {
	  try {  
	    int unShort = WritableComparator.readUnsignedShort(new byte[] {0,-128}, 0);
	    assertEquals("testReadUnsignedShort error", unShort, 128);
	  
	    unShort = WritableComparator.readUnsignedShort(new byte[] {0, 127}, 0);
	    assertEquals("readUnsignedShort error", unShort, 127);
	  
	    unShort = WritableComparator.readUnsignedShort(new byte[] {0,0,0,0,-128}, 3);
	    assertEquals("readUnsignedShort error", unShort, 128);
	  	
	    int intValue = WritableComparator.readVInt(new byte[] {1,0,0,0,0,0,0,0}, 0);	    
		assertTrue("readVInt error", intValue == 1);
		
		long longValue = WritableComparator.readVLong(new byte[] {1,0,0,0,0,0,0,0}, 0);		
		assertTrue("readVLong error", longValue == 1);
	  } catch (IOException ex) {
	    fail("error in testReadTypes");	  
	  }	  	  	 
	}		
}
