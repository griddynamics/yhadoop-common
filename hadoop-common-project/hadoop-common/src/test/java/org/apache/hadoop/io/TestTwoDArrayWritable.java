package org.apache.hadoop.io;

import java.io.IOException;
import junit.framework.TestCase;

public class TestTwoDArrayWritable extends TestCase {		
	
	static final Text[][] elements = { {new Text("fzero"), new Text("fone"), new Text("ftwo")}, {new Text("szero"), new Text("sone"), new Text("stwo")} };
	
	public void test2DArrayWriteRead() throws IOException  {			  	  	  
	  DataOutputBuffer out = new DataOutputBuffer();
	  DataInputBuffer in = new DataInputBuffer();
	  
	  TwoDArrayWritable twoDArrayWritable = new TwoDArrayWritable(Text.class);
	  twoDArrayWritable.set(elements);
	  //write
	  twoDArrayWritable.write(out);
	  
	  TwoDArrayWritable dest2DArray = new TwoDArrayWritable(Text.class);
	  in.reset(out.getData(), out.getLength());
	  dest2DArray.readFields(in);
	    
	  Writable[][] destElements = dest2DArray.get();
	  assertTrue(destElements.length == elements.length);
	  
	  for (int i = 0; i < elements.length; i++) {
	    for (int j = 0; j < elements.length; j++) {
	      assertEquals(destElements[i][j], elements[i][j]);
	    }
	  }	    	  
	}
	
	public void test2DArrayToArray() {
	  TwoDArrayWritable twoDArrayWritable = new TwoDArrayWritable(Text.class);
	  twoDArrayWritable.set(elements);
	  Object array = twoDArrayWritable.toArray();
	  
	  assertTrue("TestTwoDArrayWritable testNotNullArray error!!! ", array instanceof Text[][]);
	  Text[][] destElements = (Text[][]) array;
	  
	  for (int i = 0; i < elements.length; i++) {
	    for (int j = 0; j < elements.length; j++) {
		  assertEquals(destElements[i][j], elements[i][j]);
		}
	  }	  
	}
}
