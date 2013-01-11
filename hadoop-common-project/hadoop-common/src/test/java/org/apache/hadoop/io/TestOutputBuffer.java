package org.apache.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class TestOutputBuffer {
	
	@Test
	public void testOutputBufferWithoutResize() throws IOException {
	  byte[] bytes = Bytes.get(10);
	  InputStream in = new ByteArrayInputStream(bytes);
	  OutputBuffer out = new OutputBuffer();
	  out.write(in, bytes.length);
	  int size = 0;
	    	  
	  while(size < out.getLength()) {
        Assert.assertTrue("TestOutputBuffer testOutputBufferWithoutResize error !!!", out.getData()[size] == bytes[size]);
        size++;
	  }
	  out.close();
	}	
	
	@Test
	public void testOutputBufferWithResize() throws IOException {
	  byte[] bytes = Bytes.get(33);
	  InputStream in = new ByteArrayInputStream(bytes);
	  OutputBuffer out = new OutputBuffer();
	  out.write(in, bytes.length);
	  int size = 0;
	    	  
	  while(size < out.getLength()) {
        Assert.assertTrue("TestOutputBuffer testOutputBufferWithResize error !!!", out.getData()[size] == bytes[size]);
        size++;
	  }  
	  out.close();
	}
	
	@Test
	public void testOutputBufferReset() throws IOException {	  
	  byte[] bytes = Bytes.get(3);
	  InputStream in = new ByteArrayInputStream(bytes);
	  OutputBuffer out = new OutputBuffer();
	  out.write(in, bytes.length);	  	 
	  out.reset();	  
	  Assert.assertTrue("TestOutputBuffer testOutputBufferReset error !!!", out.getLength() == 0);
	  out.close();
	}
	
	static final class Bytes { 
      private Bytes(){}
	
	  private static final byte[] CACHE = new byte[] {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF};
	  private static final Random rnd = new Random();
	
	  public static byte[] get(int size) {
	    byte[] array = (byte[])Array.newInstance(byte.class, size);
	    for(int i=0; i< size; i++)
	      array[i] = CACHE[rnd.nextInt(CACHE.length-1)];		
	    return array;  
	  }
	}	
}