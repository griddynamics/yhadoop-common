package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.Random;
import junit.framework.Assert;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Test;

public class TestLz4CompressorDecompressor {			
	
	@Test
	public void testLz4CompressorDecompressor() {	 
      int BYTE_SIZE = 1024;
	  byte[] bytes = BytesGenerator.get(BYTE_SIZE);
      byte[] comperessedbytes = new byte[BYTE_SIZE];                  
      if (NativeCodeLoader.isNativeCodeLoaded()) {    	
	    Lz4Compressor compressor = new Lz4Compressor();            
	    try {
	      if (compressor.needsInput()) {	    
		    compressor.setInput(bytes, 0, bytes.length);	    
		    Assert.assertEquals("TestLz4CompressorDecompressor testLz4CompressorCompress getBytesRead error !!!", 
		    		BYTE_SIZE, compressor.getBytesRead());
		    compressor.compress(comperessedbytes, 0, bytes.length);		    		    		    		    		   
		    Lz4Decompressor decompressor = new Lz4Decompressor();		    
		    decompressor.setInput(bytes, 0, bytes.length);			
		    assertEquals("origin size not the same as decompress size", BYTE_SIZE, decompressor.decompress(bytes, 0, bytes.length));			
	      } else {
	    	fail("testLz4CompressorDecompressor error");
	      }
	    } catch (IOException e) {		
	      fail("testLz4CompressorDecompressor error");
		}
      }
      
	}		
		
	static final class BytesGenerator { 
      private BytesGenerator(){}
	
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
