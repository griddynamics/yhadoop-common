package org.apache.hadoop.io.lz4;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Random;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Ignore;
import org.junit.Test;

public class TestLz4CompressorDecompressor {
  
  @Test
  public void testCompressorSetInputNullPointerException() {    
    try {
      Lz4Compressor compressor = new Lz4Compressor();
      compressor.setInput(null, 0, 10);          
      fail("testCompressorSetInputNullPointerException error !!!");
    } catch(NullPointerException ex) {
      //expected
    } catch (Exception e) {
      fail("testCompressorSetInputNullPointerException ex error !!!");
    }
  }
  
  @Test
  public void testDecompressorSetInputNullPointerException() {
    try {
      Lz4Decompressor decompressor = new Lz4Decompressor();
      decompressor.setInput(null, 0, 10);
      fail("testDecompressorSetInputNullPointerException error !!!");
    } catch(NullPointerException ex) {
      //expected
    } catch (Exception e) {
      fail("testDecompressorSetInputNullPointerException ex error !!!");
    }
  }
  
  @Test
  public void testCompressorSetInputAIOBException() {
    try {
      Lz4Compressor compressor = new Lz4Compressor();
      compressor.setInput(new byte[] {}, -5, 10);      
      fail("testCompressorSetInputAIOBException error !!!");
    } catch(ArrayIndexOutOfBoundsException ex) {
      //expected
    } catch(Exception ex) {
      fail("testCompressorSetInputAIOBException ex error !!!");
    }
  }     
  
  @Test 
  public void testDecompressorSetInputAIOUBException() {
    try {
      Lz4Decompressor decompressor = new Lz4Decompressor();
      decompressor.setInput(new byte[] {}, -5, 10);
      fail("testDecompressorSetInputAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      //expected
    } catch (Exception e) {
      fail("testDecompressorSetInputAIOBException ex error !!!");
    }
  }
  
  @Test
  public void testCompressorCompressNullPointerException() {
    try {
      Lz4Compressor compressor = new Lz4Compressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(null, 0, 0);
      fail("testCompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      //expected
    } catch (Exception e) {
      fail("testCompressorCompressNullPointerException ex error !!!");
    }
  }
  
  @Test
  public void testDecompressorCompressNullPointerException() {
    try {
      Lz4Decompressor decompressor = new Lz4Decompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(null, 0, 0);
      fail("testDecompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      //expected
    } catch (Exception e) {
      fail("testDecompressorCompressNullPointerException ex error !!!");
    }
  }
  
  
  @Test
  public void testCompressorCompressAIOBException() {
    try {
      Lz4Compressor compressor = new Lz4Compressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(new byte[] {}, 0, -1);
      fail("testCompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      //expected
    } catch (Exception e) {
      fail("testCompressorCompressAIOBException ex error !!!");
    }
  }
  
  @Test
  public void testDecompressorCompressAIOBException() {
    try {
      Lz4Decompressor decompressor = new Lz4Decompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(new byte[] {}, 0, -1);
      fail("testDecompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      //expected
    } catch (Exception e) {
      fail("testDecompressorCompressAIOBException ex error !!!");
    }
  }
  
  @Test
  public void testSetInputWithBytesSizeMoreThenDefault() {    
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      int BYTES_SIZE = 1024 * 64 + 1;
      try {                       
        Lz4Compressor compressor = new Lz4Compressor();
        byte[] bytes = BytesGenerator.get(BYTES_SIZE);
        if (compressor.needsInput()) {
          compressor.setInput(bytes, 0, bytes.length);
          byte[] emptyBytes = new byte[BYTES_SIZE]; 
          int csize = compressor.compress(emptyBytes, 0, bytes.length);        
          assertTrue(" testWithBytesSizeMoreThenDefault error !!!", csize != 0);
        } else {
          fail("testSetInputWithBytesSizeMoreThenDefault error !!!");
        }      
      } catch(Exception ex) {
        fail("testWithBytesSizeMoreThenDefault ex error !!!");
      }
    }
  }
  
  @Test
  public void testLz4CompressorDecompressorMethods() {                       
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      int BYTE_SIZE = 1024 * 7;
      byte[] bytes = BytesGenerator.get(BYTE_SIZE);
      byte[] comperessedbytes = new byte[BYTE_SIZE];
      Lz4Compressor compressor = new Lz4Compressor();                
      try {        
        if(compressor.needsInput()) {      
          compressor.setInput(bytes, 0, bytes.length);          
          assertTrue("testLz4CompressorDecompressorMethods getBytesRead error !!!",  compressor.getBytesRead() > 0 );
          assertTrue("testLz4CompressorDecompressorMethods getBytesWritten before compress error !!!", 
              compressor.getBytesWritten() == 0);
          assertTrue("testLz4CompressorDecompressor compressor.compress", 
              compressor.compress(comperessedbytes, 0, bytes.length) > 0);                                      
          assertTrue("testLz4CompressorDecompressorMethods getBytesWritten after compress error !!!", 
              compressor.getBytesWritten() > 0);
          Lz4Decompressor decompressor = new Lz4Decompressor();          
          if (decompressor.needsInput()) {
            decompressor.setInput(bytes, 0, bytes.length);      
            assertTrue("origin size not the same as decompress size", 
                decompressor.decompress(bytes, 0, bytes.length) > 0 );
            decompressor.reset();
          } else {
            fail("testLz4CompressorDecompressor decompressor.needsInput error");
          }         
        } else {
          fail("testLz4CompressorDecompressor compressor.needsInput error");
        }
      } catch (Exception e) {   
        fail("testLz4CompressorDecompressor ex error!!!");
      }
    }
  }   
  
  @Test
  public void testLz4CompressorLogic() {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      int BYTE_SIZE = 1024 * 10;
      int MAX_INPUT_SIZE = 512;
      byte[] buffer = new byte[MAX_INPUT_SIZE];
      byte[] bytes = BytesGenerator.get(BYTE_SIZE);
      ByteArrayOutputStream out  = new ByteArrayOutputStream();
      Lz4Compressor compressor = new Lz4Compressor();
      assertFalse("compressor finished error !!!", compressor.finished());
      assertTrue("compressor getBytesRead error !!!", compressor.getBytesRead() == 0);
      try {
        writeBytesByBlock(bytes, BYTE_SIZE, 0, MAX_INPUT_SIZE, out, compressor, buffer);
        assertTrue("testLz4CompressorLogic size error !!!", out.size() > 0);
      } catch (Exception ex) {
        fail("testLz4CompressorLogic error !!!");
      }
    }
  }
  
  private void writeBytesByBlock(byte[] bytes, int len, int off, int maxSize, OutputStream out,
      Lz4Compressor compressor, byte[] buffer) throws IOException {    
    // Use default of 512 as bufferSize and compressionOverhead of 
    // (1% of bufferSize + 12 bytes) =  18 bytes (zlib algorithm).
    maxSize = maxSize - 18;
    if (len > maxSize) {
      rawWriteInt(out, len);
      do {
        int bufLen = Math.min(len, maxSize);
        compressor.setInput(bytes, off, bufLen);
        compressor.finish();
        while (!compressor.finished()) {
          compress(compressor, out, buffer);
        }
        compressor.reset();
        off += bufLen;
        len -= bufLen;
      } while (len > 0);
    }        
    compressor.setInput(bytes, off, len);
    if (!compressor.needsInput()) {
      rawWriteInt(out, (int)compressor.getBytesRead());
      do {
        compress(compressor, out, buffer);
      } while (!compressor.needsInput());
    }
    
  }
  
  private void compress(Lz4Compressor compressor, OutputStream out, byte[] buffer) throws IOException {
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      // Write out the compressed chunk
      rawWriteInt(out, len);
      out.write(buffer, 0, len);
    }
  }
  
  
  private void rawWriteInt(OutputStream out, int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }
  
  @Test
  public void testCompressorWithBlockCompressorStream() { 
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      int BYTE_SIZE = 1024 * 10;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Lz4Compressor compressor = new Lz4Compressor();      
      try {        
        BlockCompressorStream bcStream = new BlockCompressorStream(out, compressor);        
        byte[] bytes = BytesGenerator.get(BYTE_SIZE);
        assertTrue("testCompressorWithBlockCompressorStream output not empty",
            out.size() == 0);        
        bcStream.write(bytes, 0, bytes.length);
        bcStream.finish();                        
                
        assertTrue("testCompressorWithBlockCompressorStream output error ",
            out.size() > 0);
        
      } catch (Exception e) {     
        fail("testCompressorWithBlockCompressorStream error !!!");
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
