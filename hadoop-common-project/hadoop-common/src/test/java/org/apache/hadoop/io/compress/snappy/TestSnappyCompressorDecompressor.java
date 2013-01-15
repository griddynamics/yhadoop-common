package org.apache.hadoop.io.compress.snappy;

import static org.junit.Assert.fail;
import java.lang.reflect.Array;
import java.util.Random;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Test;

public class TestSnappyCompressorDecompressor {

  @Test
  public void testSnappyCompressorSetInputNullPointerException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyCompressor compessor = new SnappyCompressor();
        compessor.setInput(null, 0, 10);
        fail("testSnappyCompressorSetInputNullPointerException error !!!");
      } catch (NullPointerException ex) {
        // excepted
      } catch (Exception ex) {
        fail("testSnappyCompressorSetInputNullPointerException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyDecompressorSetInputNullPointerException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyDecompressor decompressor = new SnappyDecompressor();
        decompressor.setInput(null, 0, 10);
        fail("testSnappyDecompressorSetInputNullPointerException error !!!");
      } catch (NullPointerException ex) {
        // expected
      } catch (Exception e) {
        fail("testSnappyDecompressorSetInputNullPointerException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyCompressorSetInputAIOBException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyCompressor compressor = new SnappyCompressor();
        compressor.setInput(new byte[] {}, -5, 10);
        fail("testSnappyCompressorSetInputAIOBException error !!!");
      } catch (ArrayIndexOutOfBoundsException ex) {
        // expected
      } catch (Exception ex) {
        fail("testSnappyCompressorSetInputAIOBException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyDecompressorSetInputAIOUBException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyDecompressor decompressor = new SnappyDecompressor();
        decompressor.setInput(new byte[] {}, -5, 10);
        fail("testSnappyDecompressorSetInputAIOUBException error !!!");
      } catch (ArrayIndexOutOfBoundsException ex) {
        // expected
      } catch (Exception e) {
        fail("testSnappyDecompressorSetInputAIOUBException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyCompressorCompressNullPointerException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyCompressor compressor = new SnappyCompressor();
        byte[] bytes = BytesGenerator.get(1024 * 6);
        compressor.setInput(bytes, 0, bytes.length);
        compressor.compress(null, 0, 0);
        fail("testSnappyCompressorCompressNullPointerException error !!!");
      } catch (NullPointerException ex) {
        // expected
      } catch (Exception e) {
        fail("testSnappyCompressorCompressNullPointerException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyDecompressorCompressNullPointerException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyDecompressor decompressor = new SnappyDecompressor();
        byte[] bytes = BytesGenerator.get(1024 * 6);
        decompressor.setInput(bytes, 0, bytes.length);
        decompressor.decompress(null, 0, 0);
        fail("testSnappyDecompressorCompressNullPointerException error !!!");
      } catch (NullPointerException ex) {
        // expected
      } catch (Exception e) {
        fail("testSnappyDecompressorCompressNullPointerException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyCompressorCompressAIOBException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyCompressor compressor = new SnappyCompressor();
        byte[] bytes = BytesGenerator.get(1024 * 6);
        compressor.setInput(bytes, 0, bytes.length);
        compressor.compress(new byte[] {}, 0, -1);
        fail("testSnappyCompressorCompressAIOBException error !!!");
      } catch (ArrayIndexOutOfBoundsException ex) {
        // expected
      } catch (Exception e) {
        fail("testSnappyCompressorCompressAIOBException ex error !!!");
      }
    }
  }

  @Test
  public void testSnappyDecompressorCompressAIOBException() {
    if (NativeCodeLoader.isNativeCodeLoaded()
        && NativeCodeLoader.buildSupportsSnappy()) {
      try {
        SnappyDecompressor decompressor = new SnappyDecompressor();
        byte[] bytes = BytesGenerator.get(1024 * 6);
        decompressor.setInput(bytes, 0, bytes.length);
        decompressor.decompress(new byte[] {}, 0, -1);
        fail("testSnappyDecompressorCompressAIOBException error !!!");
      } catch (ArrayIndexOutOfBoundsException ex) {
        // expected
      } catch (Exception e) {
        fail("testSnappyDecompressorCompressAIOBException ex error !!!");
      }
    }
  }

  static final class BytesGenerator {
    private BytesGenerator() {
    }

    private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
        0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };
    private static final Random rnd = new Random();

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++)
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      return array;
    }
  }
}
