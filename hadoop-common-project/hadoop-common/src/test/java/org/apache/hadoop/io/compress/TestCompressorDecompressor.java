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
package org.apache.hadoop.io.compress;

import static org.junit.Assert.fail;
import java.util.Random;
import org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.junit.Test;
import com.google.common.collect.ImmutableSet;

public class TestCompressorDecompressor {

  @Test
  public void testCompressorDecompressor() {
    // no more for this data
    int SIZE = 44 * 1024;

    byte[] rawData = BytesGenerator.get(SIZE);
    try {
      CompressDecompressTester.of(rawData)
          .withCompressDecompressPair(new SnappyCompressor(),
              new SnappyDecompressor())
          .withCompressDecompressPair(new Lz4Compressor(),
              new Lz4Decompressor())
          .withCompressDecompressPair(new ZlibCompressor(),
              new ZlibDecompressor())
          .withCompressDecompressPair(new BuiltInZlibDeflater(),
              new BuiltInZlibInflater())
          .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
          .test();

    } catch (Exception ex) {
      fail("testCompressorDecompressor error !!!" + ex);
    }
  }

  @Test
  public void testCompressorDecompressorWithExeedBufferLimit() {
    int BYTE_SIZE = 100 * 1024;
    byte[] rawData = BytesGenerator.get(BYTE_SIZE);
    try {
      CompressDecompressTester.of(rawData)
          .withCompressDecompressPair(
              new SnappyCompressor(BYTE_SIZE + BYTE_SIZE / 2),
              new SnappyDecompressor(BYTE_SIZE + BYTE_SIZE / 2))
          .withCompressDecompressPair(new Lz4Compressor(BYTE_SIZE),
              new Lz4Decompressor(BYTE_SIZE))
          .withCompressDecompressPair(
              new ZlibCompressor(
                  org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel.BEST_COMPRESSION,
                  CompressionStrategy.DEFAULT_STRATEGY,
                  org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader.DEFAULT_HEADER,
                  BYTE_SIZE),
              new ZlibDecompressor(
                  org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.DEFAULT_HEADER,
                  BYTE_SIZE))

          .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
          .test();

    } catch (Exception ex) {
      fail("testCompressorDecompressorWithExeedBufferLimit error !!!" + ex);
    }
  }
  
  static final class BytesGenerator {
    private static final Random rnd = new Random(12345L);
    
    private BytesGenerator() {
    }

    public static byte[] get(int size) {
      byte[] array = new byte[size];
      for (int i = 0; i < size; i++)
        array[i] = (byte) rnd.nextInt(16);
      return array;
    }
  }
}
