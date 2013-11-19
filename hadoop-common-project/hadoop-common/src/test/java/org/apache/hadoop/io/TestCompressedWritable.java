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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class TestCompressedWritable {

  /**
   * test {@code CompressedWritable} 
   * write(DataOutputBuffer out) method
   *  
   */
  @Test
  public void testCompressedWritableWriteHeader() {
    byte[] header = new byte[0];
    CompressedWritable cWritable = new CompressedWritable() {
      @Override
      protected void readFieldsCompressed(DataInput in) throws IOException {
      }

      @Override
      protected void writeCompressed(DataOutput out) throws IOException {
      }
    };

    DataOutputBuffer out = new DataOutputBuffer();
    InputStream in = new ByteArrayInputStream(out.getData());
    try {
      cWritable.write(out);
      header = new byte[out.getLength()];
      in.read(header);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    assertEquals(
        "TestCompressedWritable testCompressedWritableWriteHeader error !!!",
        header.length, out.getLength());
  }
  /**
   * test {@link CompressedWritable} readFields() method
   */
  @Test
  public void testCompressedWritableReadFields() {
    CompressedWritable cWritable = new CompressedWritable() {
      @Override
      protected void readFieldsCompressed(DataInput in) throws IOException {
      }

      @Override
      protected void writeCompressed(DataOutput out) throws IOException {
      }
    };

    DataInput in = new DataInputStream(new ByteArrayInputStream(new byte[] {
        0x0, 0x0, 0x0, 0x2, 0x5, 0x6 }));
    try {
      cWritable.readFields(in);
      DataOutputBuffer out = new DataOutputBuffer();
      cWritable.write(out);
      assertEquals(
          "TestCompressedWritable testCompressedWritableReadFields lenght error",
          out.getLength(), 6);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
