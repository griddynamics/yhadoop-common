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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import junit.framework.TestCase;

public class TestTwoDArrayWritable extends TestCase {

  static final Text[][] elements = {
      { new Text("fzero"), new Text("fone"), new Text("ftwo") },
      { new Text("szero"), new Text("sone"), new Text("stwo") } };

  public void test2DArrayWriteRead() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    TwoDArrayWritable twoDArrayWritable = new TwoDArrayWritable(Text.class);
    twoDArrayWritable.set(elements);
    // write
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

    assertTrue("TestTwoDArrayWritable testNotNullArray error!!! ",
        array instanceof Text[][]);
    Text[][] destElements = (Text[][]) array;

    for (int i = 0; i < elements.length; i++) {
      for (int j = 0; j < elements.length; j++) {
        assertEquals(destElements[i][j], elements[i][j]);
      }
    }
  }

  /**
   * {@code TwoDArrayWritable} constructor
   *  with String[][] argument
   */
  public void test2DArrayConstructor() {
    TwoDArrayWritable twoDArrayWritable = new TwoDArrayWritable(Text.class,
        elements);
    for (int i = 0; i < elements.length; i++) {
      for (int j = 0; j < elements.length; j++) {
        assertEquals(twoDArrayWritable.get()[i][j], elements[i][j]);
      }
    }
  }

  public void testInstantiationException() {
    try {
      Writable[][] writables = new Writable[][] {
          { new NonInstantiationWritable(true) },
          { new NonInstantiationWritable(true) } };
      DataOutputBuffer out = new DataOutputBuffer();
      DataInputBuffer in = new DataInputBuffer();

      TwoDArrayWritable twoDArrayWritable = new TwoDArrayWritable(
          NonInstantiationWritable.class);
      twoDArrayWritable.set(writables);
      twoDArrayWritable.write(out);

      in.reset(out.getData(), out.getLength());
      twoDArrayWritable.readFields(in);
      fail("testInstantiationException error !!!");
    } catch (RuntimeException ex) {
    } catch (Exception ex) {
      fail("testInstantiationException error. other exception !!!");
    }
  }

  public void testIllegalAccessException() {
    try {
      Writable[][] writables = new Writable[][] {
          { new IllegalAccessWritable(true) },
          { new IllegalAccessWritable(true) } };
      DataOutputBuffer out = new DataOutputBuffer();
      DataInputBuffer in = new DataInputBuffer();

      TwoDArrayWritable twoDArrayWritable = new TwoDArrayWritable(
          NonInstantiationWritable.class);
      twoDArrayWritable.set(writables);
      twoDArrayWritable.write(out);

      in.reset(out.getData(), out.getLength());
      twoDArrayWritable.readFields(in);
      fail("testIllegalAccessException error !!!");
    } catch (RuntimeException ex) {
    } catch (Exception ex) {
      fail("testInstantiationException error !!!");
    }
  }

  static class NonInstantiationWritable implements Writable {
    public NonInstantiationWritable(boolean flag) {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  static class IllegalAccessWritable implements Writable {
    private IllegalAccessWritable() {
    }

    public IllegalAccessWritable(boolean flag) {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }
}
