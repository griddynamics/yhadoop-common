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

import java.io.EOFException;
import java.io.IOException;
import junit.framework.Assert;
import org.junit.Test;
import com.google.common.collect.ImmutableList;

public class TestMultipleIOException {

  @Test
  public void testEmptyParamIOException() {
    IOException ex = MultipleIOException.createIOException(ImmutableList.<IOException> of());
    Assert.assertNull("TestMultipleIOException testSingleParamIOException class error!!!",
            ex);
  }

  @Test
  public void testSingleParamIOException() {
    IOException ex = MultipleIOException.createIOException(ImmutableList
        .of(new IOException("test-io-ex")));
    Assert.assertTrue(
        "TestMultipleIOException testSingleParamIOException class error!!!",
        ex instanceof IOException);
  }

  @Test
  public void testMultipleIOException() {
    ImmutableList<IOException> src = ImmutableList.of(new EOFException(),
        new IOException("test-io-ex"));
    IOException ex = MultipleIOException.createIOException(src);
    Assert.assertTrue("TestMultipleIOException testMultipleIOException class error!!!",
        ex instanceof MultipleIOException);
    Assert.assertSame("TestMultipleIOException testMultipleIOException same error!!!", src,
        ((MultipleIOException) ex).getExceptions());
  }
}
