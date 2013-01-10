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
