package org.apache.hadoop.io;

import junit.framework.TestCase;

public class TestNullWritable extends TestCase {

  public void testNullableWritable() {
    NullWritable nullWritable = NullWritable.get();
    assertTrue("testNullableWritable equals error!!!",
        nullWritable.equals(NullWritable.get()));
    assertNotNull("testNullableWritable null error!!!",
        WritableComparator.get(NullWritable.class));
  }
}
