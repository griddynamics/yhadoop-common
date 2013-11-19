package org.apache.hadoop.tools.rumen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import org.junit.Test;

public class TestResourceUsageMetrics {
  
  @Test(timeout=500)
  public void testSerializeAndCompare() throws Exception {
    // create object for test
    ResourceUsageMetrics m1 = new ResourceUsageMetrics();
    m1.setCumulativeCpuUsage(1L);
    m1.setHeapUsage(2L);
    m1.setPhysicalMemoryUsage(3L);
    m1.setVirtualMemoryUsage(4L);

    // create copy of test object
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(data);
    m1.write(dataOutput);
    ResourceUsageMetrics m2 = new ResourceUsageMetrics();
    m2.readFields(new DataInputStream(new ByteArrayInputStream(data
        .toByteArray())));
    // objects should be equals
    m1.deepCompare(m2, null);
    // change data
    m2.setVirtualMemoryUsage(5L);

    // object not equals
    try {
      m1.deepCompare(m2, null);
      fail();
    } catch (DeepInequalityException e) {
      assertEquals(e.getMessage(), "Value miscompared:virtualMemory");
    }
    // test contents
    assertEquals(m2.getCumulativeCpuUsage(), 1);
    assertEquals(m2.getHeapUsage(), 2);
    assertEquals(m2.getPhysicalMemoryUsage(), 3);
    assertEquals(m2.getVirtualMemoryUsage(), 5);
  }

}
