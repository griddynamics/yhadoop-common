package org.apache.hadoop.mapred.gridmix;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestGridMixClasses {

  @Test
  public void testLoadSplit() throws Exception {

    Path[] files = { new Path("one"), new Path("two") };
    long[] start = { 1, 2 };
    long[] lengths = { 100, 200 };
    String[] locations = { "locOne", "loctwo" };

    CombineFileSplit cfsplit = new CombineFileSplit(files, start, lengths,
        locations);
    ResourceUsageMetrics metrics = new ResourceUsageMetrics();
    metrics.setCumulativeCpuUsage(200);
    ResourceUsageMetrics[] rMetrics = { metrics };

    double[] reduceBytes = { 8.1d, 8.2d };
    double[] reduceRecords = { 9.1d, 9.2d };
    long[] reduceOutputBytes = { 101L, 102L };
    long[] reduceOutputRecords = { 111L, 112L };

    LoadSplit test = new LoadSplit(cfsplit, 2, 3, 4L, 5L, 6L, 7L, reduceBytes,
        reduceRecords, reduceOutputBytes, reduceOutputRecords, metrics,
        rMetrics);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(data);
    test.write(out);
    LoadSplit copy = new LoadSplit();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(data
        .toByteArray())));

    // data should be the same
    assertEquals(test.getId(), copy.getId());
    assertEquals(test.getMapCount(), copy.getMapCount());
    assertEquals(test.getInputRecords(), copy.getInputRecords());

    assertEquals(test.getOutputBytes()[0], copy.getOutputBytes()[0]);
    assertEquals(test.getOutputRecords()[0], copy.getOutputRecords()[0]);
    assertEquals(test.getReduceBytes(0), copy.getReduceBytes(0));
    assertEquals(test.getReduceRecords(0), copy.getReduceRecords(0));
    assertEquals(test.getMapResourceUsageMetrics().getCumulativeCpuUsage(),
        copy.getMapResourceUsageMetrics().getCumulativeCpuUsage());
    assertEquals(test.getReduceResourceUsageMetrics(0).getCumulativeCpuUsage(),
        copy.getReduceResourceUsageMetrics(0).getCumulativeCpuUsage());

  }

}
