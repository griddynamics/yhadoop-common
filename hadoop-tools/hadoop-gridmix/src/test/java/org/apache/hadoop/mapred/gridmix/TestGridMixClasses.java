package org.apache.hadoop.mapred.gridmix;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.CustomOutputCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.junit.Test;


import static org.junit.Assert.*;

public class TestGridMixClasses {

  @Test
  public void testLoadSplit() throws Exception {

    LoadSplit test = getLoadSplit();

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

  @Test
  public void testGridmixSplit() throws Exception {
    Path[] files = { new Path("one"), new Path("two") };
    long[] start = { 1, 2 };
    long[] lengths = { 100, 200 };
    String[] locations = { "locOne", "loctwo" };

    CombineFileSplit cfsplit = new CombineFileSplit(files, start, lengths,
        locations);
    ResourceUsageMetrics metrics = new ResourceUsageMetrics();
    metrics.setCumulativeCpuUsage(200);

    double[] reduceBytes = { 8.1d, 8.2d };
    double[] reduceRecords = { 9.1d, 9.2d };
    long[] reduceOutputBytes = { 101L, 102L };
    long[] reduceOutputRecords = { 111L, 112L };

    GridmixSplit test = new GridmixSplit(cfsplit, 2, 3, 4L, 5L, 6L, 7L,
        reduceBytes, reduceRecords, reduceOutputBytes, reduceOutputRecords);

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(data);
    test.write(out);
    GridmixSplit copy = new GridmixSplit();
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

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void test1() throws Exception {

    Configuration conf = new Configuration();
    conf.setInt(JobContext.NUM_REDUCES, 2);

    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);

    TaskAttemptID taskid = new TaskAttemptID();
    RecordReader reader = new FakeRecordReader();

    LoadRecordWriter writer = new LoadRecordWriter();

    OutputCommitter committer = new CustomOutputCommitter();
    StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
    LoadSplit split = getLoadSplit();

    MapContext mapcontext = new MapContextImpl(conf, taskid, reader, writer,
        committer, reporter, split);
    Context ctxt = new WrappedMapper().getMapContext(mapcontext);

    reader.initialize(split, ctxt);
    ctxt.getConfiguration().setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    CompressionEmulationUtil.setCompressionEmulationEnabled(
        ctxt.getConfiguration(), true);

    // when(ctxt.getCounter(TaskCounter.SPILLED_RECORDS)).thenReturn(counter);

    LoadJob.LoadMapper mapper = new LoadJob.LoadMapper();

    mapper.run(ctxt);

    // mapper.cleanup(ctxt);
    System.out.println("OK");
    Map<Object, Object> data = writer.getData();
    assertEquals(2, data.size());

  }

  private LoadSplit getLoadSplit() throws Exception {

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

    LoadSplit result = new LoadSplit(cfsplit, 2, 1, 4L, 5L, 6L, 7L,
        reduceBytes, reduceRecords, reduceOutputBytes, reduceOutputRecords,
        metrics, rMetrics);
    return result;
  }

  protected class FakeRecordReader extends
      RecordReader<NullWritable, GridmixRecord> {

    int counter = 10;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      counter--;
      return counter > 0;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException,
        InterruptedException {

      return NullWritable.get();
    }

    @Override
    public GridmixRecord getCurrentValue() throws IOException,
        InterruptedException {
      return new GridmixRecord(100, 100L);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return counter / 10.0f;
    }

    @Override
    public void close() throws IOException {
      counter = 10;
    }
  }

  private class LoadRecordWriter extends RecordWriter<Object, Object> {
    private Map<Object, Object> data = new HashMap<Object, Object>();

    @Override
    public void write(Object key, Object value) throws IOException,
        InterruptedException {
      data.put(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
    }

    public Map<Object, Object> getData() {
      return data;
    }

  };

  @Test
  public void testLoadJobLoadSortComparator() throws Exception {
    LoadJob.LoadSortComparator test = new LoadJob.LoadSortComparator();

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(data);
    WritableUtils.writeVInt(dos, 2);
    WritableUtils.writeVInt(dos, 1);
    WritableUtils.writeVInt(dos, 4);
    WritableUtils.writeVInt(dos, 7);
    WritableUtils.writeVInt(dos, 4);

    byte[] b1 = data.toByteArray();

    byte[] b2 = data.toByteArray();

    // int s1, int l1, byte[] b2, int s2, int l2
    assertEquals(0, test.compare(b1, 0, 1, b2, 0, 1));
    b2[2] = 5;
    assertEquals(-1, test.compare(b1, 0, 1, b2, 0, 1));
    b2[2] = 2;
    assertEquals(2, test.compare(b1, 0, 1, b2, 0, 1));
    // compare arrays by first byte witch offset (2-1) because 4==4
    b2[2] = 4;
    assertEquals(1, test.compare(b1, 0, 1, b2, 1, 1));

  }
  
  @Test
  public void testGridmixJobSpecGroupingComparator () throws Exception {
    GridmixJob.SpecGroupingComparator  test = new GridmixJob.SpecGroupingComparator ();

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(data);
    WritableUtils.writeVInt(dos, 2);
    WritableUtils.writeVInt(dos, 1);
    WritableUtils.writeVInt(dos, 0);
    WritableUtils.writeVInt(dos, 7);
    WritableUtils.writeVInt(dos, 4);

    byte[] b1 = data.toByteArray();

    byte[] b2 = data.toByteArray();

    // int s1, int l1, byte[] b2, int s2, int l2
    assertEquals(0, test.compare(b1, 0, 1, b2, 0, 1));
    b2[2] = 1;
    assertEquals(-1, test.compare(b1, 0, 1, b2, 0, 1));
    // by Reduce spec
    b2[2] = 1;
    assertEquals(-1, test.compare(b1, 0, 1, b2, 0, 1));
    
   
    assertEquals(0,test.compare(new GridmixKey(GridmixKey.DATA,100,2), new GridmixKey(GridmixKey.DATA,100,2)));
    // REDUSE SPEC
    assertEquals(-1,test.compare(new GridmixKey(GridmixKey.REDUCE_SPEC,100,2), new GridmixKey(GridmixKey.DATA,100,2)));
    assertEquals(1,test.compare(new GridmixKey(GridmixKey.DATA,100,2), new GridmixKey(GridmixKey.REDUCE_SPEC,100,2)));
    // only DATA
    assertEquals(1,test.compare(new GridmixKey(GridmixKey.DATA,102,2), new GridmixKey(GridmixKey.DATA,100,2)));

  }
  /*
  @Test
  public viod testCompareGridmixJob(){
    Configureation conf= new Configuration();
    GridmixJob j1= new GridmixJob();
  }
  */
}
