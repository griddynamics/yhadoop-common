package org.apache.hadoop.mapred.gridmix;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.CustomOutputCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.yarn.YarnException;
import org.jets3t.service.io.InputStreamWrapper;
import org.junit.Ignore;
import org.junit.Test;
import static org.mockito.Mockito.*;

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
  public void testGridmixJobSpecGroupingComparator() throws Exception {
    GridmixJob.SpecGroupingComparator test = new GridmixJob.SpecGroupingComparator();

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

    assertEquals(0, test.compare(new GridmixKey(GridmixKey.DATA, 100, 2),
        new GridmixKey(GridmixKey.DATA, 100, 2)));
    // REDUSE SPEC
    assertEquals(-1, test.compare(
        new GridmixKey(GridmixKey.REDUCE_SPEC, 100, 2), new GridmixKey(
            GridmixKey.DATA, 100, 2)));
    assertEquals(1, test.compare(new GridmixKey(GridmixKey.DATA, 100, 2),
        new GridmixKey(GridmixKey.REDUCE_SPEC, 100, 2)));
    // only DATA
    assertEquals(2, test.compare(new GridmixKey(GridmixKey.DATA, 102, 2),
        new GridmixKey(GridmixKey.DATA, 100, 2)));

  }

  @Test
  public void testCompareGridmixJob() throws Exception {
    Configuration conf = new Configuration();
    Path outRoot = new Path("target");
    JobStory jobdesc = mock(JobStory.class);
    when(jobdesc.getName()).thenReturn("JobName");
    when(jobdesc.getJobConf()).thenReturn(new JobConf(conf));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    // final Configuration conf, long submissionMillis,
    // final JobStory jobdesc, Path outRoot, UserGroupInformation ugi,
    // final int seq

    GridmixJob j1 = new LoadJob(conf, 1000L, jobdesc, outRoot, ugi, 0);
    GridmixJob j2 = new LoadJob(conf, 1000L, jobdesc, outRoot, ugi, 0);
    assertTrue(j1.equals(j2));
    assertEquals(0, j1.compareTo(j2));
  }

  /*
   * test ReadRecordFactory hould read all data from inputstream
   */
  @Test
  public void testReadRecordFactory() throws Exception {

    // RecordFactory factory, InputStream src, Configuration conf
    RecordFactory rf = new FakeRecordFactory();
    FakeInputStream input = new FakeInputStream();
    ReadRecordFactory test = new ReadRecordFactory(rf, input,
        new Configuration());
    GridmixKey key = new GridmixKey(GridmixKey.DATA, 100, 2);
    GridmixRecord val = new GridmixRecord(200, 2);
    while (test.next(key, val)) {

    }
    // should be read 10* (GridmixKey.size +GridmixRecord.value)
    assertEquals(3000, input.getCounter());
    // shoutd be 0;
    assertEquals(-1, rf.getProgress(), 0.01);

    System.out.println("dd:" + input.getCounter());
    System.out.println("rf:" + rf.getProgress());
    test.close();
  }

  private class FakeRecordFactory extends RecordFactory {

    private int counter = 10;

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean next(GridmixKey key, GridmixRecord val) throws IOException {
      counter--;
      return counter >= 0;
    }

    @Override
    public float getProgress() throws IOException {
      return counter;
    }

  }

  private class FakeInputStream extends InputStream {
    private long counter;

    @Override
    public int read() throws IOException {
      return 0;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int realLen = len - off;
      counter += realLen;
      for (int i = 0; i < b.length; i++) {
        b[i] = 0;
      }
      return realLen;
    }

    public long getCounter() {
      return counter;
    }
  }
  @Ignore
  @Test
  public void testLoadJobLoadRecordReader() throws Exception{
    LoadJob.LoadRecordReader test = new  LoadJob.LoadRecordReader();
    /*CombineFileSplit cfsplit, int maps, int id, long inputBytes, 
    long inputRecords, long outputBytes, long outputRecords, 
    double[] reduceBytes, double[] reduceRecords, 
    long[] reduceOutputBytes, long[] reduceOutputRecords,
    ResourceUsageMetrics metrics,
    ResourceUsageMetrics[] rMetrics
    */
    Path[] paths= {new Path("tmp1"),new Path("tmp2")};
    long[] start = {0,0};
    long[] lengths={1000,1000};
    String[] locations={"temp1","temp2"};
    CombineFileSplit cfsplit = new CombineFileSplit(paths, start, lengths, locations);
    double[] reduceBytes={100,100};
     double[] reduceRecords={2,2}; 
    long[] reduceOutputBytes={500,500};
      long[] reduceOutputRecords={2,2};
      ResourceUsageMetrics metrics= new ResourceUsageMetrics();
      ResourceUsageMetrics[] rMetrics= {new ResourceUsageMetrics(), new ResourceUsageMetrics()};
    LoadSplit input= new LoadSplit(cfsplit,2,3,1500L,2L,3000L,2L,reduceBytes,reduceRecords,reduceOutputBytes,reduceOutputRecords,metrics,rMetrics); 
    Configuration conf = new Configuration();
    TaskAttemptID taskId= new TaskAttemptID();
    TaskAttemptContext ctxt = new TaskAttemptContextImpl(conf, taskId);
    test.initialize(input, ctxt);
    
  }
}
