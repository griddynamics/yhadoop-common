package org.apache.hadoop.mapred.pipes;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.junit.Test;


import static org.junit.Assert.*;

public class TestPipeApplication {
  private static File workSpace = new File("target",
      TestPipeApplication.class.getName() + "-workSpace");

  @Test
  public void testOne() throws Exception {
    JobConf conf = new JobConf();

    RecordReader<FloatWritable, NullWritable> rReader = new RecordReader<FloatWritable, NullWritable>() {
      private float index = 0.0f;

      @Override
      public boolean next(FloatWritable key, NullWritable value)
          throws IOException {
        key.set(index++);
        return false;
      }

      @Override
      public float getProgress() throws IOException {
        return index;
      }

      @Override
      public long getPos() throws IOException {

        return 0;
      }

      @Override
      public NullWritable createValue() {

        return NullWritable.get();
      }

      @Override
      public FloatWritable createKey() {
        FloatWritable result = new FloatWritable(index);
        return result;
      }

      @Override
      public void close() throws IOException {

      }
    };

    String classpath = System.getProperty("java.class.path");
    File fCommand = new File(workSpace + File.separator + "cache.sh");
    fCommand.deleteOnExit();
    if (!fCommand.getParentFile().exists()) {
      fCommand.getParentFile().mkdirs();
    }
    fCommand.createNewFile();
    OutputStream os = new FileOutputStream(fCommand);
    os.write("#!/bin/sh \n".getBytes());

    // /opt/yahoo/yhadoop-common/hadoop-common-project/hadoop-common
    // /opt/yahoo/yhadoop-common/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core

    os.write(("java -cp " + classpath + " org.apache.hadoop.mapred.pipes.PipeApplicatoinClient")
        .getBytes());
    os.flush();
    os.close();

    TestTaskReporter reporter = new TestTaskReporter();
    // Counters.Counter cnt =new Counters.Counter();
    // Credentials credentials =
    // UserGroupInformation.getCurrentUser().getCredentials();
    // ApplicationTokenIdentifier identifier= new ApplicationTokenIdentifier();
    // org.apache.hadoop.security.token.Token
    File psw = new File("./jobTokenPassword");
    psw.deleteOnExit();
    psw = new File("./.jobTokenPassword.crc");
    psw.deleteOnExit();
    try {

      TaskAttemptID taskid = TaskAttemptID.forName("attempt_001_02_r03_04_05");
      File stdout = TaskLog.getTaskLogFile(taskid, false,
          TaskLog.LogName.STDOUT);
      File stderr = TaskLog.getTaskLogFile(taskid, false,
          TaskLog.LogName.STDERR);
      if (!stdout.getParentFile().exists()) {
        stdout.getParentFile().mkdirs();
      }
      stdout.deleteOnExit();
      stderr.deleteOnExit();

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_001_02_r03_04_05");
      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
      Token<ApplicationTokenIdentifier> token = new Token<ApplicationTokenIdentifier>(
          "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));
      conf.getCredentials().addToken(new Text("ShuffleAndJobToken"), token);
      CombineOutputCollector<Text, Text> output = new CombineOutputCollector<Text, Text>(
          new Counters.Counter(), new Progress(), conf);
      FileSystem fs = new RawLocalFileSystem();
      fs.setConf(conf);
      Writer<Text, Text> wr = new Writer<Text, Text>(conf, fs,
          new Path(workSpace + File.separator + "outfile"), Text.class,
          Text.class, null, null);
      output.setWriter(wr);
      
      Application<WritableComparable<Object>, Writable, Text, Text> application = new Application<WritableComparable<Object>, Writable, Text, Text>(
          conf, rReader, output, reporter, Text.class, Text.class);
      application.getDownlink().flush();

      Thread.sleep(4000);
      wr.close();
      System.out.println("==================");
      printFile(stderr);
      System.out.println("==================");
      printFile(stdout);
      System.out.println("==================");
      
      assertEquals(1.0, reporter.getProgress(), 0.01);
      assertNotNull(reporter.getCounter("group", "name"));
      assertEquals(reporter.getStatus(), "PROGRESS");
    } finally {
      psw.deleteOnExit();
      psw.deleteOnExit();

    }
    System.out.println("ok!");
  }

  private void printFile(File f) throws IOException{
    InputStream in= new FileInputStream(f);
    byte [] ab = new byte[512];
    int counter=0;
    ByteArrayOutputStream output= new ByteArrayOutputStream();
    while ((counter=in.read(ab))>=0){
      output.write(ab, 0, counter);
    }
    in.close();
    System.out.println("file:" + new String(output.toByteArray()));
  }
  private class Progress implements Progressable {

    @Override
    public void progress() {
      // no think todo

    }

  }

  private class CombineOutputCollector<K extends Object, V extends Object>
      implements OutputCollector<K, V> {
    private Writer<K, V> writer;
    private Counters.Counter outCounter;
    private Progressable progressable;
    private long progressBar;

    public CombineOutputCollector(Counters.Counter outCounter,
        Progressable progressable, Configuration conf) {
      this.outCounter = outCounter;
      this.progressable = progressable;
      progressBar = conf
          .getLong(MRJobConfig.COMBINE_RECORDS_BEFORE_PROGRESS, 0);
    }

    public synchronized void setWriter(Writer<K, V> writer) {
      this.writer = writer;
    }

    public synchronized void collect(K key, V value) throws IOException {
      outCounter.increment(1);
      writer.append(key, value);
      progressable.progress();
    }
  }

  private class TestTaskReporter implements Reporter {
    private int recordNum = 0; // number of records processed
    private String status = null;
    private Counters counters = new Counters();
    private InputSplit split;

    @Override
    public void progress() {

      recordNum++;
    }

    @Override
    public void setStatus(String status) {
      this.status = status;

    }

    public String getStatus() {
      return this.status;

    }

    public Counters.Counter getCounter(String group, String name) {
      Counters.Counter counter = null;
      if (counters != null) {
        counter = counters.findCounter(group, name);
        if (counter == null) {
          Group grp = counters.addGroup(group, group);
          counter = grp.addCounter(name, name, 10);
        }
      }
      return counter;
    }

    public Counters.Counter getCounter(Enum<?> name) {
      return counters == null ? null : counters.findCounter(name);
    }

    public void incrCounter(Enum<?> key, long amount) {
      if (counters != null) {
        counters.incrCounter(key, amount);
      }
    }

    public void incrCounter(String group, String counter, long amount) {

      if (counters != null) {
        counters.incrCounter(group, counter, amount);
      }

    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return split;
    }

    public void setInputSplit(InputSplit split)
        throws UnsupportedOperationException {
      this.split = split;
    }

    @Override
    public float getProgress() {
      return recordNum;
    }

  }

}
