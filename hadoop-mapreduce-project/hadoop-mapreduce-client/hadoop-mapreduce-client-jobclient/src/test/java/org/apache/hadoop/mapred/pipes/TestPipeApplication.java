package org.apache.hadoop.mapred.pipes;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
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

  private static String taskName = "attempt_001_02_r03_04_05";

  /**
   * test PipesMapRunner
   * 
   * @throws Exception
   */
  @Test
  public void testRunner() throws Exception {

    // clean old password files
    File[] psw = cleanTokenPasswordFile();
    try {
      RecordReader<FloatWritable, NullWritable> rReader = new Reader();
      JobConf conf = new JobConf();
      conf.set(Submitter.IS_JAVA_RR, "true");
      // for stdour and stderror

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskName);

      CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(
          new Counters.Counter(), new Progress(), conf);
      FileSystem fs = new RawLocalFileSystem();
      fs.setConf(conf);
      Writer<IntWritable, Text> wr = new Writer<IntWritable, Text>(conf, fs,
          new Path(workSpace + File.separator + "outfile"), IntWritable.class,
          Text.class, null, null);
      output.setWriter(wr);
      // stub for client
      File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicatoinRunabeClient");

      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
      // token for authorization
      Token<ApplicationTokenIdentifier> token = new Token<ApplicationTokenIdentifier>(
          "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));
      conf.getCredentials().addToken(new Text("ShuffleAndJobToken"), token);

      TestTaskReporter reporter = new TestTaskReporter();
      PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text> runner = new PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text>();

      initstdout(conf);

      runner.configure(conf);
      runner.run(rReader, output, reporter);

      String stdout = readstdout(conf);

      // test part of translated data. As common file for client and test -
      // clients stdout
      assertTrue(stdout.contains("s2: org.apache.hadoop.io.FloatWritable"));
      assertTrue(stdout.contains("s2: org.apache.hadoop.io.NullWritable"));
      assertTrue(stdout
          .contains("split:org.apache.hadoop.mapred.pipes.TestPipeApplication$FakeSplit"));

    } finally {
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }

    }
  }

  /**
   * test org.apache.hadoop.mapred.pipes.Application
   * 
   * @throws Throwable
   */
  @Test
  public void testApplication() throws Throwable {
    JobConf conf = new JobConf();

    RecordReader<FloatWritable, NullWritable> rReader = new Reader();

    // client for test
    File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicatoinClient");

    TestTaskReporter reporter = new TestTaskReporter();

    File[] psw = cleanTokenPasswordFile();
    try {

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskName);
      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());

      // token for authorization
      Token<ApplicationTokenIdentifier> token = new Token<ApplicationTokenIdentifier>(
          "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));

      conf.getCredentials().addToken(new Text("ShuffleAndJobToken"), token);
      CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(
          new Counters.Counter(), new Progress(), conf);
      FileSystem fs = new RawLocalFileSystem();
      fs.setConf(conf);
      Writer<IntWritable, Text> wr = new Writer<IntWritable, Text>(conf, fs,
          new Path(workSpace + File.separator + "outfile"), IntWritable.class,
          Text.class, null, null);
      output.setWriter(wr);
      conf.set(Submitter.PRESERVE_COMMANDFILE, "true");

      Application<WritableComparable<IntWritable>, Writable, IntWritable, Text> application = new Application<WritableComparable<IntWritable>, Writable, IntWritable, Text>(
          conf, rReader, output, reporter, IntWritable.class, Text.class);
      application.getDownlink().flush();

      application.getDownlink().mapItem(new IntWritable(3), new Text("txt"));

      application.getDownlink().flush();

      application.waitForFinish();

      wr.close();

      // check getDownlink().mapItem();
      String stdout = readstdout(conf);
      assertTrue(stdout.contains("map object1:txt"));
      assertTrue(stdout.contains("map object:3"));

      // reporter test counter, and status should be sended
      assertEquals(1.0, reporter.getProgress(), 0.01);
      assertNotNull(reporter.getCounter("group", "name"));
      assertEquals(reporter.getStatus(), "PROGRESS");

      application.getDownlink().close();
      try {
        // try to abort
        application.abort(new Throwable());
        fail();
      } catch (IOException e) {
        // abort works ?
        assertEquals("pipe child exception", e.getMessage());
      }
    } finally {
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }
    }
  }

  /**
   * test org.apache.hadoop.mapred.pipes.Submitter
   * 
   * @throws Exception
   */
  @Test
  public void testSubmitter() throws Exception {

    JobConf conf = new JobConf();

    File[] psw = cleanTokenPasswordFile();

    System.setProperty("test.build.data",
        "target/tmp/build/TEST_SUBMITTER_MAPPER/data");
    conf.set("hadoop.log.dir", "target/tmp");

    // prepare configuration
    Submitter.setIsJavaMapper(conf, false);
    Submitter.setIsJavaReducer(conf, false);
    Submitter.setKeepCommandFile(conf, false);
    Submitter.setIsJavaRecordReader(conf, false);
    Submitter.setIsJavaRecordWriter(conf, false);
    PipesPartitioner<IntWritable, Text> partitioner = new PipesPartitioner<IntWritable, Text>();
    partitioner.configure(conf);

    Submitter.setJavaPartitioner(conf, partitioner.getClass());

    assertEquals(PipesPartitioner.class, (Submitter.getJavaPartitioner(conf)));
    // test going to call main method with System.exit(). Change Security
    SecurityManager securityManager = System.getSecurityManager();
    // store System.out
    PrintStream oldps = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // test without parameters
    try {
      System.setSecurityManager(new NoExitSecurityManager());

      System.setOut(new PrintStream(out));
      Submitter.main(new String[0]);
      fail();
    } catch (ExitException e) {
      // System.exit prohibited! output message test
      assertTrue(out.toString().contains(""));
      assertTrue(out.toString().contains("bin/hadoop pipes"));
      assertTrue(out.toString().contains("[-input <path>] // Input directory"));
      assertTrue(out.toString()
          .contains("[-output <path>] // Output directory"));
      assertTrue(out.toString().contains("[-jar <jar file> // jar filename"));
      assertTrue(out.toString().contains(
          "[-inputformat <class>] // InputFormat class"));
      assertTrue(out.toString().contains("[-map <class>] // Java Map class"));
      assertTrue(out.toString().contains(
          "[-partitioner <class>] // Java Partitioner"));
      assertTrue(out.toString().contains(
          "[-reduce <class>] // Java Reduce class"));
      assertTrue(out.toString().contains(
          "[-writer <class>] // Java RecordWriter"));
      assertTrue(out.toString().contains(
          "[-program <executable>] // executable URI"));
      assertTrue(out.toString().contains(
          "[-reduces <num>] // number of reduces"));
      assertTrue(out.toString().contains(
          "[-lazyOutput <true/false>] // createOutputLazily"));

      assertTrue(out
          .toString()
          .contains(
              "-conf <configuration file>     specify an application configuration file"));
      assertTrue(out.toString().contains(
          "-D <property=value>            use value for given property"));
      assertTrue(out.toString().contains(
          "-fs <local|namenode:port>      specify a namenode"));
      assertTrue(out.toString().contains(
          "-jt <local|jobtracker:port>    specify a job tracker"));
      assertTrue(out
          .toString()
          .contains(
              "-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster"));
      assertTrue(out
          .toString()
          .contains(
              "-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath."));
      assertTrue(out
          .toString()
          .contains(
              "-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines."));
    } finally {
      System.setOut(oldps);
      // restore
      System.setSecurityManager(securityManager);
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }
    }
    // test call Submitter form command line
    try {
      System.setSecurityManager(new NoExitSecurityManager());
      File fCommand = getFileCommand(null);
      String[] args = new String[22];
      File input = new File(workSpace + File.separator + "input");
      if (!input.exists()) {
        input.createNewFile();
      }
      File outpot = new File(workSpace + File.separator + "output");
      FileUtil.fullyDelete(outpot);

      args[0] = "-input";
      args[1] = input.getAbsolutePath();// "input";
      args[2] = "-output";
      args[3] = outpot.getAbsolutePath();// "output";
      args[4] = "-inputformat";
      args[5] = "org.apache.hadoop.mapred.TextInputFormat";
      args[6] = "-map";
      args[7] = "org.apache.hadoop.mapred.lib.IdentityMapper";
      args[8] = "-partitioner";
      args[9] = "org.apache.hadoop.mapred.pipes.PipesPartitioner";
      args[10] = "-reduce";
      args[11] = "org.apache.hadoop.mapred.lib.IdentityReducer";
      args[12] = "-writer";
      args[13] = "org.apache.hadoop.mapred.TextOutputFormat";
      args[14] = "-program";
      args[15] = fCommand.getAbsolutePath();// "program";
      args[16] = "-reduces";
      args[17] = "2";
      args[18] = "-lazyOutput";
      args[19] = "lazyOutput";
      args[20] = "-jobconf";
      args[21] = "mapreduce.pipes.isjavarecordwriter=false,mapreduce.pipes.isjavarecordreader=false";

      Submitter.main(args);
      fail();
    } catch (ExitException e) {
      // status should be 0
      assertEquals(e.status, 0);

    } finally {
      System.setOut(oldps);
      System.setSecurityManager(securityManager);
    }

  }

  /**
   * test org.apache.hadoop.mapred.pipes.PipesReducer
   * 
   * @throws Exception
   */
  @Test
  public void testPipesReduser() throws Exception {

    File[] psw = cleanTokenPasswordFile();
    JobConf conf = new JobConf();
    try {
      Token<ApplicationTokenIdentifier> token = new Token<ApplicationTokenIdentifier>(
          "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));
      conf.getCredentials().addToken(new Text("ShuffleAndJobToken"), token);

      File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeReducerClient");
      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());

      PipesReducer<BooleanWritable, Text, IntWritable, Text> reduser = new PipesReducer<BooleanWritable, Text, IntWritable, Text>();
      reduser.configure(conf);
      BooleanWritable bw = new BooleanWritable(true);

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskName);
      initstdout(conf);
      conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);
      CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(
          new Counters.Counter(), new Progress(), conf);
      Reporter reporter = new TestTaskReporter();
      List<Text> ltext = new ArrayList<Text>();
      ltext.add(new Text("1 boolean"));

      reduser.reduce(bw, ltext.iterator(), output, reporter);
      reduser.close();
      String stdout= readstdout(conf);
     // test data
      assertTrue(stdout.contains("reducer key :true"));
      assertTrue(stdout.contains("reduce value  :1 boolean"));

      System.out.println("ok!");
    } finally {
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }
    }

  }

  /**
   * test PipesPartitioner
   */
  @Test
  public void testPipesPartitioner() {

    PipesPartitioner<IntWritable, Text> partitioner = new PipesPartitioner<IntWritable, Text>();
    JobConf conf = new JobConf();
    Submitter.getJavaPartitioner(conf);
    partitioner.configure(new JobConf());
    IntWritable iw = new IntWritable(4);
    assertEquals(0, partitioner.getPartition(iw, new Text("test"), 2));

    PipesPartitioner.setNextPartition(3);

    assertEquals(3, partitioner.getPartition(iw, new Text("test"), 2));
  }

  /**
   * clean previous std  error and outs
   * 
   * @param conf
   */

  private void initstdout(JobConf conf) {
    TaskAttemptID taskid = TaskAttemptID.forName(conf
        .get(MRJobConfig.TASK_ATTEMPT_ID));
    File stdout = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDERR);
    // prepare folder
    if (!stdout.getParentFile().exists()) {
      stdout.getParentFile().mkdirs();
    } else { // clean logs
      stdout.deleteOnExit();
      stderr.deleteOnExit();
    }
  }

  private String readstdout(JobConf conf) throws Exception {
    TaskAttemptID taskid = TaskAttemptID.forName(conf
        .get(MRJobConfig.TASK_ATTEMPT_ID));
    File stdout = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDOUT);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream is = new FileInputStream(stdout);
    byte[] buffer = new byte[1024];
    int counter = 0;
    while ((counter = is.read(buffer)) >= 0) {
      out.write(buffer, 0, counter);
    }

    is.close();

    return out.toString();

  }

  private class Progress implements Progressable {

    @Override
    public void progress() {

    }

  }

  private File[] cleanTokenPasswordFile() throws Exception {
    File[] result = new File[2];
    result[0] = new File("./jobTokenPassword");
    if (result[0].exists()) {
      FileUtil.chmod(result[0].getAbsolutePath(), "700");
      result[0].delete();
    }
    result[1] = new File("./.jobTokenPassword.crc");
    if (result[1].exists()) {
      FileUtil.chmod(result[1].getAbsolutePath(), "700");
      result[1].delete();
    }
    return result;
  }

  private File getFileCommand(String clazz) throws Exception {
    String classpath = System.getProperty("java.class.path");
    File fCommand = new File(workSpace + File.separator + "cache.sh");
    fCommand.deleteOnExit();
    if (!fCommand.getParentFile().exists()) {
      fCommand.getParentFile().mkdirs();
    }
    fCommand.createNewFile();
    OutputStream os = new FileOutputStream(fCommand);
    os.write("#!/bin/sh \n".getBytes());
    if (clazz == null) {
      os.write(("ls ").getBytes());
    } else {
      os.write(("java -cp " + classpath + " " + clazz).getBytes());
    }
    os.flush();
    os.close();
    FileUtil.chmod(fCommand.getAbsolutePath(), "700");
    return fCommand;
  }

  private class CombineOutputCollector<K extends Object, V extends Object>
      implements OutputCollector<K, V> {
    private Writer<K, V> writer;
    private Counters.Counter outCounter;
    private Progressable progressable;

    public CombineOutputCollector(Counters.Counter outCounter,
        Progressable progressable, Configuration conf) {
      this.outCounter = outCounter;
      this.progressable = progressable;
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

  public static class FakeSplit implements InputSplit {
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    public long getLength() {
      return 0L;
    }

    public String[] getLocations() {
      return new String[0];
    }
  }

  private class TestTaskReporter implements Reporter {
    private int recordNum = 0; // number of records processed
    private String status = null;
    private Counters counters = new Counters();
    private InputSplit split = new FakeSplit();

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

    @Override
    public float getProgress() {
      return recordNum;
    }

  }

  private class Reader implements RecordReader<FloatWritable, NullWritable> {
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
  }

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }

  protected static class ExitException extends SecurityException {
    private static final long serialVersionUID = -1982617086752946683L;
    public final int status;

    public ExitException(int status) {
      super("There is no escape!");
      this.status = status;
    }
  }

}
