package org.apache.hadoop.mapred.pipes;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.security.Permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mock.*;

import static org.junit.Assert.*;

public class TestPipeApplication {
  private static File workSpace = new File("target",
      TestPipeApplication.class.getName() + "-workSpace");

  @Ignore
  @Test
  public void testRunner() throws Exception {

    File psw = new File("./jobTokenPassword");
    if (psw.exists()) {
      FileUtil.chmod(psw.getAbsolutePath(), "700");
      psw.delete();
    }
    File psw1 = new File("./.jobTokenPassword.crc");
    if (psw1.exists()) {
      FileUtil.chmod(psw1.getAbsolutePath(), "700");
      psw1.delete();
    }
    try {
      RecordReader<FloatWritable, NullWritable> rReader = new Reader();
      JobConf conf = new JobConf();
      conf.set(Submitter.IS_JAVA_RR, "true");
      conf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_001_02_r03_04_05");

      CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(
          new Counters.Counter(), new Progress(), conf);
      FileSystem fs = new RawLocalFileSystem();
      fs.setConf(conf);
      Writer<IntWritable, Text> wr = new Writer<IntWritable, Text>(conf, fs,
          new Path(workSpace + File.separator + "outfile"), IntWritable.class,
          Text.class, null, null);
      output.setWriter(wr);

      File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicatoinRunabeClient");

      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());

      Token<ApplicationTokenIdentifier> token = new Token<ApplicationTokenIdentifier>(
          "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));
      conf.getCredentials().addToken(new Text("ShuffleAndJobToken"), token);
      TestTaskReporter reporter = new TestTaskReporter();
      PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text> runner = new PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text>();
      TaskAttemptID taskid = TaskAttemptID.forName("attempt_001_02_r03_04_05");
      File stdout = TaskLog.getTaskLogFile(taskid, false,
          TaskLog.LogName.STDOUT);
      if (!stdout.getParentFile().exists()) {
        stdout.getParentFile().mkdirs();
      }

      runner.configure(conf);
      runner.run(rReader, output, reporter);
    } finally {
      psw.deleteOnExit();
      psw1.deleteOnExit();
    }
  }

  @Ignore
  @Test
  public void testOne() throws Throwable {
    JobConf conf = new JobConf();

    RecordReader<FloatWritable, NullWritable> rReader = new Reader();

    File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicatoinClient");

    TestTaskReporter reporter = new TestTaskReporter();

    cleanTokenPasswordFile();

    conf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_001_02_r03_04_05");
    conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
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

    Application<WritableComparable<Object>, Writable, IntWritable, Text> application = new Application<WritableComparable<Object>, Writable, IntWritable, Text>(
        conf, rReader, output, reporter, IntWritable.class, Text.class);
    application.getDownlink().flush();

    application.waitForFinish();

    wr.close();

    assertEquals(1.0, reporter.getProgress(), 0.01);
    assertNotNull(reporter.getCounter("group", "name"));
    assertEquals(reporter.getStatus(), "PROGRESS");

    application.getDownlink().close();
    System.out.println("ok!");
  }

  @Test
  public void testSubmitter() throws Exception {

    JobConf conf = new JobConf();

    cleanTokenPasswordFile();

    // Path wordExec = new Path("testing/bin/application");
    System.setProperty("test.build.data",
        "target/tmp/build/TEST_SUBMITTER_MAPPER/data");
    conf.set("hadoop.log.dir", "target/tmp");

    conf.set(Submitter.IS_JAVA_MAP, "false");
    conf.set(Submitter.IS_JAVA_RW, "false");
    conf.set(Submitter.IS_JAVA_REDUCE, "false");
    conf.set(Submitter.IS_JAVA_RR, "false");

    // assertEquals("/opt/yahoo/yhadoop-common/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/target/org.apache.hadoop.mapred.pipes.TestPipeApplication-workSpace/cache.sh",
    // Submitter.getExecutable(conf));

    SecurityManager securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    PrintStream oldps = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
// test without paramaters
    try {
      System.setOut(new PrintStream(out));
      Submitter.main(new String[0]);
      fail();
    } catch (Exception e) {
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
      System.setSecurityManager(securityManager);
    }

    try {
      Submitter sbm=  Mockito.mock(Submitter.class);
      
      RunningJob stm=Mockito.mock(RunningJob.class); 
      Mockito.when(sbm.runJob(conf)).thenReturn(stm);


      String [] args = new String[10];
      args[0]="input";
      args[1]="input";
      args[2]="output";
      args[3]="output";
      args[4]="jar";
      args[5]="jar";
      args[6]="inputformat";
      args[7]="inputformat";
      args[8]="map";
      args[9]="map";
      args[10]="partitioner";
      args[11]="partitioner";
      args[12]="reduce";
      args[13]="reduce";
      args[14]="writer";
      args[15]="writer";
      args[16]="reduces";
      args[17]="reduces";
      args[18]="lazyOutput";
      args[19]="lazyOutput";
      /*
      args[]="";
      args[]="";
      args[]="";
      args[]="";
      args[]="";
      args[]="";
      */
      sbm.main(new String[0]);
      System.setOut(new PrintStream(out));
      Submitter.main(new String[0]);
      fail();
    } catch (Exception e) {
      
    } finally {
      System.setOut(oldps);
      System.setSecurityManager(securityManager);
    }
    
    System.out.println("ok!");
  }

  private class Progress implements Progressable {

    @Override
    public void progress() {

    }

  }

  private void cleanTokenPasswordFile() throws Exception {
    File psw = new File("./jobTokenPassword");
    if (psw.exists()) {
      FileUtil.chmod(psw.getAbsolutePath(), "700");
      psw.delete();
    }
    File psw1 = new File("./.jobTokenPassword.crc");
    if (psw1.exists()) {
      FileUtil.chmod(psw1.getAbsolutePath(), "700");
      psw1.delete();
    }
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
