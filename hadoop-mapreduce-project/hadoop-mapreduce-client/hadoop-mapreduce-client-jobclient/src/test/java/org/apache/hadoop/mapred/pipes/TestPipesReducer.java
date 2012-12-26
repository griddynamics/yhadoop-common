package org.apache.hadoop.mapred.pipes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.pipes.TestPipeApplication.FakeSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.junit.Test;

public class TestPipesReducer {
  private static File workSpace = new File("target",
      TestPipesReducer.class.getName() + "-workSpace");
  @Test 
  public void testPipesReduser() throws Exception{
    
    cleanTokenPasswordFile();
    JobConf conf = new JobConf();
    
    Token<ApplicationTokenIdentifier> token = new Token<ApplicationTokenIdentifier>(
        "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
            "service"));
    conf.getCredentials().addToken(new Text("ShuffleAndJobToken"), token);
    
    File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeReducerClient");
    conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
    
    PipesReducer<BooleanWritable, Text, IntWritable, Text> reduser =  new PipesReducer<BooleanWritable, Text, IntWritable, Text>();
    reduser.configure(conf);
    BooleanWritable bw= new BooleanWritable(false);
    
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_001_02_r03_04_05");
    initstdout (conf);
    conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);
    CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(  new Counters.Counter(), new Progress(), conf);
    Reporter reporter= new TestTaskReporter();
    List<Text> ltext= new ArrayList<Text>();
    ltext.add(new Text("1 boolean"));
    
    reduser.reduce(bw,ltext.iterator(), output, reporter);
    reduser.close();
    System.out.println("ok");
    
  }
  
  private void initstdout(JobConf conf){
    TaskAttemptID taskid = 
        TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID));
    File stdout = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDERR);
    // prepare folder 
    if(!stdout.getParentFile().exists()){
      stdout.getParentFile().mkdirs();
    }else{ //clean logs 
      stdout.deleteOnExit();
      stderr.deleteOnExit();
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
  
  private class Progress implements Progressable {

    @Override
    public void progress() {

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
}
