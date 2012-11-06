/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.ServletException;
import java.io.IOException;
import java.io.DataOutputStream;

/**
 * Base class to test Job end notification in local and cluster mode.
 *
 * Starts up hadoop on Local or Cluster mode (by extending of the
 * HadoopTestCase class) and it starts a servlet engine that hosts
 * a servlet that will receive the notification of job finalization.
 *
 * The notification servlet returns a HTTP 400 the first time is called
 * and a HTTP 200 the second time, thus testing retry.
 *
 * In both cases local file system is used (this is irrelevant for
 * the tested functionality)
 *
 * 
 */
public abstract class NotificationTestCase extends HadoopTestCase {

  protected NotificationTestCase(int mode) throws IOException {
    super(mode, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  private int port;
  private String contextPath = "/notification";
  private String servletPath = "/mapred";
  private Server webServer;
  private NotificationServlet notificationServlet;

  private void startHttpServer() throws Exception {

    // Create the webServer
    if (webServer != null) {
      webServer.stop();
      webServer = null;
    }
    webServer = new Server(0);

    Context context = new Context(webServer, contextPath);

    // create servlet handler
    notificationServlet = new NotificationServlet();
    context.addServlet(new ServletHolder(notificationServlet), servletPath);

    // Start webServer
    webServer.start();
    port = webServer.getConnectors()[0].getLocalPort();
  }

  private void stopHttpServer() throws Exception {
    if (webServer != null) {
      webServer.stop();
      webServer.destroy();
      webServer = null;
    }
  }

  public static class NotificationServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    
    private int counter = 0;
    private int failureCounter = 0;
    
    protected synchronized void doGet(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
      final String queryString = req.getQueryString();
      switch (counter) {
        case 0:
          verifyQuery(queryString, "SUCCEEDED");//); !!!!!
          break;
        case 2:
          verifyQuery(queryString, "FAILED"); //"KILLED");
          break;
        case 4:
          verifyQuery(queryString, "FAILED");
          break;
      }
      if (counter % 2 == 0) {
        System.out.println("####### Counter = "+counter+": sending back error 400");
        res.sendError(HttpServletResponse.SC_BAD_REQUEST, "forcing error");
      } else {
        res.setStatus(HttpServletResponse.SC_OK);
      }
      counter++;
      
      System.out.println("Query:   ["+queryString+"]");
      System.out.println("Counter: ["+counter+"]");
    }

    protected void verifyQuery(String query, String expected) 
        throws IOException {
      if (query.contains(expected)) {
        System.out.println("############### The request (" + query + ") contains [" + expected 
            + "]: okay.");
        return;
      }
      failureCounter++;
      System.err.println("############### The request (" + query + ") does not contain [" + expected 
          + "]");
      assertTrue("The request (" + query + ") does not contain [" + expected 
          + "]", false);
    }
    
    public synchronized int getCounter() {
      return counter;
    }
    
    public synchronized int getFailureCounter() {
      return failureCounter;
    }
  }

  private String getNotificationUrlTemplate() {
    return "http://localhost:" + port + contextPath + servletPath +
      "?jobId=$jobId&amp;jobStatus=$jobStatus";
  }

  protected JobConf createJobConf() {
    final JobConf conf = super.createJobConf();
    conf.setJobEndNotificationURI(getNotificationUrlTemplate());
    conf.setInt(JobContext.MR_JOB_END_RETRY_ATTEMPTS, 3);
    conf.setInt(JobContext.MR_JOB_END_RETRY_INTERVAL, 200);
    conf.setInt(JobContext.MAP_MAX_ATTEMPTS, 1);
    //conf.setBoolean("yarn.dispatcher.exit-on-error", false);
//    try {
//      System.out.println("###################### Configuration:");
//      Configuration.dumpConfigurationPlain(conf, new PrintWriter(System.out));
//    } catch (IOException ioe) {
//      ioe.printStackTrace();
//    }
    return conf;
  }


  protected void setUp() throws Exception {
    super.setUp();
    startHttpServer();
  }

  protected void tearDown() throws Exception {
    stopHttpServer();
    super.tearDown();
  }

  public void testMR() throws Exception {
    String failureInfo;
    
    final String output = launchWordCount(createJobConf(), 
        "a b c d e f g h", 1, 1);
    System.out.println("Word count task output: [" + output + "]");
    
    Thread.sleep(4000);
    
    assertEquals(0, notificationServlet.getFailureCounter());
    assertEquals(2, notificationServlet.getCounter());

    Path inDir = new Path("notificationjob/input");
    Path outDir = new Path("notificationjob/output");
    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data","/tmp")
        .toString().replace(' ', '+');
      inDir = new Path(localPathRoot, inDir);
      outDir = new Path(localPathRoot, outDir);
    }
    
//    // =========================================================================
//    // run a job with KILLED status
//    final RunningJob runningJobKill = UtilsForTests.runJobKill(
//        createJobConf(), inDir, outDir);
//    final JobID jobID = runningJobKill.getID();
//    System.out.println(jobID);
//    runningJobKill.waitForCompletion();
//    
//    String failureInfo = runningJobKill.getFailureInfo();
//    System.out.println("Job-kill failure info: ["+failureInfo+"]");
//    
//    assertTrue(runningJobKill.isComplete());
//    assertTrue(!runningJobKill.isSuccessful());
//    assertEquals(JobStatus.KILLED, runningJobKill.getJobStatus().getRunState());
//    
//    assertEquals(0, notificationServlet.getFailureCounter());
//    assertEquals(4, notificationServlet.getCounter());
//    
//    Thread.sleep(4 
//        * 1000); // !!!!! sleep there causes the test results to change!
//    // So, there are more racing conditions there!
//    assertEquals(4, notificationServlet.getCounter());
//    Thread.sleep(4 
//        * 1000); // !!!!! sleep there causes the test results to change!
    
    // =========================================================================
    // run a job with FAILED status
    final RunningJob runningJobFail = UtilsForTests.runJobFail(
        createJobConf(), inDir, outDir);
    System.out.println(runningJobFail.getID());
    runningJobFail.waitForCompletion();
    
    failureInfo = runningJobFail.getFailureInfo();
    System.out.println("Job-fail failure info: ["+failureInfo+"]");
    
    assertTrue(runningJobFail.isComplete());
    assertTrue(!runningJobFail.isSuccessful());
    assertEquals(JobStatus.FAILED, runningJobFail.getJobStatus().getRunState());
    
    Thread.sleep(4000);
    
    assertEquals(0, notificationServlet.getFailureCounter());
    assertEquals(6, notificationServlet.getCounter());
  }

  private String launchWordCount(JobConf conf,
                                 String input,
                                 int numMaps,
                                 int numReduces) throws IOException {
    Path inDir = new Path("testing/wc/input");
    Path outDir = new Path("testing/wc/output");

    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data","/tmp")
        .toString().replace(' ', '+');;
      inDir = new Path(localPathRoot, inDir);
      outDir = new Path(localPathRoot, outDir);
    }

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordCount.MapClass.class);
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    
    RunningJob runningJob = JobClient.runJob(conf);
    runningJob.waitForCompletion(); 
    
    assertTrue(runningJob.isComplete());
    assertTrue(runningJob.isSuccessful());
    
    return MapReduceTestUtil.readOutput(outDir, conf);
  }

}
