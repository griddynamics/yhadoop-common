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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Test;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

/**
 * TestCounters checks the sanity and recoverability of Queue
 */
public class TestTaskLog {

  @Test
  public void testTaskLog() throws IOException {
    // test TaskLog
    System.setProperty(MRJobConfig.TASK_LOG_DIR, "testString");
    assertEquals(TaskLog.getMRv2LogDir(), "testString");
    TaskAttemptID taid= mock(TaskAttemptID.class);
    JobID jid= new JobID("job",1);
    
    when(taid.getJobID()).thenReturn(jid);
    when(taid.toString()).thenReturn("JobId");
    
   File f= TaskLog.getTaskLogFile(taid,true,LogName.STDOUT);
   assertTrue(f.getAbsolutePath().endsWith("testString/stdout"));
   
   //test getRealTaskLogFileLocation
   
   File indexFile = TaskLog.getIndexFile(taid, true);
   if(!indexFile.getParentFile().exists()){
     indexFile.getParentFile().mkdirs();
   }
   indexFile.delete();
   indexFile.createNewFile(); 
   
   
   TaskLog.syncLogs("location", taid, true);

   assertTrue(indexFile.getAbsolutePath().endsWith("userlogs/job_job_0001/JobId.cleanup/log.index"));

   f= TaskLog.getRealTaskLogFileLocation(taid,true,LogName.DEBUGOUT);
   if(f!=null){
     assertTrue(f.getAbsolutePath().endsWith("location/debugout"));
     FileUtils.copyFile(indexFile, f);
   }
   // test obtainLogDirOwner
   assertTrue(TaskLog.obtainLogDirOwner(taid).length()>0);
   // test TaskLog.Reader
   assertTrue(readTaskLog(TaskLog.LogName.DEBUGOUT,taid, true ).length()>0);

  }
  
  public  String readTaskLog(TaskLog.LogName filter,
      org.apache.hadoop.mapred.TaskAttemptID taskId, boolean isCleanup)
      throws IOException {
    // string buffer to store task log
    StringBuffer result = new StringBuffer();
    int res;

    // reads the whole tasklog into inputstream
    InputStream taskLogReader = new TaskLog.Reader(taskId, filter, 0, -1,
        isCleanup);
    // construct string log from inputstream.
    byte[] b = new byte[65536];
    while (true) {
      res = taskLogReader.read(b);
      if (res > 0) {
        result.append(new String(b));
      } else {
        break;
      }
    }
    taskLogReader.close();

    // trim the string and return it
    String str = result.toString();
    str = str.trim();
    return str;
  }

  /**
   * test without TASK_LOG_DIR
   * @throws IOException
   */
  @Test
  public void testTaskLogWithoutTaskLogDir() throws IOException {
//    TaskLog tasklog= new TaskLog();
    System.clearProperty(MRJobConfig.TASK_LOG_DIR);

    // test TaskLog
    
    assertEquals(TaskLog.getMRv2LogDir(), null);
    TaskAttemptID taid= mock(TaskAttemptID.class);
    JobID jid= new JobID("job",1);
    
    when(taid.getJobID()).thenReturn(jid);
    when(taid.toString()).thenReturn("JobId");
    
   File f= TaskLog.getTaskLogFile(taid,true,LogName.STDOUT);
   assertTrue(f.getAbsolutePath().endsWith("stdout"));
   
  }
  @SuppressWarnings("deprecation")
  @Test
  public void testExternalCall() throws IOException {
    
    List<String> setup= Arrays.asList("setup");
    List<String> cmd= Arrays.asList("cmd");

    String commandLine=TaskLog.buildCommandLine(setup, cmd, new File("stdoutFilename"),  new File("stderrFilename"), 100L, true);
    assertEquals(commandLine, " export JVM_PID=`echo $$` ; 'setup' ;('cmd'  < /dev/null  | tail -c 100 >> stdoutFilename ; exit $PIPESTATUS ) 2>&1 | tail -c 100 >> stderrFilename ; exit $PIPESTATUS");
    commandLine=TaskLog.buildDebugScriptCommandLine(cmd, "debugout");
    assertEquals(commandLine, "exec cmd  < /dev/null  >debugout 2>&1 ");
    List<String> commandLines =TaskLog.captureDebugOut(cmd, new File("debugoutFilename"));
    assertTrue(commandLines.size()==3);
    Iterator<String> it=commandLines.iterator();
    assertEquals(it.next(), "bash");
    assertEquals(it.next(), "-c");
    assertEquals(it.next(), "exec cmd  < /dev/null  >debugoutFilename 2>&1 ");

    commandLines = TaskLog.captureOutAndError(cmd, new File("stdoutFilename"), new File("stderrFilename"), 100L);
    assertTrue(commandLines.size()==3);
    it=commandLines.iterator();
    assertEquals(it.next(), "bash");
    assertEquals(it.next(), "-c");
    assertEquals(it.next(), " export JVM_PID=`echo $$` ; ('cmd'  < /dev/null  | tail -c 100 >> stdoutFilename ; exit $PIPESTATUS ) 2>&1 | tail -c 100 >> stderrFilename ; exit $PIPESTATUS");
    commandLines= TaskLog.captureOutAndError(setup, cmd, new File("stdoutFilename"), new File("stderrFilename"), 100L);
    it=commandLines.iterator();
    assertEquals(it.next(), "bash");
    assertEquals(it.next(), "-c");
    assertEquals(it.next(), " export JVM_PID=`echo $$` ; 'setup' ;('cmd'  < /dev/null  | tail -c 100 >> stdoutFilename ; exit $PIPESTATUS ) 2>&1 | tail -c 100 >> stderrFilename ; exit $PIPESTATUS");

    commandLines= TaskLog.captureOutAndError(setup, cmd, new File("stdoutFilename"), new File("stderrFilename"), 100L, true);
    it=commandLines.iterator();
    assertEquals(it.next(), "bash");
    assertEquals(it.next(), "-c");
    assertEquals(it.next(), " export JVM_PID=`echo $$` ; 'setup' ;('cmd'  < /dev/null  | tail -c 100 >> stdoutFilename ; exit $PIPESTATUS ) 2>&1 | tail -c 100 >> stderrFilename ; exit $PIPESTATUS");
    
    commandLines= TaskLog.captureOutAndError(setup, cmd, new File("stdoutFilename"), new File("stderrFilename"), 100L, "pidFileName");
    it=commandLines.iterator();
    assertEquals(it.next(), "bash");
    assertEquals(it.next(), "-c");
    assertEquals(it.next(), " export JVM_PID=`echo $$` ; 'setup' ;('cmd'  < /dev/null  | tail -c 100 >> stdoutFilename ; exit $PIPESTATUS ) 2>&1 | tail -c 100 >> stderrFilename ; exit $PIPESTATUS");
   
   System.out.println("end");
  }
}
