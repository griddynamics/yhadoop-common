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

package org.apache.hadoop.yarn.server.nodemanager;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLinuxContainerExecutorWithMocks {

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory
      .getLog(TestLinuxContainerExecutorWithMocks.class);

  private LinuxContainerExecutor mockExec = null;
  private final File mockParamFile = new File("./params.txt");
  private LocalDirsHandlerService dirsHandler;
  
  private void deleteMockParamFile() {
    if(mockParamFile.exists()) {
      mockParamFile.delete();
    }
  }
  
  private List<String> readMockParams() throws IOException {
    LinkedList<String> ret = new LinkedList<String>();
    LineNumberReader reader = new LineNumberReader(new FileReader(
        mockParamFile));
    String line;
    while((line = reader.readLine()) != null) {
      ret.add(line);
    }
    reader.close();
    return ret;
  }
  
  @Before
  public void setup() {
    File f = new File("./src/test/resources/mock-container-executor");
    if(!f.canExecute()) {
      f.setExecutable(true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    mockExec = new LinuxContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    mockExec.setConf(conf);
  }

  @After
  public void tearDown() {
    deleteMockParamFile();
  }
  
  @Test
  public void testContainerLaunch() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.LAUNCH_CONTAINER.getValue());
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();
    
    when(container.getContainerID()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    
    when(cId.toString()).thenReturn(containerId);
    
    when(context.getEnvironment()).thenReturn(env);
    
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");
    Path workDir = new Path("/tmp");
    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
    assertEquals(0, ret);
    assertEquals(Arrays.asList(appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(",", dirsHandler.getLocalDirs()),
        StringUtils.join(",", dirsHandler.getLogDirs())),
        readMockParams());

		Configuration conf = mockExec.getConf();
		conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, "/bin/sh");
		mockExec.setConf(conf);
		ret = mockExec.launchContainer(container, scriptPath, tokensPath, appSubmitter, appId, workDir, dirsHandler.getLocalDirs(), dirsHandler.getLogDirs());
		assertEquals(127, ret);
  
  }

  @Test
  public void testContainerKill() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.SIGNAL_CONTAINER.getValue());
    ContainerExecutor.Signal signal = ContainerExecutor.Signal.QUIT;
    String sigVal = String.valueOf(signal.getValue());
    
    mockExec.signalContainer(appSubmitter, "1000", signal);
    assertEquals(Arrays.asList(appSubmitter, cmd, "1000", sigVal),
        readMockParams());
  }
  
  @Test
  public void testDeleteAsUser() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.DELETE_AS_USER.getValue());
    Path dir = new Path("/tmp/testdir");
    
    mockExec.deleteAsUser(appSubmitter, dir);
    assertEquals(Arrays.asList(appSubmitter, cmd, "/tmp/testdir"),
        readMockParams());
  }
}
