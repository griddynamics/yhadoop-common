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



import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.junit.Test;


public class TestLinuxContainerExecutorExtended {
  private static final Log LOG = LogFactory
      .getLog(TestLinuxContainerExecutorExtended.class);
  
  private static File workSpace = new File("target",
      TestLinuxContainerExecutorExtended.class.getName() + "-workSpace");
  
  
  @Test
  public void testInit() throws Exception {
	  Configuration config= new Configuration();
	  String tmpExecFile=writeScriptLoclgizeFile();
	  config.set("yarn.nodemanager.linux-container-executor.path", tmpExecFile);
	  
	  LinuxContainerExecutor executer= new LinuxContainerExecutor();
	  executer.setConf(config);
	  executer.init();
	  File tmpFile=new File("--checksetup");
	  List<String> commandLine=FileUtils.readLines(tmpFile);
	  Assert.assertEquals(commandLine.size(), 1);
	  Assert.assertEquals(commandLine.get(0), "--checksetup");
	
  }

  @Test(expected = IOException.class)
  public void testInitException() throws Exception {
	  Configuration config= new Configuration();
	  config.set("yarn.nodemanager.linux-container-executor.path", "/bin/sh");
	  LinuxContainerExecutor executer= new LinuxContainerExecutor();
	  executer.setConf(config);
	  executer.init();
	// should be born IOException !  
	
  }
	@SuppressWarnings("unchecked")
	@Test
	public void testStartLocalizer() throws Exception {
		 String shFile=writeScriptLoclgizeFile();
		
		 
		 LinuxContainerExecutor executer = new LinuxContainerExecutor();
		Configuration conf = new Configuration(true);
		conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, shFile);
		executer = new LinuxContainerExecutor();
		executer.setConf(conf);
		conf.set(YarnConfiguration.NM_LOCAL_DIRS, workSpace.getAbsoluteFile().getAbsolutePath());
		conf.set(YarnConfiguration.NM_LOG_DIRS, workSpace.getAbsoluteFile().getAbsolutePath());
		LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
		dirsHandler.init(conf);

		Path nmPrivateCTokensPath = dirsHandler.getLocalPathForWrite(ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR
				+ String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, "en"));
		InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 8040);
		File tmpFile=File.createTempFile("test", "txt");
		List<String> lastArgument=  Arrays.asList(tmpFile.getAbsolutePath());
		
		
		executer.startLocalizer(nmPrivateCTokensPath, address, "test", "application_0", "12345", lastArgument, Collections.EMPTY_LIST);
		
		List<String> commandLine=FileUtils.readLines(tmpFile);
		Assert.assertEquals(commandLine.size(), 1);
		
		String[] parameters=commandLine.get(0).split(" ");
		Assert.assertEquals(parameters.length, 16);
		Assert.assertEquals(parameters[0], "test");
		Assert.assertEquals(parameters[2], "application_0");
		Assert.assertEquals(parameters[6], "-classpath");
		Assert.assertEquals(parameters[5].indexOf("java")>0, true);
		Assert.assertEquals(parameters[10], "test");
		Assert.assertEquals(parameters[11], "application_0");
		Assert.assertEquals(parameters[12], "12345");
		Assert.assertEquals(parameters[13], "localhost");
		Assert.assertEquals(parameters[14], "8040");

		tmpFile.delete();
		File commandfile=new File(shFile);
		commandfile.delete();
	}
	
	 private String writeScriptLoclgizeFile() throws IOException {
		    File f = File.createTempFile("TestLinuxLocalizeContainerExecutor", ".sh");
		    f.deleteOnExit();
		    PrintWriter p = new PrintWriter(new FileOutputStream(f));
		    p.println("#!/bin/sh");
		    p.println("eval \"last=\\${$#}\"");
		    p.println("echo $@ >$last");
		    
		    p.println();
		    p.close();
		    FsPermission permission=  FsPermission.getDefault();
			 FileContext files = FileContext.getLocalFSFileContext();

			 files.setPermission(new Path(f.getAbsolutePath()), permission);
			 
		    return f.getAbsolutePath();
		  }
}
