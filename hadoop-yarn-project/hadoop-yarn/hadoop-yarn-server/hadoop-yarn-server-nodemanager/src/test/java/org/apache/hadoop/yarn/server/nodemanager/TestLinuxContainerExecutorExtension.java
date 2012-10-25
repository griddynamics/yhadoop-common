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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLinuxContainerExecutorExtension {
	 
	private static File workSpace = new File("target", TestLinuxContainerExecutorExtension.class.getName() + "-workSpace");
	private LinuxContainerExecutor exec = null;
	private String appSubmitter = null;
	private LocalDirsHandlerService dirsHandler;

	@Before
	public void setup() throws Exception {
		FileContext files = FileContext.getLocalFSFileContext();
		Path workSpacePath = new Path(workSpace.getAbsolutePath());
		files.mkdir(workSpacePath, null, true);
		workSpace.setReadable(true, false);
		workSpace.setExecutable(true, false);
		workSpace.setWritable(true, false);
		File localDir = new File(workSpace.getAbsoluteFile(), "localDir");
		files.mkdir(new Path(localDir.getAbsolutePath()), new FsPermission("777"), false);
		File logDir = new File(workSpace.getAbsoluteFile(), "logDir");
		files.mkdir(new Path(logDir.getAbsolutePath()), new FsPermission("777"), false);
	
		Configuration conf = new Configuration(true);

		conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, "/bin/sh");
		exec = new LinuxContainerExecutor();
		exec.setConf(conf);
		conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
		conf.set(YarnConfiguration.NM_LOG_DIRS, logDir.getAbsolutePath());
		dirsHandler = new LocalDirsHandlerService();
		dirsHandler.init(conf);
	
		appSubmitter = System.getProperty("application.submitter");
		if (appSubmitter == null || appSubmitter.isEmpty()) {
			appSubmitter = "nobody";
		}
	}

	@After
	public void tearDown() throws Exception {
		FileContext.getLocalFSFileContext().delete(new Path(workSpace.getAbsolutePath()), true);
	}

	@Test
	public void testInit() throws Exception {
		Configuration config = new Configuration();
		String tmpExecFile = writeScriptFile();
		config.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, tmpExecFile);

		LinuxContainerExecutor executer = new LinuxContainerExecutor();
		executer.setConf(config);
		executer.init();
		File tmpFile = new File("--checksetup");
		List<String> commandLine = FileUtils.readLines(tmpFile);
		Assert.assertEquals(commandLine.size(), 1);
		Assert.assertEquals(commandLine.get(0), "--checksetup");

	}

	@Test
	public void testInitException() throws Exception {
		Configuration config = new Configuration();
		config.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, "/bin/sh");

		LinuxContainerExecutor executer = new LinuxContainerExecutor();
		executer.setConf(config);
		try {
			executer.init();
			Assert.fail();
		} catch (IOException e) {
			ExitCodeException parent = (ExitCodeException) e.getCause();
			Assert.assertEquals(parent.getExitCode(), 2);
		}

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testStartLocalizer() throws Exception {
		String shFile = writeScriptFile();

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
		File tmpFile = File.createTempFile("test", "txt");
		List<String> lastArgument = Arrays.asList(tmpFile.getAbsolutePath());

		executer.startLocalizer(nmPrivateCTokensPath, address, "test", "application_0", "12345", lastArgument, Collections.EMPTY_LIST);

		List<String> commandLine = FileUtils.readLines(tmpFile);
		Assert.assertEquals(commandLine.size(), 1);

		String[] parameters = commandLine.get(0).split(" ");
		Assert.assertEquals(parameters.length, 16);
		Assert.assertEquals(parameters[0], "test");
		Assert.assertEquals(parameters[2], "application_0");
		Assert.assertEquals(parameters[6], "-classpath");
		Assert.assertEquals(parameters[5].indexOf("java") > 0, true);
		Assert.assertEquals(parameters[10], "test");
		Assert.assertEquals(parameters[11], "application_0");
		Assert.assertEquals(parameters[12], "12345");
		Assert.assertEquals(parameters[13], "localhost");
		Assert.assertEquals(parameters[14], "8040");

		tmpFile.delete();
		File commandfile = new File(shFile);
		commandfile.delete();
	}

	private String writeScriptFile() throws IOException {
		File f = File.createTempFile("TestLinuxLocalizeContainerExecutor", ".sh");
		f.deleteOnExit();
		PrintWriter p = new PrintWriter(new FileOutputStream(f));
		p.println("#!/bin/sh");
		p.println("eval \"last=\\${$#}\"");
		p.println("echo $@ >$last");

		p.println();
		p.close();
		FsPermission permission = FsPermission.getDefault();
		FileContext files = FileContext.getLocalFSFileContext();

		files.setPermission(new Path(f.getAbsolutePath()), permission);

		return f.getAbsolutePath();
	}

	@Test
	public void testErrorLauncher() throws IOException  {
		int result=runAndBlock("/bin/badcommand");
		Assert.assertEquals(result, 127);
	}
	private int runAndBlock( String cmd) throws IOException {
		String appId = "APP_" + getNextId();
		ContainerId cId=getNextContainerId();
		Container container = mock(Container.class);
		ContainerLaunchContext context = mock(ContainerLaunchContext.class);
		HashMap<String, String> env = new HashMap<String, String>();

		when(container.getContainerID()).thenReturn(cId);
		when(container.getLaunchContext()).thenReturn(context);

		when(context.getEnvironment()).thenReturn(env);

		String script = writeScriptFile(cmd);

		Path scriptPath = new Path(script);
		Path tokensPath = new Path("/dev/null");
		Path workDir = new Path(workSpace.getAbsolutePath());
		Path pidFile = new Path(workDir, "pid.txt");

		exec.activateContainer(cId, pidFile);
		return exec.launchContainer(container, scriptPath, tokensPath, appSubmitter, appId, workDir, dirsHandler.getLocalDirs(), dirsHandler.getLogDirs());
	}
	 private ContainerId getNextContainerId() {
		    ContainerId cId = mock(ContainerId.class);
		    String id = "CONTAINER_"+getNextId();
		    when(cId.toString()).thenReturn(id);
		    return cId;
		  }
	private String writeScriptFile(String... cmd) throws IOException {
		File f = File.createTempFile("TestLinuxContainerExecutor", ".sh");
		f.deleteOnExit();
		PrintWriter p = new PrintWriter(new FileOutputStream(f));
		p.println("#!/bin/sh");
		p.print("exec");
		for (String part : cmd) {
			p.print(" '");
			p.print(part.replace("\\", "\\\\").replace("'", "\\'"));
			p.print("'");
		}
		p.println();
		p.close();
		return f.getAbsolutePath();
	}

	private int id = 0;

	private synchronized int getNextId() {
		id += 1;
		return id;
	}
}
