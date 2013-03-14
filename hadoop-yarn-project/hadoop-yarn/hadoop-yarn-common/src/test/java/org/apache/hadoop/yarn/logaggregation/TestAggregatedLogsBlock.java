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

package org.apache.hadoop.yarn.logaggregation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsBlockForTest;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlockForTest;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestAggregatedLogsBlock {

  @Test
  public void testAccessDenied() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    writelog(configuration, "owner");

    AggregatedLogsBlockForTest taggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "owner", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter pwriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, pwriter, 10, false);
    taggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out
        .contains("User [owner] is not authorized to view the logs for entity"));

  }

  @Test
  public void testBadLogs() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    writelog(configuration, "owner");

    AggregatedLogsBlockForTest taggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter pwriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, pwriter, 10, false);
    taggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out
        .contains("Logs not available for entity. Aggregation may not be complete, Check back later or try the nodemanager at localhost:1234"));

  }

  @Test
  public void testAggregatedLogsBlock() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    writelog(configuration, "admin");

    AggregatedLogsBlockForTest taggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter pwriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, pwriter, 10, false);
    taggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out.contains("test log1"));
    assertTrue(out.contains("test log2"));
    assertTrue(out.contains("test log3"));

  }

  private Configuration getConfiguration() {
    Configuration configuration = new Configuration();
    configuration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    configuration.set("yarn.nodemanager.remote-app-log-dir", "target/logs");
    configuration.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    configuration.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    return configuration;
  }

  private AggregatedLogsBlockForTest getAggregatedLogsBlockForTest(
      Configuration configuration, String user, String contaynerId) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(user);
    AggregatedLogsBlockForTest taggregatedBlock = new AggregatedLogsBlockForTest(
        configuration);
    taggregatedBlock.setRequest(request);
    taggregatedBlock.moreParams().put(YarnWebParams.CONTAINER_ID, contaynerId);
    taggregatedBlock.moreParams().put(YarnWebParams.NM_NODENAME,
        "localhost:1234");
    taggregatedBlock.moreParams().put(YarnWebParams.APP_OWNER, user);
    taggregatedBlock.moreParams().put("start", "");
    taggregatedBlock.moreParams().put("end", "");
    taggregatedBlock.moreParams().put(YarnWebParams.ENTITY_STRING, "entity");
    return taggregatedBlock;
  }

  private void writelog(Configuration configuration, String user)
      throws Exception {
    ContainerId containerId = new ContainerIdPBImpl();
    ApplicationAttemptId appAttemptId = new ApplicationAttemptIdPBImpl();
    ApplicationId appid = new ApplicationIdPBImpl();
    appid.setId(1);
    appAttemptId.setApplicationId(appid);
    appAttemptId.setAttemptId(1);
    containerId.setApplicationAttemptId(appAttemptId);
    containerId.setId(1);
    String path = "target/logs/" + user
        + "/logs/application_0_0001/localhost_1234";
    File f = new File(path);
    if (!f.getParentFile().exists()) {
      f.getParentFile().mkdirs();
    }
    List<String> rootLogDirs = Arrays.asList("target/logs/logs");
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    AggregatedLogFormat.LogWriter writer = new AggregatedLogFormat.LogWriter(
        configuration, new Path(path), ugi);
    writer.writeApplicationOwner(ugi.getUserName());

    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    appAcls.put(ApplicationAccessType.VIEW_APP, ugi.getUserName());
    writer.writeApplicationACLs(appAcls);

    writer.append(new AggregatedLogFormat.LogKey("container_0_0001_01_000001"),
        new AggregatedLogFormat.LogValue(rootLogDirs, containerId));
    writer.closeWriter();
  }

  private void writeLogs(String dirName) throws Exception {
    File f = new File(dirName + File.separator + "log1");
    if (!f.getParentFile().exists()) {
      f.getParentFile().mkdirs();
    }

    writeLog(dirName + File.separator + "log1", "test log1");
    writeLog(dirName + File.separator + "log2", "test log2");
    writeLog(dirName + File.separator + "log3", "test log3");
  }

  private void writeLog(String fileName, String text) throws Exception {
    File f = new File(fileName);
    Writer writer = new FileWriter(f);
    writer.write(text);
    writer.flush();
    writer.close();
  }

}
