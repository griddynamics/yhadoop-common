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
  public void testAggregatedLogsBlock() throws Exception {

    Configuration configuration = new Configuration();
    configuration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    configuration.set("yarn.nodemanager.remote-app-log-dir", "target/logs");
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    String path = "target/logs/owner/logs/application_333_0001/localhost_1234";
    File f = new File(path);
    if (!f.getParentFile().exists()) {
      f.getParentFile().mkdirs();
    }

    AggregatedLogFormat.LogWriter writer = new AggregatedLogFormat.LogWriter(
        configuration, new Path(path), ugi);
    writer.writeApplicationOwner(ugi.getUserName());

    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    appAcls.put(ApplicationAccessType.VIEW_APP, ugi.getUserName());
    writer.writeApplicationACLs(appAcls);

    List<String> rootLogDirs = Arrays.asList("target/logs/logs");

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    ContainerId containerId = new ContainerIdPBImpl();
    ApplicationAttemptId appAttemptId = new ApplicationAttemptIdPBImpl();
    ApplicationId appid = new ApplicationIdPBImpl();
    appid.setId(1);
    appAttemptId.setApplicationId(appid);
    appAttemptId.setAttemptId(1);
    containerId.setApplicationAttemptId(appAttemptId);
    containerId.setId(1);

    writer.append(
        new AggregatedLogFormat.LogKey("container_333_0001_01_000001"),
        new AggregatedLogFormat.LogValue(rootLogDirs, containerId));
    writer.closeWriter();

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn("owner");

    AggregatedLogsBlockForTest t = new AggregatedLogsBlockForTest(configuration);
    t.setRequest(request);
    t.moreParams().put(YarnWebParams.CONTAINER_ID,
        "container_333_0001_01_000001");
    t.moreParams().put(YarnWebParams.NM_NODENAME, "localhost:1234");
    t.moreParams().put(YarnWebParams.APP_OWNER, "owner");
    t.moreParams().put("start", "");
    t.moreParams().put("end", "");
    t.moreParams().put(YarnWebParams.ENTITY_STRING, "entity");

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter pwriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, pwriter, 10, false);
    t.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out.contains("test log1"));
    assertTrue(out.contains("test log2"));
    assertTrue(out.contains("test log3"));
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
