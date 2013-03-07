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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.service.CompositeService;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestWebAppProxyServlet {
  private static Server server;
  private static String host = "localhost";
  private static int port = 0;
  private static int originalPort = 0;
  private static Context context;
  private int answer = 0;

  private static final Log LOG = LogFactory
      .getLog(TestWebAppProxyServlet.class);

  @BeforeClass
  public static void start() throws Exception {
    server = new Server(0);
    context = new Context();
    context.setContextPath("/foo");
    server.setHandler(context);
    context.addServlet(new ServletHolder(TestServlet.class), "/bar/");
    server.getConnectors()[0].setHost(host);
    server.start();
    originalPort = server.getConnectors()[0].getLocalPort();
    LOG.info("Running embedded servlet container at: http://" + host + ":"
        + port);
  }

  @SuppressWarnings("serial")
  public static class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      InputStream is = req.getInputStream();
      OutputStream os = resp.getOutputStream();
      int c = is.read();
      while (c > -1) {
        os.write(c);
        c = is.read();
      }
      is.close();
      os.close();
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @Test
  public void testWebAppProxyServlet() throws Exception {

    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.PROXY_ADDRESS, host + ":9090");
    configuration.setInt("hadoop.http.max.threads", 5);// HTTP_MAX_THREADS//"hadoop.http.max.threads"
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    // wrong url
    try {
      URL wrongUrl = new URL("http://localhost:" + port + "/proxy/app");
      HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl
          .openConnection();

      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());

      URL url = new URL("http://localhost:" + port + "/proxy/application_00_0");
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      answer = 1;
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          proxyConn.getResponseCode());
      answer = 2;

      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      String s = readInputStream(proxyConn.getInputStream());
      assertTrue(s
          .contains("to continue to an Application Master web interface owned by"));
      assertTrue(s.contains("WARNING: The following page may not be safe!"));
    } finally {
      proxy.stop();
    }
  }

  @Test
  public void testWebAppProxyServer() throws Exception {

    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.PROXY_ADDRESS, host + ":9098");
    configuration.setInt("hadoop.http.max.threads", 5);
    WebAppProxyServer proxy = new WebAppProxyServer();
    proxy.init(configuration);
    proxy.start();

    // wrong url
    try {
      URL wrongUrl = new URL("http://localhost:9098/proxy/app");
      HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl
          .openConnection();
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());
    } finally {
      proxy.stop();
    }

  }

  @Test
  public void testWebAppProxyServerMain() throws Exception {
    WebAppProxyServer server = null;
    try {
      server = WebAppProxyServer.startServer(new String[0]);

      int counter = 10;

      URL wrongUrl = new URL("http://localhost:9099/proxy/app");
      HttpURLConnection proxyConn = null;
      while (counter > 0) {
        counter--;
        try {
          proxyConn = (HttpURLConnection) wrongUrl.openConnection();
          proxyConn.connect();
          proxyConn.getResponseCode();
          counter = 0;
        } catch (Throwable e) {

        }
        Thread.sleep(500);
      }
      if (proxyConn != null) {
        assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
            proxyConn.getResponseCode());
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private String readInputStream(InputStream input) throws Exception {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    byte[] buffer = new byte[512];
    int read;
    while ((read = input.read(buffer)) >= 0) {
      data.write(buffer, 0, read);
    }
    return new String(data.toByteArray(), "UTF-8");
  }

  @AfterClass
  public static void stop() throws Exception {
    try {
      server.stop();
    } catch (Exception e) {
    }

    try {
      server.destroy();
    } catch (Exception e) {
    }
  }

  private class WebAppProxyServerForTest extends CompositeService {

    private WebAppProxyForTest proxy = null;

    public WebAppProxyServerForTest() {
      super(WebAppProxyServer.class.getName());
    }

    @Override
    public synchronized void init(Configuration conf) {
      Configuration config = new YarnConfiguration(conf);
      proxy = new WebAppProxyForTest();
      addService(proxy);
      super.init(config);
    }

  }

  private class WebAppProxyForTest extends WebAppProxy {

    @Override
    public void start() {
      try {
        String bindAddress = (String) getVolumeOfField("bindAddress");
        AccessControlList acl = (AccessControlList) getVolumeOfField("acl");
        HttpServer proxyServer = new HttpServer("proxy", bindAddress, port,
            port == 0, getConfig(), acl);
        proxyServer.addServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
            ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);

        proxyServer.setAttribute(FETCHER_ATTRIBUTE,
            new AppReportFetcherForTest(getConfig()));
        Boolean isSecurityEnabled = (Boolean) getVolumeOfField("isSecurityEnabled");
        proxyServer.setAttribute(IS_SECURITY_ENABLED_ATTRIBUTE,
            isSecurityEnabled);
        proxyServer.setAttribute(IS_SECURITY_ENABLED_ATTRIBUTE, Boolean.TRUE);
        String proxyHost = (String) getVolumeOfField("proxyHost");
        proxyServer.setAttribute(PROXY_HOST_ATTRIBUTE, proxyHost);
        proxyServer.start();
        port = proxyServer.getPort();
        System.out.println("port:" + port);
      } catch (Exception e) {
        LOG.fatal("Could not start proxy web server", e);
        throw new YarnException("Could not start proxy web server", e);
      }
      // super.start();
    }

    private Object getVolumeOfField(String fieldName) throws Exception {

      Field field = this.getClass().getSuperclass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(this);
    }
  }

  private class AppReportFetcherForTest extends AppReportFetcher {
    public AppReportFetcherForTest(Configuration conf) {
      super(conf);

    }

    public ApplicationReport getApplicationReport(ApplicationId appId)
        throws YarnRemoteException {
      if (answer == 0) {
        return getDefaultApplicationReport(appId);
      } else if (answer == 1) {
        return null;
      } else if (answer == 2) {
        ApplicationReport result = getDefaultApplicationReport(appId);
        result.setUser("user");
        return result;
      }
      return null;
    }

    private ApplicationReport getDefaultApplicationReport(ApplicationId appId) {
      ApplicationReport result = new ApplicationReportPBImpl();
      result.setApplicationId(appId);
      result.setOriginalTrackingUrl(host + ":" + originalPort + "/foo/bar");
      result.setYarnApplicationState(YarnApplicationState.RUNNING);
      result.setUser(CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER);
      return result;

    }
  }
}
