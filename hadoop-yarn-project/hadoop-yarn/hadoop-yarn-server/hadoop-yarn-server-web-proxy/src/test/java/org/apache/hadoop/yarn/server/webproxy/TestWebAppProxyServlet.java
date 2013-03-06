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
  public void test1() throws Exception {

    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.PROXY_ADDRESS, host + ":9090");
    configuration.setInt("hadoop.http.max.threads", 5);// HTTP_MAX_THREADS//"hadoop.http.max.threads"
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    URL url = new URL("http://localhost:" + port + "/proxy/application_00_0");
    HttpURLConnection proxyConn = (HttpURLConnection) url.openConnection();
    proxyConn.connect();
    assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());

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
      ApplicationReport result = new ApplicationReportPBImpl();
      result.setApplicationId(appId);
      result.setOriginalTrackingUrl(host + ":" + originalPort + "/foo/bar");
      result.setYarnApplicationState(YarnApplicationState.RUNNING);
      return result;
    }
  }
}
