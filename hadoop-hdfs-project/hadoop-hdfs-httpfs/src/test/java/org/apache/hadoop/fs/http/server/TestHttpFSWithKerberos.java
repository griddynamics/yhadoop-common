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
package org.apache.hadoop.fs.http.server;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSKerberosAuthenticator;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.TestUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.KerberosTestUtils;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestHdfsHelper;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;
import org.junit.runners.model.Statement;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/* 
 * *********************************************************
 * 
 * 0. This test does not require any system properties to be passed from
 * outside via -D, but it uses MiniKdc and sets some system properties 
 * via System.setProperty(). So, this test should always be run in separate fork, 
 * and this forked process should not be reused by other tests. 
 * 
 * 1. Order of the test methods should be fixed to make the test behavior reproducible in
 * any environment (see note "4." about the JDK NegotiateAuthentication cleanup below).
 * The test methods are named accordingly to the execution order.
 * Note that @FixMethodOrder annotation is available since JUnit 4.11 only. 
 * 
 * 2. Rules order should be fixed for the same reason (see RuleChain below).
 * 
 * 3. MiniKdc must be started *before* class 
 * org.apache.hadoop.security.authentication.util.KerberosName is loaded. 
 * This is due to code 
 *   static {
 *    try {
 *     defaultRealm = KerberosUtil.getDefaultRealm();
 *    } catch (Exception ke) {
 *      LOG.debug("Kerberos krb5 configuration not found, setting default realm to empty");
 *      defaultRealm="";
 *    }
 *  }
 * The problem is that the default realm is determined using System property 
 * "java.security.krb5.conf", which is set by MiniKdc. So, if this static initializer
 * invoked before MiniKdc start, 'defaultRealm' in KerberosName is set to empty string,
 * which cause misbehavior.
 * For that reason we start MiniKdc in @BeforeClass static method, and right after that 
 * we force KerberosName class loading and check the default realm value. See #hardResetUGI()
 * method.      
 * 
 * 4. The most difficult problem is that JDK remembers hosts that failed  
 * SPNEGO authentication in field sun.net.www.protocol.http.NegotiateAuthentication#supported .
 * For that reason the negative testcase #test01InvalidadHttpFSAccess() causes positive testcase
 * #test06DelegationTokenWithWebhdfsFileSystem() to fail if executed in such order.
 * We fix this problem by explicit cleaning the JDK field 
 * sun.net.www.protocol.http.NegotiateAuthentication#supported with reflection, 
 * see method #cleanJdkNegotiateAuthentication(). 
 *     
 * *********************************************************
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHttpFSWithKerberos {
  
  private static final String DEFAULT_USER = "client";
  private static final String HTTP_LOCALHOST = "HTTP/localhost";
  
  private static String keytabFile;
  private static String user;
  private static MiniKdc miniKdc;
  
  private Server server;
  private File homeDir;

  // Using RuleChain we fix the rules application order.
  // Note that TestDirHelper is not included into the chain 
  // because TestHdfsHelper extends TestDirHelper, so this functionality is
  // already there.
  @Rule
  public final RuleChain ruleChain = RuleChain
    .outerRule(new TestJettyHelper())
    .around(new TestHdfsHelper())
    .around(new PrintTestNameRule());  

  // Service simple rule that prints beginning and end of each test case.
  // This is done for diagnostic purposes only. 
  // Note that printed markers include @Before and @After methods execution. 
  public static class PrintTestNameRule implements TestRule {
    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        private void impl(boolean begin, String testName) {
          String beginOrEnd = begin ? "BEGIN" : "END  ";
          System.out.println("========================= "+beginOrEnd+" #" + testName + "()");
        }
        @Override
        public void evaluate() throws Throwable {
          impl(true, description.getMethodName());
          try {
            base.evaluate();
          } finally {
            impl(false, description.getMethodName());
          }
        }
      };
    }
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // NB: System property "java.security.krb5.conf" should be set by MiniKdc engine before
    // static field org.apache.hadoop.security.authentication.util.KerberosName.defaultRealm  
    // gets initialized. 
    startMiniKdc();
    
    hardResetUGI(); // load KerberosName class, hard reset of UGI.
  }
  
  private static void startMiniKdc() throws Exception {
    user = DEFAULT_USER;
    
    // Delegate MiniKdc workdir creation to KerberosSecurityTestcase: 
    KerberosSecurityTestcase kst = new KerberosSecurityTestcase();
    kst.createTestDir();
    final File wd = new File(kst.getWorkDir(), "minikdc");
    System.out.println("MiniKdc work dir = ["+wd.getAbsolutePath()+"]");
    if (wd.exists()) {
      FileUtil.fullyDelete(wd, true);
    }
    wd.mkdirs();
    assertTrue(wd.exists());
    
    // create MiniKdc in wd:
    startMiniKdcImpl(wd);
  }
  
  private static void startMiniKdcImpl(final File wd) throws Exception {
    // keytab file to be generated by MiniKdc:
    File keytabFF = new File(wd, "krb5.keytab");
    keytabFF.delete();
    assertFalse(keytabFF.exists());
    keytabFile = keytabFF.getAbsolutePath();
    
    // MiniKdc creates default config:
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, "true");
    
    miniKdc = new MiniKdc(conf, wd);
    miniKdc.start();

    // Default MiniKdc realm "EXAMPLE.COM" is okay.
    String realm = miniKdc.getRealm();
    
    assertNotNull(user);
    miniKdc.createPrincipal(keytabFF, user, HTTP_LOCALHOST);
    assertTrue(keytabFF.exists());

    // ------------------------------------------------------------
    // Set necessary properties that affect security processing. 
    // Note that property "java.security.krb5.conf" is set by MiniKdc behind the scenes.
    System.setProperty("httpfs.authentication.kerberos.keytab", keytabFile);
    
    System.setProperty("httpfs.test.kerberos.keytab.file", keytabFile);
    
    System.setProperty("kerberos.realm", realm);
    System.setProperty("httpfs.test.kerberos.realm", realm);
    
    System.setProperty("httpfs.test.kerberos.client.principal", user);
    System.setProperty("httpfs.test.kerberos.server.principal", HTTP_LOCALHOST + "@"+realm);
    
    System.setProperty("dfs.web.authentication.kerberos.principal", HTTP_LOCALHOST + "@"+realm);
    
    System.setProperty("httpfs.http.hostname", miniKdc.getHost());
    System.setProperty("httpfs.hostname", miniKdc.getHost());
    System.setProperty("httpfs.authentication.type", "kerberos");
    // ------------------------------------------------------------
  }

  @AfterClass
  public static void afterClass() throws Exception {
    stopMiniKdc();
  }
  
  private static void stopMiniKdc() throws Exception { 
    MiniKdc mk = miniKdc;
    if (mk != null) { 
      mk.stop();
      miniKdc = null;
    }
  }
  
  @Before
  public void before() throws Exception {
    System.out.println("============= before()");
    // The work around to clean up JDK state after failed authentication attempt,
    // see class comment for more detail:
    cleanJdkNegotiateAuthentication();
    // To make the tests independent as possible,
    // also fully clean up UGI before each test case:
    hardResetUGI();
  }
  
  @After
  public void after() throws Exception {
    System.out.println("============= after()");
    if (server != null) {
      server.stop();
      server.join();
      assertTrue(server.isStopped() && !server.isFailed());
      server.destroy();
      server = null;
    }
    TestHdfsHelper.shutdown(true);
    if (homeDir != null) {
      FileUtil.fullyDelete(homeDir, true);
      assertTrue(!homeDir.exists());
      homeDir = null;
    }
  }
  
  private static void hardResetUGI() throws Exception {
    // Here we force KerberosName class loading, 
    // check that the default realm in KerberosName is correct,
    // and that this value corresponds to the realm MiniKdc has:
    final String defaultRealm0 = KerberosUtil.getDefaultRealm();
    System.out.println("Default realm = ["+defaultRealm0+"]");
    assertNotNull(defaultRealm0);
    assertTrue(defaultRealm0.length() > 0);
    final String miniKdcRealm = miniKdc.getRealm();
    assertEquals(miniKdcRealm, defaultRealm0);
    final String defaultRealm = new KerberosName("xxx").getDefaultRealm();
    assertEquals(defaultRealm0, defaultRealm);
    
    // Invoke method from another test because 
    // org.apache.hadoop.security.UserGroupInformation.reset() has package visibility:    
    new TestUserGroupInformation().setupUgi();
    // Basic check that UGI is okay:
    assertTrue(!UserGroupInformation.isSecurityEnabled());
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod am = ugi.getAuthenticationMethod();
    assertTrue(am == AuthenticationMethod.SIMPLE);
  }
  
  private static void cleanJdkNegotiateAuthentication() {
    try {
      Class<?> c = Class.forName("sun.net.www.protocol.http.NegotiateAuthentication");
      Field supported = c.getDeclaredField("supported");
      supported.setAccessible(true);
      Map<?,?> mapValue = (Map<?,?>)supported.get(null);
      if (mapValue != null) {
        mapValue.clear();
      }
    } catch (Throwable t) {
      System.err.println(
         "*********************************************************************************************\n" +
         " Cleanup of JDK field sun.net.www.protocol.http.NegotiateAuthentication#supported has failed.\n"+
      	 " Some tests may not work correctly after a negative authentication test.\n" +
         "*********************************************************************************************");
      t.printStackTrace();
    }
  }

  private void createHttpFSServer() throws Exception {
    homeDir = TestDirHelper.getTestDir();
    assertTrue(new File(homeDir, "conf").mkdir());
    assertTrue(new File(homeDir, "log").mkdir());
    assertTrue(new File(homeDir, "temp").mkdir());
    HttpFSServerWebApp.setHomeDirForCurrentThread(homeDir.getAbsolutePath());

    File secretFile = new File(new File(homeDir, "conf"), "secret");
    Writer w = new FileWriter(secretFile);
    w.write("secret");
    w.close();

    //HDFS configuration
    File hadoopConfDir = new File(new File(homeDir, "conf"), "hadoop-conf");
    hadoopConfDir.mkdirs();
    String fsDefaultName = TestHdfsHelper.getHdfsConf()
      .get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);
    File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    conf.writeXml(os);
    os.close();

    conf = new Configuration(false);
    conf.set("httpfs.proxyuser.client.hosts", "*");
    conf.set("httpfs.proxyuser.client.groups", "*");

    conf.set("httpfs.authentication.type", "kerberos");

    conf.set("httpfs.authentication.signature.secret.file",
             secretFile.getAbsolutePath());
    File httpfsSite = new File(new File(homeDir, "conf"), "httpfs-site.xml");
    os = new FileOutputStream(httpfsSite);
    conf.writeXml(os);
    os.close();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("webapp");
    assertNotNull("Resource 'webapp' not found in classpath.", url);
    WebAppContext context = new WebAppContext(url.getPath(), "/webhdfs");
    server = TestJettyHelper.getJettyServer();
    server.addHandler(context);
    server.start();
    HttpFSServerWebApp.get().setAuthority(TestJettyHelper.getAuthority());
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void test01InvalidadHttpFSAccess() throws Exception {
    createHttpFSServer();

    URL url = new URL(TestJettyHelper.getJettyURL(),
                    "/webhdfs/v1/?op=GETHOMEDIRECTORY");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertEquals(conn.getResponseCode(),
                      HttpURLConnection.HTTP_UNAUTHORIZED);
  }
  
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void test02ValidHttpFSAccess() throws Exception {
    createHttpFSServer();

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        URL url = new URL(TestJettyHelper.getJettyURL(),
                          "/webhdfs/v1/?op=GETHOMEDIRECTORY");
        AuthenticatedURL aUrl = new AuthenticatedURL();
        AuthenticatedURL.Token aToken = new AuthenticatedURL.Token();
        HttpURLConnection conn = aUrl.openConnection(url, aToken);
        assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
        return null;
      }
    });
  }
  
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void test03DelegationTokenHttpFSAccess() throws Exception {
    createHttpFSServer();

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        //get delegation token doing SPNEGO authentication
        URL url = new URL(TestJettyHelper.getJettyURL(),
                          "/webhdfs/v1/?op=GETDELEGATIONTOKEN");
        AuthenticatedURL aUrl = new AuthenticatedURL();
        AuthenticatedURL.Token aToken = new AuthenticatedURL.Token();
        HttpURLConnection conn = aUrl.openConnection(url, aToken);
        assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
        JSONObject json = (JSONObject) new JSONParser()
          .parse(new InputStreamReader(conn.getInputStream()));
        json =
          (JSONObject) json
            .get(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_JSON);
        String tokenStr = (String) json
          .get(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON);

        //access httpfs using the delegation token
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" +
                      tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);

        //try to renew the delegation token without SPNEGO credentials
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=" + tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        assertEquals(conn.getResponseCode(),
                            HttpURLConnection.HTTP_UNAUTHORIZED);

        //renew the delegation token with SPNEGO credentials
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=" + tokenStr);
        conn = aUrl.openConnection(url, aToken);
        conn.setRequestMethod("PUT");
        assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);

        //cancel delegation token, no need for SPNEGO credentials
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token=" +
                      tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);

        //try to access httpfs with the canceled delegation token
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" +
                      tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        assertEquals(conn.getResponseCode(),
                            HttpURLConnection.HTTP_UNAUTHORIZED);
        return null;
      }
    });
  }

  private void testDelegationTokenWithFS(Class<?> fileSystemClass)
    throws Exception {
    createHttpFSServer();
    Configuration conf = new Configuration();
    conf.set("fs.webhdfs.impl", fileSystemClass.getName());
    conf.set("fs.hdfs.impl.disable.cache", "true");
    URI uri = new URI( "webhdfs://" +
                       TestJettyHelper.getJettyURL().toURI().getAuthority());
    FileSystem fs = FileSystem.get(uri, conf);
    Token<?> tokens[] = fs.addDelegationTokens("foo", null);
    fs.close();
    assertEquals(1, tokens.length);
    fs = FileSystem.get(uri, conf);
    ((DelegationTokenRenewer.Renewable) fs).setDelegationToken(tokens[0]);
    fs.listStatus(new Path("/"));
    fs.close();
  }

  private void testDelegationTokenWithinDoAs(
    final Class<?> fileSystemClass, boolean proxyUser) throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytabFile);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    if (proxyUser) {
      ugi = UserGroupInformation.createProxyUser("foo", ugi);
    }
    conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    ugi.doAs(
      new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          testDelegationTokenWithFS(fileSystemClass);
          return null;
        }
      });
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void test04DelegationTokenWithHttpFSFileSystem() throws Exception {
    testDelegationTokenWithinDoAs(HttpFSFileSystem.class, false);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void test05DelegationTokenWithHttpFSFileSystemProxyUser()
    throws Exception {
    testDelegationTokenWithinDoAs(HttpFSFileSystem.class, true);
  }
  
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void test06DelegationTokenWithWebhdfsFileSystem() throws Exception {
    testDelegationTokenWithinDoAs(WebHdfsFileSystem.class, false);
  }

//  // TODO: WebHdfsFilesystem does work with ProxyUser HDFS-3509
//  @Test
//  @TestDir
//  @TestJetty
//  @TestHdfs
//  public void test07DelegationTokenWithWebhdfsFileSystemProxyUser()
//     throws Exception {
//    testDelegationTokenWithinDoAs(WebHdfsFileSystem.class, true);
//  }
 
}
