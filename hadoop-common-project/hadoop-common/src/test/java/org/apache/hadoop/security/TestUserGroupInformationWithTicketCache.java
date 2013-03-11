/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;


import java.io.File;
import java.security.Principal;
import java.text.Format;
import java.text.MessageFormat;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.TestUserGroupInformation.DummyLoginConfiguration;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.util.Time;
import org.junit.After;
import static org.junit.Assume.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests authentication with help of the Kerberos ticket cache.
 *   
 * The following system properties *must* be set:
 *   "user.principal"    - short name of the principal
 *   "user.ticket.cache" - path to the Krb ticket cache file
 *   
 * The following properties *may* be set if non-default values are needed:
 *   "java.security.krb5.conf"   (default = "/etc/krb5.conf" for Linux)
 *   "user.kinit.command.format" (default = "kinit -V -l 1d -r 100d -k -t {0} {2}")
 *   "user.keytab"               (default = "/etc/krb5.keytab")
 */
public class TestUserGroupInformationWithTicketCache {
  
  private String principal;
  private String realm;
  private String fullPrincipalName; 
  private String kinitCommandFormat; 
  private String ktbFilePath;
  private String ticketCacheFilePath;
  private Configuration conf;
  
  @BeforeClass
  public static void beforeClass() {
    javax.security.auth.login.Configuration.setConfiguration(
        new DummyLoginConfiguration());
  }
  
  @Before
  public void before() throws Exception {
    // NB: skip the test if corresponding properties are not specified: 
    final String userProperty = System.getProperty("user.principal");
    assumeTrue(userProperty != null);
    
    // cleanup the login state:
    UserGroupInformation.setLoginUser(null);
    
    // create new configuration:
    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 
        Integer.toString(2)/* 2 seconds */); 
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    
    // read the adjustable Kerberos configuration parameters: 
    initParameters();
  }
  
  private void initParameters() throws Exception {
    principal = getPropertyAndCheck("user.principal", null);
    
    // NB: it's significant that we work with the default realm,
    // since otherwise no rule will be applied to the principal:
    realm = KerberosUtil.getDefaultRealm();
    assertTrue("The default Kerberos realm is null or empty. " +
        "Please check the Kerberos config used. System property " +
        "'java.security.krb5.conf' can be used to configure.", 
       realm != null && !realm.isEmpty());
    
    fullPrincipalName = principal + "@" + realm;
    
    // The format parameters:
    // {0} -- keytab file,
    // {1} -- ticket cache file,
    // {2} -- full principal name.
    // NB: "-r" option with a correct period must 
    //     be given to make the ticket renewable.
    // NB: " -c {1} " may be added to use a non-default cache file. 
    kinitCommandFormat = getPropertyAndCheck("user.kinit.command.format", 
        "kinit -V -l 1d -r 100d -k -t {0} {2}");
    
    // default for Linux is "/etc/krb5.keytab":
    ktbFilePath = getPropertyAndCheck("user.keytab", "/etc/krb5.keytab");
    checkFileReadable(ktbFilePath);
    
    // NB: default for Linux is "/tmp/krb5cc_<user_id>":
    ticketCacheFilePath = getPropertyAndCheck("user.ticket.cache", null);
  }
  
  @After
  public void after() throws Exception {
    // Reset the static UGI state to initial:
    UserGroupInformation.setLoginUser(null);
    // restore the original renew window value:
    UserGroupInformation.setTicketRenewWindowFactor(0.8f);
    // cleanup the static UGI configuration:
    UserGroupInformation.setConfiguration(new Configuration());
    // remove the ticket cache:
    clearTGT();
  }
  
  private void checkFileReadable(String path) {
    File f = new File(path);
    assertTrue("File ["+f.getAbsolutePath()+"] does not exist.", f.exists());
    assertTrue("File ["+f.getAbsolutePath()+"] is not a regular file.", f.isFile());
    assertTrue("File ["+f.getAbsolutePath()+"] is not readable.", f.canRead());
  }
  
  private String getPropertyAndCheck(String sysPropertyKey, String defaultValue) {
    final String v = System.getProperty(sysPropertyKey, defaultValue);
    assertTrue("System property ["+sysPropertyKey+"] not defined or empty.", 
        v != null && !v.isEmpty());
    return v;
  }

  @Test (timeout = 5000)
  public void testGetBestUGIFromTicketCache() throws Exception {
    cacheTGT(fullPrincipalName);
    
    final UserGroupInformation ugi = UserGroupInformation.getBestUGI(
        ticketCacheFilePath, fullPrincipalName);
    
    assertNotNull(ugi);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getRealAuthenticationMethod());
    assertTrue(!ugi.isFromKeytab());
    
    assertEquals(principal, ugi.getShortUserName());
    assertEquals(fullPrincipalName, ugi.getUserName());
    
    final Credentials credentials = ugi.getCredentials();
    assertNotNull(credentials);
    
    final Subject subject = ugi.getSubject();
    assertNotNull(subject);
    Set<Principal> principals = subject.getPrincipals();
    for (Principal p: principals) {
      assertEquals(fullPrincipalName, p.getName());
    }
    assertTrue(!subject.isReadOnly());
    
    final Set<KerberosKey> krbKeySet = subject.getPrivateCredentials(KerberosKey.class);
    assertTrue(krbKeySet.isEmpty());
    final Set<KerberosTicket> krbTicketSet = subject.getPrivateCredentials(KerberosTicket.class);
    assertTrue(!krbTicketSet.isEmpty());
    
    for (final KerberosTicket kt: krbTicketSet) {
      assertNotNull(kt);
      final KerberosPrincipal kp = kt.getClient();
      assertNotNull(kp);
      assertEquals(fullPrincipalName, kp.getName());
      assertEquals(realm, kp.getRealm());
        
      assertTrue(kt.isCurrent());
      assertTrue(!kt.isDestroyed());
      assertNotNull(kt.getSessionKey());
    }
  }

  @Test (timeout = 5000)
  public void testLoginAutoRenewalFromTicketCache() throws Exception {
    cacheTGT(fullPrincipalName);

    // Reset the ticket renew window to a very small value to ensure 
    // fast refresh time:
    UserGroupInformation.setTicketRenewWindowFactor(1e-8f);
    
    // The renewals are guaranteed to start *after* this time:
    final long startTime = Time.now();  
    // NB: this actually triggers the auto-renewal thread:
    final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    
    assertTrue(!UserGroupInformation.isLoginKeytabBased());
    
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(principal, ugi.getShortUserName());
    
    // Ensure that one of the principals is the current user: 
    final String currentUser = System.getProperty("user.name");
    assertNotNull(currentUser);
    final Set<Principal> principalSet = ugi.getSubject().getPrincipals();
    boolean found = false;
    for (Principal p: principalSet) {
      if (currentUser.equals(p.getName())) {
        found = true;
        break;
      }
    }
    assertTrue("Current system user ["+currentUser
        +"] not found among the principals.", found);
    
    final Subject subject = ugi.getSubject();
    final User user = subject.getPrincipals(User.class).iterator().next();

    // we're trying to track the renewals from the main thread
    // by monitoring the last login time:
    long t = startTime;
    final long finishTime = startTime + 12 * 1000L;
    int renewCount = 0;
    while (t < finishTime) {
      long lastLogin = user.getLastLogin();
      if (lastLogin > t) {
        renewCount++;
        t = lastLogin;
      }
      Thread.sleep(10);
    }
    
    // Renewals should happen each ~2 seconds.
    // Since we're waiting for 12+ seconds, we may state that 
    // with a high probability the refresh will happen at least 3 times:
    assertTrue("Renew count "+renewCount+" did not reach expected value of 3.", 
        renewCount >= 3);
    
    final Thread renewalThread = ugi.getRenewalThread();
    assertNotNull(renewalThread);
    assertTrue(renewalThread.isAlive());
    // interrupt the renewal thread and wait it to finish:
    while (renewalThread.isAlive()) {
      // NB: must interrupt several times 
      // because there are Thread#join() invocations in Shell.execCommand()
      // that can "eat" the interrupted status:
      renewalThread.interrupt();
      renewalThread.join(200L);
    }
  }

  private void clearTGT() {
    // NB: it may be null in case if the property is not given,
    // so the test gets skipped, but #after() is executed anyway.
    if (ticketCacheFilePath != null) {
      final File cacheFile = new File(ticketCacheFilePath);
      // delete the cache file, if any:
      if (cacheFile.exists()) {
        cacheFile.delete();
      }
      assertTrue(!cacheFile.exists());
    }
  }
  
  private void cacheTGT(final String princ) throws Exception {
    // ensure once again the keytab file exists:
    checkFileReadable(ktbFilePath);
    
    clearTGT();
    
    Format format = new MessageFormat(kinitCommandFormat);
    String command = format.format(new String[] { ktbFilePath, 
        ticketCacheFilePath, princ});
    Process process = Runtime.getRuntime().exec(command);
    int status = process.waitFor();
    assertEquals("Process [" +command+ "] exited with status code " 
        + status + ".", 0, status);
    
    // NB: cache file must exist at this point:
    checkFileReadable(ticketCacheFilePath);
  }

  @Test (timeout = 5000)
  public void testCheckTGTAndReloginFromKeytab() throws Exception {
    conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    // clear cached ticket, if any:
    clearTGT();
    // Reset the ticket renew window to a very small value to ensure 
    // fast renew allowed:
    UserGroupInformation.setTicketRenewWindowFactor(1e-8f);
    // initial login from the keytab:
    final UserGroupInformation ugi = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(fullPrincipalName, ktbFilePath);
    // check UGI is correct:
    assertNotNull(ugi);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getRealAuthenticationMethod());
    assertTrue(ugi.isFromKeytab()); // NB: but not from the cache.
    assertEquals(principal, ugi.getShortUserName());
    assertEquals(fullPrincipalName, ugi.getUserName());
    
    final Subject subject = ugi.getSubject();
    final User user = subject.getPrincipals(User.class).iterator().next();
    final long loginTime0 = user.getLastLogin();
    // NB: for some reason the last login time is updated *only* upon a re-login. 
    // Initial login time is always zero:  
    assertTrue(loginTime0 == 0L);
    final LoginContext loginContext0 = user.getLogin();

    // no auto-renew thread should be started in this case:
    final Thread renewThread = ugi.getRenewalThread();
    assertNull(renewThread);
    
    // 1. ask the UGI to re-login from the keytab:
    ugi.checkTGTAndReloginFromKeytab();
    // check that re-login really happened (because loginTime0 == 0, see above):
    final long loginTime1 = user.getLastLogin();
    final LoginContext loginContext1 = user.getLogin();
    assertTrue(loginTime1 > loginTime0);
    assertTrue(loginContext0 != loginContext1);
    
    // 2. now try to re-login again, and check that 
    // the new login did *not* happen: its too early to login again 
    // (60 sec is the default minimum time gap between the login attempts):
    ugi.checkTGTAndReloginFromKeytab();
    final long loginTime2 = user.getLastLogin();
    final LoginContext loginContext2 = user.getLogin();
    assertTrue(loginTime2 == loginTime1);
    assertTrue(loginContext2 == loginContext1);
    
    // 3. Now reset the configuration to allow more frequent re-logins 
    // with 1 sec time gap: 
    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 
        Integer.toString(1)/* 1 second */); 
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    // wait more than the min time gap and re-login again:
    Thread.sleep(2000L);
    ugi.checkTGTAndReloginFromKeytab();
    // check that re-login has really happened:
    final long loginTime3 = user.getLastLogin();
    final LoginContext loginContext3 = user.getLogin();
    assertTrue(loginTime3 > loginTime2);
    assertTrue(loginContext3 != loginContext2);
  }
}
