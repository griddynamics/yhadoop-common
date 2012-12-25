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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.TestUserGroupInformation.DummyLoginConfiguration;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
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
 * The following system properties must be set:
 *   "user.principal"    - short name of the principal
 *   "user.ticket.cache" - path to the Krb ticket cache file
 *   
 * The following properties may be set if non-default values are needed:
 *   "user.realm"                (default = "EXAMPLE.COM")
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
  public void before() {
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
  
  private void initParameters() {
    principal = getPropertyAndCheck("user.principal", null);
    realm = getPropertyAndCheck("user.realm", "EXAMPLE.COM");
    fullPrincipalName = principal + "@" + realm;
    
    // The format parameters:
    // {0} -- keytab file,
    // {1} -- ticket cache file,
    // {2} -- principal name.
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
    // Reset the UGI state to initial:
    UserGroupInformation.setLoginUser(null);
    // restore the original renew window value:
    UserGroupInformation.setTicketRenewWindowFactor(0.8f);
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

  @Test
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

  @Test
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
    assertTrue("Current system user ["+currentUser+"] not found among the principals.", found);
    
    final Subject subject = ugi.getSubject();
    final User user = subject.getPrincipals(User.class).iterator().next();

    // we're trying to track the renewals from the main thread
    // by monitoring the last login time:
    long t = startTime;
    final long finishTime = startTime + 10 * 1000L;
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
    // Since we're waiting for 10+ seconds, we may state that 
    // with a high probability the refresh will happen at least 3 times:
    assertTrue("Renew count "+renewCount+" did not reach expected value of 3.", 
        renewCount >= 3);
    
    // special hack to stop the renewal thread:
    // this causes an IOException to be thrown, then   
    user.setLogin(null);
    // wait the thread to finish (no ref to it, so cannot join directly):
    Thread.sleep(4000L);
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
    String command = format.format(new String[] { ktbFilePath, ticketCacheFilePath, princ});
    Process process = Runtime.getRuntime().exec(command);
    int status = process.waitFor();
    assertEquals("Process [" +command+ "] exited with status code " + status + ".", 0, status);
    
    // NB: cache file must exist at this point:
    checkFileReadable(ticketCacheFilePath);
  }

}
