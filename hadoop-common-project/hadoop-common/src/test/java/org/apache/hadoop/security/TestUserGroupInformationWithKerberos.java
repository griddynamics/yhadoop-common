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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;

import org.apache.directory.kerberos.client.KdcConfig;
import org.apache.directory.kerberos.client.KdcConnection;
import org.apache.directory.kerberos.client.Kinit;
import org.apache.directory.kerberos.credentials.cache.CredentialsCache;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.shared.kerberos.KerberosUtils;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.TestUserGroupInformation.DummyLoginConfiguration;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests authentication with Kerberos keytab and/or ticket cache.
 * The test uses MiniKdc server. 
 */
public class TestUserGroupInformationWithKerberos {

  private static File workDir;
  private static MiniKdc miniKdc;

  private static String principal;
  private static String password;
  
  private static String realm;
  private static String fullPrincipalName;
  private static String ktbFilePath;
  private static String ticketCacheFilePath;
  
  private Configuration conf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    workDir = createWd();
    initMiniKdc(workDir);
    
    principal = "foo"; 
    password = "secret";

    // add the principal to the keytab:
    File ktb = new File(workDir, "krb5.keytab");
    miniKdc.createPrincipal(ktb, new String[] { password }, new String[] { principal });
    ktbFilePath = ktb.getAbsolutePath();
    checkFileReadable(ktbFilePath); // keytab file should be created by MiniKdc

    // NB: it's significant that we work with the default realm,
    // since otherwise no rule will be applied to the principal:
    realm = KerberosUtil.getDefaultRealm();
    assertTrue("The default Kerberos realm is null or empty. "
        + "Please check the Kerberos config used. System property "
        + "'java.security.krb5.conf' can be used to configure.", realm != null && !realm.isEmpty());
    assertEquals(realm, miniKdc.getRealm());

    fullPrincipalName = principal + "@" + realm;
    
    ticketCacheFilePath = new File(workDir, "krb5cc_XXX").getAbsolutePath();
    
    javax.security.auth.login.Configuration.setConfiguration(new DummyLoginConfiguration());
  }

  @AfterClass
  public static void afterClass() {
    disposeMiniKdc();
  }

  @Before
  public void before() throws Exception {
    clearTGT();
    UserGroupInformation.reset();
    UserGroupInformation.setTicketRenewWindowFactor(0.8f);
  }

  private static void checkFileReadable(String path) {
    File f = new File(path);
    assertTrue("File [" + f.getAbsolutePath() + "] does not exist.", f.exists());
    assertTrue("File [" + f.getAbsolutePath() + "] is not a regular file.", f.isFile());
    assertTrue("File [" + f.getAbsolutePath() + "] is not readable.", f.canRead());
  }

  @Test
  public void testGetBestUGIFromTicketCache() throws Exception {
    conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    
    cacheTGT(fullPrincipalName); // cache the ticket

    final UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCacheFilePath,
        fullPrincipalName);

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
    for (Principal p : principals) {
      assertEquals(fullPrincipalName, p.getName());
    }
    assertTrue(!subject.isReadOnly());

    final Set<KerberosKey> krbKeySet = subject.getPrivateCredentials(KerberosKey.class);
    assertTrue(krbKeySet.isEmpty());
    final Set<KerberosTicket> krbTicketSet = subject.getPrivateCredentials(KerberosTicket.class);
    assertTrue(!krbTicketSet.isEmpty());

    for (final KerberosTicket kt : krbTicketSet) {
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
  public void testUserGroupInformationMain() throws Exception {
    checkFileReadable(ktbFilePath);
    // Get current user:
    final String currentUser = System.getProperty("user.name");
    assertNotNull(currentUser);

    // 0. test #main(String[]) with no args:
    UserGroupInformation.reset();
    UserGroupInformation.main(new String[0]);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getRealAuthenticationMethod());
    assertNull(ugi.getRealUser());
    assertEquals(currentUser, ugi.getUserName());
    assertEquals(currentUser, ugi.getShortUserName());
    Subject subj = ugi.getSubject();
    assertTrue(subj.getPrivateCredentials().size() == 0);

    final String[] args = new String[] { principal, ktbFilePath };
    // 1. Invoke with simple auth:
    UserGroupInformation.reset();
    UserGroupInformation.main(args);
    ugi = UserGroupInformation.getLoginUser();
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.SIMPLE, ugi.getRealAuthenticationMethod());
    assertNull(ugi.getRealUser());
    assertEquals(currentUser, ugi.getUserName());
    assertEquals(currentUser, ugi.getShortUserName());
    subj = ugi.getSubject();
    assertTrue(subj.getPrivateCredentials().size() == 0);

    // 2. Set Kerberos auth method and invoke again:
    UserGroupInformation.reset();
    final Configuration c = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, c);
    UserGroupInformation.setConfiguration(c);
    UserGroupInformation.main(args);
    ugi = UserGroupInformation.getLoginUser();
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getRealAuthenticationMethod());
    assertNull(ugi.getRealUser());
    assertEquals(fullPrincipalName, ugi.getUserName());
    assertEquals(principal, ugi.getShortUserName());
    subj = ugi.getSubject();
    assertTrue(subj.getPrivateCredentials().size() > 0);
  }

  private static void clearTGT() {
    // it may be null in case if the property is not given,
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

  private static void cacheTGT(final String princ) throws Exception {
    // ensure the keytab file exists:
    checkFileReadable(ktbFilePath);
    clearTGT();
    cacheTgtImpl(princ);
    checkFileReadable(ticketCacheFilePath);
  }

  // NB: we could use miniKdc.kdc.getConfig().getEncryptionTypes(), but miniKdc#kdc is private.
  // We could also use field org.apache.directory.kerberos.client.KdcConfig.DEFAULT_ENCRYPTION_TYPES,
  // but it is also private.
  // So, calculate the default enc types on our own:
  private static Set<EncryptionType> getDefaultEncryptionTypes() {
    Set<EncryptionType> set = new HashSet<EncryptionType>();
    for (String etStr: KerberosConfig.DEFAULT_ENCRYPTION_TYPES) {
      EncryptionType et = EncryptionType.getByName(etStr);
      set.add(et);
    }
    set = KerberosUtils.orderEtypesByStrength(set);
    return set;
  }
  
  private static void cacheTgtImpl(String princ) throws Exception {
    KdcConfig kdcConfig = KdcConfig.getDefaultConfig();
    kdcConfig.setUseUdp( false );
    kdcConfig.setKdcPort( miniKdc.getPort() );
    kdcConfig.setEncryptionTypes( getDefaultEncryptionTypes() );
    kdcConfig.setTimeout( Integer.MAX_VALUE );
    
    KdcConnection kdcConnection = new KdcConnection(kdcConfig);
    
    Kinit kinit = new Kinit(kdcConnection);
    File ticketCacheFile = new File(ticketCacheFilePath);
    kinit.setCredCacheFile(ticketCacheFile);
    kinit.kinit(princ, password);
    
    // cache file must exist at this point:
    checkFileReadable(ticketCacheFilePath);
    // check ticket cache file has appropriate content:
    CredentialsCache credentialsCache = CredentialsCache.load(ticketCacheFile);
    assertNotNull(credentialsCache);
  } 
  
  @Test
  public void testCheckTGTAndReloginFromKeytab() throws Exception {
    conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    clearTGT();
    // Reset the ticket renew window to a very small value to ensure
    // fast renew allowed:
    UserGroupInformation.setTicketRenewWindowFactor(1e-8f);
    // initial login from the keytab:
    final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        fullPrincipalName, ktbFilePath);
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
    // NB: for some reason the last login time is updated *only* upon a
    // re-login.
    // Initial login time is always zero:
    assertTrue(loginTime0 == 0L);
    final LoginContext loginContext0 = user.getLogin();

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

  static File createWd() {
    KerberosSecurityTestcase kst = new KerberosSecurityTestcase();
    kst.createTestDir(); // this does not create dir, but only composes its name. 
    File testDir = kst.getWorkDir();
    File wd = new File(testDir, "minikdc");
    if (wd.exists()) {
      FileUtil.fullyDelete(wd, true);
    }
    assertFalse(wd.exists());
    wd.mkdirs();
    assertTrue(wd.exists());
    return wd;
  }
  
  private static void initMiniKdc(File wd) throws Exception {
    Properties p = MiniKdc.createConf();
    p.put(MiniKdc.DEBUG, "true");
    miniKdc = new MiniKdc(p, wd);
    miniKdc.start();
    // Check properties that should be set by MiniKdc:
    assertEquals("true", System.getProperty("sun.security.krb5.debug"));
    assertEquals(miniKdc.getKrb5conf().getAbsolutePath(),
        System.getProperty("java.security.krb5.conf"));
  }

  private static void disposeMiniKdc() {
    final MiniKdc mk = miniKdc;
    if (mk != null) {
      mk.stop();
      miniKdc = null;
    }
  }

}
