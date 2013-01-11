package org.apache.hadoop.security;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Shell;
import org.junit.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * This test checks netgroup information fetching and caching.  
 * Real OS netgroup configuration is used. If the netgroup config is not found 
 * or does not appear to contain expected data expected by tests (see below), the 
 * tests get skipped.
 *  
 * This test better to be run with "-Pnative" profile to check also the native
 * netgroup fetching implementation. If the native code is not loaded, corresponding 
 * tests also get skipped.
 * 
 * The test can only be run on a Unix platform.
 */
public class TestNetGroupCaching {
  
  /*
   * The following netgroup configuration file (/etc/netgroup) can be used for the testing:
   * -------------------------------------
   *sysadmins    (-,sshah,) (-,heidis,) (-,jnguyen,) (-,mpham,) (-,giraffe,)
   *research-1   (-,boson,) (-,jyom,) (-,weals,) (-,jaffe,)
   *research-2   (-,sangeet,) (-,mona,) (-,paresh,) (-,manjari,) (-,jagdish,) (-,giraffe,)
   *consultants  (-,arturo,) (-,giraffe,)
   *allusers       sysadmins research-1 research-2 consultants
   * ------------------------------------- 
   */
  private static final String netgroupConfigFile = "/etc/netgroup";  
  private static final String currentNetUser = "giraffe";
  private static final Set<String> expectedNetGroupSet = new HashSet<String>(
      Arrays.asList(new String[] { "@sysadmins", "@consultants", "@research-2" }));
  
  private static final int groupCacheTimeoutSeconds = 1;

  private static final Log log = LogFactory.getLog(TestNetGroupCaching.class);
  
  private Configuration conf;
  
  private static boolean isNetgroupConfigCorrect() throws IOException {
    File f = new File(netgroupConfigFile);
    if (!f.exists()) {
      log.warn("Netgroup config file ["+netgroupConfigFile+"] does not exist.");
      return false;
    }
    if (!f.canRead()) {
      log.warn("Netgroup config file ["+netgroupConfigFile+"] is not readable.");
      return false;
    }
    FileInputStream fis = new FileInputStream(f);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(2048); 
    try {
      int b;
      while (true) {
        b = fis.read();
        if (b < 0) {
          break;
        } else {
          baos.write(b);
        }
      }
      String content = new String(baos.toByteArray(), "UTF-8");
      if (content.contains(currentNetUser)) {
        return true;
      } else {
        log.warn("The config file ["+netgroupConfigFile+"] does not contain " +
        		"expected net user ["+currentNetUser+"].");
        return false;
      }
    } finally {
      fis.close();
    }
  }
  
  @BeforeClass
  public static void beforeClass() throws IOException {
    assumeTrue(isNetgroupConfigCorrect());
  }
  
  @Before
  public void before() {
    cleanupImpl();
  }
  
  @After
  public void after() {
    cleanupImpl();
  } 
  
  private void cleanupImpl() {
    Groups.resetGroups();
    NetgroupCache.clear();
  }

  /*
   * loads the configuration with the given parameters
   */
  private void setGroupMappingServiceProviderImplClass(Class<? extends GroupMappingServiceProvider> c, final long cacheTimeoutSec) {
    conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        c, 
        GroupMappingServiceProvider.class);
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 
        cacheTimeoutSec/*sec*/);
  }
  
  /*
   * This test must unconditionally pass.
   */
  @Test
  public void testShellBasedUnixGroupsMappingWithCaching() throws Exception {
    testUnixGroupsMappingWithCachingImpl(ShellBasedUnixGroupsMapping.class);
  }

  /*
   * This test is skipped in the native code is not loaded. 
   */
  @Test
  public void testJniBasedUnixGroupsMappingWithCaching() throws Exception {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    testUnixGroupsMappingWithCachingImpl(JniBasedUnixGroupsMapping.class);
  }

  /*
   * This test must also always pass since it uses the fallback implementation. 
   */
  @Test
  public void testJniBasedUnixGroupsMappingWithFallbackWithCaching() throws Exception {
    testUnixGroupsMappingWithCachingImpl(JniBasedUnixGroupsMappingWithFallback.class);
  }

  
  private void testUnixGroupsMappingWithCachingImpl(final Class<? extends GroupMappingServiceProvider> c) throws Exception {
    setGroupMappingServiceProviderImplClass(c, groupCacheTimeoutSeconds);
    
    final Groups groups = Groups.getUserToGroupsMappingService(conf);
    
    final String currentUser = System.getProperty("user.name");
    assertTrue(currentUser != null && currentUser.length() > 0);
    
    // expected groups:
    final String[] cmd = Shell.getGroupsForUserCommand(currentUser);
    final String out = Shell.execCommand(cmd);
    final String[] expectedGroupArr = out.trim().split("\\s+");
    log.info("Expected groups of user ["+currentUser+"]: " + Arrays.toString(expectedGroupArr));
    final Set<String> expectedGroupSet = new HashSet<String>(Arrays.asList(expectedGroupArr));
    
    // Some repeated group might disappear. This should not happen:  
    assertEquals(expectedGroupArr.length, expectedGroupSet.size());
    
    testCachingImpl(groups, 
        currentUser, 
        expectedGroupArr,
        expectedGroupSet,
        true);
  }

  // ===========================================================================
  
  /*
   * This test must unconditionally pass.
   */
  @Test
  public void testShellBasedUnixGroupsNetgroupMappingWithCaching() throws Exception {
    testGroupsNetgroupMappingWithCachingImpl(ShellBasedUnixGroupsNetgroupMapping.class);
  }

  /*
   * This test is skipped in the native code is not loaded. 
   */
  @Test
  public void testJniBasedUnixGroupsNetgroupMappingWithCaching() throws Exception {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    testGroupsNetgroupMappingWithCachingImpl(JniBasedUnixGroupsNetgroupMapping.class);
  }
  
  /*
   * This test must also always pass since it uses the fallback implementation. 
   */
  @Test
  public void testJniBasedUnixGroupsNetgroupMappingWithFallbackWithCaching() throws Exception {
    testGroupsNetgroupMappingWithCachingImpl(JniBasedUnixGroupsNetgroupMappingWithFallback.class);
  }
  
  private void testGroupsNetgroupMappingWithCachingImpl(Class<? extends GroupMappingServiceProvider> c) throws Exception {
    // Set the configuration:
    setGroupMappingServiceProviderImplClass(
        c, groupCacheTimeoutSeconds);
    // construct the Groups object to be tested:
    final Groups groups = Groups.getUserToGroupsMappingService(conf);
    testCachingImpl(groups, 
        currentNetUser, 
        new String[] { "@sysadmins", "@research-1", "@research-2", "@consultants" },
        expectedNetGroupSet,
        false);
  }
  
  private void testCachingImpl(final Groups groups, 
      final String user, 
      final String[] groupsToBeCached, 
      final Set<String> expectedGroupSet, 
      final boolean getGroupsWithoutCachingIsOkay) throws Exception {
    groups.refresh(); // nothing done, but ensure no error.
    List<String> groupsWithoutCaching;
    try {
      groupsWithoutCaching = groups.getGroups(user);
      if (getGroupsWithoutCachingIsOkay) { 
        checkGroups(expectedGroupSet, groupsWithoutCaching);
      } else {
        assertTrue(groupsWithoutCaching + "", false); // exception expected.
      }
    } catch (IOException ioe) {
      if (getGroupsWithoutCachingIsOkay) {
        throw ioe;
      } else {
        // okay, IOE expected.
      }
    }
    
    log.info("Caching the groups.");
    groups.cacheGroupsAdd(Arrays.asList(groupsToBeCached));
    
    log.info("Getting groups:");
    final List<String> actualGroupList0 = groups.getGroups(user);
    checkGroups(expectedGroupSet, actualGroupList0);
    log.info("Getting groups again:");
    final List<String> actualGroupList1 = groups.getGroups(user);
    // These should be the cached groups:
    assertTrue(actualGroupList0 == actualGroupList1);
    
    Thread.sleep((groupCacheTimeoutSeconds * 1000L * 11)/10); /* +10% */
    log.info("Getting groups after cache expiration:");
    final List<String> actualGroupList2 = groups.getGroups(user);
    // These are *no* cached groups because the cache is expired:
    assertTrue(actualGroupList1 != actualGroupList2);
    checkGroups(expectedGroupSet, actualGroupList2);
    
    log.info("Refreshing groups:");
    groups.refresh();
    Thread.sleep((groupCacheTimeoutSeconds * 1000L * 11)/10); /* +10% */
    log.info("Getting groups after cache expiration again:");
    final List<String> actualGroupList3 = groups.getGroups(user);
    // These are *no* cached groups because 
    // the groups are cached only upon #getGroups():
    assertTrue(actualGroupList2 != actualGroupList3);
    checkGroups(expectedGroupSet, actualGroupList3);
  }

  /*
   * Check that passed in groups content is equal to the expected one.  
   */
  private void checkGroups(final Set<String> expectedGroupSet, 
      final List<String> actualGroupList) {
    log.info("Actual netgroups of user ["+currentNetUser+"]: " + Arrays.toString(actualGroupList.toArray()));
    final Set<String> actualGroupSet = new HashSet<String>(actualGroupList);
    assertEquals(actualGroupList.size(), actualGroupSet.size());
    assertEquals(expectedGroupSet.size(), actualGroupSet.size());
    assertEquals(expectedGroupSet, actualGroupSet);
  }
}
