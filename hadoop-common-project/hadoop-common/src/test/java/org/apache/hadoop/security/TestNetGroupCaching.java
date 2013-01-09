package org.apache.hadoop.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;
import org.junit.*;
import static org.junit.Assert.*;

// TODO: check also ...WithFallback implementations.
// Possibly add this to test org.apache.hadoop.security.TestGroupFallback
public class TestNetGroupCaching {

  private final Log log = LogFactory.getLog(getClass());
  
  private Configuration conf;
  
  @Before
  public void before() {
    Groups.resetGroups();
    NetgroupCache.clear();
  }
  
  @After
  public void after() {
    Groups.resetGroups();
    NetgroupCache.clear();
  } 

  private void setGroupMappingServiceProviderImplClass(Class<? extends GroupMappingServiceProvider> c, final long cacheTimeoutSec) {
    conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        c, 
        GroupMappingServiceProvider.class);
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 
        cacheTimeoutSec/*sec*/);
  }
  
  @Test
  public void testShellBasedUnixGroupsMapping() throws IOException {
    setGroupMappingServiceProviderImplClass(ShellBasedUnixGroupsMapping.class, 60);
    
    final Groups groups = Groups.getUserToGroupsMappingService(conf);
    
    groups.refresh(); // in fact, does nothing with this impl.
    groups.cacheGroupsAdd(Arrays.asList(new String[] { "servers" })); // also does nothing
    
    final String currentUser = System.getProperty("user.name");
    assertTrue(currentUser != null && currentUser.length() > 0);
    
    // expected groups:
    final String[] cmd = Shell.getGroupsForUserCommand(currentUser);
    final String out = Shell.execCommand(cmd);
    final String[] expectedGroupArr = out.trim().split("\\s+");
    log.info("Expected groups of user ["+currentUser+"]: " + Arrays.toString(expectedGroupArr));
    final Set<String> expectedGroupSet = new HashSet<String>(Arrays.asList(expectedGroupArr));
    // some repeated group might disappear. This should not happen:  
    assertEquals(expectedGroupArr.length, expectedGroupSet.size());
    
    final List<String> actualGroupList = groups.getGroups(currentUser);
    log.info("Actual groups of user ["+currentUser+"]: " + Arrays.toString(actualGroupList.toArray()));
    final Set<String> actualGroupSet = new HashSet<String>(actualGroupList);
    assertEquals(actualGroupList.size(), actualGroupSet.size());
    assertEquals(expectedGroupSet.size(), actualGroupSet.size());
    assertEquals(expectedGroupSet, actualGroupSet);
  }
  
  private final String currentNetUser = "giraffe";
  private final Set<String> expectedNetGroupSet = new HashSet<String>(
      Arrays.asList(new String[] { "@sysadmins", "@consultants", "@research-2" }));

  @Test
  public void testShellBasedUnixGroupsNetgroupMapping() throws Exception {
    // Set the configuration:
    setGroupMappingServiceProviderImplClass(
        ShellBasedUnixGroupsNetgroupMapping.class, 2/*2 sec*/);
    // construct the Groups object to be tested:
    final Groups groups = Groups.getUserToGroupsMappingService(conf);
    
    groups.refresh(); // nothing done, but ensure no error.
    try {
      // Expect failure because no net groups added yet: 
      groups.getGroups(currentNetUser);
      assertTrue(false);
    } catch (IOException ioe) {
      // okay, expected.
    }
    
    log.info("=== setting the groups.");
    groups.cacheGroupsAdd(Arrays.asList(new String[] { "@sysadmins", "@research-1", "@research-2" }));
    groups.cacheGroupsAdd(Arrays.asList(new String[] { "@consultants" }));
    log.info("=== groups set.");
    
    log.info("=== get groups 0:");
    final List<String> actualGroupList0 = groups.getGroups(currentNetUser);
    checkGroups(actualGroupList0);
    log.info("=== get groups 1:");
    final List<String> actualGroupList1 = groups.getGroups(currentNetUser);
    // These are the cached groups:
    assertTrue(actualGroupList0 == actualGroupList1);
    
    Thread.sleep(2500L);
    log.info("=== get groups 2:");
    final List<String> actualGroupList2 = groups.getGroups(currentNetUser);
    // These are *not* cached groups:
    assertTrue(actualGroupList1 != actualGroupList2);
    checkGroups(actualGroupList2);
    
    log.info("=== refreshing groups:");
    groups.refresh();
    Thread.sleep(2500L);
    log.info("=== get groups 3:");
    final List<String> actualGroupList3 = groups.getGroups(currentNetUser);
    // These are *not* cached groups because 
    // the groups are cached only upon #getGroups():
    assertTrue(actualGroupList2 != actualGroupList3);
    checkGroups(actualGroupList3);
  }

  /*
   * Check that passed in groups content is equal to the expected one.  
   */
  private void checkGroups(final List<String> actualGroupList) {
    log.info("Actual netgroups of user ["+currentNetUser+"]: " + Arrays.toString(actualGroupList.toArray()));
    final Set<String> actualGroupSet = new HashSet<String>(actualGroupList);
    assertEquals(actualGroupList.size(), actualGroupSet.size());
    assertEquals(expectedNetGroupSet.size(), actualGroupSet.size());
    assertEquals(expectedNetGroupSet, actualGroupSet);
  }
}
