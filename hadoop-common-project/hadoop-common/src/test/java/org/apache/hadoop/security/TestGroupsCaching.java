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
package org.apache.hadoop.security;

import java.io.IOException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;


public class TestGroupsCaching {
  public static final Log LOG = LogFactory.getLog(TestGroupsCaching.class);
  private static final Configuration conf = new Configuration();
  private static final String[] myGroups = {"grp1", "grp2"};

  @BeforeClass
  public static void beforeClass() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
      FakeGroupMapping.class,
      ShellBasedUnixGroupsMapping.class);
  }
  
  @Before
  public void before() {
    NetgroupCache.clear();
    FakeGroupMapping.allGroups.clear();
    FakeGroupMapping.blackList.clear();
  }

  @After
  public void after() {
    NetgroupCache.clear();
    FakeGroupMapping.allGroups.clear();
    FakeGroupMapping.blackList.clear();
  }
  
  public static class FakeGroupMapping extends ShellBasedUnixGroupsMapping {
    // any to n mapping
    private static Set<String> allGroups = new HashSet<String>();
    private static Set<String> blackList = new HashSet<String>();

    @Override
    public List<String> getGroups(String user) throws IOException {
      LOG.info("Getting groups for " + user);
      if (blackList.contains(user)) {
        return new LinkedList<String>();
      }
      return new LinkedList<String>(allGroups);
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      LOG.info("Cache is being refreshed.");
      clearBlackList();
      return;
    }

    public static void clearBlackList() throws IOException {
      LOG.info("Clearing the blacklist");
      blackList.clear();
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      LOG.info("Adding " + groups + " to groups.");
      allGroups.addAll(groups);
    }

    public static void addToBlackList(String user) throws IOException {
      LOG.info("Adding " + user + " to the blacklist");
      blackList.add(user);
    }
  }

  @Test
  public void testGroupsCaching() throws Exception {
    final Groups groups = new Groups(conf);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();
    FakeGroupMapping.addToBlackList("user1");

    // regular entry
    assertEquals(2, groups.getGroups("me").size());

    // this must be cached. blacklisting should have no effect.
    FakeGroupMapping.addToBlackList("me");
    assertEquals(2, groups.getGroups("me").size());

    // ask for a negative entry
    try {
      LOG.error("We are not supposed to get here." + groups.getGroups("user1").toString());
      fail();
    } catch (IOException ioe) {
      if(!ioe.getMessage().startsWith("No groups found")) {
        LOG.error("Got unexpected exception: " + ioe.getMessage());
        fail();
      }
    }

    // this shouldn't be cached. remove from the black list and retry.
    FakeGroupMapping.clearBlackList();
    assertEquals(2, groups.getGroups("user1").size());
  }
}
