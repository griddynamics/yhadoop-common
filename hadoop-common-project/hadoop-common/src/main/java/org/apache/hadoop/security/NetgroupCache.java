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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Class that caches the netgroups and inverts group-to-user map
 * to user-to-group map, primarily intended for use with
 * netgroups (as returned by getent netgrgoup) which only returns
 * group to user mapping.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetgroupCache {

  private static final ConcurrentMap<String, Set<String>> netgroupToUsersMap 
    = new ConcurrentHashMap<String, Set<String>>(32);

  private static final AtomicReference<Map<String, Set<String>>> userToNetgroupMapReference 
    = new AtomicReference<Map<String, Set<String>>>(null); 


  /**
   * Get netgroups for a given user
   *
   * @param user get groups for this user
   * @param groupsContainer put groups into this List
   */
  public static void getNetgroups(final String user,
      List<String> groupsContainer) {
    // get existing or rebuild the user-to-netgroups mapping:
    final Map<String,Set<String>> userToNetgroupsMap = getUserToNetgroupMapImpl();
    assert (userToNetgroupsMap != null);
    final Set<String> groupSet = userToNetgroupsMap.get(user);
    if (groupSet != null) {
      groupsContainer.addAll(groupSet);
    }
  }
  
  /*
   * Gets or rebuilds the reverse user-to-netgroup mapping.
   * If an existing map is present, it is returned.
   * If not, it is rebuilt and saved in "userToNetgroupMapReference".  
   */
  private static Map<String,Set<String>> getUserToNetgroupMapImpl() {
    Map<String,Set<String>> newUserToNgMap = null;
    while (true) {
      final Map<String,Set<String>> existingU2NgMap = userToNetgroupMapReference.get();
      if (existingU2NgMap != null) {
        return existingU2NgMap; // return existing map
      }
      if (newUserToNgMap == null) {
        newUserToNgMap = new HashMap<String,Set<String>>(32);
      } else {
        newUserToNgMap.clear(); // reuse the same local instance in loop
      }
      // try to build a new map:
      for (Entry<String,Set<String>> e: netgroupToUsersMap.entrySet()) {
        String netgroup = e.getKey();
        for (String netuser: e.getValue()) {
          Set<String> groupSet = newUserToNgMap.get(netuser);
          if (groupSet == null) {
            groupSet = new HashSet<String>();
            newUserToNgMap.put(netuser, groupSet);
          }
          groupSet.add(netgroup);
        }
      }
      if (userToNetgroupMapReference.compareAndSet(null, newUserToNgMap)) {
        // ok, we set the new map, so return the new value:
        return newUserToNgMap;
      }
    }
  }

  /**
   * Get the list of cached netgroups
   *
   * @return list of cached groups
   */
  public static List<String> getNetgroupNames() {
    return new LinkedList<String>(netgroupToUsersMap.keySet());
  }

  /**
   * Returns true if a given netgroup is cached
   *
   * @param group check if this group is cached
   * @return true if group is cached, false otherwise
   */
  public static boolean isCached(String group) {
    return netgroupToUsersMap.containsKey(group);
  }

  /**
   * Clear the cache
   */
  public static void clear() {
    netgroupToUsersMap.clear();
    userToNetgroupMapReference.set(null);
  }

  /**
   * Add group to cache
   *
   * @param group name of the group to add to cache
   * @param users list of users for a given group
   */
  public static void add(final String group, List<String> users) {
    final Set<String> newSet = new HashSet<String>(users);
    // Preserve the existing contract of the method:
    // if the group was already cached, do not change it.
    final Set<String> previousUserSet = netgroupToUsersMap.putIfAbsent(group,
        newSet);
    // Update only in case the group was not previously cached:
    if (previousUserSet == null) {
      // clear the reference to indicate that the user-to-netgroup map
      // needs to be rebuilt:
      userToNetgroupMapReference.set(null);
    }
  }
}
