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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;


import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCapacitySchedulerExtended {
  private static final Log LOG = LogFactory.getLog(TestCapacitySchedulerExtended.class);
  
  private static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String A1 = A + ".a1";
  private static final String A2 = A + ".a2";
  private static final String B1 = B + ".b1";
  private static final String B2 = B + ".b2";
  private static final String B3 = B + ".b3";
  private static float A_CAPACITY = 10.5f;
  private static float B_CAPACITY = 89.5f;
  private static float A1_CAPACITY = 30;
  private static float A2_CAPACITY = 70;
  private static float B1_CAPACITY = 50;
  private static float B2_CAPACITY = 30;
  private static float B3_CAPACITY = 20;

  private ResourceManager resourceManager = null;
  
  @Before
  public void setUp() throws Exception {
    Store store = StoreFactory.getStore(new Configuration());
    resourceManager = new ResourceManager(store);
    CapacitySchedulerConfiguration csConf 
       = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, 
        CapacityScheduler.class, ResourceScheduler.class);
    resourceManager.init(conf);
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
  }

  @After
  public void tearDown() throws Exception {
    resourceManager.stop();
  }
  
  
  @Test
  public void testCapacitySchedulerInfo() throws Exception {
  	QueueInfo queueInfo= resourceManager.getResourceScheduler().getQueueInfo("a",true,true);
  	Assert.assertEquals(queueInfo.getQueueName(), "a");
  	Assert.assertEquals(queueInfo.getChildQueues().size(), 2);
  	
  	List<QueueUserACLInfo> userACLInfo= resourceManager.getResourceScheduler().getQueueUserAclInfo();
  	Assert.assertNotNull(userACLInfo);
  	for (QueueUserACLInfo queueUserACLInfo : userACLInfo) {
  		Assert.assertEquals(getQueueCount(userACLInfo,queueUserACLInfo.getQueueName()), 1);
  	}
  	
  }

  private int getQueueCount(List<QueueUserACLInfo> queueInformation, String queueName){
  	int result=0;
  	for (QueueUserACLInfo queueUserACLInfo : queueInformation) {
  		if(queueName.equals(queueUserACLInfo.getQueueName())){
  			result++;
  		}
  	}
  	return result;
  }

  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    
    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);
    
    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);
    
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    LOG.info("Setup top-level queues a and b");
  }
  
  
  
}
