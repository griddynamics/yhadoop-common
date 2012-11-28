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
package org.apache.hadoop.mapred;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.Test;
import static junit.framework.Assert.*;

/**
 * TestCounters checks the sanity and recoverability of Queue
 */
public class TestQueue {

  @Test
  public void testQueue() throws IOException {
    File f = null;
    try {
      f = writeFile();
      
      QueueManager manager = new QueueManager(f.getCanonicalPath(), true);
      Queue root = manager.getRoot();
      assertTrue(root.getChildren().size()==2);
      Iterator<Queue> iterator=root.getChildren().iterator();
      Queue firstSubQueue= iterator.next();
      assertTrue(firstSubQueue.getName().equals("first"));
      assertEquals(firstSubQueue.getAcls().get("mapred.queue.first.acl-submit-job").toString(),"Users [user1, user2] and members of the groups [group1, group2] are allowed");
      Queue secondSubQueue= iterator.next();
      assertTrue(secondSubQueue.getName().equals("second"));
      assertEquals(secondSubQueue.getProperties().getProperty("key"), "value");
      assertEquals(secondSubQueue.getProperties().getProperty("key1"), "value1");
      // test status 
      assertEquals(firstSubQueue.getState().getStateName(), "running");
      assertEquals(secondSubQueue.getState().getStateName(), "stopped");

      Set<String> template = new HashSet<String>();
      template.add("first");
      template.add("second");
      assertEquals(manager.getLeafQueueNames(),template);


    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }
  /**
   * test for Qmanager with configuration
   * @throws IOException
   */
  
  @Test
  public void test2Queue() throws IOException {
      Configuration conf= new Configuration();
      conf.set(DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY, "first,second");
      conf.set(QueueManager.QUEUE_CONF_PROPERTY_NAME_PREFIX +  "first.acl-submit-job","user1,user2 group1,group2" );
      conf.set(MRConfig.MR_ACLS_ENABLED, "true");
      conf.set(QueueManager.QUEUE_CONF_PROPERTY_NAME_PREFIX+"first.state","running" );
      conf.set(QueueManager.QUEUE_CONF_PROPERTY_NAME_PREFIX+"second.state","stopped" );

      QueueManager manager = new QueueManager(conf);
      Queue root = manager.getRoot();
      assertTrue(root.getChildren().size()==2);
      Iterator<Queue> iterator=root.getChildren().iterator();
      Queue firstSubQueue= iterator.next();
      assertTrue(firstSubQueue.getName().equals("first"));
      assertEquals(firstSubQueue.getAcls().get("mapred.queue.first.acl-submit-job").toString(),"Users [user1, user2] and members of the groups [group1, group2] are allowed");
      Queue secondSubQueue= iterator.next();
      assertTrue(secondSubQueue.getName().equals("second"));
     
      assertEquals(firstSubQueue.getState().getStateName(), "running");
      assertEquals(secondSubQueue.getState().getStateName(), "stopped");

      Set<String> template = new HashSet<String>();
      template.add("first");
      template.add("second");
      assertEquals(manager.getLeafQueueNames(),template);


  
  }
  
  private File writeFile() throws IOException {

    File f = null;
    f = File.createTempFile("tst", "xml");
    BufferedWriter out = new BufferedWriter(new FileWriter(f));
    String properties="<properties><property key=\"key\" value=\"value\"/><property key=\"key1\" value=\"value1\"/></properties>";
    out.write("<queues>");
    out.newLine();
    out.write("<queue><name>first</name><acl-submit-job>user1,user2 group1,group2</acl-submit-job><acl-administer-jobs>user3,user4 group3,group4</acl-administer-jobs><state>running</state></queue>");
    out.newLine();
    out.write("<queue><name>second</name><acl-submit-job>u1,u2 g1,g2</acl-submit-job>"+properties+"<state>stopped</state></queue>");
    out.newLine();
    out.write("</queues>");
    out.flush();
    out.close();
    return f;

  }
}
