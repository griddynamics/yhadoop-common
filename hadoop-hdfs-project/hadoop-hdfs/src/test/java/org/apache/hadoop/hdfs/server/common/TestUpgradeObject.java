package org.apache.hadoop.hdfs.server.common;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.SortedSet;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.junit.Test;
import org.mockito.Mockito;


public class TestUpgradeObject {

  @Test
  public void testUpgradeObjectCollection() {
    UpgradeObjectCollection.UOSignature fuoSignature = null;
    UpgradeObjectCollection.UOSignature suoSignature = null;

    fuoSignature = new UpgradeObjectCollection.UOSignature(
        createObjectInstance(1, NodeType.DATA_NODE));
    suoSignature = new UpgradeObjectCollection.UOSignature(
        createObjectInstance(1, NodeType.DATA_NODE));

    assertTrue(fuoSignature.compareTo(suoSignature) == 0);
    assertTrue(fuoSignature.equals(suoSignature));
    assertTrue(fuoSignature.hashCode() == fuoSignature.hashCode());
    assertTrue(fuoSignature.hashCode() == suoSignature.hashCode());
    assertTrue(fuoSignature.getClassName().equals(
        UpgradeObjectImpl.class.getCanonicalName()));
    assertTrue(fuoSignature.getVersion() == suoSignature.getVersion() ? fuoSignature
        .getVersion() == 1 : false);

    fuoSignature = new UpgradeObjectCollection.UOSignature(
        createObjectInstance(1, NodeType.DATA_NODE));
    suoSignature = new UpgradeObjectCollection.UOSignature(
        createObjectInstance(2, NodeType.DATA_NODE));
    assertTrue(fuoSignature.compareTo(suoSignature) < 0);
    assertFalse(fuoSignature.compareTo(suoSignature) >= 0);
    assertFalse(fuoSignature.equals(suoSignature));
    assertTrue(fuoSignature.hashCode() == fuoSignature.hashCode());
    assertFalse(fuoSignature.hashCode() == suoSignature.hashCode());

    fuoSignature = new UpgradeObjectCollection.UOSignature(
        createObjectInstance(2, NodeType.DATA_NODE));
    suoSignature = new UpgradeObjectCollection.UOSignature(
        createObjectInstance(1, NodeType.DATA_NODE));
    assertTrue(fuoSignature.compareTo(suoSignature) > 0);
    assertFalse(fuoSignature.compareTo(suoSignature) <= 0);
    assertFalse(fuoSignature.equals(suoSignature));
    assertTrue(fuoSignature.hashCode() == fuoSignature.hashCode());
    assertFalse(fuoSignature.hashCode() == suoSignature.hashCode());
  }

  @Test
  public void testUpgradeObject() throws IOException {
    UpgradeObject fObject = null;
    UpgradeObject sObject = null;
    String TEST_PATTERN = "Upgrade object for {0} layout version {1}";
    fObject = createObjectInstance(1, NodeType.DATA_NODE);
    sObject = createObjectInstance(1, NodeType.DATA_NODE);
    assertTrue(fObject.compareTo(sObject) == 0);
    assertTrue(fObject.equals(sObject));
    assertTrue(fObject.hashCode() == sObject.hashCode());
    assertTrue(fObject.getUpgradeStatus() == sObject.getUpgradeStatus());
    assertTrue(fObject.getUpgradeStatus() == 0);
    assertNotNull(fObject.getUpgradeStatusReport(true));
    assertNotNull(fObject.completeUpgrade());
    assertNotNull(fObject.startUpgrade());
    assertTrue(fObject.getDescription().equals(
        MessageFormat.format(TEST_PATTERN, fObject.getType(),
            fObject.getVersion())));

    fObject = createObjectInstance(1, NodeType.DATA_NODE);
    sObject = createObjectInstance(2, NodeType.DATA_NODE);
    assertTrue(fObject.compareTo(sObject) > 0);
    assertFalse(fObject.equals(sObject));
    assertFalse(fObject.hashCode() == sObject.hashCode());
    assertTrue(fObject.getUpgradeStatus() == sObject.getUpgradeStatus());
    assertTrue(fObject.getUpgradeStatus() == 0);

    fObject = createObjectInstance(2, NodeType.DATA_NODE);
    sObject = createObjectInstance(1, NodeType.DATA_NODE);
    assertTrue(fObject.compareTo(sObject) < 0);
    assertFalse(fObject.equals(sObject));
    assertFalse(fObject.hashCode() == sObject.hashCode());
    assertTrue(fObject.getUpgradeStatus() == sObject.getUpgradeStatus());
    assertTrue(fObject.getUpgradeStatus() == 0);

    fObject = createObjectInstance(2, NodeType.NAME_NODE);
    sObject = createObjectInstance(2, NodeType.DATA_NODE);
    assertTrue(fObject.compareTo(sObject) > 0);
    assertFalse(fObject.equals(sObject));
    assertFalse(fObject.hashCode() == sObject.hashCode());
    assertTrue(fObject.getUpgradeStatus() == sObject.getUpgradeStatus());
    assertTrue(fObject.getUpgradeStatus() == 0);

    fObject = createObjectInstance(2, NodeType.NAME_NODE);
    sObject = createObjectInstance(2, NodeType.DATA_NODE);
    assertTrue(fObject.compareTo(sObject) > 0);
    assertFalse(fObject.equals(sObject));
    assertFalse(fObject.hashCode() == sObject.hashCode());
    assertTrue(fObject.getUpgradeStatus() == sObject.getUpgradeStatus() ? sObject
        .getUpgradeStatus() == 0 : false);
  }

  @Test
  public void testRegisterUpgrade() throws IOException {
    UpgradeObjectCollection.initialize();
    UpgradeObjectCollection.registerUpgrade(createObjectInstance(1,
        NodeType.DATA_NODE));
    UpgradeObjectCollection.registerUpgrade(createObjectInstance(2,
        NodeType.DATA_NODE));
    UpgradeObjectCollection.registerUpgrade(createObjectInstance(3,
        NodeType.DATA_NODE));
    SortedSet<Upgradeable> result = UpgradeObjectCollection
        .getDistributedUpgrades(1, NodeType.DATA_NODE);
    assertTrue(result.size() != 0);
  }

  private static UpgradeObject createObjectInstance(int version,
      NodeType nodeType) {
    return new UpgradeObjectImpl(version, nodeType);
  }
}
