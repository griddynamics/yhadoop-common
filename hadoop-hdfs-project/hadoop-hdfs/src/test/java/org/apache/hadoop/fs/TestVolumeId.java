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
package org.apache.hadoop.fs;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestVolumeId {

  @Test
  public void testEquality() {
    final VolumeId id1 = new HdfsVolumeId(new byte[] { (byte)0, (byte)0 }, true);
    testEq(true, id1, id1);

    final VolumeId id2 = new HdfsVolumeId(new byte[] { (byte)0, (byte)1 }, true);
    testEq(true, id2, id2);
    testEq(false, id1, id2);

    final VolumeId id3 = new HdfsVolumeId(new byte[] { (byte)-1, (byte)-1 }, true);
    testEq(true, id3, id3);
    testEq(false, id1, id3);
    
    // same as 2, but "invalid":
    final VolumeId id2copy1 = new HdfsVolumeId(new byte[] { (byte)0, (byte)1 }, false);
    
    testEq(true, id2, id2copy1);

    // same as 2copy1: 
    final VolumeId id2copy2 = new HdfsVolumeId(new byte[] { (byte)0, (byte)1 }, false);
    
    testEq(true, id2, id2copy2);
    
    testEq3(true, id2, id2copy1, id2copy2);
    
    testEq3(false, id1, id2, id3);
  }
  
  private void testEq(final boolean eq, VolumeId id1, VolumeId id2) {
    final int h1 = id1.hashCode();
    final int h2 = id2.hashCode();
    
    // eq reflectivity:
    assertTrue(id1.equals(id1));
    assertTrue(id2.equals(id2));
    assertEquals(0, id1.compareTo(id1));
    assertEquals(0, id2.compareTo(id2));

    // eq symmetry:
    assertEquals(eq, id1.equals(id2));
    assertEquals(eq, id2.equals(id1));
    
    
    // compareTo:
    assertEquals(eq, 0 == id1.compareTo(id2));
    assertEquals(eq, 0 == id2.compareTo(id1));
    // compareTo must be antisymmetric:
    assertEquals(sign(id1.compareTo(id2)), -sign(id2.compareTo(id1)));
    
    // check that hash codes did not change:
    assertEquals(h1, id1.hashCode());
    assertEquals(h2, id2.hashCode());
    if (eq) {
      // in this case the hash codes must be the same:
      assertEquals(h1, h2);
    }
  }
  
  private static int sign(int x) {
    if (x == 0) {
      return 0;
    } else if (x > 0) {
      return 1;
    } else {
      return -1;
    }
  }
  
  private void testEq3(final boolean eq, VolumeId id1, VolumeId id2, VolumeId id3) {
    testEq(eq, id1, id2);
    testEq(eq, id2, id3);
    testEq(eq, id1, id3);
    
    // comparison relationship must be acyclic:
    int cycleSum = sign(id1.compareTo(id2)) + sign(id2.compareTo(id3)) + sign(id3.compareTo(id1));
    assertTrue(Math.abs(cycleSum) < 3);
  }
  
}
