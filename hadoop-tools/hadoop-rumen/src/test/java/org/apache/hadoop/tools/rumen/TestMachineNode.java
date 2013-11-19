package org.apache.hadoop.tools.rumen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestMachineNode {
  
  @Test (timeout=100)
  public void testCloning() {
    MachineNode mn = new MachineNode.Builder("node", 1).setMapSlots(2)
        .setMemory(100).setMemoryPerMapSlot(50).setMemoryPerReduceSlot(50)
        .setNumCores(4).setReduceSlots(2).build();

    assertEquals(1, mn.getLevel());
    assertEquals(2, mn.getMapSlots());
    assertEquals(100, mn.getMemory());
    assertEquals(50, mn.getMemoryPerMapSlot());
    assertEquals(50, mn.getMemoryPerReduceSlot());
    assertEquals("node", mn.getName());
    assertEquals(4, mn.getNumCores());
    assertEquals(2, mn.getReduceSlots());
    assertNull(mn.getParent());
    assertNull(mn.getRackNode());
    
    MachineNode copy = new MachineNode.Builder("node2", 2).cloneFrom(mn)
        .build();
    assertEquals(2, copy.getLevel());
    assertEquals(2, copy.getMapSlots());
    assertEquals(100, copy.getMemory());
    assertEquals(50, copy.getMemoryPerMapSlot());
    assertEquals(50, copy.getMemoryPerReduceSlot());
    assertEquals("node2", copy.getName());
    assertEquals(4, copy.getNumCores());
    assertEquals(2, copy.getReduceSlots());
    assertNull(copy.getParent());
    assertNull(copy.getRackNode());

    assertFalse("Nodes with different names are equal", mn.equals(copy));

    copy = new MachineNode.Builder("node", 2).cloneFrom(mn).build();
    assertTrue("Nodes with the same name are not equal", mn.equals(copy));
  }

}
