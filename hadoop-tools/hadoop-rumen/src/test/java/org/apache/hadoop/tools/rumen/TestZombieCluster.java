package org.apache.hadoop.tools.rumen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.util.Random;

import org.junit.Test;

public class TestZombieCluster {

  private static File workSpace = new File("src" + File.separator + "test"
      + File.separator + "resources" + File.separator + "data");

  @Test(timeout=1000)
  public void testParseTopology() throws Exception {
    File cluster = new File(workSpace.getAbsolutePath() + File.separator
        + "19-jobs.topology");

    // read topology data
    ZombieCluster zcl = new ZombieCluster(new FileInputStream(cluster), null);
    // check data
    Node root = zcl.getClusterTopology();
    Node chiNode = root.getChildren().iterator().next();
    assertEquals(1, zcl.distance(root, chiNode));

    // check getting random hosts from cluster
    Random r = new Random(System.currentTimeMillis());
    assertEquals(2, zcl.getRandomMachines(2, r).length);
    assertEquals(1545, zcl.getMachines().size());
    assertEquals(41, zcl.getRacks().size());

    // test data in MachineNode
    MachineNode mn = zcl.getMachines().iterator().next();
    assertEquals(2, mn.getLevel());
    assertEquals(1, mn.getMapSlots());

    assertNotNull(zcl.getRackByName("/192\\.30\\.116\\.128"));
    assertNull(zcl.getRackByName("/192\\.30\\.116\\.111"));
  }

}
