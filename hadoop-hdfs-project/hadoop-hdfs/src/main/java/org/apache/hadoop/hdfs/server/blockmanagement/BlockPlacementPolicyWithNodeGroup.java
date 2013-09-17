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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/** The class is responsible for choosing the desired number of targets
 * for placing block replicas on environment with node-group layer.
 * The replica placement strategy is adjusted to:
 * If the writer is on a datanode, the 1st replica is placed on the local 
 *     node (or local node-group), otherwise a random datanode. 
 * The 2nd replica is placed on a datanode that is on a different rack with 1st
 *     replica node. 
 * The 3rd replica is placed on a datanode which is on a different node-group
 *     but the same rack as the second replica node.
 */
public class BlockPlacementPolicyWithNodeGroup extends BlockPlacementPolicyDefault {

  BlockPlacementPolicyWithNodeGroup(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap);
  }

  BlockPlacementPolicyWithNodeGroup() {
  }

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
          NetworkTopology clusterMap) {
    super.initialize(conf, stats, clusterMap);
  }

  /** choose local node of localMachine as the target.
   * if localMachine is not available, choose a node on the same nodegroup or 
   * rack instead.
   * @return the chosen node
   */
  @Override
  protected DatanodeDescriptor chooseLocalNode(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeDescriptor> results, boolean avoidStaleNodes)
        throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null)
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
          blocksize, maxNodesPerRack, results, avoidStaleNodes);

    if (localMachine instanceof DatanodeDescriptor) {
      DatanodeDescriptor localDataNode = (DatanodeDescriptor)localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine)) { // was not in the excluded list
        if (addIfIsGoodTarget(localDataNode, excludedNodes, blocksize,
            maxNodesPerRack, false, results, avoidStaleNodes) >= 0) {
          return localDataNode;
        }
      }
    }

    // try a node on local node group
    DatanodeDescriptor chosenNode = chooseLocalNodeGroup(
        (NetworkTopologyWithNodeGroup)clusterMap, localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results, avoidStaleNodes);
    if (chosenNode != null) {
      return chosenNode;
    }
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results, avoidStaleNodes);
  }

  
  @Override
  protected DatanodeDescriptor chooseLocalRack(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeDescriptor> results, boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results, 
                          avoidStaleNodes);
    }

    // choose one from the local rack, but off-nodegroup
    try {
      return chooseRandom(NetworkTopology.getFirstHalf(
                              localMachine.getNetworkLocation()),
                          excludedNodes, blocksize, 
                          maxNodesPerRack, results, 
                          avoidStaleNodes);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(DatanodeDescriptor nextNode : results) {
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(
              clusterMap.getRack(newLocal.getNetworkLocation()), excludedNodes,
              blocksize, maxNodesPerRack, results, avoidStaleNodes);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
      }
    }
  }

  @Override
  protected void chooseRemoteRack(int numOfReplicas,
      DatanodeDescriptor localMachine, Set<Node> excludedNodes,
      long blocksize, int maxReplicasPerRack, List<DatanodeDescriptor> results,
      boolean avoidStaleNodes) throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();

    final String rackLocation = NetworkTopology.getFirstHalf(
        localMachine.getNetworkLocation());
    try {
      // randomly choose from remote racks
      chooseRandom(numOfReplicas, "~" + rackLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes);
    } catch (NotEnoughReplicasException e) {
      // fall back to the local rack
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas),
          rackLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes);
    }
  }

  /* choose one node from the nodegroup that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the nodegroup where
   * a second replica is on.
   * if still no such node is available, choose a random node in the cluster.
   * @return the chosen node
   */
  private DatanodeDescriptor chooseLocalNodeGroup(
      NetworkTopologyWithNodeGroup clusterMap, Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeDescriptor> results, boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
      blocksize, maxNodesPerRack, results, avoidStaleNodes);
    }

    // choose one from the local node group
    try {
      return chooseRandom(
          clusterMap.getNodeGroup(localMachine.getNetworkLocation()),
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(DatanodeDescriptor nextNode : results) {
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(
              clusterMap.getNodeGroup(newLocal.getNetworkLocation()),
              excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
      }
    }
  }

  @Override
  protected String getRack(final DatanodeInfo cur) {
    String nodeGroupString = cur.getNetworkLocation();
    return NetworkTopology.getFirstHalf(nodeGroupString);
  }
  
  /**
   * Find other nodes in the same nodegroup of <i>localMachine</i> and add them
   * into <i>excludeNodes</i> as replica should not be duplicated for nodes 
   * within the same nodegroup
   * @return number of new excluded nodes
   */
  @Override
  protected int addToExcludedNodes(DatanodeDescriptor chosenNode,
      Set<Node> excludedNodes) {
    int countOfExcludedNodes = 0;
    String nodeGroupScope = chosenNode.getNetworkLocation();
    List<Node> leafNodes = clusterMap.getLeaves(nodeGroupScope);
    for (Node leafNode : leafNodes) {
      if (excludedNodes.add(leafNode)) {
        // not a existing node in excludedNodes
        countOfExcludedNodes++;
      }
    }
    return countOfExcludedNodes;
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * If first is not empty, divide first set into two subsets:
   *   moreThanOne contains nodes on nodegroup with more than one replica
   *   exactlyOne contains the remaining nodes in first set
   * then pickup priSet if not empty.
   * If first is empty, then pick second.
   */
  @Override
  public Collection<DatanodeDescriptor> pickupReplicaSet(
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {
    // If no replica within same rack, return directly.
    if (first.isEmpty()) {
      return second;
    }
    // Split data nodes in the first set into two sets, 
    // moreThanOne contains nodes on nodegroup with more than one replica
    // exactlyOne contains the remaining nodes
    Map<String, List<DatanodeDescriptor>> nodeGroupMap = 
        new HashMap<String, List<DatanodeDescriptor>>();
    
    for(DatanodeDescriptor node : first) {
      final String nodeGroupName = 
          NetworkTopology.getLastHalf(node.getNetworkLocation());
      List<DatanodeDescriptor> datanodeList = 
          nodeGroupMap.get(nodeGroupName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
        nodeGroupMap.put(nodeGroupName, datanodeList);
      }
      datanodeList.add(node);
    }
    
    final List<DatanodeDescriptor> moreThanOne = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> exactlyOne = new ArrayList<DatanodeDescriptor>();
    // split nodes into two sets
    for(List<DatanodeDescriptor> datanodeList : nodeGroupMap.values()) {
      if (datanodeList.size() == 1 ) {
        // exactlyOne contains nodes on nodegroup with exactly one replica
        exactlyOne.add(datanodeList.get(0));
      } else {
        // moreThanOne contains nodes on nodegroup with more than one replica
        moreThanOne.addAll(datanodeList);
      }
    }
    
    return moreThanOne.isEmpty()? exactlyOne : moreThanOne;
  }
  
}
