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

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class represents the primary identifier for a Datanode.
 * Datanodes are identified by how they can be contacted (hostname
 * and ports) and their storage ID, a unique number that associates
 * the Datanodes blocks with a particular Datanode.
 *
 * {@link DatanodeInfo#getName()} should be used to get the network
 * location (for topology) of a datanode, instead of using
 * {@link DatanodeID#getXferAddr()} here. Helpers are defined below
 * for each context in which a DatanodeID is used.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeID implements Comparable<DatanodeID> {
  public static final DatanodeID[] EMPTY_ARRAY = {};

  private String ipAddr;     // IP address
  private String hostName;   // hostname claimed by datanode
  private String peerHostName; // hostname from the actual connection
  private String storageID;  // unique per cluster storageID
  private int xferPort;      // data streaming port
  private int infoPort;      // info server port
  private int ipcPort;       // IPC server port

  public DatanodeID(DatanodeID from) {
    this(from.getIpAddr(),
        from.getHostName(),
        from.getStorageID(),
        from.getXferPort(),
        from.getInfoPort(),
        from.getIpcPort());
    this.peerHostName = from.getPeerHostName();
  }
  
  /**
   * Create a DatanodeID
   * @param ipAddr IP
   * @param hostName hostname
   * @param storageID data storage ID
   * @param xferPort data transfer port
   * @param infoPort info server port 
   * @param ipcPort ipc server port
   */
  public DatanodeID(String ipAddr, String hostName, String storageID,
      int xferPort, int infoPort, int ipcPort) {
    this.ipAddr = ipAddr;
    this.hostName = hostName;
    this.storageID = storageID;
    this.xferPort = xferPort;
    this.infoPort = infoPort;
    this.ipcPort = ipcPort;
  }
  
  public void setIpAddr(String ipAddr) {
    this.ipAddr = ipAddr;
  }

  public void setPeerHostName(String peerHostName) {
    this.peerHostName = peerHostName;
  }
  
  public void setStorageID(String storageID) {
    this.storageID = storageID;
  }

  /**
   * @return ipAddr;
   */
  public String getIpAddr() {
    return ipAddr;
  }

  /**
   * @return hostname
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * @return hostname from the actual connection 
   */
  public String getPeerHostName() {
    return peerHostName;
  }
  
  /**
   * @return IP:xferPort string
   */
  public String getXferAddr() {
    return ipAddr + ":" + xferPort;
  }

  /**
   * @return IP:ipcPort string
   */
  private String getIpcAddr() {
    return ipAddr + ":" + ipcPort;
  }

  /**
   * @return IP:infoPort string
   */
  public String getInfoAddr() {
    return ipAddr + ":" + infoPort;
  }

  /**
   * @return hostname:xferPort
   */
  public String getXferAddrWithHostname() {
    return hostName + ":" + xferPort;
  }

  /**
   * @return hostname:ipcPort
   */
  private String getIpcAddrWithHostname() {
    return hostName + ":" + ipcPort;
  }

  /**
   * @param useHostname true to use the DN hostname, use the IP otherwise
   * @return name:xferPort
   */
  public String getXferAddr(boolean useHostname) {
    return useHostname ? getXferAddrWithHostname() : getXferAddr();
  }

  /**
   * @param useHostname true to use the DN hostname, use the IP otherwise
   * @return name:ipcPort
   */
  public String getIpcAddr(boolean useHostname) {
    return useHostname ? getIpcAddrWithHostname() : getIpcAddr();
  }

  /**
   * @return data storage ID.
   */
  public String getStorageID() {
    return storageID;
  }

  /**
   * @return xferPort (the port for data streaming)
   */
  public int getXferPort() {
    return xferPort;
  }

  /**
   * @return infoPort (the port at which the HTTP server bound to)
   */
  public int getInfoPort() {
    return infoPort;
  }

  /**
   * @return ipcPort (the port at which the IPC server bound to)
   */
  public int getIpcPort() {
    return ipcPort;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    }
    if (!(to instanceof DatanodeID)) {
      return false;
    }
    return (getXferAddr().equals(((DatanodeID)to).getXferAddr()) &&
            storageID.equals(((DatanodeID)to).getStorageID()));
  }
  
  @Override
  public int hashCode() {
    return getXferAddr().hashCode()^ storageID.hashCode();
  }
  
  @Override
  public String toString() {
    return getXferAddr();
  }
  
  /**
   * Update fields when a new registration request comes in.
   * Note that this does not update storageID.
   */
  public void updateRegInfo(DatanodeID nodeReg) {
    ipAddr = nodeReg.getIpAddr();
    hostName = nodeReg.getHostName();
    peerHostName = nodeReg.getPeerHostName();
    xferPort = nodeReg.getXferPort();
    infoPort = nodeReg.getInfoPort();
    ipcPort = nodeReg.getIpcPort();
  }
    
  /**
   * Compare based on data transfer address.
   *
   * @param that
   * @return as specified by Comparable
   */
  @Override
  public int compareTo(DatanodeID that) {
    return getXferAddr().compareTo(that.getXferAddr());
  }
}
