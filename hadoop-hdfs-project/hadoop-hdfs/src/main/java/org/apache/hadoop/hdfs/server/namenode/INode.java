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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
public abstract class INode implements Comparable<byte[]>, FSInodeInfo {
  /*
   *  The inode name is in java UTF8 encoding; 
   *  The name in HdfsFileStatus should keep the same encoding as this.
   *  if this encoding is changed, implicitly getFileInfo and listStatus in
   *  clientProtocol are changed; The decoding at the client
   *  side should change accordingly.
   */
  protected byte[] name;
  protected INodeDirectory parent;
  protected long modificationTime;
  protected long accessTime;

  /** Simple wrapper for two counters : 
   *  nsCount (namespace consumed) and dsCount (diskspace consumed).
   */
  static class DirCounts {
    long nsCount = 0;
    long dsCount = 0;
    
    /** returns namespace count */
    long getNsCount() {
      return nsCount;
    }
    /** returns diskspace count */
    long getDsCount() {
      return dsCount;
    }
  }
  
  //Only updated by updatePermissionStatus(...).
  //Other codes should not modify it.
  private long permission;

  private static enum PermissionStatusFormat {
    MODE(0, 16),
    GROUP(MODE.OFFSET + MODE.LENGTH, 25),
    USER(GROUP.OFFSET + GROUP.LENGTH, 23);

    final int OFFSET;
    final int LENGTH; //bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }
  }

  protected INode() {
    name = null;
    parent = null;
    modificationTime = 0;
    accessTime = 0;
  }

  INode(PermissionStatus permissions, long mTime, long atime) {
    this.name = null;
    this.parent = null;
    this.modificationTime = mTime;
    setAccessTime(atime);
    setPermissionStatus(permissions);
  }

  protected INode(String name, PermissionStatus permissions) {
    this(permissions, 0L, 0L);
    setLocalName(name);
  }
  
  /** copy constructor
   * 
   * @param other Other node to be copied
   */
  INode(INode other) {
    setLocalName(other.getLocalName());
    this.parent = other.getParent();
    setPermissionStatus(other.getPermissionStatus());
    setModificationTime(other.getModificationTime());
    setAccessTime(other.getAccessTime());
  }

  /**
   * Check whether this is the root inode.
   */
  boolean isRoot() {
    return name.length == 0;
  }

  /** Set the {@link PermissionStatus} */
  protected void setPermissionStatus(PermissionStatus ps) {
    setUser(ps.getUserName());
    setGroup(ps.getGroupName());
    setPermission(ps.getPermission());
  }
  /** Get the {@link PermissionStatus} */
  protected PermissionStatus getPermissionStatus() {
    return new PermissionStatus(getUserName(),getGroupName(),getFsPermission());
  }
  private synchronized void updatePermissionStatus(
      PermissionStatusFormat f, long n) {
    permission = f.combine(n, permission);
  }
  /** Get user name */
  public String getUserName() {
    int n = (int)PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }
  /** Set user */
  protected void setUser(String user) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }
  /** Get group name */
  public String getGroupName() {
    int n = (int)PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }
  /** Set group */
  protected void setGroup(String group) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }
  /** Get the {@link FsPermission} */
  public FsPermission getFsPermission() {
    return new FsPermission(
        (short)PermissionStatusFormat.MODE.retrieve(permission));
  }
  protected short getFsPermissionShort() {
    return (short)PermissionStatusFormat.MODE.retrieve(permission);
  }
  /** Set the {@link FsPermission} of this {@link INode} */
  protected void setPermission(FsPermission permission) {
    updatePermissionStatus(PermissionStatusFormat.MODE, permission.toShort());
  }

  /**
   * Check whether it's a directory
   */
  public abstract boolean isDirectory();

  /**
   * Collect all the blocks in all children of this INode.
   * Count and return the number of files in the sub tree.
   * Also clears references since this INode is deleted.
   */
  abstract int collectSubtreeBlocksAndClear(List<Block> v);

  /** Compute {@link ContentSummary}. Blocking computation. */
  public final ContentSummary computeContentSummary() {
    return computeAndConvertContentSummary(
        new ContentSummaryComputationContext());
  }

  /** Compute {@link ContentSummary} */
  public final ContentSummary computeAndConvertContentSummary(
      ContentSummaryComputationContext summary) {
    long[] a = computeContentSummary(summary).getCounts();
    return new ContentSummary(a[0], a[1], a[2], getNsQuota(), 
                              a[3], getDsQuota());
  }
  /**
   * @return ContentSummaryComputationContext containing
   * content counts.
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract ContentSummaryComputationContext computeContentSummary(
      ContentSummaryComputationContext summary);
  
  /**
   * Get the quota set for this inode
   * @return the quota if it is set; -1 otherwise
   */
  long getNsQuota() {
    return -1;
  }

  long getDsQuota() {
    return -1;
  }
  
  boolean isQuotaSet() {
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }
  
  /**
   * Adds total number of names and total disk space taken under 
   * this tree to counts.
   * Returns updated counts object.
   */
  abstract DirCounts spaceConsumedInTree(DirCounts counts);
  
  /**
   * Get local file name
   * @return local file name
   */
  String getLocalName() {
    return DFSUtil.bytes2String(name);
  }


  String getLocalParentDir() {
    INode inode = isRoot() ? this : getParent();
    String parentDir = "";
    if (inode != null) {
      parentDir = inode.getFullPathName();
    }
    return (parentDir != null) ? parentDir : "";
  }

  /**
   * Get local file name
   * @return local file name
   */
  byte[] getLocalNameBytes() {
    return name;
  }

  /**
   * Set local file name
   */
  void setLocalName(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }

  /**
   * Set local file name
   */
  void setLocalName(byte[] name) {
    this.name = name;
  }

  /** {@inheritDoc} */
  public String getFullPathName() {
    // Get the full path name of this inode.
    return FSDirectory.getFullPathName(this);
  }

  /** {@inheritDoc} */
  public String toString() {
    return "\"" + getFullPathName() + "\":"
    + getUserName() + ":" + getGroupName() + ":"
    + (isDirectory()? "d": "-") + getFsPermission();
  }

  /**
   * Get parent directory 
   * @return parent INode
   */
  INodeDirectory getParent() {
    return this.parent;
  }

  /** 
   * Get last modification time of inode.
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }

  /**
   * Set last modification time of inode.
   */
  void setModificationTime(long modtime) {
    assert isDirectory();
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Always set the last modification time of inode.
   */
  void setModificationTimeForce(long modtime) {
    this.modificationTime = modtime;
  }

  /**
   * Get access time of inode.
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  void setAccessTime(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
    return false;
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isLink() {
    return false;
  }

  /**
   * Breaks file path into components.
   * @param path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /** Convert strings to byte arrays for path components. */
  static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    return bytes;
  }

  /**
   * Splits an absolute path into an array of path components.
   * @param path
   * @throws AssertionError if the given path is invalid.
   * @return array of path components.
   */
  static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  /**
   * Given some components, create a path name.
   * @param components The path components
   * @param start index
   * @param end index
   * @return concatenated path
   */
  static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  boolean removeNode() {
    if (parent == null) {
      return false;
    } else {
      parent.removeChild(this);
      parent = null;
      return true;
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(byte[] o) {
    return compareBytes(name, o);
  }

  public boolean equals(Object o) {
    if (!(o instanceof INode)) {
      return false;
    }
    return Arrays.equals(this.name, ((INode)o).name);
  }

  public int hashCode() {
    return Arrays.hashCode(this.name);
  }

  //
  // static methods
  //
  /**
   * Compare two byte arrays.
   * 
   * @return a negative integer, zero, or a positive integer 
   * as defined by {@link #compareTo(byte[])}.
   */
  static int compareBytes(byte[] a1, byte[] a2) {
    if (a1==a2)
        return 0;
    int len1 = (a1==null ? 0 : a1.length);
    int len2 = (a2==null ? 0 : a2.length);
    int n = Math.min(len1, len2);
    byte b1, b2;
    for (int i=0; i<n; i++) {
      b1 = a1[i];
      b2 = a2[i];
      if (b1 != b2)
        return b1 - b2;
    }
    return len1 - len2;
  }
  
  /**
   * Create an INode; the inode's name is not set yet
   * 
   * @param permissions permissions
   * @param blocks blocks if a file
   * @param symlink symblic link if a symbolic link
   * @param replication replication factor
   * @param modificationTime modification time
   * @param atime access time
   * @param nsQuota namespace quota
   * @param dsQuota disk quota
   * @param preferredBlockSize block size
   * @return an inode
   */
  static INode newINode(PermissionStatus permissions,
                        BlockInfo[] blocks,
                        String symlink,
                        short replication,
                        long modificationTime,
                        long atime,
                        long nsQuota,
                        long dsQuota,
                        long preferredBlockSize) {
    if (symlink.length() != 0) { // check if symbolic link
      return new INodeSymlink(symlink, modificationTime, atime, permissions);
    }  else if (blocks == null) { //not sym link and blocks null? directory!
      if (nsQuota >= 0 || dsQuota >= 0) {
        return new INodeDirectoryWithQuota(
            permissions, modificationTime, nsQuota, dsQuota);
      } 
      // regular directory
      return new INodeDirectory(permissions, modificationTime);
    }
    // file
    return new INodeFile(permissions, blocks, replication,
        modificationTime, atime, preferredBlockSize);
  }
}
