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

package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSLinkResolver;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
@InterfaceAudience.LimitedPrivate({ "MapReduce", "HBase" })
@InterfaceStability.Unstable
public class DistributedFileSystem extends FileSystem {
  private Path workingDir;
  private URI uri;

  DFSClient dfs;
  private boolean verifyChecksum = true;
  
  static{
    HdfsConfiguration.init();
  }

  public DistributedFileSystem() {
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>hdfs</code>
   */
  @Override
  public String getScheme() {
    return HdfsConstants.HDFS_URI_SCHEME;
  }

  @Deprecated
  public DistributedFileSystem(InetSocketAddress namenode,
    Configuration conf) throws IOException {
    initialize(NameNode.getUri(namenode), conf);
  }

  @Override
  public URI getUri() { return uri; }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: "+ uri);
    }

    this.dfs = new DFSClient(uri, conf, statistics);
    this.uri = URI.create(uri.getScheme()+"://"+uri.getAuthority());
    this.workingDir = getHomeDirectory();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public long getDefaultBlockSize() {
    return dfs.getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication() {
    return dfs.getDefaultReplication();
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    String result = fixRelativePart(dir).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = fixRelativePart(dir);
  }

  
  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path("/user/" + dfs.ugi.getShortUserName()));
  }

  /**
   * Checks that the passed URI belongs to this filesystem and returns
   * just the path component. Expects a URI with an absolute path.
   * 
   * @param file URI with absolute path
   * @return path component of {file}
   * @throws IllegalArgumentException if URI does not belong to this DFS
   */
  private String getPathName(Path file) {
    checkPath(file);
    String result = file.toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
                                         file+" is not a valid DFS filename.");
    }
    return result;
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }
    return getFileBlockLocations(file.getPath(), start, len);
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(Path p, 
      final long start, final long len) throws IOException {
    statistics.incrementReadOps(1);
    final Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<BlockLocation[]>() {
      @Override
      public BlockLocation[] doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.getBlockLocations(getPathName(p), start, len);
      }
      @Override
      public BlockLocation[] next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileBlockLocations(p, start, len);
      }
    }.resolve(this, absF);
  }

  /**
   * Used to query storage location information for a list of blocks. This list
   * of blocks is normally constructed via a series of calls to
   * {@link DistributedFileSystem#getFileBlockLocations(Path, long, long)} to
   * get the blocks for ranges of a file.
   * 
   * The returned array of {@link BlockStorageLocation} augments
   * {@link BlockLocation} with a {@link VolumeId} per block replica. The
   * VolumeId specifies the volume on the datanode on which the replica resides.
   * The VolumeId has to be checked via {@link VolumeId#isValid()} before being
   * used because volume information can be unavailable if the corresponding
   * datanode is down or if the requested block is not found.
   * 
   * This API is unstable, and datanode-side support is disabled by default. It
   * can be enabled by setting "dfs.datanode.hdfs-blocks-metadata.enabled" to
   * true.
   * 
   * @param blocks
   *          List of target BlockLocations to query volume location information
   * @return volumeBlockLocations Augmented array of
   *         {@link BlockStorageLocation}s containing additional volume location
   *         information for each replica of each block.
   */
  @InterfaceStability.Unstable
  public BlockStorageLocation[] getFileBlockStorageLocations(
      List<BlockLocation> blocks) throws IOException, 
      UnsupportedOperationException, InvalidBlockTokenException {
    return dfs.getBlockStorageLocations(blocks);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  /** 
   * Start the lease recovery of a file
   *
   * @param f a file
   * @return true if the file is already closed
   * @throws IOException if an error occurs
   */
  public boolean recoverLease(final Path f) throws IOException {
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.recoverLease(getPathName(p));
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.recoverLease(p);
        }
        throw new UnsupportedOperationException("Cannot recoverLease through" +
            " a symlink to a non-DistributedFileSystem: " + f + " -> " + p);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataInputStream open(Path f, final int bufferSize)
      throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataInputStream>() {
      @Override
      public FSDataInputStream doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return new HdfsDataInputStream(
            dfs.open(getPathName(p), bufferSize, verifyChecksum));
      }
      @Override
      public FSDataInputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.open(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataOutputStream append(Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.append(getPathName(p), bufferSize, progress, statistics);
      }
      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.append(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return this.create(f, permission,
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
            : EnumSet.of(CreateFlag.CREATE), bufferSize, replication,
        blockSize, progress, null);
  }

  /**
   * Same as  
   * {@link #create(Path, FsPermission, boolean, int, short, long, 
   * Progressable)} with the addition of favoredNodes that is a hint to 
   * where the namenode should place the file blocks.
   * The favored nodes hint is not persisted in HDFS. Hence it may be honored
   * at the creation time only. HDFS could move the blocks during balancing or
   * replication, to move the blocks from favored nodes. A value of null means
   * no favored nodes for this create
   */
  public HdfsDataOutputStream create(final Path f,
      final FsPermission permission, final boolean overwrite,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final InetSocketAddress[] favoredNodes)
          throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<HdfsDataOutputStream>() {
      @Override
      public HdfsDataOutputStream doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        final DFSOutputStream out = dfs.create(getPathName(f), permission,
            overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
                : EnumSet.of(CreateFlag.CREATE),
            true, replication, blockSize, progress, bufferSize, null,
            favoredNodes);
        return new HdfsDataOutputStream(out, statistics);
      }
      @Override
      public HdfsDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.create(p, permission, overwrite, bufferSize, replication,
              blockSize, progress, favoredNodes);
        }
        throw new UnsupportedOperationException("Cannot create with" +
            " favoredNodes through a symlink to a non-DistributedFileSystem: "
            + f + " -> " + p);
      }
    }.resolve(this, absF);
  }
  
  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
    final EnumSet<CreateFlag> cflags, final int bufferSize,
    final short replication, final long blockSize, final Progressable progress,
    final ChecksumOpt checksumOpt) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return new HdfsDataOutputStream(dfs.create(getPathName(p), permission,
            cflags, replication, blockSize, progress, bufferSize, checksumOpt),
            statistics);
      }
      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.create(p, permission, cflags, bufferSize,
            replication, blockSize, progress, checksumOpt);
      }
    }.resolve(this, absF);
  }

  @Override
  protected HdfsDataOutputStream primitiveCreate(Path f,
    FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
    short replication, long blockSize, Progressable progress,
    ChecksumOpt checksumOpt) throws IOException {
    statistics.incrementWriteOps(1);
    return new HdfsDataOutputStream(dfs.primitiveCreate(
        getPathName(fixRelativePart(f)),
        absolutePermission, flag, true, replication, blockSize,
        progress, bufferSize, checksumOpt),statistics);
   }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   */
  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    if (flag.contains(CreateFlag.OVERWRITE)) {
      flag.add(CreateFlag.CREATE);
    }
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        return new HdfsDataOutputStream(dfs.create(getPathName(p), permission,
            flag, false, replication, blockSize, progress, bufferSize, null),
            statistics);
      }

      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.createNonRecursive(p, permission, flag, bufferSize,
            replication, blockSize, progress);
      }
    }.resolve(this, absF);
  }

  @Override
  public boolean setReplication(Path src, 
                                final short replication
                               ) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(src);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.setReplication(getPathName(p), replication);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.setReplication(p, replication);
      }
    }.resolve(this, absF);
  }
  
  /**
   * Move blocks from srcs to trg and delete srcs afterwards.
   * The file block sizes must be the same.
   * 
   * @param trg existing file to append to
   * @param psrcs list of files (same block size, same replication)
   * @throws IOException
   */
  @Override
  public void concat(Path trg, Path [] psrcs) throws IOException {
    statistics.incrementWriteOps(1);
    // Make target absolute
    Path absF = fixRelativePart(trg);
    // Make all srcs absolute
    Path[] srcs = new Path[psrcs.length];
    for (int i=0; i<psrcs.length; i++) {
      srcs[i] = fixRelativePart(psrcs[i]);
    }
    // Try the concat without resolving any links
    String[] srcsStr = new String[psrcs.length];
    try {
      for (int i=0; i<psrcs.length; i++) {
        srcsStr[i] = getPathName(srcs[i]);
      }
      dfs.concat(getPathName(trg), srcsStr);
    } catch (UnresolvedLinkException e) {
      // Exception could be from trg or any src.
      // Fully resolve trg and srcs. Fail if any of them are a symlink.
      FileStatus stat = getFileLinkStatus(absF);
      if (stat.isSymlink()) {
        throw new IOException("Cannot concat with a symlink target: "
            + trg + " -> " + stat.getPath());
      }
      absF = fixRelativePart(stat.getPath());
      for (int i=0; i<psrcs.length; i++) {
        stat = getFileLinkStatus(srcs[i]);
        if (stat.isSymlink()) {
          throw new IOException("Cannot concat with a symlink src: "
              + psrcs[i] + " -> " + stat.getPath());
        }
        srcs[i] = fixRelativePart(stat.getPath());
      }
      // Try concat again. Can still race with another symlink.
      for (int i=0; i<psrcs.length; i++) {
        srcsStr[i] = getPathName(srcs[i]);
      }
      dfs.concat(getPathName(absF), srcsStr);
    }
  }

  
  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    statistics.incrementWriteOps(1);

    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);

    // Try the rename without resolving first
    try {
      return dfs.rename(getPathName(absSrc), getPathName(absDst));
    } catch (UnresolvedLinkException e) {
      // Fully resolve the source
      final Path source = getFileLinkStatus(absSrc).getPath();
      // Keep trying to resolve the destination
      return new FileSystemLinkResolver<Boolean>() {
        @Override
        public Boolean doCall(final Path p)
            throws IOException, UnresolvedLinkException {
          return dfs.rename(getPathName(source), getPathName(p));
        }
        @Override
        public Boolean next(final FileSystem fs, final Path p)
            throws IOException {
          // Should just throw an error in FileSystem#checkPath
          return doCall(p);
        }
      }.resolve(this, absDst);
    }
  }

  /** 
   * This rename operation is guaranteed to be atomic.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, final Options.Rename... options)
      throws IOException {
    statistics.incrementWriteOps(1);
    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);
    // Try the rename without resolving first
    try {
      dfs.rename(getPathName(absSrc), getPathName(absDst), options);
    } catch (UnresolvedLinkException e) {
      // Fully resolve the source
      final Path source = getFileLinkStatus(absSrc).getPath();
      // Keep trying to resolve the destination
      new FileSystemLinkResolver<Void>() {
        @Override
        public Void doCall(final Path p)
            throws IOException, UnresolvedLinkException {
          dfs.rename(getPathName(source), getPathName(p), options);
          return null;
        }
        @Override
        public Void next(final FileSystem fs, final Path p)
            throws IOException {
          // Should just throw an error in FileSystem#checkPath
          return doCall(p);
        }
      }.resolve(this, absDst);
    }
  }
  
  @Override
  public boolean delete(Path f, final boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.delete(getPathName(p), recursive);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.delete(p, recursive);
      }
    }.resolve(this, absF);
  }
  
  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<ContentSummary>() {
      @Override
      public ContentSummary doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.getContentSummary(getPathName(p));
      }
      @Override
      public ContentSummary next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getContentSummary(p);
      }
    }.resolve(this, absF);
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long) 
   */
  public void setQuota(Path src, final long namespaceQuota,
      final long diskspaceQuota) throws IOException {
    Path absF = fixRelativePart(src);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.setQuota(getPathName(p), namespaceQuota, diskspaceQuota);
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        // setQuota is not defined in FileSystem, so we only can resolve
        // within this DFS
        return doCall(p);
      }
    }.resolve(this, absF);
  }

  private FileStatus[] listStatusInternal(Path p) throws IOException {
    String src = getPathName(p);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = dfs.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);

    if (thisListing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    
    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = partialListing[i].makeQualified(getUri(), p);
      }
      statistics.incrementReadOps(1);
      return stats;
    }

    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries =
      partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing =
      new ArrayList<FileStatus>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(fileStatus.makeQualified(getUri(), p));
    }
    statistics.incrementLargeReadOps(1);
 
    // now fetch more entries
    do {
      thisListing = dfs.listPaths(src, thisListing.getLastName());
 
      if (thisListing == null) { // the directory is deleted
        throw new FileNotFoundException("File " + p + " does not exist.");
      }
 
      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(fileStatus.makeQualified(getUri(), p));
      }
      statistics.incrementLargeReadOps(1);
    } while (thisListing.hasMore());
 
    return listing.toArray(new FileStatus[listing.size()]);
  }

  /**
   * List all the entries of a directory
   *
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<FileStatus[]>() {
      @Override
      public FileStatus[] doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return listStatusInternal(p);
      }
      @Override
      public FileStatus[] next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.listStatus(p);
      }
    }.resolve(this, absF);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter)
  throws IOException {
    final Path absF = fixRelativePart(p);
    return new RemoteIterator<LocatedFileStatus>() {
      private DirectoryListing thisListing;
      private int i;
      private String src;
      private LocatedFileStatus curStat = null;

      { // initializer
        // Fully resolve symlinks in path first to avoid additional resolution
        // round-trips as we fetch more batches of listings
        src = getPathName(resolvePath(absF));
        // fetch the first batch of entries in the directory
        thisListing = dfs.listPaths(src, HdfsFileStatus.EMPTY_NAME, true);
        statistics.incrementReadOps(1);
        if (thisListing == null) { // the directory does not exist
          throw new FileNotFoundException("File " + p + " does not exist.");
        }
      }

      @Override
      public boolean hasNext() throws IOException {
        while (curStat == null && hasNextNoFilter()) {
          LocatedFileStatus next = 
              ((HdfsLocatedFileStatus)thisListing.getPartialListing()[i++])
              .makeQualifiedLocated(getUri(), absF);
          if (filter.accept(next.getPath())) {
            curStat = next;
          }
        }
        return curStat != null;
      }
      
      /** Check if there is a next item before applying the given filter */
      private boolean hasNextNoFilter() throws IOException {
        if (thisListing == null) {
          return false;
        }
        if (i>=thisListing.getPartialListing().length
            && thisListing.hasMore()) { 
          // current listing is exhausted & fetch a new listing
          thisListing = dfs.listPaths(src, thisListing.getLastName(), true);
          statistics.incrementReadOps(1);
          if (thisListing == null) {
            return false;
          }
          i = 0;
        }
        return (i<thisListing.getPartialListing().length);
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          LocatedFileStatus tmp = curStat;
          curStat = null;
          return tmp;
        } 
        throw new java.util.NoSuchElementException("No more entry in " + p);
      }
    };
  }
  
  /**
   * Create a directory, only when the parent directories exist.
   *
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f           The path to create
   * @param permission  The permission.  See FsPermission#applyUMask for 
   *                    details about how this is used to calculate the
   *                    effective permission.
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    return mkdirsInternal(f, permission, false);
  }

  /**
   * Create a directory and its parent directories.
   *
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f           The path to create
   * @param permission  The permission.  See FsPermission#applyUMask for 
   *                    details about how this is used to calculate the
   *                    effective permission.
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mkdirsInternal(f, permission, true);
  }

  private boolean mkdirsInternal(Path f, final FsPermission permission,
      final boolean createParent) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.mkdirs(getPathName(p), permission, createParent);
      }

      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        // FileSystem doesn't have a non-recursive mkdir() method
        // Best we can do is error out
        if (!createParent) {
          throw new IOException("FileSystem does not support non-recursive"
              + "mkdir");
        }
        return fs.mkdirs(p, permission);
      }
    }.resolve(this, absF);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
    throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.primitiveMkdir(getPathName(f), absolutePermission);
  }

 
  @Override
  public void close() throws IOException {
    try {
      dfs.closeOutputStreams(false);
      super.close();
    } finally {
      dfs.close();
    }
  }

  @Override
  public String toString() {
    return "DFS[" + dfs + "]";
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public DFSClient getClient() {
    return dfs;
  }        
  
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getDiskStatus();
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   * 
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  /**
   * Returns count of blocks with one of more replica missing.
   * 
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    return dfs.getUnderReplicatedBlocksCount();
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   * 
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    return new CorruptFileBlockIterator(dfs, path);
  }

  /** @return datanode statistics. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return getDataNodeStats(DatanodeReportType.ALL);
  }

  /** @return datanode statistics for the given type. */
  public DatanodeInfo[] getDataNodeStats(final DatanodeReportType type
      ) throws IOException {
    return dfs.datanodeReport(type);
  }

  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    HdfsConstants.SafeModeAction,boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action) 
  throws IOException {
    return setSafeMode(action, false);
  }

  /**
   * Enter, leave or get safe mode.
   * 
   * @param action
   *          One of SafeModeAction.ENTER, SafeModeAction.LEAVE and
   *          SafeModeAction.GET
   * @param isChecked
   *          If true check only for Active NNs status, else check first NN's
   *          status
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(SafeModeAction, boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    return dfs.setSafeMode(action, isChecked);
  }

  /**
   * Save namespace image.
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public void saveNamespace() throws AccessControlException, IOException {
    dfs.saveNamespace();
  }
  
  /**
   * Rolls the edit log on the active NameNode.
   * Requires super-user privileges.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#rollEdits()
   * @return the transaction ID of the newly created segment
   */
  public long rollEdits() throws AccessControlException, IOException {
    return dfs.rollEdits();
  }

  /**
   * enable/disable/check restoreFaileStorage
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#restoreFailedStorage(String arg)
   */
  public boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException {
    return dfs.restoreFailedStorage(arg);
  }
  

  /**
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    dfs.refreshNodes();
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    dfs.finalizeUpgrade();
  }

  /*
   * Requests the namenode to dump data strcutures into specified 
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    dfs.metaSave(pathname);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults();
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  // We do not see a need for user to report block checksum errors and do not  
  // want to rely on user to report block corruptions.
  @Deprecated
  public boolean reportChecksumFailure(Path f, 
    FSDataInputStream in, long inPos, 
    FSDataInputStream sums, long sumsPos) {
    
    if(!(in instanceof HdfsDataInputStream && sums instanceof HdfsDataInputStream))
      throw new IllegalArgumentException(
          "Input streams must be types of HdfsDataInputStream");
    
    LocatedBlock lblocks[] = new LocatedBlock[2];

    // Find block in data stream.
    HdfsDataInputStream dfsIn = (HdfsDataInputStream) in;
    ExtendedBlock dataBlock = dfsIn.getCurrentBlock();
    if (dataBlock == null) {
      LOG.error("Error: Current block in data stream is null! ");
      return false;
    }
    DatanodeInfo[] dataNode = {dfsIn.getCurrentDatanode()}; 
    lblocks[0] = new LocatedBlock(dataBlock, dataNode);
    LOG.info("Found checksum error in data stream at "
        + dataBlock + " on datanode="
        + dataNode[0]);

    // Find block in checksum stream
    HdfsDataInputStream dfsSums = (HdfsDataInputStream) sums;
    ExtendedBlock sumsBlock = dfsSums.getCurrentBlock();
    if (sumsBlock == null) {
      LOG.error("Error: Current block in checksum stream is null! ");
      return false;
    }
    DatanodeInfo[] sumsNode = {dfsSums.getCurrentDatanode()}; 
    lblocks[1] = new LocatedBlock(sumsBlock, sumsNode);
    LOG.info("Found checksum error in checksum stream at "
        + sumsBlock + " on datanode=" + sumsNode[0]);

    // Ask client to delete blocks.
    dfs.reportChecksumFailure(f.toString(), lblocks);

    return true;
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileStatus>() {
      @Override
      public FileStatus doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        HdfsFileStatus fi = dfs.getFileInfo(getPathName(p));
        if (fi != null) {
          return fi.makeQualified(getUri(), p);
        } else {
          throw new FileNotFoundException("File does not exist: " + p);
        }
      }
      @Override
      public FileStatus next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileStatus(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException {
    statistics.incrementWriteOps(1);
    final Path absF = fixRelativePart(link);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        dfs.createSymlink(target.toString(), getPathName(p), createParent);
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException, UnresolvedLinkException {
        fs.createSymlink(target, p, createParent);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    statistics.incrementReadOps(1);
    final Path absF = fixRelativePart(f);
    FileStatus status = new FileSystemLinkResolver<FileStatus>() {
      @Override
      public FileStatus doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        HdfsFileStatus fi = dfs.getFileLinkInfo(getPathName(p));
        if (fi != null) {
          return fi.makeQualified(getUri(), p);
        } else {
          throw new FileNotFoundException("File does not exist: " + p);
        }
      }
      @Override
      public FileStatus next(final FileSystem fs, final Path p)
        throws IOException, UnresolvedLinkException {
        return fs.getFileLinkStatus(p);
      }
    }.resolve(this, absF);
    // Fully-qualify the symlink
    if (status.isSymlink()) {
      Path targetQual = FSLinkResolver.qualifySymlinkTarget(this.getUri(),
          status.getPath(), status.getSymlink());
      status.setSymlink(targetQual);
    }
    return status;
  }

  @Override
  public Path getLinkTarget(final Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    statistics.incrementReadOps(1);
    final Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Path>() {
      @Override
      public Path doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        HdfsFileStatus fi = dfs.getFileLinkInfo(getPathName(p));
        if (fi != null) {
          return fi.makeQualified(getUri(), p).getSymlink();
        } else {
          throw new FileNotFoundException("File does not exist: " + p);
        }
      }
      @Override
      public Path next(final FileSystem fs, final Path p)
        throws IOException, UnresolvedLinkException {
        return fs.getLinkTarget(p);
      }
    }.resolve(this, absF);
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    statistics.incrementReadOps(1);
    String target = dfs.getLinkTarget(getPathName(fixRelativePart(f)));
    if (target == null) {
      throw new FileNotFoundException("File does not exist: " + f.toString());
    }
    return new Path(target);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileChecksum>() {
      @Override
      public FileChecksum doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.getFileChecksum(getPathName(p));
      }

      @Override
      public FileChecksum next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileChecksum(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public void setPermission(Path p, final FsPermission permission
      ) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.setPermission(getPathName(p), permission);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setPermission(p, permission);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void setOwner(Path p, final String username, final String groupname
      ) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.setOwner(getPathName(p), username, groupname);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setOwner(p, username, groupname);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void setTimes(Path p, final long mtime, final long atime
      ) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.setTimes(getPathName(p), mtime, atime);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setTimes(p, mtime, atime);
        return null;
      }
    }.resolve(this, absF);
  }
  

  @Override
  protected int getDefaultPort() {
    return NameNode.DEFAULT_PORT;
  }

  @Override
  public 
  Token<DelegationTokenIdentifier> getDelegationToken(String renewer
  ) throws IOException {
    Token<DelegationTokenIdentifier> result =
      dfs.getDelegationToken(renewer == null ? null : new Text(renewer));
    return result;
  }

  /*
   * Delegation Token Operations
   * These are DFS only operations.
   */
  
  /**
   * Get a valid Delegation Token.
   * 
   * @param renewer Name of the designated renewer for the token
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   * @deprecated use {@link #getDelegationToken(String)}
   */
  @Deprecated
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return getDelegationToken(renewer.toString());
  }
  
  /**
   * Renew an existing delegation token.
   * 
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   * @deprecated Use Token.renew instead.
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
      return token.renew(getConf());
    } catch (InterruptedException ie) {
      throw new RuntimeException("Caught interrupted", ie);
    }
  }

  /**
   * Cancel an existing delegation token.
   * 
   * @param token delegation token
   * @throws IOException
   * @deprecated Use Token.cancel instead.
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    try {
      token.cancel(getConf());
    } catch (InterruptedException ie) {
      throw new RuntimeException("Caught interrupted", ie);
    }
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.balance.bandwidthPerSec.
   * The bandwidth parameter is the max number of bytes per second of network
   * bandwidth to be used by a datanode during balancing.
   *
   * @param bandwidth Balancer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    dfs.setBalancerBandwidth(bandwidth);
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    return dfs.getCanonicalServiceName();
  }
  
  @Override
  protected URI canonicalizeUri(URI uri) {
    if (HAUtil.isLogicalUri(getConf(), uri)) {
      // Don't try to DNS-resolve logical URIs, since the 'authority'
      // portion isn't a proper hostname
      return uri;
    } else {
      return NetUtils.getCanonicalUri(uri, getDefaultPort());
    }
  }

  /**
   * Utility function that returns if the NameNode is in safemode or not. In HA
   * mode, this API will return only ActiveNN's safemode status.
   * 
   * @return true if NameNode is in safemode, false otherwise.
   * @throws IOException
   *           when there is an issue communicating with the NameNode
   */
  public boolean isInSafeMode() throws IOException {
    return setSafeMode(SafeModeAction.SAFEMODE_GET, true);
  }

  /** @see HdfsAdmin#allowSnapshot(Path) */
  public void allowSnapshot(final Path path) throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.allowSnapshot(getPathName(p));
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.allowSnapshot(p);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }
  
  /** @see HdfsAdmin#disallowSnapshot(Path) */
  public void disallowSnapshot(final Path path) throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.disallowSnapshot(getPathName(p));
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.disallowSnapshot(p);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }
  
  @Override
  public Path createSnapshot(final Path path, final String snapshotName) 
      throws IOException {
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<Path>() {
      @Override
      public Path doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return new Path(dfs.createSnapshot(getPathName(p), snapshotName));
      }

      @Override
      public Path next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.createSnapshot(p);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }
  
  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.renameSnapshot(getPathName(p), snapshotOldName, snapshotNewName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.renameSnapshot(p, snapshotOldName, snapshotNewName);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }
  
  /**
   * @return All the snapshottable directories
   * @throws IOException
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    return dfs.getSnapshottableDirListing();
  }
  
  @Override
  public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
      throws IOException {
    Path absF = fixRelativePart(snapshotDir);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        dfs.deleteSnapshot(getPathName(p), snapshotName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.deleteSnapshot(p, snapshotName);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + snapshotDir + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Get the difference between two snapshots, or between a snapshot and the
   * current tree of a directory.
   * 
   * @see DFSClient#getSnapshotDiffReport(Path, String, String)
   */
  public SnapshotDiffReport getSnapshotDiffReport(final Path snapshotDir,
      final String fromSnapshot, final String toSnapshot) throws IOException {
    Path absF = fixRelativePart(snapshotDir);
    return new FileSystemLinkResolver<SnapshotDiffReport>() {
      @Override
      public SnapshotDiffReport doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.getSnapshotDiffReport(getPathName(p), fromSnapshot,
            toSnapshot);
      }

      @Override
      public SnapshotDiffReport next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.getSnapshotDiffReport(p, fromSnapshot, toSnapshot);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + snapshotDir + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }
 
  /**
   * Get the close status of a file
   * @param src The path to the file
   *
   * @return return true if file is closed
   * @throws FileNotFoundException if the file does not exist.
   * @throws IOException If an I/O error occurred     
   */
  public boolean isFileClosed(final Path src) throws IOException {
    Path absF = fixRelativePart(src);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return dfs.isFileClosed(getPathName(p));
      }

      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.isFileClosed(p);
        } else {
          throw new UnsupportedOperationException("Cannot call isFileClosed"
              + " on a symlink to a non-DistributedFileSystem: "
              + src + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  /**
   * Add a new PathBasedCacheDirective.
   * 
   * @param directive A directive to add.
   * @return the ID of the directive that was created.
   * @throws IOException if the directive could not be added
   */
  public long addPathBasedCacheDirective(
      PathBasedCacheDirective directive) throws IOException {
    Preconditions.checkNotNull(directive.getPath());
    Path path = new Path(getPathName(fixRelativePart(directive.getPath()))).
        makeQualified(getUri(), getWorkingDirectory());
    return dfs.addPathBasedCacheDirective(
        new PathBasedCacheDirective.Builder(directive).
            setPath(path).
            build());
  }
  
  public void modifyPathBasedCacheDirective(
      PathBasedCacheDirective directive) throws IOException {
    if (directive.getPath() != null) {
      directive = new PathBasedCacheDirective.Builder(directive).
          setPath(new Path(getPathName(fixRelativePart(directive.getPath()))).
              makeQualified(getUri(), getWorkingDirectory())).build();
    }
    dfs.modifyPathBasedCacheDirective(directive);
  }

  /**
   * Remove a PathBasedCacheDirective.
   * 
   * @param id identifier of the PathBasedCacheDirective to remove
   * @throws IOException if the directive could not be removed
   */
  public void removePathBasedCacheDirective(long id)
      throws IOException {
    dfs.removePathBasedCacheDirective(id);
  }
  
  /**
   * List the set of cached paths of a cache pool. Incrementally fetches results
   * from the server.
   * 
   * @param filter Filter parameters to use when listing the directives, null to
   *               list all directives visible to us.
   * @return A RemoteIterator which returns PathBasedCacheDirective objects.
   */
  public RemoteIterator<PathBasedCacheDirective> listPathBasedCacheDirectives(
      PathBasedCacheDirective filter) throws IOException {
    if (filter == null) {
      filter = new PathBasedCacheDirective.Builder().build();
    }
    if (filter.getPath() != null) {
      filter = new PathBasedCacheDirective.Builder(filter).
          setPath(new Path(getPathName(fixRelativePart(filter.getPath())))).
          build();
    }
    final RemoteIterator<PathBasedCacheDirective> iter =
        dfs.listPathBasedCacheDirectives(filter);
    return new RemoteIterator<PathBasedCacheDirective>() {
      @Override
      public boolean hasNext() throws IOException {
        return iter.hasNext();
      }

      @Override
      public PathBasedCacheDirective next() throws IOException {
        // Although the paths we get back from the NameNode should always be
        // absolute, we call makeQualified to add the scheme and authority of
        // this DistributedFilesystem.
        PathBasedCacheDirective desc = iter.next();
        Path p = desc.getPath().makeQualified(getUri(), getWorkingDirectory());
        return new PathBasedCacheDirective.Builder(desc).setPath(p).build();
      }
    };
  }

  /**
   * Add a cache pool.
   *
   * @param info
   *          The request to add a cache pool.
   * @throws IOException 
   *          If the request could not be completed.
   */
  public void addCachePool(CachePoolInfo info) throws IOException {
    CachePoolInfo.validate(info);
    dfs.addCachePool(info);
  }

  /**
   * Modify an existing cache pool.
   *
   * @param info
   *          The request to modify a cache pool.
   * @throws IOException 
   *          If the request could not be completed.
   */
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    CachePoolInfo.validate(info);
    dfs.modifyCachePool(info);
  }
    
  /**
   * Remove a cache pool.
   *
   * @param poolName
   *          Name of the cache pool to remove.
   * @throws IOException 
   *          if the cache pool did not exist, or could not be removed.
   */
  public void removeCachePool(String poolName) throws IOException {
    CachePoolInfo.validateName(poolName);
    dfs.removeCachePool(poolName);
  }

  /**
   * List all cache pools.
   *
   * @return A remote iterator from which you can get CachePoolInfo objects.
   *          Requests will be made as needed.
   * @throws IOException
   *          If there was an error listing cache pools.
   */
  public RemoteIterator<CachePoolInfo> listCachePools() throws IOException {
    return dfs.listCachePools();
  }
}
