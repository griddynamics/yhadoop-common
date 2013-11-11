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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * Manages caching for an FsDatasetImpl by using the mmap(2) and mlock(2)
 * system calls to lock blocks into memory. Block checksums are verified upon
 * entry into the cache.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsDatasetCache {
  /**
   * Keys which identify MappableBlocks.
   */
  private static final class Key {
    /**
     * Block id.
     */
    final long id;

    /**
     * Block pool id.
     */
    final String bpid;

    Key(long id, String bpid) {
      this.id = id;
      this.bpid = bpid;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      if (!(o.getClass() == getClass())) {
        return false;
      }
      Key other = (Key)o;
      return ((other.id == this.id) && (other.bpid.equals(this.bpid)));
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(id).append(bpid).hashCode();
    }
  };

  /**
   * MappableBlocks that we know about.
   */
  private static final class Value {
    final State state;
    final MappableBlock mappableBlock;

    Value(MappableBlock mappableBlock, State state) {
      this.mappableBlock = mappableBlock;
      this.state = state;
    }
  }

  private enum State {
    /**
     * The MappableBlock is in the process of being cached.
     */
    CACHING,

    /**
     * The MappableBlock was in the process of being cached, but it was
     * cancelled.  Only the FsDatasetCache#WorkerTask can remove cancelled
     * MappableBlock objects.
     */
    CACHING_CANCELLED,

    /**
     * The MappableBlock is in the cache.
     */
    CACHED,

    /**
     * The MappableBlock is in the process of uncaching.
     */
    UNCACHING;

    /**
     * Whether we should advertise this block as cached to the NameNode and
     * clients.
     */
    public boolean shouldAdvertise() {
      return (this == CACHED);
    }
  }

  private static final Log LOG = LogFactory.getLog(FsDatasetCache.class);

  /**
   * Stores MappableBlock objects and the states they're in.
   */
  private final HashMap<Key, Value> mappableBlockMap = new HashMap<Key, Value>();

  private final FsDatasetImpl dataset;

  private final ThreadPoolExecutor uncachingExecutor;

  /**
   * The approximate amount of cache space in use.
   *
   * This number is an overestimate, counting bytes that will be used only
   * if pending caching operations succeed.  It does not take into account
   * pending uncaching operations.
   *
   * This overestimate is more useful to the NameNode than an underestimate,
   * since we don't want the NameNode to assign us more replicas than
   * we can cache, because of the current batch of operations.
   */
  private final UsedBytesCount usedBytesCount;

  public static class PageRounder {
    private final long osPageSize = NativeIO.getOperatingSystemPageSize();

    /**
     * Round up a number to the operating system page size.
     */
    public long round(long count) {
      long newCount = 
          (count + (osPageSize - 1)) / osPageSize;
      return newCount * osPageSize;
    }
  }

  private class UsedBytesCount {
    private final AtomicLong usedBytes = new AtomicLong(0);
    
    private PageRounder rounder = new PageRounder();

    /**
     * Try to reserve more bytes.
     *
     * @param count    The number of bytes to add.  We will round this
     *                 up to the page size.
     *
     * @return         The new number of usedBytes if we succeeded;
     *                 -1 if we failed.
     */
    long reserve(long count) {
      count = rounder.round(count);
      while (true) {
        long cur = usedBytes.get();
        long next = cur + count;
        if (next > maxBytes) {
          return -1;
        }
        if (usedBytes.compareAndSet(cur, next)) {
          return next;
        }
      }
    }
    
    /**
     * Release some bytes that we're using.
     *
     * @param count    The number of bytes to release.  We will round this
     *                 up to the page size.
     *
     * @return         The new number of usedBytes.
     */
    long release(long count) {
      count = rounder.round(count);
      return usedBytes.addAndGet(-count);
    }
    
    long get() {
      return usedBytes.get();
    }
  }

  /**
   * The total cache capacity in bytes.
   */
  private final long maxBytes;

  /**
   * Number of cache commands that could not be completed successfully
   */
  AtomicLong numBlocksFailedToCache = new AtomicLong(0);
  /**
   * Number of uncache commands that could not be completed successfully
   */
  AtomicLong numBlocksFailedToUncache = new AtomicLong(0);

  public FsDatasetCache(FsDatasetImpl dataset) {
    this.dataset = dataset;
    this.maxBytes = dataset.datanode.getDnConf().getMaxLockedMemory();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsDatasetCache-%d-" + dataset.toString())
        .build();
    this.usedBytesCount = new UsedBytesCount();
    this.uncachingExecutor = new ThreadPoolExecutor(
            0, 1,
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            workerFactory);
    this.uncachingExecutor.allowCoreThreadTimeOut(true);
  }

  /**
   * @return List of cached blocks suitable for translation into a
   * {@link BlockListAsLongs} for a cache report.
   */
  synchronized List<Long> getCachedBlocks(String bpid) {
    List<Long> blocks = new ArrayList<Long>();
    for (Iterator<Entry<Key, Value>> iter =
        mappableBlockMap.entrySet().iterator(); iter.hasNext(); ) {
      Entry<Key, Value> entry = iter.next();
      if (entry.getKey().bpid.equals(bpid)) {
        if (entry.getValue().state.shouldAdvertise()) {
          blocks.add(entry.getKey().id);
        }
      }
    }
    return blocks;
  }

  /**
   * Attempt to begin caching a block.
   */
  synchronized void cacheBlock(long blockId, String bpid,
      String blockFileName, long length, long genstamp,
      Executor volumeExecutor) {
    Key key = new Key(blockId, bpid);
    Value prevValue = mappableBlockMap.get(key);
    if (prevValue != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Block with id " + blockId + ", pool " + bpid +
            " already exists in the FsDatasetCache with state " +
            prevValue.state);
      }
      numBlocksFailedToCache.incrementAndGet();
      return;
    }
    mappableBlockMap.put(key, new Value(null, State.CACHING));
    volumeExecutor.execute(
        new CachingTask(key, blockFileName, length, genstamp));
  }

  synchronized void uncacheBlock(String bpid, long blockId) {
    Key key = new Key(blockId, bpid);
    Value prevValue = mappableBlockMap.get(key);

    if (prevValue == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Block with id " + blockId + ", pool " + bpid + " " +
            "does not need to be uncached, because it is not currently " +
            "in the mappableBlockMap.");
      }
      numBlocksFailedToUncache.incrementAndGet();
      return;
    }
    switch (prevValue.state) {
    case CACHING:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cancelling caching for block with id " + blockId +
            ", pool " + bpid + ".");
      }
      mappableBlockMap.put(key,
          new Value(prevValue.mappableBlock, State.CACHING_CANCELLED));
      break;
    case CACHED:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Block with id " + blockId + ", pool " + bpid + " " +
            "has been scheduled for uncaching.");
      }
      mappableBlockMap.put(key,
          new Value(prevValue.mappableBlock, State.UNCACHING));
      uncachingExecutor.execute(new UncachingTask(key));
      break;
    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Block with id " + blockId + ", pool " + bpid + " " +
            "does not need to be uncached, because it is " +
            "in state " + prevValue.state + ".");
      }
      numBlocksFailedToUncache.incrementAndGet();
      break;
    }
  }

  /**
   * Background worker that mmaps, mlocks, and checksums a block
   */
  private class CachingTask implements Runnable {
    private final Key key; 
    private final String blockFileName;
    private final long length;
    private final long genstamp;

    CachingTask(Key key, String blockFileName, long length, long genstamp) {
      this.key = key;
      this.blockFileName = blockFileName;
      this.length = length;
      this.genstamp = genstamp;
    }

    @Override
    public void run() {
      boolean success = false;
      FileInputStream blockIn = null, metaIn = null;
      MappableBlock mappableBlock = null;
      ExtendedBlock extBlk =
          new ExtendedBlock(key.bpid, key.id, length, genstamp);
      long newUsedBytes = usedBytesCount.reserve(length);
      if (newUsedBytes < 0) {
        LOG.warn("Failed to cache block id " + key.id + ", pool " + key.bpid +
            ": could not reserve " + length + " more bytes in the " +
            "cache: " + DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY +
            " of " + maxBytes + " exceeded.");
        numBlocksFailedToCache.incrementAndGet();
        return;
      }
      try {
        try {
          blockIn = (FileInputStream)dataset.getBlockInputStream(extBlk, 0);
          metaIn = (FileInputStream)dataset.getMetaDataInputStream(extBlk)
              .getWrappedStream();
        } catch (ClassCastException e) {
          LOG.warn("Failed to cache block with id " + key.id + ", pool " +
                key.bpid + ": Underlying blocks are not backed by files.", e);
          return;
        } catch (FileNotFoundException e) {
          LOG.info("Failed to cache block with id " + key.id + ", pool " +
                key.bpid + ": failed to find backing files.");
          return;
        } catch (IOException e) {
          LOG.warn("Failed to cache block with id " + key.id + ", pool " +
                key.bpid + ": failed to open file", e);
          return;
        }
        try {
          mappableBlock = MappableBlock.
              load(length, blockIn, metaIn, blockFileName);
        } catch (ChecksumException e) {
          // Exception message is bogus since this wasn't caused by a file read
          LOG.warn("Failed to cache block " + key.id + " in " + key.bpid + ": " +
                   "checksum verification failed.");
          return;
        } catch (IOException e) {
          LOG.warn("Failed to cache block " + key.id + " in " + key.bpid, e);
          return;
        }
        synchronized (FsDatasetCache.this) {
          Value value = mappableBlockMap.get(key);
          Preconditions.checkNotNull(value);
          Preconditions.checkState(value.state == State.CACHING ||
                                   value.state == State.CACHING_CANCELLED);
          if (value.state == State.CACHING_CANCELLED) {
            mappableBlockMap.remove(key);
            LOG.warn("Caching of block " + key.id + " in " + key.bpid +
                " was cancelled.");
            return;
          }
          mappableBlockMap.put(key, new Value(mappableBlock, State.CACHED));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully cached block " + key.id + " in " + key.bpid +
              ".  We are now caching " + newUsedBytes + " bytes in total.");
        }
        success = true;
      } finally {
        if (!success) {
          newUsedBytes = usedBytesCount.release(length);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Caching of block " + key.id + " in " +
              key.bpid + " was aborted.  We are now caching only " +
              newUsedBytes + " + bytes in total.");
          }
          IOUtils.closeQuietly(blockIn);
          IOUtils.closeQuietly(metaIn);
          if (mappableBlock != null) {
            mappableBlock.close();
          }
          numBlocksFailedToCache.incrementAndGet();
        }
      }
    }
  }

  private class UncachingTask implements Runnable {
    private final Key key; 

    UncachingTask(Key key) {
      this.key = key;
    }

    @Override
    public void run() {
      Value value;
      
      synchronized (FsDatasetCache.this) {
        value = mappableBlockMap.get(key);
      }
      Preconditions.checkNotNull(value);
      Preconditions.checkArgument(value.state == State.UNCACHING);
      // TODO: we will eventually need to do revocation here if any clients
      // are reading via mmap with checksums enabled.  See HDFS-5182.
      IOUtils.closeQuietly(value.mappableBlock);
      synchronized (FsDatasetCache.this) {
        mappableBlockMap.remove(key);
      }
      long newUsedBytes =
          usedBytesCount.release(value.mappableBlock.getLength());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Uncaching of block " + key.id + " in " + key.bpid +
            " completed.  usedBytes = " + newUsedBytes);
      }
    }
  }

  // Stats related methods for FSDatasetMBean

  /**
   * Get the approximate amount of cache space used.
   */
  public long getDnCacheUsed() {
    return usedBytesCount.get();
  }

  /**
   * Get the maximum amount of bytes we can cache.  This is a constant.
   */
  public long getDnCacheCapacity() {
    return maxBytes;
  }

  public long getNumBlocksFailedToCache() {
    return numBlocksFailedToCache.get();
  }

  public long getNumBlocksFailedToUncache() {
    return numBlocksFailedToUncache.get();
  }

}
