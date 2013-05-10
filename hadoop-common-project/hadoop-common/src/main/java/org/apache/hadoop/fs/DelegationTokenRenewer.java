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

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * A daemon thread that waits for the next file system to renew.
 */
@InterfaceAudience.Private
public class DelegationTokenRenewer
    extends Thread {
  private static final Log LOG = LogFactory
      .getLog(DelegationTokenRenewer.class);
  /** The renewable interface used by the renewer. */
  public interface Renewable {
    /** @return the renew token. */
    Token<?> getRenewToken();

    /** Set delegation token. */
    <T extends TokenIdentifier> void setDelegationToken(Token<T> token);
  }

  /**
   * Class duplicating (partially) {@link DelayQueue} functionality with the
   * only added method {@link #blockingPeek()}, which is a "non-removing
   * #take()" or "blocking #peek()".
   */
  private static class BlockingPeekDelayQueue<T extends Delayed> 
      implements Iterable<T> {
    
    private final Lock queueLock = new ReentrantLock();
    private final Condition queueContentChangedCondition = queueLock.newCondition();
    private final DelayQueue<T> dq = new DelayQueue<T>(); 
    
    public BlockingPeekDelayQueue() {
    }
    
    /**
     * Blocks the caller thread until an element with the expired delay
     * is available, and returns it, but does not remove it from the queue. 
     * @return the expired element, never null. 
     * @throws InterruptedException if the caller thread is interrupted.
     */
    public T blockingPeek() throws InterruptedException {
      final Lock lock = queueLock;
      lock.lockInterruptibly();
      try {
        while (true) {
          final T first = dq.peek();
          if (first == null) {
            queueContentChangedCondition.await();
          } else {
            long delay = first.getDelay(TimeUnit.NANOSECONDS);
            if (delay > 0) {
              queueContentChangedCondition.awaitNanos(delay);
            } else {
              return first;
            }
          }
        }
      } finally {
        lock.unlock();
      }
    }
    
    public boolean add(T t) {
      Lock lock = queueLock;
      lock.lock();
      try {
        final boolean added = dq.add(t);
        // NB: added is always true, so
        // don't put if() there:
        queueContentChangedCondition.signalAll();
        return added;
      } finally {
        lock.unlock();
      }
    }
    
    public boolean remove(T t) {
      Lock lock = queueLock;
      lock.lock();
      try {
        final boolean removed = dq.remove(t);
        if (removed) {
          queueContentChangedCondition.signalAll();
        }
        return removed;
      } finally {
        lock.unlock();
      }
    }
    
    public T poll() {
      Lock lock = queueLock;
      lock.lock();
      try {
        final T t = dq.poll();
        if (t != null) {
          queueContentChangedCondition.signalAll();
        }
        return t;
      } finally {
        lock.unlock();
      }
    }
    
    @Override
    public Iterator<T> iterator() {
      return dq.iterator();
    }
  }
  
  /**
   * An action that will renew and replace the file system's delegation 
   * tokens automatically.
   */
  private static class RenewAction<T extends FileSystem & Renewable>
      implements Delayed {
    /** when should the renew happen */
    private long renewalTime;
    /** a weak reference to the file system so that it can be garbage collected */
    private final WeakReference<T> weakFs;

    private RenewAction(final T fs) {
      this.weakFs = new WeakReference<T>(fs);
      updateRenewalTime();
    }
 
    /** Get the delay until this event should happen. */
    @Override
    public long getDelay(final TimeUnit unit) {
      final long millisLeft = renewalTime - Time.now();
      return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(final Delayed delayed) {
      final RenewAction<?> that = (RenewAction<?>)delayed;
      return this.renewalTime < that.renewalTime? -1
          : this.renewalTime == that.renewalTime? 0: 1;
    }

    @Override
    public int hashCode() {
      return (int)renewalTime ^ (int)(renewalTime >>> 32);
    }

    @Override
    public boolean equals(final Object that) {
      if (that == null || !(that instanceof RenewAction)) {
        return false;
      }
      return compareTo((Delayed)that) == 0;
    }
    
    /**
     * Set a new time for the renewal.
     * It can only be called when the action is not in the queue.
     * @param newTime the new time
     */
    private void updateRenewalTime() {
      renewalTime = renewCycle + Time.now();
    }

    /**
     * Renew or replace the delegation token for this file system.
     * @return
     * @throws IOException
     */
    private boolean renew() throws IOException, InterruptedException {
      final T fs = weakFs.get();
      final boolean b = (fs != null);
      if (b) {
        synchronized(fs) {
          try {
            fs.getRenewToken().renew(fs.getConf());
          } catch (IOException ie) {
            try {
              Token<?>[] tokens = fs.addDelegationTokens(null, null);
              if (tokens.length == 0) {
                throw new IOException("addDelegationTokens returned no tokens");
              }
              fs.setDelegationToken(tokens[0]);
            } catch (IOException ie2) {
              throw new IOException("Can't renew or get new delegation token ", ie);
            }
          }
        }
      }
      return b;
    }

    @Override
    public String toString() {
      Renewable fs = weakFs.get();
      return fs == null? "evaporated token renew"
          : "The token will be renewed in " + getDelay(TimeUnit.SECONDS)
            + " secs, renewToken=" + fs.getRenewToken();
    }
  }

  /** Wait for 95% of a day between renewals */
  private static final int RENEW_CYCLE = 24 * 60 * 60 * 950; 

  @InterfaceAudience.Private
  protected static long renewCycle = RENEW_CYCLE;

  /** Queue to maintain the RenewActions to be processed by the {@link #run()} */ 
  private final BlockingPeekDelayQueue<RenewAction<?>> queue = new BlockingPeekDelayQueue<RenewAction<?>>();
  private final AtomicBoolean renewerThreadStarted = new AtomicBoolean(false);
  
  /**
   * Create the singleton instance. However, the thread can be started lazily in
   * {@link #addRenewAction(FileSystem)}
   * 
   * The attribute has package visibility for testing purposes only. Normally it 
   * should never be assigned outside of this class.
   */
  @VisibleForTesting
  static DelegationTokenRenewer INSTANCE = null;

  protected DelegationTokenRenewer(final Class<? extends FileSystem> clazz) {
    super(clazz.getSimpleName() + "-" + DelegationTokenRenewer.class.getSimpleName());
    setDaemon(true);
  }
  
  public static synchronized DelegationTokenRenewer getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DelegationTokenRenewer(FileSystem.class);
    }
    return INSTANCE;
  }
  
  /** 
   * Add a renew action to the queue.
   * 
   * Note that the addition is asynchronous.
   * 
   * Note that this method also starts the renewer thread 
   * when the first element is added.
   * 
   * Implementation note: looks like currently this method does not need
   * to be synchronized because there are no restrictions on the element
   * addition.   
   */
  public <T extends FileSystem & Renewable> void addRenewAction(final T fs) {
    // start the thread if not already started:
    if (renewerThreadStarted.compareAndSet(false, true)) {
      start();
    }
    // check if the thread is already dead:
    final State threadState = getState();
    if (threadState == State.TERMINATED) {
      throw new IllegalStateException("Token cannot be added if the Renewer " +
      		"thread has already been terminated."); 
    }
    // now add the renew action to the queue:
    final RenewAction<T> renewAction = new RenewAction<T>(fs); 
    queue.add(renewAction);
  }
  
  /** 
   * Remove the associated renew action from the queue.
   * 
   * Note that only one RenewAction is removed,
   * so, if there are several RenewAction-s associated to the same file-system,
   * only one of them will be removed. 
   * 
   * @throws IOException
   */
  public synchronized <T extends FileSystem & Renewable> boolean removeRenewAction(
      final T fs) throws IOException {
    if (fs == null) {
      throw new NullPointerException("fs");
    }
    for (RenewAction<?> action: queue) {
      if (action.weakFs.get() == fs) {
        final boolean removed = queue.remove(action);
        if (removed) {
          try {
            fs.getRenewToken().cancel(fs.getConf());
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while canceling token for " + fs.getUri()
              + " filesystem");
            if (LOG.isDebugEnabled()) {
              LOG.debug(ie.getStackTrace());
            }
          }
        }
        return removed;
      }
    }
    return false;
  }
  
  @Override
  /*
   * Sync challenge is there: if we took an action and are in progress
   * of renewing it, #removeRenewAction(fs) will not find it in the 
   * queue returning 'false', and after that the action 
   * will be returned to the queue with the updated renew delay.  
   */
  public void run() {
    RenewAction<?> action = null;
    for(;;) {
      try {
        // block here until an expired element appears in the queue,
        // but do not remove it from the queue:
        action = queue.blockingPeek();
        // Now sync to avoid conflicts with #removeRenewAction() method:
        synchronized (this) {
          // #poll() is a non-blocking invocation: immediately removes 
          // and returns an expired element, or 'null' if there is no one:
          action = queue.poll();
          if (action != null) {
            if (action.renew()) {
              // NB: update the delay must be done *only* when the action
              // is not in the queue, because the actions are compared
              // using the delay value
              action.updateRenewalTime();
              queue.add(action); // put it back into the queue
            }
          }
        }
      } catch (InterruptedException ie) {
        return; // the only exit point of this method.
      } catch (Exception e) {
        FileSystem.LOG.warn("Failed to renew token, action=" + action, e);
      }
    }
  }
}
