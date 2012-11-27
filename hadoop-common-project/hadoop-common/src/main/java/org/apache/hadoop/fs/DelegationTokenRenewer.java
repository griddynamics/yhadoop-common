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
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;

/**
 * A daemon thread that waits for the next file system to renew.
 */
@InterfaceAudience.Private
public class DelegationTokenRenewer
    extends Thread {
  /** The renewable interface used by the renewer. */
  public interface Renewable {
    /** @return the renew token. */
    Token<?> getRenewToken();

    /** Set delegation token. */
    <T extends TokenIdentifier> void setDelegationToken(Token<T> token);
  }

  /**
   * An action that will renew and replace the file system's delegation 
   * tokens automatically.
   */
  private static class RenewAction<T extends FileSystem & Renewable>
      implements Delayed {
    //private final long renewCycleDurationMillis;
    /** when should the renew happen */
    private long renewalTime;
    /** a weak reference to the file system so that it can be garbage collected */
    private final WeakReference<T> weakFs;

    private RenewAction(final T fs) {
    //private RenewAction(long renewCycleMillis, final T fs) {
    //  renewCycleDurationMillis = renewCycleMillis;
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
      final boolean b = fs != null;
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
  private final DelayQueue<RenewAction<?>> queue = new DelayQueue<RenewAction<?>>();
  private boolean started = false;
  
  /**
   * Create the singleton instance. However, the thread can be started lazily in
   * {@link #addRenewAction(FileSystem)}
   * 
   * The attribute has package visibility for testing purposes only. Normally it 
   * should never be assigned outside of this class.
   */
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
  
  /** Add a renew action to the queue. */
  public synchronized <T extends FileSystem & Renewable> void addRenewAction(final T fs) {
    queue.add(new RenewAction<T>(fs));
    if (!started) {
      // NB: !isAlive() should not be used there
      // since isAlive() == false if the thread is already finished.
      started = true;
      start();
    }
  }

  /** Remove the associated renew action from the queue.
   * Note that only one RenewAction is removed,
   * so, if there are several RenewAction-s associated to the same file-system,
   * only one of them will be removed. 
   */
  public synchronized <T extends FileSystem & Renewable> boolean removeRenewAction(
      final T fs) {
    for (RenewAction<?> action: queue) {
      if (action.weakFs.get() == fs) {
        boolean removed = queue.remove(action);
        return removed;
      }
    }
    return false;
  }

  @Override
  public void run() {
    for(;;) {
      RenewAction<?> action = null;
      try {
        synchronized (this) {
          action = queue.take();
          if (action.renew()) {
            action.updateRenewalTime();
            queue.add(action);
          }
        }
      } catch (InterruptedException ie) {
        return;
      } catch (Exception ie) {
        FileSystem.LOG.warn("Failed to renew token, action=" + action,
          ie);
      }
    }
  }
}
