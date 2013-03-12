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
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StateSynchronizer;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import static org.junit.Assert.*;
import static java.lang.System.out;

/**
 * The test checks {@link DelegationTokenRenewer} class functionality. 
 */
public class TestDelegationTokenRenewer {

  /*
   * This is the custom review interval, in milliseconds.
   * This value is used as a time scale of entire the test: all the
   * time periods in the test are defined relatively to it.
   * The value can be adjusted. Lower values speed up the test execution,
   * but likely make it somewhat less reliable.
   */ 
  private static final long renewIntervalMillis = 200L;
  
  final StateSynchronizer<RenewState> sync = new StateSynchronizer<RenewState>();
  
  private enum RenewState {
    created,                      // objects are created.
    renewerThreadStarted,         // method #run() started  
    renewStartAllowed,            // signal to the Token to start update 
                                  // the renew counter
    renewedOneTime,
    renewedThreeTimes,
    fsFinalized,                  // file-system object has been finalized
    tokenRenewalsAreStopped,
    renewerThreadIsAboutToFinish; // corresponds to the end of #run() method.
  }
  
  static class CountingRenewsToken extends Token<TokenIdentifier> {
    private int renewCount;
    private volatile long countLastUpdatedTimeMillis;
    private final StateSynchronizer<RenewState> syncronizer;
    public volatile boolean cancelled = false;
    public CountingRenewsToken(StateSynchronizer<RenewState> s) {
      syncronizer = s;
    }
    @Override
    public long renew(Configuration conf) throws IOException,
        InterruptedException {
      if (renewCount == 0) {
        // before the 1st renew block here 
        // waiting for signal from the main thread:
        syncronizer.waitForState(RenewState.renewStartAllowed);
      }
      renewCount++;
      countLastUpdatedTimeMillis = System.currentTimeMillis();
      if (renewCount == 1) {
        assertTrue(syncronizer.compareAndSetState(
            RenewState.renewStartAllowed, RenewState.renewedOneTime));
      } else if (renewCount == 3) {
        assertTrue(syncronizer.compareAndSetState(
            RenewState.renewedOneTime, RenewState.renewedThreeTimes));
      } 
      // NB: the returned value should not anyhow directly affect the 
      // schedule of the renewals, so we return anything more-or-less reasonable:
      return Time.now();
    }
    long getCountLastUpdatedTimeMillis() {
      return countLastUpdatedTimeMillis;
    }
    @Override
    public void cancel(Configuration conf) {
      cancelled = true;
    }
  }
  
  static class SimpleRenewableFileSystem extends LocalFileSystem
    implements DelegationTokenRenewer.Renewable {
    private final Token<?> token;
    public SimpleRenewableFileSystem(Token<?> t) {
      token = t;
    }
    @Override
    public Token<?> getRenewToken() {
      return token;
    }
    @Override
    public <T extends TokenIdentifier> void setDelegationToken(Token<T> token) {
      // noop
    }
  } 
  
  /*
   * Checks token addition, renewals, and implicit removal when the 
   * corresponding file system gets no longer used.
   */
  @Test(timeout=50000)
  public void testImplicitTokenEvaporation() throws Exception {
    testImpl(true);
  }

  /*
   * Checks token addition, renewals, and explicit removal.
   */
  @Test(timeout=50000)
  public void testExplicitTokenRemoval() throws Exception {
    testImpl(false);
  }
  
  /*
   * This is the test implementation that allows some variance in the
   * behavior: if 'disposeFs' is true, the renewing is terminated with
   * loosing reference to the file system object. Otherwise, the renewing process
   * id terminated by removing the token with    
   */
  private void testImpl(final boolean disposeFs) throws Exception {
    // set up a custom renew interval:
    DelegationTokenRenewer.renewCycle = renewIntervalMillis;
    
    final DelegationTokenRenewer dtr = 
        new DelegationTokenRenewer(SimpleRenewableFileSystem.class) {
      @Override
      public void run() {
        sync.waitForStateAndSet(RenewState.created, RenewState.renewerThreadStarted);
        try {
          super.run();
        } finally {
          sync.setState(RenewState.renewerThreadIsAboutToFinish);
        }
      }
    };
    // set the singleton instance to the custom object: 
    DelegationTokenRenewer.INSTANCE = dtr;

    assertTrue(sync.compareAndSetState(null, RenewState.created));

    final DelegationTokenRenewer renewerSingleton = DelegationTokenRenewer.getInstance();
    assertTrue(renewerSingleton == dtr);
    
    try {
      final CountingRenewsToken countingRenewsToken 
        = new CountingRenewsToken(sync);
      final SimpleRenewableFileSystem[] simpleRenewableFileSystemContainer = 
          new SimpleRenewableFileSystem[] { null };
      simpleRenewableFileSystemContainer[0] 
        = new SimpleRenewableFileSystem(countingRenewsToken) {
        @Override
        protected void finalize() throws Throwable {
          sync.setState(RenewState.fsFinalized);
          super.finalize();
        }
      };
      Configuration conf = new Configuration();
      Path path = new Path("file:///foo/moo/zoo");
      simpleRenewableFileSystemContainer[0].initialize(path.toUri(), conf);
      
      // This will cause transition to the state "renewActionAdded": 
      renewerSingleton.addRenewAction(simpleRenewableFileSystemContainer[0]);
      
      sync.waitForState(RenewState.renewerThreadStarted);
      
      // signal using transition to "renewStartAllowed" and
      // wait for further transitions to happen:
      assertTrue(sync.compareAndSetAndWaitForStateSequence(
          RenewState.renewerThreadStarted, 
          RenewState.renewStartAllowed, 
            Arrays.asList(
              new RenewState[] { 
                RenewState.renewedOneTime, 
                RenewState.renewedThreeTimes })));

      // start a dedicated thread to see if renews are still happening
      // with expected interval:
      final Thread lastRenewAgeCheckerThread = new Thread( new Runnable(){
        @Override
        public void run() {
          long countAge;
          while (true) {
            try { 
              Thread.sleep(renewIntervalMillis / 20);
            } catch (InterruptedException ie) {
              ie.printStackTrace();
              throw new RuntimeException(ie);
            }
            countAge = System.currentTimeMillis() 
                - countingRenewsToken.getCountLastUpdatedTimeMillis();
            if (countAge > 5 * renewIntervalMillis) {
              // No token update in 5 renew periods - 
              // looks like the token updates are stopped:
              sync.setState(RenewState.tokenRenewalsAreStopped);
              break;
            }
          }      
        }
      });
      lastRenewAgeCheckerThread.start();
      
      // check that the renewal attempts are still happening: 
      assertFalse(sync.waitForState(RenewState.tokenRenewalsAreStopped, 
          10 * renewIntervalMillis));
      if (disposeFs) {
        assertTrue(sync.invokeAndWaitForStateSequence(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // null the file-system reference causing the updates to stop:
            simpleRenewableFileSystemContainer[0] = null;
            System.gc();
            return null;
          }
        }, Arrays.asList(new RenewState[] { 
          // wait while fs object GC-ed:
          RenewState.fsFinalized,
          // after that the token renewals should be stopped:
          RenewState.tokenRenewalsAreStopped }),
          30 * renewIntervalMillis/* timeout for each state*/));
      } else {
        assertTrue(sync.invokeAndWaitForStateSequence(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // remove the token for this FS:
            boolean rm = renewerSingleton.removeRenewAction(simpleRenewableFileSystemContainer[0]);
            out.println("Action removed: " + rm);
            assertTrue(rm);
            // check that the token is cancelled:
            assertTrue("The token is not cancelled.", 
                countingRenewsToken.cancelled);
            return null;
          }
        },
        // after that the token renewals should be stopped:
        Collections.singletonList(RenewState.tokenRenewalsAreStopped), 
        30 * renewIntervalMillis));
      }
      // join renew checker thread:
      lastRenewAgeCheckerThread.join(30 * renewIntervalMillis);
      assertTrue(!lastRenewAgeCheckerThread.isAlive());
      
      assertTrue(sync.invokeAndWaitForStateSequence(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // now stop the token renewal thread (there is no other way to do that):
            out.println("Interrupting the renewer thread.");
            renewerSingleton.interrupt();
            return null;
          }
        }, 
        Collections.singletonList(RenewState.renewerThreadIsAboutToFinish), 
          30 * renewIntervalMillis));
      
      // wait for the renewer thread to finish:
      renewerSingleton.join(30 * renewIntervalMillis);
      assertTrue(!renewerSingleton.isAlive());
      // try to add an action 
      final SimpleRenewableFileSystem fs2 = 
          new SimpleRenewableFileSystem(new Token<TokenIdentifier>());
      try {
        renewerSingleton.addRenewAction(fs2);
        assertTrue("IllegalStateException expected upon a token addition " +
        		"if the renewer is already terminated.", false);
      } catch (IllegalStateException ise) {
        // okay
      }
    } catch (Exception e) {
      e.printStackTrace(out);
      throw e;
    } finally {
      // must guarantee to join the daemon thread 
      // to avoid its influence to other tests:
      if (renewerSingleton.isAlive()) {
        renewerSingleton.interrupt();
        renewerSingleton.join(30 * renewIntervalMillis);
        assertTrue(!renewerSingleton.isAlive());
      }
    }
  }
}
