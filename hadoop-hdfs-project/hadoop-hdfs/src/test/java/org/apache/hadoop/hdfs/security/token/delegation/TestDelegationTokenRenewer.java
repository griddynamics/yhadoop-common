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
package org.apache.hadoop.hdfs.security.token.delegation;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StateSynchronizer;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * The test checks {@link DelegationTokenRenewer} class functionality. 
 */
public class TestDelegationTokenRenewer {

  private static final long renewIntervalMillis = 1000L; 
  
  static final StateSynchronizer<RenewState> sync = new StateSynchronizer<RenewState>();
  
  private enum RenewState {
    created,                      // objects are created.
    renewerThreadStarted,         // method #run() started  
    renewActionAdded,
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
    @Override
    public long renew(Configuration conf) throws IOException,
        InterruptedException {
      if (renewCount == 0) {
        // before the 1st renew block here 
        // waiting for signal from the main thread:
        sync.waitForState(RenewState.renewStartAllowed);
      }
      renewCount++;
      countLastUpdatedTimeMillis = System.currentTimeMillis();
      if (renewCount == 1) {
        assertTrue(sync.compareAndSetState(
            RenewState.renewStartAllowed, RenewState.renewedOneTime));
      } else if (renewCount == 3) {
        assertTrue(sync.compareAndSetState(
            RenewState.renewedOneTime, RenewState.renewedThreeTimes));
      } 
      // NB: the returned value should not anyhow directly affect the 
      // schedule of the renewals, so we return anything more-or-less reasonable:
      return System.currentTimeMillis();
    }
    long getCountLastUpdatedTimeMillis() {
      return countLastUpdatedTimeMillis;
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
  
  @Test
  public void testDelegationTokenRenewer() throws Exception {
    
    final DelegationTokenRenewer<SimpleRenewableFileSystem> dtr = 
        new DelegationTokenRenewer<SimpleRenewableFileSystem>(SimpleRenewableFileSystem.class) {
      @Override
      protected long getRenewCycleDurationMillis() {
        // non-default interval suitable for test:
        return renewIntervalMillis;
      }
      @Override
      public void addRenewAction(SimpleRenewableFileSystem fs) {
        super.addRenewAction(fs);
        assertTrue(sync.compareAndSetState(RenewState.renewerThreadStarted, RenewState.renewActionAdded));
      }
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

    assertTrue(sync.compareAndSetState(null, RenewState.created));
    
    // Let's follow the production usage pattern -- the DelegationTokenRenewer 
    // thread is normally started in the very beginning:
    dtr.start();
    
    try {
      final CountingRenewsToken countingRenewsToken 
        = new CountingRenewsToken();
      SimpleRenewableFileSystem simpleRenewableFileSystem 
        = new SimpleRenewableFileSystem(countingRenewsToken) {
        @Override
        protected void finalize() throws Throwable {
          sync.setState(RenewState.fsFinalized);
          super.finalize();
        }
      };
      Configuration conf = new Configuration();
      Path path = new Path("file:///foo/moo/zoo"); 
      simpleRenewableFileSystem.initialize(path.toUri(), conf);

      // ensure the thread is running now:
      sync.waitForState(RenewState.renewerThreadStarted);
      
      // this will cause transition to the state "renewActionAdded": 
      dtr.addRenewAction(simpleRenewableFileSystem);
      
      // signal using transition to "renewStartAllowed" and
      // wait for further transitions to happen:
      assertTrue(sync.compareAndSetAndWaitForStateSequence(
          RenewState.renewActionAdded, 
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
              Thread.sleep(500L);
            } catch (InterruptedException ie) {
              ie.printStackTrace();
              throw new RuntimeException(ie);
            }
            countAge = System.currentTimeMillis() 
                - countingRenewsToken.getCountLastUpdatedTimeMillis();
            if (countAge > 5000L) {
              // No token update in 5s: looks like the token updates are stopped:
              sync.setState(RenewState.tokenRenewalsAreStopped);
              break;
            }
          }      
        }
      });
      lastRenewAgeCheckerThread.start();
      
      // check that the renewal attempts are still happening: 
      assertFalse(sync.waitForState(RenewState.tokenRenewalsAreStopped, 10 * 1000L));

      // now null the file-system reference causing the updates to stop:
      simpleRenewableFileSystem = null;
      System.gc();

      // wait while fs object GC-ed:
      sync.waitForState(RenewState.fsFinalized);

      // check that the token renewals are stopped:
      assertTrue(sync.waitForState(RenewState.tokenRenewalsAreStopped, 30 * 1000L));
      lastRenewAgeCheckerThread.join();
      
      // now stop the token renewal thread (there is no other way to do that): 
      dtr.interrupt();
      
      // now the thread should finish, so wait for that:
      sync.waitForState(RenewState.renewerThreadIsAboutToFinish);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      // must guarantee to join the daemon thread 
      // to avoid its influence to other tests:
      if (dtr.isAlive()) {
        dtr.interrupt();
        dtr.join();
      }
    }
  }
  
}
