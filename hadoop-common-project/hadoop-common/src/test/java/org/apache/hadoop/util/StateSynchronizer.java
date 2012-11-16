/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static java.lang.System.out;

/**
 * An utility to be used in tests that allows 
 * to reliably synchronize various threads when each of them
 * achieves certain state.
 * The emum of the possible states is to be set as the class parameter.
 * The initial state can be set in the constructor. If not set, the 
 * synchronizer initialized with null state.
 * 
 * This implementation ignores {@link InterruptedException}s: if a thread 
 * blocked in any method of this class, it will not return from there if 
 * that thread is {@link Thread#interrupt()}-ed: the blockage will continue
 * until the waiting condition is met.
 */
public class StateSynchronizer<T extends Enum<?>> {
  
  private final AtomicReference<T> state;
  private final Lock lock = new ReentrantLock(true);
  private final Condition stateChangedCondition = lock.newCondition();
  private final boolean logTransitions; 
  
  /**
   * Makes a new sync with the given (possibly null) initial state. 
   * @param initialState the initial state
   */
  public StateSynchronizer(T initialState, boolean logTransitions0) {
    state = new AtomicReference<T>(initialState);
    logTransitions = logTransitions0;
  }
  
  /**
   * makes a new sync with null initial state
   */
  public StateSynchronizer() {
    this (null, true);
  }
  
  /*
   * Prints the state transition to console.
   * Is to be invoked *only* when the lock is held. 
   */
  private void logTransitionIf(T from, T to) {
    if (logTransitions) {
      String syncName = null;
      if (from != null) {
        syncName = from.getClass().getSimpleName();  
      } else if (to != null) {
        syncName = to.getClass().getSimpleName();
      } 
      out.println(syncName + ": ["+from+"] -> ["+to+"]");
    }
  }
  
  /**
   * Sets the newState only if the current one equals to the expected.
   * @param expected the expected state.
   * @param newState the new state.
   * @return if the new state was set.
   */
  public boolean compareAndSetState(T expected, T newState) {
    lock.lock();
    try {
      boolean changed = state.compareAndSet(expected, newState);
      if (changed) {
        logTransitionIf(expected, newState);
        stateChangedCondition.signalAll();
      } 
      return changed;
    } finally {
      lock.unlock();
    }
  } 
  
  /**
   * Unconditionally, but still synchronously, 
   * makes a transition to another state.
   * Returns the previous state.
   * @param newState
   * @return the previous state
   */
  public T setState(T newState) {
    lock.lock();
    try {
      final T oldState = state.getAndSet(newState);
      if (oldState != newState) {
        logTransitionIf(oldState, newState);
        stateChangedCondition.signalAll();
      }
      return oldState;
    } finally {
      lock.unlock();
    }
  } 

  /**
   * Asynchronously gets the current state of the synchronizer.
   * Does never block.
   * ! Use for diagnostic purposes only ! 
   * @return
   */
  public T getState() {
    return state.get();
  }  
  
  /**
   * Blocks until the state of the synchronizer gets 'expectedState'. 
   * @param stateId
   */
  public void waitForState(T expectedState) {
    lock.lock();
    try {
      while (state.get() != expectedState) {
        // NB: ignore interrupts
        stateChangedCondition.awaitUninterruptibly();
      }
    } finally {
      lock.unlock();
    }
  }  

  /**
   * Blocks until the state of the synchronizer gets 'expectedState'. 
   * When done, changes the state to 'newState'. 
   * @param expectedState the state to wait for.
   * @param the new state to set afterwards.
   */
  public void waitForStateAndSet(T expectedState, T newState) {
    lock.lock();
    try {
      while (state.get() != expectedState) {
        // NB: ignore interrupts
        stateChangedCondition.awaitUninterruptibly();
      }
      T oldState = state.getAndSet(newState);
      logTransitionIf(oldState, newState);
      if (oldState != newState) {
        stateChangedCondition.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }  
  
  /**
   * Sets a new state if the current state is the expected one, then 
   * sequentially waits for each state in the specified chain to happen.
   * Note that any Collection or an ad-hoc {@link Iterable} 
   * can be used as the 3rd parameter. 
   * @param expectedStatesAfter an {@link Iterable} of expected statuses.
   */
  public boolean compareAndSetAndWaitForStateSequence(T expected, T newState, Iterable<T> expectedStatesAfter) {
    lock.lock();
    try {
      boolean changed = state.compareAndSet(expected, newState);
      if (changed) {
        logTransitionIf(expected, newState);
        stateChangedCondition.signalAll();
      } else {
        return false; // starting transition failed.
      }
      // Now sequentially wait for each state in the chain:
      for (T expectedState: expectedStatesAfter) {
        while (state.get() != expectedState) {
          // NB: ignore interrupts
          stateChangedCondition.awaitUninterruptibly();
        }
      }
      return true;
    } finally {
      lock.unlock();
    }
  }  
  
  
  /**
   * Blocks no longer than 'timeoutMillis' until the new state is achieved.  
   * @param stateId
   * @param timeoutMillis
   * @return true if the expected state was achieved. false otherwise.
   */
  public boolean waitForState(T expectedState, long timeoutMillis) {
    final long finish = System.currentTimeMillis() + timeoutMillis;
    lock.lock();
    try {
      while (true) {
        if (state.get() == expectedState) {
          return true;
        }
        long toWait = finish - System.currentTimeMillis();
        if (toWait > 0) {
          try {
            if (!stateChangedCondition.await(toWait, TimeUnit.MILLISECONDS)) {
              return false; // timeout.
            }
          } catch (InterruptedException ie) {
            // ignore it.
          } 
        } else {
          return false; // timeout.
        }
      }
    } finally {
      lock.unlock();
    }
  }  
  
}