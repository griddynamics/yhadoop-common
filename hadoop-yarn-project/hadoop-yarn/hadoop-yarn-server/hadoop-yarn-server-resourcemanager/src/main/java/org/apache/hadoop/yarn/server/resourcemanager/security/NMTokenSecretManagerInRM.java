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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;


public class NMTokenSecretManagerInRM extends BaseNMTokenSecretManager {

  private static Log LOG = LogFactory
      .getLog(NMTokenSecretManagerInRM.class);

  private MasterKeyData nextMasterKey;
  private Configuration conf;

  private final Timer timer;
  private final long rollingInterval;
  private final long activationDelay;
  
  public NMTokenSecretManagerInRM(Configuration conf) {
    this.conf = conf;
    timer = new Timer();
    rollingInterval = this.conf.getLong(
        YarnConfiguration.RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS)
        * 1000;
    // Add an activation delay. This is to address the following race: RM may
    // roll over master-key, scheduling may happen at some point of time, an
    // NMToken created with a password generated off new master key, but NM
    // might not have come again to RM to update the shared secret: so AM has a
    // valid password generated off new secret but NM doesn't know about the
    // secret yet.
    // Adding delay = 1.5 * expiry interval makes sure that all active NMs get
    // the updated shared-key.
    this.activationDelay =
        (long) (conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS) * 1.5);
    LOG.info("NMTokenKeyRollingInterval: " + this.rollingInterval
        + "ms and NMTokenKeyActivationDelay: " + this.activationDelay
        + "ms");
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 2 X "
              + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS);
    }
  }
  
  /**
   * Creates a new master-key and sets it as the primary.
   */
  @Private
  public void rollMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Rolling master-key for nm-tokens");
      if (this.currentMasterKey == null) { // Setting up for the first time.
        this.currentMasterKey = createNewMasterKey();
      } else {
        this.nextMasterKey = createNewMasterKey();
        LOG.info("Going to activate master-key with key-id "
            + this.nextMasterKey.getMasterKey().getKeyId() + " in "
            + this.activationDelay + "ms");
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    } finally {
      super.writeLock.unlock();
    }
  }

  @Private
  public MasterKey getNextKey() {
    super.readLock.lock();
    try {
      if (this.nextMasterKey == null) {
        return null;
      } else {
        return this.nextMasterKey.getMasterKey();
      }
    } finally {
      super.readLock.unlock();
    }
  }

  /**
   * Activate the new master-key
   */
  @Private
  public void activateNextMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
    } finally {
      super.writeLock.unlock();
    }
  }

  public void start() {
    rollMasterKey();
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
        rollingInterval);
  }

  public void stop() {
    this.timer.cancel();
  }

  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }
  
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      // Activation will happen after an absolute time interval. It will be good
      // if we can force activation after an NM updates and acknowledges a
      // roll-over. But that is only possible when we move to per-NM keys. TODO:
      activateNextMasterKey();
    }
  }
}
