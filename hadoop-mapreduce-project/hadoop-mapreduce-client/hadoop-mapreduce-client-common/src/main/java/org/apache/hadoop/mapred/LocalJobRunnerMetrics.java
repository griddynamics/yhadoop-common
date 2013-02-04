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
package org.apache.hadoop.mapred;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

class LocalJobRunnerMetrics implements MetricsSource {
  private static final String CONTEXT = "mapred";
  private static final String RECORD_NAME = "jobtracker";

  private static final MetricsInfo mapsLaunchedCountMI = Interns.info("maps_launched", "Maps launched count.");
  private static final MetricsInfo mapsCompletedCountMI = Interns.info("maps_completed", "Maps completed count.");
  
  private static final MetricsInfo reducesLaunchedCountMI = Interns.info("reduces_launched", "Reduces launched count.");
  private static final MetricsInfo reducesCompletedCountMI = Interns.info("reduces_completed", "Reduces completed count.");

  private static final MetricsInfo waitingMapsGaugeMI = Interns.info( "waiting_maps", "Waiting maps gauge.");
  private static final MetricsInfo waitingReducesGaugeMI = Interns.info( "waiting_reduces", "Waiting reduces gauge.");
  
  private final String sessionId;
  private final MetricsSystem metricsSystem;

  private int numMapTasksLaunched = 0;
  private int numMapTasksCompleted = 0;
  private int numReduceTasksLaunched = 0;
  private int numReduceTasksCompleted = 0;
  private int numWaitingMaps = 0;
  private int numWaitingReduces = 0;
  
  @SuppressWarnings("deprecation")
  public LocalJobRunnerMetrics(JobConf conf) {
    sessionId = conf.getSessionId();
    
    metricsSystem = DefaultMetricsSystem.instance();
    DefaultMetricsSystem.setMiniClusterMode(true);
    metricsSystem.init("JobTracker");
    metricsSystem.register(getClass().getName(), 
        "Metrics source for LocalJobRunner.", this);
  }
  
  @Override
  public void getMetrics(final MetricsCollector collector, boolean all) {
    final MetricsRecordBuilder mrb = collector.addRecord(RECORD_NAME);
    mrb.setContext(CONTEXT);
    mrb.tag(MsInfo.SessionId, sessionId);
    synchronized (this) {
      mrb.addGauge(mapsLaunchedCountMI, numMapTasksLaunched);
      mrb.addGauge(mapsCompletedCountMI, numMapTasksCompleted);
      
      mrb.addGauge(reducesLaunchedCountMI, numReduceTasksLaunched);
      mrb.addGauge(reducesCompletedCountMI, numReduceTasksCompleted);
      
      mrb.addGauge(waitingMapsGaugeMI, numWaitingMaps);
      mrb.addGauge(waitingReducesGaugeMI, numWaitingReduces);
      
      mrb.endRecord();
      
      numMapTasksLaunched = 0;
      numMapTasksCompleted = 0;
      numReduceTasksLaunched = 0;
      numReduceTasksCompleted = 0;
      numWaitingMaps = 0;
      numWaitingReduces = 0;
    }
  }

  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksLaunched;
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksCompleted;
  }

  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksLaunched;
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksCompleted;
  }

  private synchronized void decWaitingMaps(JobID id, int task) {
    numWaitingMaps -= task;
  }
  
  private synchronized void decWaitingReduces(JobID id, int task){
    numWaitingReduces -= task;
  }

}
