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
package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
//import org.apache.hadoop.metrics.MetricsContext;
//import org.apache.hadoop.metrics.MetricsRecord;
//import org.apache.hadoop.metrics.MetricsUtil;
//import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class ShuffleClientMetrics implements MetricsSource {

  private static final String CONTEXT = "mapred"; 
  private static final String RECORD_NAME = "shuffleInput";
  
  private static final MetricsInfo tagUser = Interns.info("user", "");
  private static final MetricsInfo tagJobName = Interns.info("jobName", "");
  private static final MetricsInfo tagJobId = Interns.info("jobId", "");
  private static final MetricsInfo tagTaskId = Interns.info("taskId", "");
  
  private static final MetricsInfo shuffleInputBytesMI 
    = Interns.info("shuffle_input_bytes", "Shuffle input bytes.");
  private static final MetricsInfo shuffleFailedFetchesMI 
    = Interns.info("shuffle_failed_fetches", "Shuffle failed fetches.");
  private static final MetricsInfo shuffleSuccessFetchesMI 
    = Interns.info("shuffle_success_fetches", "Shuffle success fetches.");
  private static final MetricsInfo shuffleFetchersBusyPercentMI 
    = Interns.info("shuffle_fetchers_busy_percent", "Shuffle fetches busy percent.");
  
  // metrics:
  private int numFailedFetches = 0;
  private int numSuccessFetches = 0;
  private long numBytes = 0;
  private int numThreadsBusy = 0;
  
  private final int numCopiers;
  
  private final TaskAttemptID reduceId;
  private final JobConf jobConf;
  
  ShuffleClientMetrics(TaskAttemptID reduceId0, JobConf jobConf0) {
    reduceId = reduceId0;
    jobConf = jobConf0;
    
    numCopiers = jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
    
    final MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    //DefaultMetricsSystem.setMiniClusterMode(true); // ???
    metricsSystem.init(CONTEXT);
    metricsSystem.register(getClass().getName(), 
        "Metrics source ShuffleClient.", this);
    
  }
  
  public synchronized void inputBytes(long numBytes) {
    this.numBytes += numBytes;
  }
  
  public synchronized void failedFetch() {
    ++numFailedFetches;
  }
  
  public synchronized void successFetch() {
    ++numSuccessFetches;
  }
  
  public synchronized void threadBusy() {
    ++numThreadsBusy;
  }
  
  public synchronized void threadFree() {
    --numThreadsBusy;
  }
  
  @Override
  public void getMetrics(final MetricsCollector collector, boolean all) {
    final MetricsRecordBuilder mrb = collector.addRecord(RECORD_NAME);
    mrb.setContext(CONTEXT);
    
    mrb.tag(tagUser, jobConf.getUser());
    mrb.tag(tagJobName, jobConf.getJobName());
    mrb.tag(tagJobId, reduceId.getJobID().toString());
    mrb.tag(tagTaskId, reduceId.toString());
    mrb.tag(MsInfo.SessionId, jobConf.getSessionId());
    
    synchronized (this) {
      //shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
      mrb.addGauge(shuffleInputBytesMI, numBytes);
      
//      shuffleMetrics.incrMetric("shuffle_failed_fetches", 
//                                numFailedFetches);
      mrb.addGauge(shuffleFailedFetchesMI, numFailedFetches);
      
//      shuffleMetrics.incrMetric("shuffle_success_fetches", 
//                                numSuccessFetches);
      mrb.addGauge(shuffleSuccessFetchesMI, numSuccessFetches);
      
      final float busyPercent;
      if (numCopiers != 0) {
//        shuffleMetrics.setMetric("shuffle_fetchers_busy_percent",
//            100*((float)numThreadsBusy/numCopiers));
        busyPercent = 100*((float)numThreadsBusy/numCopiers);
      } else {
//        shuffleMetrics.setMetric("shuffle_fetchers_busy_percent", 0);
        busyPercent = 0f;
      }
      mrb.addGauge(shuffleFetchersBusyPercentMI, busyPercent);
      mrb.endRecord();
      
      numBytes = 0;
      numSuccessFetches = 0;
      numFailedFetches = 0;
    }
    
  }
}
