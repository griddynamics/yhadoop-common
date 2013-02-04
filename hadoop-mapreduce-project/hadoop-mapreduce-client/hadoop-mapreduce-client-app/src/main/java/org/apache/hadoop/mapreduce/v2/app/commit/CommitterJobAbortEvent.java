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

package org.apache.hadoop.mapreduce.v2.app.commit;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class CommitterJobAbortEvent extends CommitterEvent {

  private JobId jobID;
  private JobContext jobContext;
  private JobStatus.State finalState;

  public CommitterJobAbortEvent(JobId jobID, JobContext jobContext,
      JobStatus.State finalState) {
    super(CommitterEventType.JOB_ABORT);
    this.jobID = jobID;
    this.jobContext = jobContext;
    this.finalState = finalState;
  }

  public JobId getJobID() {
    return jobID;
  }

  public JobContext getJobContext() {
    return jobContext;
  }

  public JobStatus.State getFinalState() {
    return finalState;
  }
}
