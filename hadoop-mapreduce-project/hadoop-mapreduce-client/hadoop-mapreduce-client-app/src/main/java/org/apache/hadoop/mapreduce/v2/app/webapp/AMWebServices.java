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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.BlacklistedNodesInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Inject;

@Path("/ws/v1/mapreduce")
public class AMWebServices {
  private final AppContext appCtx;
  private final App app;

  private @Context HttpServletResponse response;
  
  @Inject
  public AMWebServices(final App app, final AppContext context) {
    this.appCtx = context;
    this.app = app;
  }

  Boolean hasAccess(Job job, HttpServletRequest request) {
    String remoteUser = request.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null && !job.checkAccess(callerUGI, JobACL.VIEW_JOB)) {
      return false;
    }
    return true;
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  /**
   * convert a job id string to an actual job and handle all the error checking.
   */
 public static Job getJobFromJobIdString(String jid, AppContext appCtx) throws NotFoundException {
    JobId jobId;
    Job job;
    try {
      jobId = MRApps.toJobID(jid);
    } catch (YarnRuntimeException e) {
      // TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
      // anymore but keeping it for now just in case other stuff starts failing.
      // Also, the webservice should ideally return BadRequest (HTTP:400) when
      // the id is malformed instead of NotFound (HTTP:404). The webserver on
      // top of which AMWebServices is built seems to automatically do that for
      // unhandled exceptions
      throw new NotFoundException(e.getMessage());
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    }
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    job = appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    return job;
  }

  /**
   * convert a task id string to an actual task and handle all the error
   * checking.
   */
  public static Task getTaskFromTaskIdString(String tid, Job job) throws NotFoundException {
    TaskId taskID;
    Task task;
    try {
      taskID = MRApps.toTaskID(tid);
    } catch (YarnRuntimeException e) {
      // TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
      // anymore but keeping it for now just in case other stuff starts failing.
      // Also, the webservice should ideally return BadRequest (HTTP:400) when
      // the id is malformed instead of NotFound (HTTP:404). The webserver on
      // top of which AMWebServices is built seems to automatically do that for
      // unhandled exceptions
      throw new NotFoundException(e.getMessage());
    } catch (NumberFormatException ne) {
      throw new NotFoundException(ne.getMessage());
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    } 
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    return task;
  }

  /**
   * convert a task attempt id string to an actual task attempt and handle all
   * the error checking.
   */
  public static TaskAttempt getTaskAttemptFromTaskAttemptString(String attId, Task task)
      throws NotFoundException {
    TaskAttemptId attemptId;
    TaskAttempt ta;
    try {
      attemptId = MRApps.toTaskAttemptID(attId);
    } catch (YarnRuntimeException e) {
      // TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
      // anymore but keeping it for now just in case other stuff starts failing.
      // Also, the webservice should ideally return BadRequest (HTTP:400) when
      // the id is malformed instead of NotFound (HTTP:404). The webserver on
      // top of which AMWebServices is built seems to automatically do that for
      // unhandled exceptions
      throw new NotFoundException(e.getMessage());
    } catch (NumberFormatException ne) {
      throw new NotFoundException(ne.getMessage());
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    }
    if (attemptId == null) {
      throw new NotFoundException("task attempt id " + attId
          + " not found or invalid");
    }
    ta = task.getAttempt(attemptId);
    if (ta == null) {
      throw new NotFoundException("Error getting info on task attempt id "
          + attId);
    }
    return ta;
  }


  /**
   * check for job access.
   *
   * @param job
   *          the job that is being accessed
   */
  void checkAccess(Job job, HttpServletRequest request) {
    if (!hasAccess(job, request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo get() {
    return getAppInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getAppInfo() {
    init();
    return new AppInfo(this.app, this.app.context);
  }
  
  @GET
  @Path("/blacklistednodes")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public BlacklistedNodesInfo getBlacklistedNodes() {
    init();
    return new BlacklistedNodesInfo(this.app.context);
  }

  @GET
  @Path("/jobs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobsInfo getJobs(@Context HttpServletRequest hsr) {
    init();
    JobsInfo allJobs = new JobsInfo();
    for (Job job : appCtx.getAllJobs().values()) {
      // getAllJobs only gives you a partial we want a full
      Job fullJob = appCtx.getJob(job.getID());
      if (fullJob == null) {
        continue;
      }
      allJobs.add(new JobInfo(fullJob, hasAccess(fullJob, hsr)));
    }
    return allJobs;
  }

  @GET
  @Path("/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobInfo getJob(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    return new JobInfo(job, hasAccess(job, hsr));
  }

  @GET
  @Path("/jobs/{jobid}/jobattempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AMAttemptsInfo getJobAttempts(@PathParam("jobid") String jid) {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    AMAttemptsInfo amAttempts = new AMAttemptsInfo();
    for (AMInfo amInfo : job.getAMInfos()) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo, MRApps.toString(
            job.getID()), job.getUserName());
      amAttempts.add(attempt);
    }
    return amAttempts;
  }

  @GET
  @Path("/jobs/{jobid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobCounterInfo getJobCounters(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    return new JobCounterInfo(this.appCtx, job);
  }

  @GET
  @Path("/jobs/{jobid}/conf")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ConfInfo getJobConf(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    ConfInfo info;
    try {
      info = new ConfInfo(job);
    } catch (IOException e) {
      throw new NotFoundException("unable to load configuration for job: "
          + jid);
    }
    return info;
  }

  @GET
  @Path("/jobs/{jobid}/tasks")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TasksInfo getJobTasks(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @QueryParam("type") String type) {

    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    TasksInfo allTasks = new TasksInfo();
    for (Task task : job.getTasks().values()) {
      TaskType ttype = null;
      if (type != null && !type.isEmpty()) {
        try {
          ttype = MRApps.taskType(type);
        } catch (YarnRuntimeException e) {
          throw new BadRequestException("tasktype must be either m or r");
        }
      }
      if (ttype != null && task.getType() != ttype) {
        continue;
      }
      allTasks.add(new TaskInfo(task));
    }
    return allTasks;
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskInfo getJobTask(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {

    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    return new TaskInfo(task);
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskCounterInfo getSingleTaskCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid) {

    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    return new JobTaskCounterInfo(task);
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptsInfo getJobTaskAttempts(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {

    init();
    TaskAttemptsInfo attempts = new TaskAttemptsInfo();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);

    for (TaskAttempt ta : task.getAttempts().values()) {
      if (ta != null) {
        if (task.getType() == TaskType.REDUCE) {
          attempts.add(new ReduceTaskAttemptInfo(ta, task.getType()));
        } else {
          attempts.add(new TaskAttemptInfo(ta, task.getType(), true));
        }
      }
    }
    return attempts;
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptInfo getJobTaskAttemptId(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid,
      @PathParam("attemptid") String attId) {

    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    if (task.getType() == TaskType.REDUCE) {
      return new ReduceTaskAttemptInfo(ta, task.getType());
    } else {
      return new TaskAttemptInfo(ta, task.getType(), true);
    }
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskAttemptCounterInfo getJobTaskAttemptIdCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid, @PathParam("attemptid") String attId) {

    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    return new JobTaskAttemptCounterInfo(ta);
  }
}
