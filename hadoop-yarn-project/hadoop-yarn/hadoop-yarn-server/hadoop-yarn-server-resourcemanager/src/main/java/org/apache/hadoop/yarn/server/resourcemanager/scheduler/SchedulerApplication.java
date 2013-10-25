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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Represents an application attempt from the viewpoint of the scheduler.
 * Each running app attempt in the RM corresponds to one instance
 * of this class.
 */
@Private
@Unstable
public abstract class SchedulerApplication {
  
  private static final Log LOG = LogFactory.getLog(SchedulerApplication.class);

  protected final AppSchedulingInfo appSchedulingInfo;
  
  protected final Map<ContainerId, RMContainer> liveContainers =
      new HashMap<ContainerId, RMContainer>();
  protected final Map<Priority, Map<NodeId, RMContainer>> reservedContainers = 
      new HashMap<Priority, Map<NodeId, RMContainer>>();

  private final Multiset<Priority> reReservations = HashMultiset.create();
  
  protected final Resource currentReservation = Resource.newInstance(0, 0);
  private Resource resourceLimit = Resource.newInstance(0, 0);
  protected final Resource currentConsumption = Resource.newInstance(0, 0);

  protected List<RMContainer> newlyAllocatedContainers = 
      new ArrayList<RMContainer>();

  /**
   * Count how many times the application has been given an opportunity
   * to schedule a task at each priority. Each time the scheduler
   * asks the application for a task at this priority, it is incremented,
   * and each time the application successfully schedules a task, it
   * is reset to 0.
   */
  Multiset<Priority> schedulingOpportunities = HashMultiset.create();
  
  // Time of the last container scheduled at the current allowed level
  protected Map<Priority, Long> lastScheduledContainer =
      new HashMap<Priority, Long>();

  protected final Queue queue;
  protected boolean isStopped = false;
  
  protected final RMContext rmContext;
  
  public SchedulerApplication(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    this.rmContext = rmContext;
    this.appSchedulingInfo = 
        new AppSchedulingInfo(applicationAttemptId, user, queue,  
            activeUsersManager);
    this.queue = queue;
  }
  
  /**
   * Get the live containers of the application.
   * @return live containers of the application
   */
  public synchronized Collection<RMContainer> getLiveContainers() {
    return new ArrayList<RMContainer>(liveContainers.values());
  }
  
  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public boolean isPending() {
    return appSchedulingInfo.isPending();
  }
  
  /**
   * Get {@link ApplicationAttemptId} of the application master.
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appSchedulingInfo.getApplicationAttemptId();
  }
  
  public ApplicationId getApplicationId() {
    return appSchedulingInfo.getApplicationId();
  }
  
  public String getUser() {
    return appSchedulingInfo.getUser();
  }

  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    return appSchedulingInfo.getResourceRequests(priority);
  }

  public int getNewContainerId() {
    return appSchedulingInfo.getNewContainerId();
  }

  public Collection<Priority> getPriorities() {
    return appSchedulingInfo.getPriorities();
  }
  
  public ResourceRequest getResourceRequest(Priority priority, String resourceName) {
    return this.appSchedulingInfo.getResourceRequest(priority, resourceName);
  }

  public synchronized int getTotalRequiredResources(Priority priority) {
    return getResourceRequest(priority, ResourceRequest.ANY).getNumContainers();
  }

  public Resource getResource(Priority priority) {
    return appSchedulingInfo.getResource(priority);
  }

  public String getQueueName() {
    return appSchedulingInfo.getQueueName();
  }
  
  public synchronized RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  protected synchronized void resetReReservations(Priority priority) {
    reReservations.setCount(priority, 0);
  }

  protected synchronized void addReReservation(Priority priority) {
    reReservations.add(priority);
  }

  public synchronized int getReReservations(Priority priority) {
    return reReservations.count(priority);
  }

  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   */
  @Stable
  @Private
  public synchronized Resource getCurrentReservation() {
    return currentReservation;
  }
  
  public Queue getQueue() {
    return queue;
  }
  
  public synchronized void updateResourceRequests(
      List<ResourceRequest> requests) {
    if (!isStopped) {
      appSchedulingInfo.updateResourceRequests(requests);
    }
  }
  
  public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // Cleanup all scheduling information
    isStopped = true;
    appSchedulingInfo.stop(rmAppAttemptFinalState);
  }

  public synchronized boolean isStopped() {
    return isStopped;
  }

  /**
   * Get the list of reserved containers
   * @return All of the reserved containers.
   */
  public synchronized List<RMContainer> getReservedContainers() {
    List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
    for (Map.Entry<Priority, Map<NodeId, RMContainer>> e : 
      this.reservedContainers.entrySet()) {
      reservedContainers.addAll(e.getValue().values());
    }
    return reservedContainers;
  }
  
  public synchronized RMContainer reserve(SchedulerNode node, Priority priority,
      RMContainer rmContainer, Container container) {
    // Create RMContainer if necessary
    if (rmContainer == null) {
      rmContainer = 
          new RMContainerImpl(container, getApplicationAttemptId(), 
              node.getNodeID(), rmContext.getDispatcher().getEventHandler(), 
              rmContext.getContainerAllocationExpirer());
        
      Resources.addTo(currentReservation, container.getResource());
      
      // Reset the re-reservation count
      resetReReservations(priority);
    } else {
      // Note down the re-reservation
      addReReservation(priority);
    }
    rmContainer.handle(new RMContainerReservedEvent(container.getId(), 
        container.getResource(), node.getNodeID(), priority));
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(priority, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);
    
    LOG.info("Application " + getApplicationId() 
        + " reserved container " + rmContainer
        + " on node " + node + ", currently has " + reservedContainers.size()
        + " at priority " + priority 
        + "; currentReservation " + currentReservation.getMemory());
    
    return rmContainer;
  }
  
  /**
   * Has the application reserved the given <code>node</code> at the
   * given <code>priority</code>?
   * @param node node to be checked
   * @param priority priority of reserved container
   * @return true is reserved, false if not
   */
  public synchronized boolean isReserved(SchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers != null) {
      return reservedContainers.containsKey(node.getNodeID());
    }
    return false;
  }
  
  public synchronized void setHeadroom(Resource globalLimit) {
    this.resourceLimit = globalLimit; 
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public synchronized Resource getHeadroom() {
    // Corner case to deal with applications being slightly over-limit
    if (resourceLimit.getMemory() < 0) {
      resourceLimit.setMemory(0);
    }
    
    return resourceLimit;
  }
  
  public synchronized int getNumReservedContainers(Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    return (reservedContainers == null) ? 0 : reservedContainers.size();
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void containerLaunchedOnNode(ContainerId containerId,
      NodeId nodeId) {
    // Inform the container
    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      // Some unknown container sneaked into the system. Kill it.
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeCleanContainerEvent(nodeId, containerId));
      return;
    }

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.LAUNCHED));
  }
  
  public synchronized void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + getApplicationId() + 
              " headRoom=" + getHeadroom() + 
              " currentConsumption=" + currentConsumption.getMemory());
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " request=" + request);
          }
        }
      }
    }
  }
  
  public Resource getCurrentConsumption() {
    return currentConsumption;
  }

  public synchronized List<Container> pullNewlyAllocatedContainers() {
    List<Container> returnContainerList = new ArrayList<Container>(
        newlyAllocatedContainers.size());
    for (RMContainer rmContainer : newlyAllocatedContainers) {
      rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
          RMContainerEventType.ACQUIRED));
      returnContainerList.add(rmContainer.getContainer());
    }
    newlyAllocatedContainers.clear();
    return returnContainerList;
  }

  public synchronized void updateBlacklist(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (!isStopped) {
      this.appSchedulingInfo.updateBlacklist(
          blacklistAdditions, blacklistRemovals);
    }
  }
  
  public boolean isBlacklisted(String resourceName) {
    return this.appSchedulingInfo.isBlacklisted(resourceName);
  }

  public synchronized void addSchedulingOpportunity(Priority priority) {
    schedulingOpportunities.setCount(priority,
        schedulingOpportunities.count(priority) + 1);
  }
  
  public synchronized void subtractSchedulingOpportunity(Priority priority) {
    int count = schedulingOpportunities.count(priority) - 1;
    this.schedulingOpportunities.setCount(priority, Math.max(count,  0));
  }

  /**
   * Return the number of times the application has been given an opportunity
   * to schedule a task at the given priority since the last time it
   * successfully did so.
   */
  public synchronized int getSchedulingOpportunities(Priority priority) {
    return schedulingOpportunities.count(priority);
  }
  
  /**
   * Should be called when an application has successfully scheduled a container,
   * or when the scheduling locality threshold is relaxed.
   * Reset various internal counters which affect delay scheduling
   *
   * @param priority The priority of the container scheduled.
   */
  public synchronized void resetSchedulingOpportunities(Priority priority) {
    resetSchedulingOpportunities(priority, System.currentTimeMillis());
  }
  // used for continuous scheduling
  public synchronized void resetSchedulingOpportunities(Priority priority,
      long currentTimeMs) {
    lastScheduledContainer.put(priority, currentTimeMs);
    schedulingOpportunities.setCount(priority, 0);
  }

}
