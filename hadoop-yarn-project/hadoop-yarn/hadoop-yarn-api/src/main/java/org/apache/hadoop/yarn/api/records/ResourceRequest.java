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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>ResourceRequest</code> represents the request made by an
 * application to the <code>ResourceManager</code> to obtain various 
 * <code>Container</code> allocations.</p>
 * 
 * <p>It includes:
 *   <ul>
 *     <li>{@link Priority} of the request.</li>
 *     <li>
 *       The <em>name</em> of the machine or rack on which the allocation is 
 *       desired. A special value of <em>*</em> signifies that 
 *       <em>any</em> host/rack is acceptable to the application.
 *     </li>
 *     <li>{@link Resource} required for each request.</li>
 *     <li>
 *       Number of containers of such specifications which are required 
 *       by the application.
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see Resource
 * @see AMRMProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Stable
public abstract class ResourceRequest implements Comparable<ResourceRequest> {

  @Public
  @Stable
  public static ResourceRequest newInstance(Priority priority,
      String hostName, Resource capability, int numContainers) {
    ResourceRequest request = Records.newRecord(ResourceRequest.class);
    request.setPriority(priority);
    request.setHostName(hostName);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    return request;
  }

  /**
   * The constant string representing no locality.
   * It should be used by all references that want to pass an arbitrary host
   * name in.
   */
  public static final String ANY = "*";

  /**
   * Check whether the given <em>host/rack</em> string represents an arbitrary
   * host name.
   *
   * @param hostName <em>host/rack</em> on which the allocation is desired
   * @return whether the given <em>host/rack</em> string represents an arbitrary
   * host name
   */
  public static boolean isAnyLocation(String hostName) {
    return ANY.equals(hostName);
  }

  /**
   * Get the <code>Priority</code> of the request.
   * @return <code>Priority</code> of the request
   */
  @Public
  @Stable
  public abstract Priority getPriority();

  /**
   * Set the <code>Priority</code> of the request
   * @param priority <code>Priority</code> of the request
   */
  @Public
  @Stable
  public abstract void setPriority(Priority priority);
  
  /**
   * Get the <em>host/rack</em> on which the allocation is desired.
   * 
   * A special value of <em>*</em> signifies that <em>any</em> host/rack is 
   * acceptable.
   * 
   * @return <em>host/rack</em> on which the allocation is desired
   */
  @Public
  @Stable
  public abstract String getHostName();

  /**
   * Set <em>host/rack</em> on which the allocation is desired.
   * 
   * A special value of <em>*</em> signifies that <em>any</em> host/rack is 
   * acceptable.
   * 
   * @param hostName <em>host/rack</em> on which the allocation is desired
   */
  @Public
  @Stable
  public abstract void setHostName(String hostName);
  
  /**
   * Get the <code>Resource</code> capability of the request.
   * @return <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract Resource getCapability();
  
  /**
   * Set the <code>Resource</code> capability of the request
   * @param capability <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract void setCapability(Resource capability);

  /**
   * Get the number of containers required with the given specifications.
   * @return number of containers required with the given specifications
   */
  @Public
  @Stable
  public abstract int getNumContainers();
  
  /**
   * Set the number of containers required with the given specifications
   * @param numContainers number of containers required with the given 
   *                      specifications
   */
  @Public
  @Stable
  public abstract void setNumContainers(int numContainers);

  /**
   * Get whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>. Defaults to true.
   * 
   * @return whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>.
   */
  @Public
  @Stable
  public abstract boolean getRelaxLocality();
  
  /**
   * For a request at a network hierarchy level, set whether locality can be relaxed
   * to that level and beyond.
   * 
   * If the flag is off on a rack-level <code>ResourceRequest</code>,
   * containers at that request's priority will not be assigned to nodes on that
   * request's rack unless requests specifically for those nodes have also been
   * submitted.
   * 
   * If the flag is off on an {@link ResourceRequest#ANY}-level
   * <code>ResourceRequest</code>, containers at that request's priority will
   * only be assigned on racks for which specific requests have also been
   * submitted.
   * 
   * For example, to request a container strictly on a specific node, the
   * corresponding rack-level and any-level requests should have locality
   * relaxation set to false.  Similarly, to request a container strictly on a
   * specific rack, the corresponding any-level request should have locality
   * relaxation set to false.
   * 
   * @param relaxLocality whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>.
   */
  @Public
  @Stable
  public abstract void setRelaxLocality(boolean relaxLocality);
  
  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    Resource capability = getCapability();
    String hostName = getHostName();
    Priority priority = getPriority();
    result =
        prime * result + ((capability == null) ? 0 : capability.hashCode());
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + getNumContainers();
    result = prime * result + ((priority == null) ? 0 : priority.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ResourceRequest other = (ResourceRequest) obj;
    Resource capability = getCapability();
    if (capability == null) {
      if (other.getCapability() != null)
        return false;
    } else if (!capability.equals(other.getCapability()))
      return false;
    String hostName = getHostName();
    if (hostName == null) {
      if (other.getHostName() != null)
        return false;
    } else if (!hostName.equals(other.getHostName()))
      return false;
    if (getNumContainers() != other.getNumContainers())
      return false;
    Priority priority = getPriority();
    if (priority == null) {
      if (other.getPriority() != null)
        return false;
    } else if (!priority.equals(other.getPriority()))
      return false;
    return true;
  }

  @Override
  public int compareTo(ResourceRequest other) {
    int priorityComparison = this.getPriority().compareTo(other.getPriority());
    if (priorityComparison == 0) {
      int hostNameComparison =
          this.getHostName().compareTo(other.getHostName());
      if (hostNameComparison == 0) {
        int capabilityComparison =
            this.getCapability().compareTo(other.getCapability());
        if (capabilityComparison == 0) {
          int numContainersComparison =
              this.getNumContainers() - other.getNumContainers();
          if (numContainersComparison == 0) {
            return 0;
          } else {
            return numContainersComparison;
          }
        } else {
          return capabilityComparison;
        }
      } else {
        return hostNameComparison;
      }
    } else {
      return priorityComparison;
    }
  }
}
