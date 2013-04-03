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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;

/**
 * Component tracking resources all of the same {@link LocalResourceVisibility}
 * 
 */
interface LocalResourcesTracker
    extends EventHandler<ResourceEvent>, Iterable<LocalizedResource> {

  // TODO: Not used at all!!
  boolean contains(LocalResourceRequest resource);

  boolean remove(LocalizedResource req, DeletionService delService);

  Path getPathForLocalization(LocalResourceRequest req, Path localDirPath);

  String getUser();

  // TODO: Remove this in favour of EventHandler.handle
  void localizationCompleted(LocalResourceRequest req, boolean success);

}
