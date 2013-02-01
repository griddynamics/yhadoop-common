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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestMapReduceTrackingUriPlugin {
  @Test
  public void testProducesHistoryServerUriForAppId() throws URISyntaxException {
    final String historyAddress = "example.net:424242";

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS, historyAddress);
    MapReduceTrackingUriPlugin plugin = new MapReduceTrackingUriPlugin();
    plugin.setConf(conf);
    ApplicationId id = BuilderUtils.newApplicationId(6384623l, 5);
    String jobSuffix = id.toString().replaceFirst("^application_", "job_");
    URI expected =
        new URI("http://" + historyAddress + "/jobhistory/job/" + jobSuffix);
    URI actual = plugin.getTrackingUri(id);
    assertEquals(expected, actual);
  }
}