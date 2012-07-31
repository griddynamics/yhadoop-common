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
package org.apache.hadoop.hdfs;

import org.apache.commons.lang.StringUtils;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * Class that implements a JUnit {@link RunListener} to be called before and after each
 *  test method to check the resources used:
 *   - file descriptors
 *   - threads
 *   
 *  @see ResourceChecker
 */
public class ResourceCheckerListener extends RunListener {

  private ResourceChecker cu;
  private boolean endDone;

  @Override
  public void testStarted(Description description) throws Exception {
    cu = new ResourceChecker("before " + descriptionToShortTestName(description));
    endDone = false;
  }

  @Override
  public void testFinished(Description description) throws Exception {
    if (!endDone) {
      String testName = descriptionToShortTestName(description);
      endDone = true;
      cu.logInfo("after " + testName);
      cu.check("after "+ testName);
    }
  }

  /**
   * Get the test name from the JUnit Description
   * @param description
   * @return the string for the short test name
   */
  private String descriptionToShortTestName(
    org.junit.runner.Description description) {
    final int toRemove = StringUtils.ordinalIndexOf(description.getTestClass().getName(), ".", 4) + 1;
    return description.getTestClass().getName().substring(toRemove) +
      "#" + description.getMethodName();
  }

}
