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

package org.apache.hadoop.yarn.webapp.hamlet;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TestHamletGen {
  
  private static final String OUTPUT_PACKAGE = "org.apache.hadoop.yarn.webapp.hamlet";
  private static final String OUTPUT_CLASS = "./src/main/java/org/apache/hadoop/yarn/webapp/hamlet/TestHamlet2";  

  @Before
  public void setup() {
    File file = new File(OUTPUT_CLASS + ".java");
    if (file.exists())
      assertTrue("TestHamletGen Can't delete file " + file.getPath(), file.delete());
  }

  /**
   * Generate file through {@code HamletGen} and check it on exists
   * 
   * @throws IOException
   */
  @Test(timeout = 10000)
  public void testHamletGeToFile() throws IOException {
    Class<?> specClass = HamletSpec.class;
    Class<?> implClass = HamletImpl.class;
    try {
      new HamletGen().generate(specClass, implClass, OUTPUT_CLASS,
          OUTPUT_PACKAGE);
    } catch (IOException cause) {
      throw new IOException("TestHamletGen ex error in testHamletGen ", cause);
    }

    File file = new File(OUTPUT_CLASS + ".java");
    assertTrue("TestHamletGen Output file not exist ", file.exists());
    file.delete();
  }
}
