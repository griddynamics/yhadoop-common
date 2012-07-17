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
package org.apache.hadoop.fs.viewfs;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFSMainOperationsLocalFileSystem extends FSMainOperationsBaseTest {
   static FileSystem fcTarget;
  @Before
  public void setUp() throws Exception {
    Configuration conf = ViewFileSystemTestSetup.createConfig();
    fcTarget = FileSystem.getLocal(conf);
    fSys = ViewFileSystemTestSetup.setupForViewFileSystem(conf, fcTarget);
    super.setUp();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    ViewFileSystemTestSetup.tearDown(fcTarget);
  }
  
  @Test
  @Override
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = FileSystemTestHelper.getTestRootPath(fSys,
        "test/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());

  }
}
