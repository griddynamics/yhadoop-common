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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.ClusterStatus;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.DecommissionStatus;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

public class TestClusterJspHelper {
  
  private MiniDFSCluster cluster;
  private final int dataNodeNumber = 2;
  private final int nameNodePort = 45541;
  private final int nameNodeHttpPort = 50070;
  private URL originFileUrl;
  private File originFile;
  private File renamedFile;
  private static final String ORIGINAL_FILE_NAME = "hdfs-site.xml";
  private static final String RENAMED_FILE_NAME = "hdfs-site_.xml";
  private static final String FAKE_FILE_CONTEXT = "<?xml version=\"1.0\"?> <?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>"
      + "<configuration> <property> <name>fs.defaultFS</name> <value>hdfs://localhost.localdomain:45541</value> <description></description> </property> "
      + "<property> <name>hadoop.security.authentication</name> <value>simple</value> </property>" 
      + " </configuration>";

  private static final Logger logger = Logger
      .getLogger(TestClusterJspHelper.class);

  @SuppressWarnings("static-access")
  private void renameConfigFile() throws IOException {    
    originFileUrl = getClass().getClassLoader().getSystemResource(
        ORIGINAL_FILE_NAME);
    originFile = new File(originFileUrl.getPath());
    if (originFile.exists()) {
      String originPath = originFileUrl.getPath();
      String renamedFilePath = new String(originPath.substring(0,
          originPath.lastIndexOf("/"))
          + "/" + RENAMED_FILE_NAME);
      renamedFile = new File(renamedFilePath);
      assertTrue("file rename fail ", originFile.renameTo(renamedFile));
      Writer out = new BufferedWriter(new FileWriter(new File(originPath)));
      out.write(FAKE_FILE_CONTEXT);
      out.close();
    }
  }

  @Test
  public void testClusterJspHelperReports() {
    try {
      renameConfigFile();
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(
              MiniDFSNNTopology.simpleSingleNN(nameNodePort, nameNodeHttpPort))
          .numDataNodes(dataNodeNumber).build();
      cluster.waitClusterUp();

      ClusterJspHelper clusterJspHelper = new ClusterJspHelper();
      ClusterStatus clusterStatus = clusterJspHelper
          .generateClusterHealthReport();
      assertNotNull("testClusterJspHelperReports ClusterStatus is null",
          clusterStatus);
      assertTrue("testClusterJspHelperReports wrong ClusterStatus clusterid",
          clusterStatus.clusterid.equals("testClusterID"));
      DecommissionStatus decommissionStatus = clusterJspHelper
          .generateDecommissioningReport();
      assertNotNull("testClusterJspHelperReports DecommissionStatus is null",
          decommissionStatus);
      assertTrue(
          "testClusterJspHelperReports wrong DecommissionStatus clusterid",
          decommissionStatus.clusterid.equals("testClusterID"));
    } catch (Exception ex) {
      fail("testClusterJspHelperReports error " + ex);
    } finally {
      if (renamedFile != null & renamedFile.exists()) {
        try {
          Files.move(renamedFile, originFile);
        } catch (IOException e) {
          logger.error("fail rename config file");
        }
      }
    }
  }
}
