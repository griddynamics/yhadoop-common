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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import com.google.common.collect.ImmutableSet;

public class TestNamenodeJspHelper {
  
  private static MiniDFSCluster cluster;
  private static Configuration conf = new Configuration();
  private static final int dataNodeNumber = 2;
  private static final int nameNodePort = 45541;
  private static final int nameNodeHttpPort = 50070;
  
  @BeforeClass
  public static void before() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(
            MiniDFSNNTopology.simpleSingleNN(nameNodePort, nameNodeHttpPort))
        .numDataNodes(dataNodeNumber).build();
    cluster.waitClusterUp();
  }

  @AfterClass
  public static void after() {
    cluster.shutdown();
  }
  
  @Test
  public void testGetRandomDatanode() {
    ImmutableSet<String> set = ImmutableSet.of();
    NameNode nameNode = cluster.getNameNode();
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();         
    for (DataNode dataNode : cluster.getDataNodes()) { 
      builder.add(dataNode.getDisplayName());      
    }    
    set = builder.build();
            
    for (int i = 0; i < 10; i++) {
      DatanodeDescriptor dnDescriptor = NamenodeJspHelper.getRandomDatanode(nameNode);
      assertTrue(set.contains(dnDescriptor.toString()));
    }              
  } 
  
  @Test
  public void testNamenodeJspHelperRedirectToRandomDataNode() {
    String NAMENODE_ATTRIBUTE_KEY = "name.node";
    String REDIRECT_URL_EPISODE = "/browseDirectory.jsp?namenodeInfoPort=";
    ServletContext context = mock(ServletContext.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);

    when(request.getParameter(UserParam.NAME)).thenReturn("localuser");
    when(context.getAttribute(NAMENODE_ATTRIBUTE_KEY)).thenReturn(
        cluster.getNameNode());
    when(context.getAttribute(JspHelper.CURRENT_CONF)).thenReturn(conf);
    try {
      ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
      doAnswer(new Answer<String>() {
        @Override
        public String answer(InvocationOnMock invocation) throws Throwable {
          return null;
        }
      }).when(resp).sendRedirect(captor.capture());

      NamenodeJspHelper.redirectToRandomDataNode(context, request, resp);
      assertTrue(captor.getValue().contains(REDIRECT_URL_EPISODE));
    } catch (Exception e) {
      fail("testNamenodeJspHelperRedirectToRandomDataNode ex error " + e);
    }
  }
  
  @Test
  public void testNodeListJspGenerateNodesList() {
    String postfix = "test-end";
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute("name.node")).thenReturn(cluster.getNameNode());
    when(
        context.getAttribute(NameNodeHttpServer.NAMENODE_ADDRESS_ATTRIBUTE_KEY))
        .thenReturn(cluster.getNameNode().getHttpAddress());

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter("whatNodes")).thenReturn("LIVE");
    JspWriter out = mock(JspWriter.class);
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    final StringBuffer buffer = new StringBuffer();
    NamenodeJspHelper.NodeListJsp nodelistjsp = new NamenodeJspHelper.NodeListJsp();
    try {
      doAnswer(new Answer<String>() {
        @Override
        public String answer(InvocationOnMock invok) {
          Object[] args = invok.getArguments();
          buffer.append((String) args[0]);
          return null;
        }
      }).when(out).print(captor.capture());

      nodelistjsp.generateNodesList(context, out, request);

      // last action on out.print
      out.print(postfix);
      String line = buffer.toString();
      assertNotNull("testNodeListJspGenerateNodesList null buffer error !!!",
          line);
      assertTrue("testNodeListJspGenerateNodesList buffer content error !!!",
          line.endsWith(postfix) && line.length() > postfix.length());
    } catch (Exception ex) {
      fail("testNodeListJspGenerateNodesList error !!!" + ex);
    }
  }
  
  @Test
  public void testNamenodeJspHelper() {
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem fsn = nameNode.getNamesystem();
    String line = NamenodeJspHelper.getInodeLimitText(fsn);
    assertTrue(line.contains("files and directories"));
    assertTrue(line.contains("Heap Memory used"));
    assertTrue(line.contains("Non Heap Memory used"));
  }
}
