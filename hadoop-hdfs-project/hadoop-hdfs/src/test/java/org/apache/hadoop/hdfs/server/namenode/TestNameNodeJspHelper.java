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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.google.common.collect.ImmutableSet;

public class TestNameNodeJspHelper {

  private MiniDFSCluster cluster;
  private Configuration conf;
  private static final int dataNodeNumber = 2;
  private static final int nameNodePort = 45541;
  private static final int nameNodeHttpPort = 50070;
  private static final String NAMENODE_ATTRIBUTE_KEY = "name.node";  
  
  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(
            MiniDFSNNTopology.simpleSingleNN(nameNodePort, nameNodeHttpPort))
        .numDataNodes(dataNodeNumber).build();
    cluster.waitClusterUp();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }

  @Test
  public void testDelegationToken() throws IOException, InterruptedException {
    NamenodeProtocols nn = cluster.getNameNodeRpc();
    HttpServletRequest request = mock(HttpServletRequest.class);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("auser");
    String tokenString = NamenodeJspHelper.getDelegationToken(nn, request,
        conf, ugi);
    // tokenString returned must be null because security is disabled
    Assert.assertEquals(null, tokenString);
  }

  @Test
  public void testSecurityModeText() {
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    String securityOnOff = NamenodeJspHelper.getSecurityModeText();
    Assert.assertTrue("security mode doesn't match. Should be ON",
        securityOnOff.contains("ON"));
    // Security is enabled
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "simple");
    UserGroupInformation.setConfiguration(conf);

    securityOnOff = NamenodeJspHelper.getSecurityModeText();
    Assert.assertTrue("security mode doesn't match. Should be OFF",
        securityOnOff.contains("OFF"));
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
      DatanodeDescriptor dnDescriptor = NamenodeJspHelper
          .getRandomDatanode(nameNode);
      assertTrue("testGetRandomDatanode error",
          set.contains(dnDescriptor.toString()));
    }
  }
      
  @Test
  public void testNamenodeJspHelperRedirectToRandomDataNode() throws IOException, InterruptedException {
    final String urlExp = "http://localhost.localdomain:\\d+/browseDirectory.jsp\\?namenodeInfoPort="
        + nameNodeHttpPort + "&dir=/&nnaddr=([[\\d]+[\\.]+]+[\\d]|localhost.localdomain):" + nameNodePort;    
    final Pattern pattern = Pattern.compile(urlExp);        
    
    ServletContext context = mock(ServletContext.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);          
    
    when(request.getParameter(UserParam.NAME)).thenReturn("localuser");
    when(context.getAttribute(NAMENODE_ATTRIBUTE_KEY)).thenReturn(
        cluster.getNameNode());
    when(context.getAttribute(JspHelper.CURRENT_CONF)).thenReturn(conf);    
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    doAnswer(new Answer<String>() {
      @Override
     public String answer(InvocationOnMock invocation) throws Throwable {
        return null;
        }
    }).when(resp).sendRedirect(captor.capture());

    NamenodeJspHelper.redirectToRandomDataNode(context, request, resp);      
    assertTrue(pattern.matcher(captor.getValue()).matches());    
  }
  
  private enum DataNodeStatus {
    LIVE("[Live Datanodes(| +):(| +)]\\d"), 
    DEAD("[Dead Datanodes(| +):(| +)]\\d");

    private Pattern pattern;

    public Pattern getPattern() {
      return pattern;
    }

    DataNodeStatus(String line) {
      this.pattern = Pattern.compile(line);
    }
  }

  private void checkDeadLiveNodes(NameNode nameNode, int deadCount,
      int lifeCount) {
    FSNamesystem ns = nameNode.getNamesystem();
    DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    dm.fetchDatanodes(live, dead, true);
    assertTrue("checkDeadLiveNodes error !!!", (live.size() == lifeCount)
        && dead.size() == deadCount);
  }

  @Test
  public void testNodeListJspGenerateNodesList() throws IOException {
    String output;
    NameNode nameNode = cluster.getNameNode();
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute("name.node")).thenReturn(nameNode);
    when(context.getAttribute(NameNodeHttpServer.NAMENODE_ADDRESS_ATTRIBUTE_KEY))
        .thenReturn(cluster.getNameNode().getHttpAddress());    
    checkDeadLiveNodes(nameNode, 0, dataNodeNumber);
    output = getOutputFromGeneratedNodesList(context, DataNodeStatus.LIVE);
    assertCounts(DataNodeStatus.LIVE, output, dataNodeNumber);
    output = getOutputFromGeneratedNodesList(context, DataNodeStatus.DEAD);
    assertCounts(DataNodeStatus.DEAD, output, 0);    
  }

  private void assertCounts(DataNodeStatus dataNodeStatus, String output,
      int expectedCount) {
    Matcher matcher = DataNodeStatus.LIVE.getPattern().matcher(output);
    if (matcher.find()) {
      String digitLine = output.substring(matcher.start(), matcher.end())
          .trim();
      assertTrue("assertCounts error. actual != expected",
          Integer.valueOf(digitLine) == expectedCount);
    } else {
      fail("assertCount matcher error");
    }
  }

  private String getOutputFromGeneratedNodesList(ServletContext context,
      DataNodeStatus dnStatus) throws IOException {
    JspWriter out = mock(JspWriter.class);
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    NamenodeJspHelper.NodeListJsp nodelistjsp = new NamenodeJspHelper.NodeListJsp();
    final StringBuffer buffer = new StringBuffer();
    doAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invok) {
        Object[] args = invok.getArguments();
        buffer.append((String) args[0]);
        return null;
      }
    }).when(out).print(captor.capture());
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter("whatNodes")).thenReturn(dnStatus.name());
    nodelistjsp.generateNodesList(context, out, request);
    return buffer.toString();
  }

  @Test
  public void testGetInodeLimitText() {
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem fsn = nameNode.getNamesystem();
    ImmutableSet<String> patterns = 
        ImmutableSet.of("files and directories", "Heap Memory used", "Non Heap Memory used");        
    String line = NamenodeJspHelper.getInodeLimitText(fsn);    
    for(String pattern: patterns) {
      assertTrue("testInodeLimitText error " + pattern,
          line.contains(pattern));
    }    
  }
  
  @Test
  public void testGetVersionTable() {
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem fsn = nameNode.getNamesystem();
    ImmutableSet<String> patterns = ImmutableSet.of(VersionInfo.getVersion(), 
        VersionInfo.getRevision(), VersionInfo.getUser(), VersionInfo.getBranch(),
        fsn.getClusterId(), fsn.getBlockPoolId());
    String line = NamenodeJspHelper.getVersionTable(fsn);
    for(String pattern: patterns) {
       assertTrue("testGetVersionTable error " + pattern,
           line.contains(pattern));
    }
  }  
}
