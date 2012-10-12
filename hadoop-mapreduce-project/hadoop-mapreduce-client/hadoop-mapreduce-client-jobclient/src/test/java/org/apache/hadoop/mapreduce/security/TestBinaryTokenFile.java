/** Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.mapreduce.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBinaryTokenFile {

  private static final String KEY_HDFS_FOO_BAR = "HdfsFooBar";
  private static final String KEY_SECURITY_TOKEN = "key-security-token-file";
  
  // my sleep class
  static class MySleepMapper extends SleepJob.SleepMapper {
    
    /**
     * attempts to access tokenCache as from client
     */
    @Override
    public void map(IntWritable key, IntWritable value, Context context)
    throws IOException, InterruptedException {
      // get token storage and a key
      final Credentials credentials = context.getCredentials();
      final Collection<Token<? extends TokenIdentifier>> dts = credentials.getAllTokens();
      
      if (dts.size() != 1) { // only the job token 
        throw new RuntimeException("tokens are not available: size = " + dts.size()); // fail the test
      }  
      
      Token<? extends TokenIdentifier> dt = credentials.getToken(new Text(KEY_HDFS_FOO_BAR));
      if (dt != null) {
        throw new RuntimeException("This token should *not* be passed into the job context."); 
      }
      
      final String tokenFile = context.getConfiguration().get(KEY_SECURITY_TOKEN);
      if (tokenFile == null) {
        throw new RuntimeException("Token file key ["+KEY_SECURITY_TOKEN+"] not found in the configuration.");
      }
      Credentials cred = new Credentials();
      cred.readTokenStorageStream(new DataInputStream(new FileInputStream(
          tokenFile)));
      Assert.assertNotNull("Token must be correctly read.", cred.getToken(new Text(KEY_HDFS_FOO_BAR)));
      
      super.map(key, value, context);
    }
  }
  
  static class MySleepJob extends SleepJob {
    @Override
    public Job createJob(int numMapper, int numReducer, 
        long mapSleepTime, int mapSleepCount, 
        long reduceSleepTime, int reduceSleepCount) 
    throws IOException {
      final Job job = super.createJob(numMapper, numReducer,
           mapSleepTime, mapSleepCount, 
          reduceSleepTime, reduceSleepCount);
      
      job.setMapperClass(MySleepMapper.class);
      //Populate tokens here because security is disabled.
      setupBinaryTokenFile(job);
      return job;
    }
    
    private void setupBinaryTokenFile(Job job) {
    // Credentials in the job will not have delegation tokens
    // because security is disabled. Fetch delegation tokens
    // and store in binary token file.
      try {
        final Credentials cred1 = new Credentials();
        TokenCache.obtainTokensForNamenodesInternal(cred1, new Path[] { p1 },
            job.getConfiguration());
        final Credentials cred2 = new Credentials();
        for (Token<? extends TokenIdentifier> t: cred1.getAllTokens()) {
          cred2.addToken(new Text(KEY_HDFS_FOO_BAR), t);
        }
        DataOutputStream os = new DataOutputStream(new FileOutputStream(
            binaryTokenFileName.toString()));
        try {
          cred2.writeTokenStorageToStream(os);
        } finally {
          os.close();
        }
        job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY,
            binaryTokenFileName.toString());
        // NB: the MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY key now gets deleted from config, 
        // so its not accessible in the log. So, we use another key to pass the file name into the job:  
        job.getConfiguration().set(KEY_SECURITY_TOKEN, 
            binaryTokenFileName.toString());
        
        // NB: now (after we set MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY key and written the 
        // corresponding file) invoke this method one more time to populate the job's credentials with the same tokens:
        TokenCache.obtainTokensForNamenodesInternal(job.getCredentials(), new Path[] { p1 },
            job.getConfiguration());
        
        Assert.assertNotNull("Token must be deserialized and set into the job.getCredentials()", job.getCredentials().getToken(new Text(KEY_HDFS_FOO_BAR)));
      } catch (IOException e) {
        e.printStackTrace(System.out);
        Assert.fail("Exception " + e);
      }
    }
  }
  
  private static MiniMRYarnCluster mrCluster;
  private static MiniDFSCluster dfsCluster;
  
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data","/tmp"));
  private static final Path binaryTokenFileName = new Path(TEST_DIR, "tokenFile.binary");
  
  private static final int numSlaves = 1; // num of data nodes
  private static final int noOfNMs = 1;
  
  private static Path p1;
  
  @BeforeClass
  public static void setUp() throws Exception {
    final Configuration conf = new Configuration();
    
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.set(YarnConfiguration.RM_PRINCIPAL, "jt_id/" + SecurityUtil.HOSTNAME_PATTERN + "@APACHE.ORG");
    
    final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.checkExitOnShutdown(true);
    builder.numDataNodes(numSlaves);
    builder.format(true);
    builder.racks(null);
    dfsCluster = builder.build();
    
    mrCluster = new MiniMRYarnCluster(TestBinaryTokenFile.class.getName(), noOfNMs);
    mrCluster.init(conf);
    mrCluster.start();

    NameNodeAdapter.getDtSecretManager(dfsCluster.getNamesystem()).startThreads(); 
    
    FileSystem fs = dfsCluster.getFileSystem(); 
    p1 = new Path("file1");
    p1 = fs.makeQualified(p1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    if(dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }
  
  /**
   * run a distributed job and verify that TokenCache is available
   * @throws IOException
   */
  @Test
  public void testBinaryTokenFile() throws Exception {
    Configuration conf = mrCluster.getConfig();
    
    // provide namenodes names for the job to get the delegation tokens for
    final String nnUri = dfsCluster.getURI(0).toString();
    conf.set(MRJobConfig.JOB_NAMENODES, nnUri + "," + nnUri);
    
    // using argument to pass the file name
    final String[] args = { 
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
        };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new MySleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with " + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }
}
