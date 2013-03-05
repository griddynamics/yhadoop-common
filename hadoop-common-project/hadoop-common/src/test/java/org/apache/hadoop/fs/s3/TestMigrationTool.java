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


package org.apache.hadoop.fs.s3;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.security.AWSCredentials;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Migration tool test should reads from source bucket and moves to target bucket.
 * Source data should be removed
 */
public class TestMigrationTool {
  private static File workspace = new File("target" + File.separator
          + "s3FileSystem");
  private Configuration configuration;

  private S3ServerStub stubSource;
  private File target;
  private AWSCredentials awsCredentials;

  @Test(timeout = 1000)
  public void testMigrationTool() throws Exception {

    configuration = new Configuration();
    String url = "s3://abc:xyz@hostname/";
    configuration.set("fs.defaultFS", url);

    MigrationTool tool = new MigrationToolForTest();
    String[] args = {url};

    File source = new File(workspace.getAbsolutePath() + File.separator + "source");

    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(new URI(url), configuration);

    awsCredentials = new AWSCredentials(
            s3Credentials.getAccessKey(), s3Credentials.getSecretAccessKey());

    stubSource = new S3ServerStub(awsCredentials, source.getAbsolutePath());

    FileSystemStore sourceStore = new Jets3tFileSystemStore();
    Configuration sourceConfiguration = new Configuration();
    sourceConfiguration.set("fs.defaultFS", url);
    sourceStore.initialize(new URI(url), sourceConfiguration);
    Field internalService = sourceStore.getClass().getDeclaredField("s3Service");
    internalService.setAccessible(true);
    internalService.set(sourceStore, stubSource);

    // prepare source data
    Path path = new Path("/testNode");
    Block[] blocks = new Block[2];
    blocks[0] = new Block(0, 10);
    blocks[1] = new Block(1, 10);

    INode node = new INode(FileType.FILE, blocks);

    sourceStore.storeINode(path, node);

    assertTrue(new File(source.getAbsolutePath() + "hostname" + File.separator
            + "testNode").exists());
    // run test
    int res = ToolRunner.run(tool, args);
    assertEquals(0, res);
    // test that the source has been deleted
    assertFalse(new File(source.getAbsolutePath() + "hostname" + File.separator
            + "testNode").exists());
    // test that the target  exists

    assertTrue(new File(target.getAbsolutePath() + "hostname" + File.separator
            + "testNode").exists());

  }

  private class MigrationToolForTest extends MigrationTool {

    @Override
    protected FileSystemStore getFileSystemStore(URI uri) throws IOException {
      try {

        FileSystemStore store = new Jets3tFileSystemStore();
        store.initialize(uri, configuration);

        Field internalService = store.getClass().getDeclaredField("s3Service");
        internalService.setAccessible(true);

        target = new File(workspace.getAbsolutePath() + File.separator
                + "target");
        S3ServerStub stubTarget = new S3ServerStub(awsCredentials,
                target.getAbsolutePath());
        internalService.set(store, stubTarget);

        internalService = this.getClass().getSuperclass()
                .getDeclaredField("s3Service");
        internalService.setAccessible(true);
        internalService.set(this, stubSource);

        return store;
      } catch (Exception e) {
        throw new IOException(e);
      }

    }
  }

}
