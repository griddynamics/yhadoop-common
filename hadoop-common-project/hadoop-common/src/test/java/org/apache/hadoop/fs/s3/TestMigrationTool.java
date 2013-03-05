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

public class TestMigrationTool {
  private static File workspace = new File("target" + File.separator
      + "s3FileSystem");
  private Configuration conf;

  private FileSystemStore sourceStore;
  private S3ServerStub stubSource;
  private File source;
  private File target;
  private String url="s3://abc:xyz@hostname/";
  private AWSCredentials awsCredentials ;

  @Test
  public void testMigrationTool() throws Exception {

    conf = new Configuration();
    conf.set("fs.defaultFS", url);

    MigrationTool tool = new MigrationToolForTest();
    String[] args = { url };

    source = new File(workspace.getAbsolutePath() + File.separator + "source");

    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(new URI(url), conf);

    awsCredentials = new AWSCredentials(
        s3Credentials.getAccessKey(), s3Credentials.getSecretAccessKey());

    stubSource = new S3ServerStub(awsCredentials, source.getAbsolutePath());

    sourceStore = new Jets3tFileSystemStore();
    Configuration sourceConf = new Configuration();
    sourceConf.set("fs.defaultFS", url);
    sourceStore.initialize(new URI(url), sourceConf);
    Field inServeice = sourceStore.getClass().getDeclaredField("s3Service");
    inServeice.setAccessible(true);
    inServeice.set(sourceStore, stubSource);

    // File f = getDummiTextFile("node file");
    Path path = new Path("/testNode");
    Block[] blocks = new Block[2];
    blocks[0] = new Block(0, 10);
    blocks[1] = new Block(1, 10);

    INode node = new INode(FileType.FILE, blocks);

    sourceStore.storeINode(path, node);

    assertTrue(new File(source.getAbsolutePath() + "hostname" + File.separator
        + "testNode").exists());
    int res = ToolRunner.run(tool, args);
    assertEquals(0, res);
    assertFalse(new File(source.getAbsolutePath() + "hostname" + File.separator
        + "testNode").exists());
    assertTrue(new File(target.getAbsolutePath() + "hostname" + File.separator
        + "testNode").exists());

  }

  private class MigrationToolForTest extends MigrationTool {

    @Override
    protected FileSystemStore getFileSystemStore(URI uri) throws IOException {
      try {

        FileSystemStore store = new Jets3tFileSystemStore();
        store.initialize(uri, conf);

        Field inServeice = store.getClass().getDeclaredField("s3Service");
        inServeice.setAccessible(true);

        target = new File(workspace.getAbsolutePath() + File.separator
            + "target");
        S3ServerStub stubTarget = new S3ServerStub(awsCredentials,
            target.getAbsolutePath());
        inServeice.set(store, stubTarget);

        inServeice = this.getClass().getSuperclass()
            .getDeclaredField("s3Service");
        inServeice.setAccessible(true);
        inServeice.set(this, stubSource);

        return store;
      } catch (Exception e) {
        throw new IOException(e);
      }

    }
  }

}
