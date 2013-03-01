package org.apache.hadoop.fs.s3;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.jets3t.service.security.AWSCredentials;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


public class TestJets3tFileSystemStore {

  private static Jets3tFileSystemStore store=null;
  private static File workspace= new File("target"+File.separator+"s3FileSystem");
  @BeforeClass
  public static void start() throws Exception{
    final Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "s3://abc:xyz@hostname/");
    URI fakeUri=new URI("s3://abc:xyz@hostname/");
    store = new Jets3tFileSystemStore();
    store.initialize(fakeUri, conf);
    
    Field inserveice=store.getClass().getDeclaredField("s3Service");
    inserveice.setAccessible(true);
    
    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(fakeUri, conf);
      AWSCredentials awsCredentials =
        new AWSCredentials(s3Credentials.getAccessKey(),
            s3Credentials.getSecretAccessKey());
    inserveice.set(store,new S3ServerStub(awsCredentials, workspace.getAbsolutePath()));
  }
  
  @Test
  public void testBlock() throws Exception{
    
    File f=  getDummiTextFile("block file");
    Block block= new Block(1, f.length());
    store.storeBlock(block, f);
    assertTrue(store.blockExists(1));
    File result=new File(workspace.getAbsolutePath()+"hostname"+File.separator+"block_"+block.getId());
    assertTrue(result.exists());
    assertEquals(10,result.length());
    File newFile= store.retrieveBlock(block, 0);
    assertNotNull(newFile);
    assertEquals(f.length(), newFile.length());
    
    
    store.deleteBlock(block);
    assertFalse(result.exists());
    assertFalse(store.blockExists(1));

  }
  @Test
  public void testNode() throws Exception{
    
    File f=  getDummiTextFile("node file");
    Path path=new Path("/testNode");
    Block [] blocks= new Block[2];
    blocks[0]=new Block(0, f.length());
    blocks[1]=new Block(1, f.length());
    
    INode node = new INode(FileType.FILE, blocks);
    store.storeINode(path, node);
    assertTrue(store.inodeExists(path));
    File result=new File(workspace.getAbsolutePath()+"hostname"+File.separator+"testNode");
    assertTrue(result.exists());
    assertTrue(result.length()>0);
    node= store.retrieveINode(path);
    assertEquals(2, node.getBlocks().length);
    assertEquals(FileType.FILE, node.getFileType());
    
    
    store.deleteINode(path);
    assertFalse(store.inodeExists(path));
    assertFalse(result.exists());
  }
  
  @Test
  public void testListSubPaths() throws Exception{
    Set<Path> subPaths= store.listSubPaths(new Path("/"));
    assertEquals(1,subPaths.size());
    
    subPaths= store.listDeepSubPaths(new Path("/"));
    assertEquals(1,subPaths.size());
    
  }
  @Test
  public void testGetVersion() throws Exception{
    assertEquals("1", store.getVersion());
  }
  private File getDummiTextFile(String text) throws Exception{
    File result = new File(workspace.getParent()+File.separator+"tmpFile.txt");
    Writer writer= new FileWriter(result);
    writer.write(text);
    writer.flush();
    writer.close();
    return result;
    
  }
}
