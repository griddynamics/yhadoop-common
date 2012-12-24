package org.apache.hadoop.mapred.pipes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;

public class PipeApplicatoinRunabeClient {

  public static void main(String[] args) {
    PipeApplicatoinRunabeClient client = new PipeApplicatoinRunabeClient();
    client.binaryProtocolStub();
  }

  public void binaryProtocolStub() {
    Socket socket =null;
    try {

      int port = Integer 
          .parseInt(System.getenv("mapreduce.pipes.command.port"));
      
      java.net.InetAddress addr = java.net.InetAddress.getLocalHost();
      
      
      socket = new Socket(addr.getHostName(), port);
      InputStream input = socket.getInputStream();
      OutputStream output = socket.getOutputStream();

      // try to read
      DataInputStream dataInput = new DataInputStream(input);

      int i = WritableUtils.readVInt(dataInput);

      String str = Text.readString(dataInput);

      Text.readString(dataInput);

      DataOutputStream dataout = new DataOutputStream(output);
      WritableUtils.writeVInt(dataout, 57);
      String s = createDigest("password".getBytes(), str);

      Text.writeString(dataout, s);

      // start

      i = WritableUtils.readVInt(dataInput);
      i = WritableUtils.readVInt(dataInput);

  // get conf
      // should be MessageType.SET_JOB_CONF.code
      i = WritableUtils.readVInt(dataInput);
          // array length

      int j = WritableUtils.readVInt(dataInput);
      for (i = 0; i < j; i++) {
        String key=Text.readString(dataInput);
        i++;
        String val= Text.readString(dataInput);
        System.out.println("key:"+key+"val:"+val);
      }
      System.out.println("cfg  222s ok:" + ":"+System.currentTimeMillis());


// RUN_MAP.code
      //should be 3

      i = WritableUtils.readVInt(dataInput);
      
      System.out.println("3=:" +i);
      TestPipeApplication.FakeSplit split= new TestPipeApplication.FakeSplit() ; 
      readObject(split, dataInput);
      System.out.println("split=:" +split);
      i = WritableUtils.readVInt(dataInput);
      System.out.println("numReduces=:" +i);
      i = WritableUtils.readVInt(dataInput);
      System.out.println("pipedInput=:" +i);
      
      //should be 2
      
      i = WritableUtils.readVInt(dataInput);
      System.out.println("2=:" +i);
      s= Text.readString(dataInput);
      System.out.println("s=:" +s);
      s= Text.readString(dataInput);
      System.out.println("s=:" +s);
/*
      
      System.out.println("start translate:" );

      for (int k=0;k<20;k++){
        FloatWritable fw= new FloatWritable(k);
        fw.write(dataout);
        Text tx = new Text("k:"+k);
         tx.write(dataout);
      }
      System.out.println("finish translate:" );
   */
      
      // done
      WritableUtils.writeVInt(dataout, 54);
      dataout.writeFloat(50.5f);
      System.out.println("14");
  
      dataout.flush();
      dataout.close();
      System.out.println("15");

    } catch (Exception x) {
      x.printStackTrace();
    }finally{
      if( socket!=null )
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
    
    }
  }

  private String createDigest(byte[] password, String data) throws IOException {
    SecretKey key = JobTokenSecretManager.createSecretKey(password);

    return SecureShuffleUtils.hashFromString(data, key);

  }

  private void readObject(Writable obj , DataInputStream inStream)  throws IOException {
    int numBytes = WritableUtils.readVInt(inStream);
    byte[] buffer;
    // For BytesWritable and Text, use the specified length to set the length
    // this causes the "obvious" translations to work. So that if you emit
    // a string "abc" from C++, it shows up as "abc".
    if (obj instanceof BytesWritable) {
      buffer = new byte[numBytes];
      inStream.readFully(buffer);
      ((BytesWritable) obj).set(buffer, 0, numBytes);
    } else if (obj instanceof Text) {
      buffer = new byte[numBytes];
      inStream.readFully(buffer);
      ((Text) obj).set(buffer);
    } else {
      obj.readFields(inStream);
    }
  }


  private void writeObject(Writable obj, DataOutputStream stream)
      throws IOException {
    // For Text and BytesWritable, encode them directly, so that they end up
    // in C++ as the natural translations.
    DataOutputBuffer buffer = new DataOutputBuffer();
System.out.println("writeObject");
    if (obj instanceof Text) {
      Text t = (Text) obj;
      int len = t.getLength();
      System.out.println("writeObject Text:"+len);
      WritableUtils.writeVLong(stream, len);
      stream.flush();
      System.out.println("flush:"+System.currentTimeMillis());

      stream.write(t.getBytes(), 0, len);
      stream.flush();
      System.out.println("flush2:"+System.currentTimeMillis());

    } else if (obj instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) obj;
      int len = b.getLength();
      System.out.println("writeObject BytesWritable:"+len);
      WritableUtils.writeVLong(stream, len);
      stream.write(b.getBytes(), 0, len);
    } else {
      buffer.reset();
      obj.write(buffer);
      int length = buffer.getLength();
      System.out.println("writeObject :"+length);

      WritableUtils.writeVInt(stream, length);
      stream.write(buffer.getData(), 0, length);
    }
    stream.flush();
    System.out.println("flush3:"+System.currentTimeMillis());

  }

}
