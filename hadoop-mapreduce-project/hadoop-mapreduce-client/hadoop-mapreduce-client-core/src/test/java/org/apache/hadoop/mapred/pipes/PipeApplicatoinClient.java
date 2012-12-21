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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;

public class PipeApplicatoinClient {

  public static void main(String[] args) {
    PipeApplicatoinClient client = new PipeApplicatoinClient();
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
      System.out.println("port1:" + port);

      // try to read
      DataInputStream dataInput = new DataInputStream(input);

      int i = WritableUtils.readVInt(dataInput);

      String str = Text.readString(dataInput);

      Text.readString(dataInput);

      DataOutputStream dataout = new DataOutputStream(output);
      WritableUtils.writeVInt(dataout, 57);
      String s = createDigest("password".getBytes(), str);

      Text.writeString(dataout, s);
      System.out.println("security ok"+System.currentTimeMillis());

      // start

      i = WritableUtils.readVInt(dataInput);
      System.out.println("code:" + i+":"+System.currentTimeMillis());
      i = WritableUtils.readVInt(dataInput);

      System.out.println("version:" + i+":"+System.currentTimeMillis());
  // get conf
      // should be MessageType.SET_JOB_CONF.code
      i = WritableUtils.readVInt(dataInput);
          // array length

      int j = WritableUtils.readVInt(dataInput);
      System.out.println("i:"+i+" j:"+j);
      for (i = 0; i < j; i++) {
        String key = Text.readString(dataInput);
        i++;
        String value = Text.readString(dataInput);
        System.out.println("key:" + key + " value:" + value);
      }
      System.out.println("cfg ok:" + ":"+System.currentTimeMillis());

      // output code
      WritableUtils.writeVInt(dataout, 50);

 //     iw.write(dataout);
      System.out.println("2!"+":"+System.currentTimeMillis());

      writeObject(new Text("key"), dataout);
      System.out.println("3!"+":"+System.currentTimeMillis());
  
      writeObject(new Text("value"), dataout);
      dataout.flush();
      System.out.println("4!"+":"+System.currentTimeMillis());
      // STATUS

      WritableUtils.writeVInt(dataout, 52);
      Text.writeString(dataout, "PROGRESS");
      dataout.flush();

      // progress
      WritableUtils.writeVInt(dataout, 53);
      dataout.writeFloat(50.5f);
      // register cunter
      WritableUtils.writeVInt(dataout, 55);
      // id
      WritableUtils.writeVInt(dataout, 0);
      Text.writeString(dataout, "group");
      Text.writeString(dataout, "name");
      // increment counter
      System.out.println("10");
      WritableUtils.writeVInt(dataout, 56);
      WritableUtils.writeVInt(dataout, 0);

      WritableUtils.writeVLong(dataout, 2);

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
