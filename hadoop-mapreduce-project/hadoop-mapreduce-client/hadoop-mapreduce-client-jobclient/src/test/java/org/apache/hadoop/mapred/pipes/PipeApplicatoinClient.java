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
        Text.readString(dataInput);
        i++;
         Text.readString(dataInput);
      }
      System.out.println("cfg 1s ok:" + ":"+System.currentTimeMillis());

      // output code
      WritableUtils.writeVInt(dataout, 50);
      IntWritable wt= new IntWritable();
      wt.set(123);
      writeObject(wt, dataout);
      writeObject(new Text("value"), dataout);
      
    //  PARTITIONED_OUTPUT
      WritableUtils.writeVInt(dataout, 51);
      WritableUtils.writeVInt(dataout, 0);
      writeObject(wt, dataout);
      writeObject(new Text("value"), dataout);

      
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

      // map item
     i=  WritableUtils.readVInt(dataInput);
     System.out.println("map item:"+i);
     IntWritable iw= new IntWritable();
     readObject(iw, dataInput);
     System.out.println("map object:"+iw);
     Text txt= new Text();
     readObject(txt, dataInput);
     System.out.println("map object1:"+txt);

      // done

      WritableUtils.writeVInt(dataout, 54);

      dataout.writeFloat(50.5f);
      
  
      dataout.flush();
      dataout.close();

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
    if (obj instanceof Text) {
      Text t = (Text) obj;
      int len = t.getLength();
      WritableUtils.writeVLong(stream, len);
      stream.flush();

      stream.write(t.getBytes(), 0, len);
      stream.flush();

    } else if (obj instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) obj;
      int len = b.getLength();
      WritableUtils.writeVLong(stream, len);
      stream.write(b.getBytes(), 0, len);
    } else {
      buffer.reset();
      obj.write(buffer);
      int length = buffer.getLength();

      WritableUtils.writeVInt(stream, length);
      stream.write(buffer.getData(), 0, length);
    }
    stream.flush();

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

}
