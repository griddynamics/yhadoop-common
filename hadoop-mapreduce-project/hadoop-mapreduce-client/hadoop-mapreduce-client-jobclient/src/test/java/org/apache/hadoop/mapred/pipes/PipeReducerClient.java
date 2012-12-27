package org.apache.hadoop.mapred.pipes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;

public class PipeReducerClient {

  public static void main(String[] args) {
    PipeReducerClient client = new PipeReducerClient();
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


      //should be 5
  //RUN_REDUCE
      i = WritableUtils.readVInt(dataInput);
      i = WritableUtils.readVInt(dataInput);
      i = WritableUtils.readVInt(dataInput);
// reduce key
      i = WritableUtils.readVInt(dataInput);
      // value of reduce key
      BooleanWritable value= new BooleanWritable();
      readObject(value, dataInput);
      System.out.println("reducer key :"+value);  
      // reduce value code:
      i = WritableUtils.readVInt(dataInput);
      Text txt= new Text();
      // vakue
      readObject(txt, dataInput);
      System.out.println("reduce value  :"+txt);      

      
      // done
      WritableUtils.writeVInt(dataout, 54);
  
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
