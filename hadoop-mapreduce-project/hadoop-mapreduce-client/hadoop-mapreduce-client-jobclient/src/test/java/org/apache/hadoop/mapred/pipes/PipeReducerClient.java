package org.apache.hadoop.mapred.pipes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;

public class PipeReducerClient {

  public static void main(String[] args) {
    PipeReducerClient client = new PipeReducerClient();
    client.binaryProtocolStub();
  }

  public void binaryProtocolStub() {
    System.out.println("1");
    Socket socket =null;
    try {

      int port = Integer 
          .parseInt(System.getenv("mapreduce.pipes.command.port"));
      
      java.net.InetAddress addr = java.net.InetAddress.getLocalHost();
      
      
      socket = new Socket(addr.getHostName(), port);
      InputStream input = socket.getInputStream();
      OutputStream output = socket.getOutputStream();
      System.out.println("5");

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
        String value= Text.readString(dataInput);
        System.out.println("key:"+key + " value:"+value);
      }


// RUN_MAP.code
      //should be 3

      i = WritableUtils.readVInt(dataInput);
      System.out.println("reduce code:"+i);      
      i = WritableUtils.readVInt(dataInput);
      System.out.println("reduce :"+i);      

      i = WritableUtils.readVInt(dataInput);
      System.out.println("pipeout :"+i);      

   

      
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

}
