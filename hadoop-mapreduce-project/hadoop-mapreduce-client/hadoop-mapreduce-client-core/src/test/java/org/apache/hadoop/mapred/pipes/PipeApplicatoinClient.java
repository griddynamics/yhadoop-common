package org.apache.hadoop.mapred.pipes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;


public class PipeApplicatoinClient {

  public static void main(String[] args) {
    PipeApplicatoinClient client= new PipeApplicatoinClient();
    client.binaryProtocolStub();
  }

  public void binaryProtocolStub(){
       try {

      int port = Integer
          .parseInt(System.getenv("mapreduce.pipes.command.port"));
      Socket socket = new Socket("localhost", port);
      InputStream input = socket.getInputStream();
      OutputStream output = socket.getOutputStream();
      System.out.println("port1:"     +port);

      // try to read
      DataInputStream dataInput = new DataInputStream(input);
      int i=WritableUtils.readVInt(dataInput);
      System.out.println("i:" +i);

      String str =   Text.readString(dataInput);
      System.out.println("str:" +str);

      String str1=   Text.readString(dataInput);
      System.out.println("str2:" +str1);

      DataOutputStream dataout= new DataOutputStream(output);
      WritableUtils.writeVInt(dataout,  57);
      String s=createDigest("password".getBytes(),str);
      System.out.println("s:" +s);

      Text.writeString(dataout,s);



    } catch (Exception x) {
      x.printStackTrace();
    }
  }
  
  public  String createDigest(byte[] password, String data)
      throws IOException {
    SecretKey key = JobTokenSecretManager.createSecretKey(password);
    
    return SecureShuffleUtils.hashFromString(data, key);
    
  }
  
  
 
  
  
}
