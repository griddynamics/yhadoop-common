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
package org.apache.hadoop.ipc;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocksSocketFactory;
import org.apache.hadoop.net.StandardSocketFactory;
import org.junit.Test;

public class TestSocketFactory {

  private volatile int port;

  @Test
  public void testSocketFactoryAsKeyInMap() throws Exception {
    Map<SocketFactory, Integer> dummyCache = new HashMap<SocketFactory, Integer>();
    int toBeCached1 = 1;
    int toBeCached2 = 2;
    Configuration conf = new Configuration();
    conf.set("hadoop.rpc.socket.factory.class.default",
        "org.apache.hadoop.ipc.TestSocketFactory$DummySocketFactory");
    final SocketFactory dummySocketFactory = NetUtils
        .getDefaultSocketFactory(conf);
    dummyCache.put(dummySocketFactory, toBeCached1);

    conf.set("hadoop.rpc.socket.factory.class.default",
        "org.apache.hadoop.net.StandardSocketFactory");
    final SocketFactory defaultSocketFactory = NetUtils
        .getDefaultSocketFactory(conf);
    dummyCache.put(defaultSocketFactory, toBeCached2);

    Assert
        .assertEquals("The cache contains two elements", 2, dummyCache.size());
    Assert.assertEquals("Equals of both socket factory shouldn't be same",
        defaultSocketFactory.equals(dummySocketFactory), false);

    assertSame(toBeCached2, dummyCache.remove(defaultSocketFactory));
    dummyCache.put(defaultSocketFactory, toBeCached2);
    assertSame(toBeCached1, dummyCache.remove(dummySocketFactory));

  }

  /**
   * A dummy socket factory class that extends the StandardSocketFactory.
   */
  static class DummySocketFactory extends StandardSocketFactory {

  }

  @Test
  public void testSocksSocketFactory() throws Exception {
    ServerThread serverThread = new ServerThread();
    Thread server = new Thread(serverThread);
    server.start();
    while (!serverThread.isReady()) {
      Thread.sleep(10);
    }
    try {

      InetAddress addr = InetAddress.getLocalHost();
      SocksSocketFactory socketFactory = new SocksSocketFactory();
      Socket socket = socketFactory.createSocket(addr, port);
      checkSoket(socket);
      socket.close();

      socket = socketFactory.createSocket(addr, port,
          InetAddress.getLocalHost(), 0);
      checkSoket(socket);
      socket.close();

      socket = socketFactory.createSocket("localhost", port);
      checkSoket(socket);
      socket.close();

      socket = socketFactory.createSocket("localhost", port,
          InetAddress.getLocalHost(), 0);
      checkSoket(socket);
      socket.close();

    } finally {
      serverThread.stop();
    }
  }

  @Test
  public void testProxy() throws Exception {
    SocksSocketFactory templateWithoutProxy = new SocksSocketFactory();
    Proxy proxy = new Proxy(Type.SOCKS, InetSocketAddress.createUnresolved(
        "localhost", 0));

    SocksSocketFactory templateWithProxy = new SocksSocketFactory(proxy);
    assertFalse(templateWithoutProxy.equals(templateWithProxy));

    Configuration conf = new Configuration();
    conf.set("hadoop.socks.server", "localhost:0");

    templateWithoutProxy.setConf(conf);
    assertTrue(templateWithoutProxy.equals(templateWithProxy));

  }

  private void checkSoket(Socket socket) throws Exception {
    BufferedReader input = new BufferedReader(new InputStreamReader(
        socket.getInputStream()));
    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    out.writeBytes("test\n");
    String answer = input.readLine();

    System.out.println("ss:" + answer);
    assertEquals("TEST", answer);

  }

  private class ServerThread implements Runnable {

    private volatile boolean works = true;
    private ServerSocket testSocket;
    private volatile boolean ready = false;

    @Override
    public void run() {
      try {
        testSocket = new ServerSocket(0);
        port = testSocket.getLocalPort();
        System.out.println("port:" + port);
        ready = true;
        while (works) {
          try {
            Socket connectionSocket = testSocket.accept();
            BufferedReader input = new BufferedReader(new InputStreamReader(
                connectionSocket.getInputStream()));
            DataOutputStream out = new DataOutputStream(
                connectionSocket.getOutputStream());
            String inData = input.readLine();

            String outData = inData.toUpperCase() + "\n";
            out.writeBytes(outData);
          } catch (SocketException e) {
            ;
          }
        }
      } catch (IOException e) {
        ;
      }

    }

    public void stop() {
      try {
        testSocket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      works = false;
    }

    public boolean isReady() {
      return ready;
    }
  }

}
