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
package org.apache.hadoop.oncrpc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * A simple UDP based RPC client which just sends one request to a server.
 */
public class SimpleUdpClient {
  protected final String host;
  protected final int port;
  protected final XDR request;
  protected final boolean oneShot;

  public SimpleUdpClient(String host, int port, XDR request) {
    this(host, port, request, true);
  }

  public SimpleUdpClient(String host, int port, XDR request, Boolean oneShot) {
    this.host = host;
    this.port = port;
    this.request = request;
    this.oneShot = oneShot;
  }

  public void run() throws IOException {
    DatagramSocket clientSocket = new DatagramSocket();
    InetAddress IPAddress = InetAddress.getByName(host);
    byte[] sendData = request.getBytes();
    byte[] receiveData = new byte[65535];

    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
        IPAddress, port);
    clientSocket.send(sendPacket);
    DatagramPacket receivePacket = new DatagramPacket(receiveData,
        receiveData.length);
    clientSocket.receive(receivePacket);

    // Check reply status
    XDR xdr = new XDR();
    xdr.writeFixedOpaque(Arrays.copyOfRange(receiveData, 0,
        receivePacket.getLength()));
    RpcReply reply = RpcReply.read(xdr);
    if (reply.getState() != RpcReply.ReplyState.MSG_ACCEPTED) {
      throw new IOException("Request failed: " + reply.getState());
    }

    clientSocket.close();
  }
}
