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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import static org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION;
import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public abstract class Server {
  private final boolean authorize;
  private List<AuthMethod> enabledAuthMethods;
  private RpcSaslProto negotiateResponse;
  private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();
  
  public void addTerseExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addTerseExceptions(exceptionClass);
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling
   * e.g., terse exception group for concise logging messages
   */
  static class ExceptionsHandler {
    private volatile Set<String> terseExceptions = new HashSet<String>();

    /**
     * Add exception class so server won't log its stack trace.
     * Modifying the terseException through this method is thread safe.
     *
     * @param exceptionClass exception classes 
     */
    void addTerseExceptions(Class<?>... exceptionClass) {

      // Make a copy of terseException for performing modification
      final HashSet<String> newSet = new HashSet<String>(terseExceptions);

      // Add all class names into the HashSet
      for (Class<?> name : exceptionClass) {
        newSet.add(name.toString());
      }
      // Replace terseException set
      terseExceptions = Collections.unmodifiableSet(newSet);
    }

    boolean isTerse(Class<?> t) {
      return terseExceptions.contains(t.toString());
    }
  }

  
  /**
   * If the user accidentally sends an HTTP GET to an IPC port, we detect this
   * and send back a nicer response.
   */
  private static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap(
      "GET ".getBytes());
  
  /**
   * An HTTP response to send back if we detect an HTTP request to our IPC
   * port.
   */
  static final String RECEIVED_HTTP_REQ_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n" +
    "Content-type: text/plain\r\n\r\n" +
    "It looks like you are making an HTTP request to a Hadoop IPC port. " +
    "This is not the correct port for the web interface on this daemon.\r\n";

  /**
   * Initial and max size of response buffer
   */
  static int INITIAL_RESP_BUF_SIZE = 10240;
  
  static class RpcKindMapValue {
    final Class<? extends Writable> rpcRequestWrapperClass;
    final RpcInvoker rpcInvoker;
    RpcKindMapValue (Class<? extends Writable> rpcRequestWrapperClass,
          RpcInvoker rpcInvoker) {
      this.rpcInvoker = rpcInvoker;
      this.rpcRequestWrapperClass = rpcRequestWrapperClass;
    }   
  }
  static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new
      HashMap<RPC.RpcKind, RpcKindMapValue>(4);
  
  

  /**
   * Register a RPC kind and the class to deserialize the rpc request.
   * 
   * Called by static initializers of rpcKind Engines
   * @param rpcKind
   * @param rpcRequestWrapperClass - this class is used to deserialze the
   *  the rpc request.
   *  @param rpcInvoker - use to process the calls on SS.
   */
  
  public static void registerProtocolEngine(RPC.RpcKind rpcKind, 
          Class<? extends Writable> rpcRequestWrapperClass,
          RpcInvoker rpcInvoker) {
    RpcKindMapValue  old = 
        rpcKindMap.put(rpcKind, new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker));
    if (old != null) {
      rpcKindMap.put(rpcKind, old);
      throw new IllegalArgumentException("ReRegistration of rpcKind: " +
          rpcKind);      
    }
    LOG.debug("rpcKind=" + rpcKind + 
        ", rpcRequestWrapperClass=" + rpcRequestWrapperClass + 
        ", rpcInvoker=" + rpcInvoker);
  }
  
  public Class<? extends Writable> getRpcRequestWrapper(
      RpcKindProto rpcKind) {
    if (rpcRequestClass != null)
       return rpcRequestClass;
    RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
    return (val == null) ? null : val.rpcRequestWrapperClass; 
  }
  
  public static RpcInvoker  getRpcInvoker(RPC.RpcKind rpcKind) {
    RpcKindMapValue val = rpcKindMap.get(rpcKind);
    return (val == null) ? null : val.rpcInvoker; 
  }
  

  public static final Log LOG = LogFactory.getLog(Server.class);
  public static final Log AUDITLOG = 
    LogFactory.getLog("SecurityLogger."+Server.class.getName());
  private static final String AUTH_FAILED_FOR = "Auth failed for ";
  private static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  
  private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

  private static final Map<String, Class<?>> PROTOCOL_CACHE = 
    new ConcurrentHashMap<String, Class<?>>();
  
  static Class<?> getProtocolClass(String protocolName, Configuration conf) 
  throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }
  
  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
  
  /**
   * Returns the currently active RPC call's sequential ID number.  A negative
   * call ID indicates an invalid value, such as if there is no currently active
   * RPC call.
   * 
   * @return int sequential ID number of currently active RPC call
   */
  static int getCallId() {
    Call call = CurCall.get();
    return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
  }

  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.getHostInetAddress();
    }
    return null;
  }
  
  /**
   * Returns the clientId from the current RPC request
   */
  public static byte[] getClientId() {
    Call call = CurCall.get();
    return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
  }
  
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /** Returns the RPC remote user when invoked inside an RPC.  Note this
   *  may be different than the current user if called within another doAs
   *  @return connection's UGI or null if not an RPC
   */
  public static UserGroupInformation getRemoteUser() {
    Call call = CurCall.get();
    return (call != null) ? call.connection.user : null;
  }
 
  /** Return true if the invocation was through an RPC.
   */
  public static boolean isRpcInvocation() {
    return CurCall.get() != null;
  }

  private String bindAddress; 
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private int readThreads;                        // number of read threads
  private Class<? extends Writable> rpcRequestClass;   // class used for deserializing the rpc request
  private int maxIdleTime;                        // the maximum idle time after 
                                                  // which a client may be disconnected
  private int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle 
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of 
                                                  // connections to nuke
                                                  //during a cleanup
  
  protected RpcMetrics rpcMetrics;
  protected RpcDetailedMetrics rpcDetailedMetrics;
  
  private Configuration conf;
  private String portRangeConfig = null;
  private SecretManager<TokenIdentifier> secretManager;
  private ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager();

  private int maxQueueSize;
  private final int maxRespSize;
  private int socketSendBufferSize;
  private final int maxDataLength;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private boolean running = true;         // true while server runs
  private BlockingQueue<Call> callQueue; // queued calls

  private List<Connection> connectionList = 
    Collections.synchronizedList(new LinkedList<Connection>());
  //maintain a list
  //of client connections
  private Listener listener = null;
  private Responder responder = null;
  private int numConnections = 0;
  private Handler[] handlers = null;

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address, 
                          int backlog) throws IOException {
    bind(socket, address, backlog, null, null);
  }

  public static void bind(ServerSocket socket, InetSocketAddress address, 
      int backlog, Configuration conf, String rangeConf) throws IOException {
    try {
      IntegerRanges range = null;
      if (rangeConf != null) {
        range = conf.getRange(rangeConf, "");
      }
      if (range == null || range.isEmpty() || (address.getPort() != 0)) {
        socket.bind(address, backlog);
      } else {
        for (Integer port : range) {
          if (socket.isBound()) break;
          try {
            InetSocketAddress temp = new InetSocketAddress(address.getAddress(),
                port);
            socket.bind(temp, backlog);
          } catch(BindException e) {
            //Ignored
          }
        }
        if (!socket.isBound()) {
          throw new BindException("Could not find a free port in "+range);
        }
      }
    } catch (SocketException e) {
      throw NetUtils.wrapException(null,
          0,
          address.getHostName(),
          address.getPort(), e);
    }
  }
  
  /**
   * Returns a handle to the rpcMetrics (required in tests)
   * @return rpc metrics
   */
  @VisibleForTesting
  public RpcMetrics getRpcMetrics() {
    return rpcMetrics;
  }

  @VisibleForTesting
  public RpcDetailedMetrics getRpcDetailedMetrics() {
    return rpcDetailedMetrics;
  }
  
  @VisibleForTesting
  Iterable<? extends Thread> getHandlers() {
    return Arrays.asList(handlers);
  }

  @VisibleForTesting
  List<Connection> getConnections() {
    return connectionList;
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server.
   */
  public void refreshServiceAcl(Configuration conf, PolicyProvider provider) {
    serviceAuthorizationManager.refresh(conf, provider);
  }

  /**
   * Returns a handle to the serviceAuthorizationManager (required in tests)
   * @return instance of ServiceAuthorizationManager for this server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public ServiceAuthorizationManager getServiceAuthorizationManager() {
    return serviceAuthorizationManager;
  }

  /** A call queued for handling. */
  private static class Call {
    private final int callId;             // the client's call id
    private final Writable rpcRequest;    // Serialized Rpc request from client
    private final Connection connection;  // connection to client
    private long timestamp;               // time received when response is null
                                          // time served when response is not null
    private ByteBuffer rpcResponse;       // the response for this call
    private final RPC.RpcKind rpcKind;
    private final byte[] clientId;

    private Call(int id, Writable param, Connection connection) {
      this(id, param, connection, RPC.RpcKind.RPC_BUILTIN,
          RpcConstants.DUMMY_CLIENT_ID);
    }

    private Call(int id, Writable param, Connection connection,
        RPC.RpcKind kind, byte[] clientId) {
      this.callId = id;
      this.rpcRequest = param;
      this.connection = connection;
      this.timestamp = Time.now();
      this.rpcResponse = null;
      this.rpcKind = kind;
      this.clientId = clientId;
    }
    
    @Override
    public String toString() {
      return rpcRequest + " from " + connection + " Call#" + callId;
    }

    public void setResponse(ByteBuffer response) {
      this.rpcResponse = response;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {
    
    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private Reader[] readers = null;
    private int currentReader = 0;
    private InetSocketAddress address; //the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between 
                                          //two cleanup runs
    private int backlogLength = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    
    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();
      readers = new Reader[readThreads];
      for (int i = 0; i < readThreads; i++) {
        Reader reader = new Reader(
            "Socket Reader #" + (i + 1) + " for port " + port);
        readers[i] = reader;
        reader.start();
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }
    
    private class Reader extends Thread {
      private volatile boolean adding = false;
      private final Selector readSelector;

      Reader(String name) throws IOException {
        super(name);

        this.readSelector = Selector.open();
      }
      
      @Override
      public void run() {
        LOG.info("Starting " + getName());
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (IOException ioe) {
            LOG.error("Error closing read selector in " + this.getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop() {
        while (running) {
          SelectionKey key = null;
          try {
            readSelector.select();
            while (adding) {
              this.wait(1000);
            }              

            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
              key = null;
            }
          } catch (InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(getName() + " unexpectedly interrupted", e);
            }
          } catch (IOException ex) {
            LOG.error("Error in Reader", ex);
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel
       * to be registered with readSelector. If it was waiting in select()
       * the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait
       * in while(adding) for finishAdd call
       */
      public void startAdd() {
        adding = true;
        readSelector.wakeup();
      }
      
      public synchronized SelectionKey registerChannel(SocketChannel channel)
                                                          throws IOException {
          return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd() {
        adding = false;
        this.notify();        
      }

      void shutdown() {
        assert !running;
        readSelector.wakeup();
        try {
          join();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = Time.now();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = Time.now();
      }
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        SelectionKey key = null;
        try {
          getSelector().select();
          Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException e) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          cleanupConnections(true);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException e) { }

        selector= null;
        acceptChannel= null;
        
        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
          closeConnection(c);
          c = null;
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }
    
    void doAccept(SelectionKey key) throws IOException,  OutOfMemoryError {
      Connection c = null;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel channel;
      while ((channel = server.accept()) != null) {

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        
        Reader reader = getReader();
        try {
          reader.startAdd();
          SelectionKey readKey = reader.registerChannel(channel);
          c = new Connection(readKey, channel, Time.now());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          if (LOG.isDebugEnabled())
            LOG.debug("Server connection from " + c.toString() +
                "; # active connections: " + numConnections +
                "; # queued calls: " + callQueue.size());          
        } finally {
          reader.finishAdd(); 
        }
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(Time.now());
      
      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        LOG.info(getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (Exception e) {
        // log stack trace for "interesting" exceptions not sent to client
        LOG.info(getName() + ": readAndProcess from client " +
            c.getHostAddress() + " threw exception [" + e + "]",
            (e instanceof WrappedRpcServerException) ? null : e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + ": disconnecting client " + 
                    c + ". Number of active connections: "+
                    numConnections);
        closeConnection(c);
        c = null;
      }
      else {
        c.setLastContact(Time.now());
      }
    }   

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
      for (Reader r : readers) {
        r.shutdown();
      }
    }
    
    synchronized Selector getSelector() { return selector; }
    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private final Selector writeSelector;
    private int pending;         // connections waiting to register
    
    final static int PURGE_INTERVAL = 900000; // 15mins

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      try {
        doRunLoop();
      } finally {
        LOG.info("Stopping " + this.getName());
        try {
          writeSelector.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close write selector in " + this.getName(), ioe);
        }
      }
    }
    
    private void doRunLoop() {
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = Time.now();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if(LOG.isDebugEnabled()) {
            LOG.debug("Checking for old call responses.");
          }
          ArrayList<Call> calls;
          
          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }
          
          for(Call call : calls) {
            try {
              doPurge(call, now);
            } catch (IOException e) {
              LOG.warn("Error in purging old calls " + e);
            }
          }
        } catch (OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          LOG.warn("Exception in Responder", e);
        }
      }
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue 
    // for a long time.
    //
    private void doPurge(Call call, long now) throws IOException {
      LinkedList<Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<Call> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(LinkedList<Call> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements = 0;
      Call call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to " + call);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.rpcResponse);
          if (numBytes < 0) {
            return true;
          }
          if (!call.rpcResponse.hasRemaining()) {
            //Clear out the response buffer so it can be collected
            call.rpcResponse = null;
            call.connection.decRpcCount();
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to " + call
                  + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then 
            // insert in Selector queue. 
            //
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = Time.now();
              
              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call 
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to " + call
                  + " Wrote partial " + numBytes + " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  @InterfaceAudience.Private
  public static enum AuthProtocol {
    NONE(0),
    SASL(-33);
    
    public final int callId;
    AuthProtocol(int callId) {
      this.callId = callId;
    }
    
    static AuthProtocol valueOf(int callId) {
      for (AuthProtocol authType : AuthProtocol.values()) {
        if (authType.callId == callId) {
          return authType;
        }
      }
      return null;
    }
  };
  
  /**
   * Wrapper for RPC IOExceptions to be returned to the client.  Used to
   * let exceptions bubble up to top of processOneRpc where the correct
   * callId can be associated with the response.  Also used to prevent
   * unnecessary stack trace logging if it's not an internal server error. 
   */
  @SuppressWarnings("serial")
  private static class WrappedRpcServerException extends RpcServerException {
    private final RpcErrorCodeProto errCode;
    public WrappedRpcServerException(RpcErrorCodeProto errCode, IOException ioe) {
      super(ioe.toString(), ioe);
      this.errCode = errCode;
    }
    public WrappedRpcServerException(RpcErrorCodeProto errCode, String message) {
      this(errCode, new RpcServerException(message));
    }
    @Override
    public RpcErrorCodeProto getRpcErrorCodeProto() {
      return errCode;
    }
    @Override
    public String toString() {
      return getCause().toString();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private boolean connectionHeaderRead = false; // connection  header is read?
    private boolean connectionContextRead = false; //if connection context that
                                            //follows connection header is read

    private SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    private LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    private InetAddress addr;
    
    IpcConnectionContextProto connectionContext;
    String protocolName;
    SaslServer saslServer;
    private AuthMethod authMethod;
    private AuthProtocol authProtocol;
    private boolean saslContextEstablished;
    private boolean skipInitialSaslHandshake;
    private ByteBuffer connectionHeaderBuf = null;
    private ByteBuffer unwrappedData;
    private ByteBuffer unwrappedDataLengthBuffer;
    private int serviceClass;
    
    UserGroupInformation user = null;
    public UserGroupInformation attemptingUser = null; // user name before auth

    // Fake 'call' for failed authorization response
    private static final int AUTHORIZATION_FAILED_CALLID = -1;
    
    private final Call authFailedCall = 
      new Call(AUTHORIZATION_FAILED_CALLID, null, this);
    private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();
    
    private final Call saslCall = new Call(AuthProtocol.SASL.callId, null, this);
    private final ByteArrayOutputStream saslResponse = new ByteArrayOutputStream();
    
    private boolean sentNegotiate = false;
    private boolean useWrap = false;
    
    public Connection(SelectionKey key, SocketChannel channel, 
                      long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }   

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort; 
    }
    
    public String getHostAddress() {
      return hostAddress;
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }
    
    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }
    
    /* Decrement the outstanding RPC count */
    private void decRpcCount() {
      rpcCount--;
    }
    
    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }
    
    private boolean timedOut(long currentTime) {
      if (isIdle() && currentTime -  lastContact > maxIdleTime)
        return true;
      return false;
    }
    
    private UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws InvalidToken, AccessControlException {
      if (authMethod == AuthMethod.TOKEN) {
        TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new AccessControlException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId);
      }
    }

    private RpcSaslProto saslReadAndProcess(DataInputStream dis) throws
        WrappedRpcServerException, InterruptedException {
      if (saslContextEstablished) {
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            new SaslException("Negotiation is already complete"));
      }
      RpcSaslProto saslResponse = null;
      try {
        try {
          saslResponse = processSaslMessage(dis);
        } catch (IOException e) {
          IOException sendToClient = e;
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof InvalidToken) {
              sendToClient = (InvalidToken) cause;
              break;
            }
            cause = cause.getCause();
          }
          rpcMetrics.incrAuthenticationFailures();
          String clientIP = this.toString();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser +
            " (" + e.getLocalizedMessage() + ")");
          throw sendToClient;
        }
        
        if (saslServer != null && saslServer.isComplete()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Negotiated QoP is "
                + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server successfully authenticated client: " + user);
          }
          rpcMetrics.incrAuthenticationSuccesses();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      } catch (WrappedRpcServerException wrse) { // don't re-wrap
        throw wrse;
      } catch (IOException ioe) {
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ioe);
      }
      return saslResponse; 
    }
    
    private RpcSaslProto processSaslMessage(DataInputStream dis)
        throws IOException, InterruptedException {
      final RpcSaslProto saslMessage =
          decodeProtobufFromStream(RpcSaslProto.newBuilder(), dis);
      RpcSaslProto saslResponse = null;
      final SaslState state = saslMessage.getState(); // required      
      switch (state) {
        case NEGOTIATE: {
          if (sentNegotiate) {
            throw new AccessControlException(
                "Client already attempted negotiation");
          }
          saslResponse = buildSaslNegotiateResponse();
          break;
        }
        case INITIATE: {
          if (saslMessage.getAuthsCount() != 1) {
            throw new SaslException("Client mechanism is malformed");
          }
          String authMethodName = saslMessage.getAuths(0).getMethod();
          authMethod = createSaslServer(authMethodName);
          if (authMethod == null) { // the auth method is not supported
            if (sentNegotiate) {
              throw new AccessControlException(
                  authMethodName + " authentication is not enabled."
                      + "  Available:" + enabledAuthMethods);
            }
            saslResponse = buildSaslNegotiateResponse();
            break;
          }
          // fallthru to process sasl token
        }
        case RESPONSE: {
          if (!saslMessage.hasToken()) {
            throw new SaslException("Client did not send a token");
          }
          byte[] saslToken = saslMessage.getToken().toByteArray();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.length
                + " for processing by saslServer.evaluateResponse()");
          }
          saslToken = saslServer.evaluateResponse(saslToken);
          saslResponse = buildSaslResponse(
              saslServer.isComplete() ? SaslState.SUCCESS : SaslState.CHALLENGE,
              saslToken);
          break;
        }
        default:
          throw new SaslException("Client sent unsupported state " + state);
      }
      return saslResponse;
    }
    
    private RpcSaslProto buildSaslResponse(SaslState state, byte[] replyToken) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will send " + state + " token of size "
            + ((replyToken != null) ? replyToken.length : null)
            + " from saslServer.");
      }
      RpcSaslProto.Builder response = RpcSaslProto.newBuilder();
      response.setState(state);
      if (replyToken != null) {
        response.setToken(ByteString.copyFrom(replyToken));
      }
      return response.build();
    }
    
    private void doSaslReply(Message message) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending sasl message "+message);
      }
      setupResponse(saslResponse, saslCall,
          RpcStatusProto.SUCCESS, null,
          new RpcResponseWrapper(message), null, null);
      responder.doRespond(saslCall);
    }
    
    private void doSaslReply(Exception ioe) throws IOException {
      setupResponse(authFailedResponse, authFailedCall,
          RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          null, ioe.getClass().getName(), ioe.getLocalizedMessage());
      responder.doRespond(authFailedCall);
    }
    
    private void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
        } catch (SaslException ignored) {
        }
      }
    }

    private void checkDataLength(int dataLength) throws IOException {
      if (dataLength < 0) {
        String error = "Unexpected data length " + dataLength +
                       "!! from " + getHostAddress();
        LOG.warn(error);
        throw new IOException(error);
      } else if (dataLength > maxDataLength) {
        String error = "Requested data length " + dataLength +
              " is longer than maximum configured RPC length " + 
            maxDataLength + ".  RPC came from " + getHostAddress();
        LOG.warn(error);
        throw new IOException(error);
      }
    }

    public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */    
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0) 
            return count;
        }
        
        if (!connectionHeaderRead) {
          //Every connection is expected to send the header.
          if (connectionHeaderBuf == null) {
            connectionHeaderBuf = ByteBuffer.allocate(3);
          }
          count = channelRead(channel, connectionHeaderBuf);
          if (count < 0 || connectionHeaderBuf.remaining() > 0) {
            return count;
          }
          int version = connectionHeaderBuf.get(0);
          // TODO we should add handler for service class later
          this.setServiceClass(connectionHeaderBuf.get(1));
          dataLengthBuffer.flip();
          
          // Check if it looks like the user is hitting an IPC port
          // with an HTTP GET - this is a common error, so we can
          // send back a simple string indicating as much.
          if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
            setupHttpRequestOnIpcPortResponse();
            return -1;
          }
          
          if (!RpcConstants.HEADER.equals(dataLengthBuffer)
              || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " + 
                     hostAddress + ":" + remotePort +
                     " got version " + version + 
                     " expected version " + CURRENT_VERSION);
            setupBadVersionResponse(version);
            return -1;
          }
          
          // this may switch us into SIMPLE
          authProtocol = initializeAuthContext(connectionHeaderBuf.get(2));          
          
          dataLengthBuffer.clear();
          connectionHeaderBuf = null;
          connectionHeaderRead = true;
          continue;
        }
        
        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();
          if ((dataLength == RpcConstants.PING_CALL_ID) && (!useWrap)) {
            // covers the !useSasl too
            dataLengthBuffer.clear();
            return 0; // ping message
          }
          checkDataLength(dataLength);
          data = ByteBuffer.allocate(dataLength);
        }
        
        count = channelRead(channel, data);
        
        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          boolean isHeaderRead = connectionContextRead;
          processRpcRequestPacket(data.array());
          data = null;
          if (!isHeaderRead) {
            continue;
          }
        } 
        return count;
      }
    }

    private AuthProtocol initializeAuthContext(int authType)
        throws IOException, InterruptedException {
      AuthProtocol authProtocol = AuthProtocol.valueOf(authType);
      if (authProtocol == null) {
        IOException ioe = new IpcException("Unknown auth protocol:" + authType);
        doSaslReply(ioe);
        throw ioe;        
      }
      boolean isSimpleEnabled = enabledAuthMethods.contains(AuthMethod.SIMPLE);
      switch (authProtocol) {
        case NONE: {
          // don't reply if client is simple and server is insecure
          if (!isSimpleEnabled) {
            IOException ioe = new AccessControlException(
                "SIMPLE authentication is not enabled."
                    + "  Available:" + enabledAuthMethods);
            doSaslReply(ioe);
            throw ioe;
          }
          break;
        }
        case SASL: {
          // switch to simple hack, but don't switch if other auths are
          // supported, ex. tokens
          if (isSimpleEnabled && enabledAuthMethods.size() == 1) {
            authProtocol = AuthProtocol.NONE;
            skipInitialSaslHandshake = true;
            doSaslReply(buildSaslResponse(SaslState.SUCCESS, null));
          }
          // else wait for a negotiate or initiate
          break;
        }
      }
      return authProtocol;
    }

    private RpcSaslProto buildSaslNegotiateResponse()
        throws IOException, InterruptedException {
      RpcSaslProto negotiateMessage = negotiateResponse;
      // accelerate token negotiation by sending initial challenge
      // in the negotiation response
      if (enabledAuthMethods.contains(AuthMethod.TOKEN)) {
        saslServer = createSaslServer(AuthMethod.TOKEN);
        byte[] challenge = saslServer.evaluateResponse(new byte[0]);
        RpcSaslProto.Builder negotiateBuilder =
            RpcSaslProto.newBuilder(negotiateResponse);
        negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
            .setChallenge(ByteString.copyFrom(challenge));
        negotiateMessage = negotiateBuilder.build();
      }
      sentNegotiate = true;
      return negotiateMessage;
    }
    
    private AuthMethod createSaslServer(String authMethodName)
        throws IOException, InterruptedException {
      AuthMethod authMethod;
      try {
        authMethod = AuthMethod.valueOf(authMethodName);
        if (!enabledAuthMethods.contains(authMethod)) {
          authMethod = null;
        }
      } catch (IllegalArgumentException iae) {
        authMethod = null;
      }
      if (authMethod != null &&
          // sasl server for tokens may already be instantiated
          (saslServer == null || authMethod != AuthMethod.TOKEN)) {
        saslServer = createSaslServer(authMethod);
      }
      return authMethod;
    }

    private SaslServer createSaslServer(AuthMethod authMethod)
        throws IOException, InterruptedException {
      return new SaslRpcServer(authMethod).create(this, secretManager);
    }
    
    /**
     * Try to set up the response to indicate that the client version
     * is incompatible with the server. This can contain special-case
     * code to speak enough of past IPC protocols to pass back
     * an exception to the caller.
     * @param clientVersion the version the caller is using 
     * @throws IOException
     */
    private void setupBadVersionResponse(int clientVersion) throws IOException {
      String errMsg = "Server IPC version " + CURRENT_VERSION +
      " cannot communicate with client version " + clientVersion;
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      
      if (clientVersion >= 9) {
        // Versions >>9  understand the normal response
        Call fakeCall =  new Call(-1, null, this);
        setupResponse(buffer, fakeCall, 
            RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
            null, VersionMismatch.class.getName(), errMsg);
        responder.doRespond(fakeCall);
      } else if (clientVersion >= 3) {
        Call fakeCall =  new Call(-1, null, this);
        // Versions 3 to 8 use older response
        setupResponseOldVersionFatal(buffer, fakeCall,
            null, VersionMismatch.class.getName(), errMsg);

        responder.doRespond(fakeCall);
      } else if (clientVersion == 2) { // Hadoop 0.18.3
        Call fakeCall =  new Call(0, null, this);
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(0); // call ID
        out.writeBoolean(true); // error
        WritableUtils.writeString(out, VersionMismatch.class.getName());
        WritableUtils.writeString(out, errMsg);
        fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
        
        responder.doRespond(fakeCall);
      }
    }
    
    private void setupHttpRequestOnIpcPortResponse() throws IOException {
      Call fakeCall =  new Call(0, null, this);
      fakeCall.setResponse(ByteBuffer.wrap(
          RECEIVED_HTTP_REQ_RESPONSE.getBytes()));
      responder.doRespond(fakeCall);
    }

    /** Reads the connection context following the connection header
     * @param dis - DataInputStream from which to read the header 
     * @throws WrappedRpcServerException - if the header cannot be
     *         deserialized, or the user is not authorized
     */ 
    private void processConnectionContext(DataInputStream dis)
        throws WrappedRpcServerException {
      // allow only one connection context during a session
      if (connectionContextRead) {
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection context already processed");
      }
      connectionContext = decodeProtobufFromStream(
          IpcConnectionContextProto.newBuilder(), dis);
      protocolName = connectionContext.hasProtocol() ? connectionContext
          .getProtocol() : null;

      UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
      if (saslServer == null) {
        user = protocolUser;
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However, 
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.TOKEN) {
            // Not allowed to doAs if token authentication is used
            throw new WrappedRpcServerException(
                RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                new AccessControlException("Authenticated user (" + user
                    + ") doesn't match what the client claims to be ("
                    + protocolUser + ")"));
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
          }
        }
      }
      authorizeConnection();
      // don't set until after authz because connection isn't established
      connectionContextRead = true;
    }
    
    /**
     * Process a RPC Request - if SASL wrapping is enabled, unwrap the
     * requests and process each one, else directly process the request 
     * @param buf - single request or SASL wrapped requests
     * @throws IOException - connection failed to authenticate or authorize,
     *   or the request could not be decoded into a Call
     * @throws InterruptedException
     */    
    private void processRpcRequestPacket(byte[] buf) throws IOException,
        InterruptedException {
      if (saslContextEstablished && useWrap) {
        if (LOG.isDebugEnabled())
          LOG.debug("Have read input token of size " + buf.length
              + " for processing by saslServer.unwrap()");        
        final byte[] plaintextData = saslServer.unwrap(buf, 0, buf.length);
        // loops over decoded data and calls processOneRpc
        unwrapPacketAndProcessRpcs(plaintextData);
      } else {
        processOneRpc(buf);
      }
    }
    
    private void unwrapPacketAndProcessRpcs(byte[] inBuf) throws IOException,
        InterruptedException {
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
          inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == RpcConstants.PING_CALL_ID) {
            if (LOG.isDebugEnabled())
              LOG.debug("Received ping message");
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData.array());
          unwrappedData = null;
        }
      }
    }
    
    /**
     * Process an RPC Request - handle connection setup and decoding of
     * request into a Call
     * @param buf - contains the RPC request header and the rpc request
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws WrappedRpcServerException - an exception to be sent back to
     *         the client that does not require verbose logging by the
     *         Listener thread
     * @throws InterruptedException
     */    
    private void processOneRpc(byte[] buf)
        throws IOException, WrappedRpcServerException, InterruptedException {
      int callId = -1;
      try {
        final DataInputStream dis =
            new DataInputStream(new ByteArrayInputStream(buf));
        final RpcRequestHeaderProto header =
            decodeProtobufFromStream(RpcRequestHeaderProto.newBuilder(), dis);
        callId = header.getCallId();
        if (LOG.isDebugEnabled()) {
          LOG.debug(" got #" + callId);
        }
        checkRpcHeaders(header);
        
        if (callId < 0) { // callIds typically used during connection setup
          processRpcOutOfBandRequest(header, dis);
        } else if (!connectionContextRead) {
          throw new WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection context not established");
        } else {
          processRpcRequest(header, dis);
        }
      } catch (WrappedRpcServerException wrse) { // inform client of error
        Throwable ioe = wrse.getCause();
        final Call call = new Call(callId, null, this);
        setupResponse(authFailedResponse, call,
            RpcStatusProto.FATAL, wrse.getRpcErrorCodeProto(), null,
            ioe.getClass().getName(), ioe.getMessage());
        responder.doRespond(call);
        throw wrse;
      }
    }

    /**
     * Verify RPC header is valid
     * @param header - RPC request header
     * @throws WrappedRpcServerException - header contains invalid values 
     */
    private void checkRpcHeaders(RpcRequestHeaderProto header)
        throws WrappedRpcServerException {
      if (!header.hasRpcOp()) {
        String err = " IPC Server: No rpc op in rpcRequestHeader";
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      if (header.getRpcOp() != 
          RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
        String err = "IPC Server does not implement rpc header operation" + 
                header.getRpcOp();
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      // If we know the rpc kind, get its class so that we can deserialize
      // (Note it would make more sense to have the handler deserialize but 
      // we continue with this original design.
      if (!header.hasRpcKind()) {
        String err = " IPC Server: No rpc kind in rpcRequestHeader";
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
    }

    /**
     * Process an RPC Request - the connection headers and context must
     * have been already read
     * @param header - RPC request header
     * @param dis - stream to request payload
     * @throws WrappedRpcServerException - due to fatal rpc layer issues such
     *   as invalid header or deserialization error. In this case a RPC fatal
     *   status response will later be sent back to client.
     * @throws InterruptedException
     */
    private void processRpcRequest(RpcRequestHeaderProto header,
        DataInputStream dis) throws WrappedRpcServerException,
        InterruptedException {
      Class<? extends Writable> rpcRequestClass = 
          getRpcRequestWrapper(header.getRpcKind());
      if (rpcRequestClass == null) {
        LOG.warn("Unknown rpc kind "  + header.getRpcKind() + 
            " from client " + getHostAddress());
        final String err = "Unknown rpc kind in rpc header"  + 
            header.getRpcKind();
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);   
      }
      Writable rpcRequest;
      try { //Read the rpc request
        rpcRequest = ReflectionUtils.newInstance(rpcRequestClass, conf);
        rpcRequest.readFields(dis);
      } catch (Throwable t) { // includes runtime exception from newInstance
        LOG.warn("Unable to read call parameters for client " +
                 getHostAddress() + "on connection protocol " +
            this.protocolName + " for rpcKind " + header.getRpcKind(),  t);
        String err = "IPC server unable to read call parameters: "+ t.getMessage();
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
      }
        
      Call call = new Call(header.getCallId(), rpcRequest, this,
          ProtoUtil.convert(header.getRpcKind()), header.getClientId()
              .toByteArray());
      callQueue.put(call);              // queue the call; maybe blocked here
      incRpcCount();  // Increment the rpc count
    }


    /**
     * Establish RPC connection setup by negotiating SASL if required, then
     * reading and authorizing the connection header
     * @param header - RPC header
     * @param dis - stream to request payload
     * @throws WrappedRpcServerException - setup failed due to SASL
     *         negotiation failure, premature or invalid connection context,
     *         or other state errors 
     * @throws IOException - failed to send a response back to the client
     * @throws InterruptedException
     */
    private void processRpcOutOfBandRequest(RpcRequestHeaderProto header,
        DataInputStream dis) throws WrappedRpcServerException, IOException,
        InterruptedException {
      final int callId = header.getCallId();
      if (callId == CONNECTION_CONTEXT_CALL_ID) {
        // SASL must be established prior to connection context
        if (authProtocol == AuthProtocol.SASL && !saslContextEstablished) {
          throw new WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection header sent during SASL negotiation");
        }
        // read and authorize the user
        processConnectionContext(dis);
      } else if (callId == AuthProtocol.SASL.callId) {
        // if client was switched to simple, ignore first SASL message
        if (authProtocol != AuthProtocol.SASL) {
          if (!skipInitialSaslHandshake) {
            throw new WrappedRpcServerException(
                RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                "SASL protocol not requested by client");
          }
          skipInitialSaslHandshake = false;
          return;
        }
        RpcSaslProto response = saslReadAndProcess(dis);
        // send back response if any, may throw IOException
        if (response != null) {
          doSaslReply(response);
        }
      } else {
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Unknown out of band call #" + callId);
      }
    }    

    /**
     * Authorize proxy users to access this server
     * @throws WrappedRpcServerException - user is not allowed to proxy
     */
    private void authorizeConnection() throws WrappedRpcServerException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.TOKEN)) {
          ProxyUsers.authorize(user, this.getHostAddress(), conf);
        }
        authorize(user, protocolName, getHostInetAddress());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully authorized " + connectionContext);
        }
        rpcMetrics.incrAuthorizationSuccesses();
      } catch (AuthorizationException ae) {
        LOG.info("Connection from " + this
            + " for protocol " + connectionContext.getProtocol()
            + " is unauthorized for user " + user);
        rpcMetrics.incrAuthorizationFailures();
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ae);
      }
    }
    
    /**
     * Decode the a protobuf from the given input stream 
     * @param builder - Builder of the protobuf to decode
     * @param dis - DataInputStream to read the protobuf
     * @return Message - decoded protobuf
     * @throws WrappedRpcServerException - deserialization failed
     */
    @SuppressWarnings("unchecked")
    private <T extends Message> T decodeProtobufFromStream(Builder builder,
        DataInputStream dis) throws WrappedRpcServerException {
      try {
        builder.mergeDelimitedFrom(dis);
        return (T)builder.build();
      } catch (Exception ioe) {
        Class<?> protoClass = builder.getDefaultInstanceForType().getClass();
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
            "Error decoding " + protoClass.getSimpleName() + ": "+ ioe);
      }
    }

    /**
     * Get service class for connection
     * @return the serviceClass
     */
    public int getServiceClass() {
      return serviceClass;
    }

    /**
     * Set service class for connection
     * @param serviceClass the serviceClass to set
     */
    public void setServiceClass(int serviceClass) {
      this.serviceClass = serviceClass;
    }

    private synchronized void close() throws IOException {
      disposeSasl();
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {
        LOG.debug("Ignoring socket shutdown exception", e);
      }
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception e) {}
      }
      try {socket.close();} catch(Exception e) {}
    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    @Override
    public void run() {
      LOG.debug(getName() + ": starting");
      SERVER.set(Server.this);
      ByteArrayOutputStream buf = 
        new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
      while (running) {
        try {
          final Call call = callQueue.take(); // pop the queue; maybe blocked here
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": " + call + " for RpcKind " + call.rpcKind);
          }
          String errorClass = null;
          String error = null;
          RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
          RpcErrorCodeProto detailedErr = null;
          Writable value = null;

          CurCall.set(call);
          try {
            // Make the call as the user via Subject.doAs, thus associating
            // the call with the Subject
            if (call.connection.user == null) {
              value = call(call.rpcKind, call.connection.protocolName, call.rpcRequest, 
                           call.timestamp);
            } else {
              value = 
                call.connection.user.doAs
                  (new PrivilegedExceptionAction<Writable>() {
                     @Override
                     public Writable run() throws Exception {
                       // make the call
                       return call(call.rpcKind, call.connection.protocolName, 
                                   call.rpcRequest, call.timestamp);

                     }
                   }
                  );
            }
          } catch (Throwable e) {
            if (e instanceof UndeclaredThrowableException) {
              e = e.getCause();
            }
            String logMsg = getName() + ", call " + call + ": error: " + e;
            if (e instanceof RuntimeException || e instanceof Error) {
              // These exception types indicate something is probably wrong
              // on the server side, as opposed to just a normal exceptional
              // result.
              LOG.warn(logMsg, e);
            } else if (exceptionsHandler.isTerse(e.getClass())) {
             // Don't log the whole stack trace of these exceptions.
              // Way too noisy!
              LOG.info(logMsg);
            } else {
              LOG.info(logMsg, e);
            }
            if (e instanceof RpcServerException) {
              RpcServerException rse = ((RpcServerException)e); 
              returnStatus = rse.getRpcStatusProto();
              detailedErr = rse.getRpcErrorCodeProto();
            } else {
              returnStatus = RpcStatusProto.ERROR;
              detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
            }
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
            // Remove redundant error class name from the beginning of the stack trace
            String exceptionHdr = errorClass + ": ";
            if (error.startsWith(exceptionHdr)) {
              error = error.substring(exceptionHdr.length());
            }
          }
          CurCall.set(null);
          synchronized (call.connection.responseQueue) {
            // setupResponse() needs to be sync'ed together with 
            // responder.doResponse() since setupResponse may use
            // SASL to encrypt response data and SASL enforces
            // its own message ordering.
            setupResponse(buf, call, returnStatus, detailedErr, 
                value, errorClass, error);
            
            // Discard the large buf and reset it back to smaller size 
            // to free up heap
            if (buf.size() > maxRespSize) {
              LOG.warn("Large response size " + buf.size() + " for call "
                  + call.toString());
              buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
            }
            responder.doRespond(call);
          }
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " unexpectedly interrupted", e);
          }
        } catch (Exception e) {
          LOG.info(getName() + " caught an exception", e);
        }
      }
      LOG.debug(getName() + ": exiting");
    }

  }
  
  protected Server(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount, 
                  Configuration conf)
    throws IOException 
  {
    this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
        .toString(port), null, null);
  }
  
  protected Server(String bindAddress, int port,
      Class<? extends Writable> rpcRequestClass, int handlerCount,
      int numReaders, int queueSizePerHandler, Configuration conf,
      String serverName, SecretManager<? extends TokenIdentifier> secretManager)
    throws IOException {
    this(bindAddress, port, rpcRequestClass, handlerCount, numReaders, 
        queueSizePerHandler, conf, serverName, secretManager, null);
  }
  
  /** 
   * Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
   * from configuration. Otherwise the configuration will be picked up.
   * 
   * If rpcRequestClass is null then the rpcRequestClass must have been 
   * registered via {@link #registerProtocolEngine(RPC.RpcKind,
   *  Class, RPC.RpcInvoker)}
   * This parameter has been retained for compatibility with existing tests
   * and usage.
   */
  @SuppressWarnings("unchecked")
  protected Server(String bindAddress, int port,
      Class<? extends Writable> rpcRequestClass, int handlerCount,
      int numReaders, int queueSizePerHandler, Configuration conf,
      String serverName, SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig)
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.portRangeConfig = portRangeConfig;
    this.port = port;
    this.rpcRequestClass = rpcRequestClass; 
    this.handlerCount = handlerCount;
    this.socketSendBufferSize = 0;
    this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    if (queueSizePerHandler != -1) {
      this.maxQueueSize = queueSizePerHandler;
    } else {
      this.maxQueueSize = handlerCount * conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);      
    }
    this.maxRespSize = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
    if (numReaders != -1) {
      this.readThreads = numReaders;
    } else {
      this.readThreads = conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    }
    this.callQueue  = new LinkedBlockingQueue<Call>(maxQueueSize); 
    this.maxIdleTime = 2 * conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
    this.maxConnectionsToNuke = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
    this.thresholdIdleConnections = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
    this.authorize = 
      conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, 
                      false);

    // configure supported authentications
    this.enabledAuthMethods = getAuthMethods(secretManager, conf);
    this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);
    
    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();    
    this.rpcMetrics = RpcMetrics.create(this);
    this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);
    this.tcpNoDelay = conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);

    // Create the responder here
    responder = new Responder();
    
    if (secretManager != null) {
      SaslRpcServer.init(conf);
    }
    
    this.exceptionsHandler.addTerseExceptions(StandbyException.class);
  }
  
  private RpcSaslProto buildNegotiateResponse(List<AuthMethod> authMethods)
      throws IOException {
    RpcSaslProto.Builder negotiateBuilder = RpcSaslProto.newBuilder();
    negotiateBuilder.setState(SaslState.NEGOTIATE);
    for (AuthMethod authMethod : authMethods) {
      if (authMethod == AuthMethod.SIMPLE) { // not a SASL method
        continue;
      }
      SaslRpcServer saslRpcServer = new SaslRpcServer(authMethod);      
      negotiateBuilder.addAuthsBuilder()
          .setMethod(authMethod.toString())
          .setMechanism(saslRpcServer.mechanism)
          .setProtocol(saslRpcServer.protocol)
          .setServerId(saslRpcServer.serverId);
    }
    return negotiateBuilder.build();
  }

  // get the security type from the conf. implicitly include token support
  // if a secret manager is provided, or fail if token is the conf value but
  // there is no secret manager
  private List<AuthMethod> getAuthMethods(SecretManager<?> secretManager,
                                             Configuration conf) {
    AuthenticationMethod confAuthenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);        
    List<AuthMethod> authMethods = new ArrayList<AuthMethod>();
    if (confAuthenticationMethod == AuthenticationMethod.TOKEN) {
      if (secretManager == null) {
        throw new IllegalArgumentException(AuthenticationMethod.TOKEN +
            " authentication requires a secret manager");
      } 
    } else if (secretManager != null) {
      LOG.debug(AuthenticationMethod.TOKEN +
          " authentication enabled for secret manager");
      // most preferred, go to the front of the line!
      authMethods.add(AuthenticationMethod.TOKEN.getAuthMethod());
    }
    authMethods.add(confAuthenticationMethod.getAuthMethod());        
    
    LOG.debug("Server accepts auth methods:" + authMethods);
    return authMethods;
  }
  
  private void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    try {
      connection.close();
    } catch (IOException e) {
    }
  }
  
  /**
   * Setup response for the IPC Call.
   * 
   * @param responseBuf buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream responseBuf,
                             Call call, RpcStatusProto status, RpcErrorCodeProto erCode,
                             Writable rv, String errorClass, String error) 
  throws IOException {
    responseBuf.reset();
    DataOutputStream out = new DataOutputStream(responseBuf);
    RpcResponseHeaderProto.Builder headerBuilder =  
        RpcResponseHeaderProto.newBuilder();
    headerBuilder.setCallId(call.callId);
    headerBuilder.setStatus(status);
    headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);

    if (status == RpcStatusProto.SUCCESS) {
      RpcResponseHeaderProto header = headerBuilder.build();
      final int headerLen = header.getSerializedSize();
      int fullLength  = CodedOutputStream.computeRawVarint32Size(headerLen) +
          headerLen;
      try {
        if (rv instanceof ProtobufRpcEngine.RpcWrapper) {
          ProtobufRpcEngine.RpcWrapper resWrapper = 
              (ProtobufRpcEngine.RpcWrapper) rv;
          fullLength += resWrapper.getLength();
          out.writeInt(fullLength);
          header.writeDelimitedTo(out);
          rv.write(out);
        } else { // Have to serialize to buffer to get len
          final DataOutputBuffer buf = new DataOutputBuffer();
          rv.write(buf);
          byte[] data = buf.getData();
          fullLength += buf.getLength();
          out.writeInt(fullLength);
          header.writeDelimitedTo(out);
          out.write(data, 0, buf.getLength());
        }
      } catch (Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        setupResponse(responseBuf, call, RpcStatusProto.ERROR,
            RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
            null, t.getClass().getName(),
            StringUtils.stringifyException(t));
        return;
      }
    } else { // Rpc Failure
      headerBuilder.setExceptionClassName(errorClass);
      headerBuilder.setErrorMsg(error);
      headerBuilder.setErrorDetail(erCode);
      RpcResponseHeaderProto header = headerBuilder.build();
      int headerLen = header.getSerializedSize();
      final int fullLength  = 
          CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen;
      out.writeInt(fullLength);
      header.writeDelimitedTo(out);
    }
    if (call.connection.useWrap) {
      wrapWithSasl(responseBuf, call);
    }
    call.setResponse(ByteBuffer.wrap(responseBuf.toByteArray()));
  }
  
  /**
   * Setup response for the IPC Call on Fatal Error from a 
   * client that is using old version of Hadoop.
   * The response is serialized using the previous protocol's response
   * layout.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponseOldVersionFatal(ByteArrayOutputStream response, 
                             Call call,
                             Writable rv, String errorClass, String error) 
  throws IOException {
    final int OLD_VERSION_FATAL_STATUS = -1;
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.callId);                // write call id
    out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
    WritableUtils.writeString(out, errorClass);
    WritableUtils.writeString(out, error);

    if (call.connection.useWrap) {
      wrapWithSasl(response, call);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }
  
  
  private void wrapWithSasl(ByteArrayOutputStream response, Call call)
      throws IOException {
    if (call.connection.saslServer != null) {
      byte[] token = response.toByteArray();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (call.connection.saslServer) {
        token = call.connection.saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      response.reset();
      DataOutputStream saslOut = new DataOutputStream(response);
      saslOut.writeInt(token.length);
      saslOut.write(token, 0, token.length);
    }
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];
    
    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
    if (this.rpcDetailedMetrics != null) {
      this.rpcDetailedMetrics.shutdown();
    }
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }
  
  /** 
   * Called for each call. 
   * @deprecated Use  {@link #call(RPC.RpcKind, String,
   *  Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws Exception {
    return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
  }
  
  /** Called for each call. */
  public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
      Writable param, long receiveTime) throws Exception;
  
  /**
   * Authorize the incoming client connection.
   * 
   * @param user client user
   * @param protocolName - the protocol
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  private void authorize(UserGroupInformation user, String protocolName,
      InetAddress addr) throws AuthorizationException {
    if (authorize) {
      if (protocolName == null) {
        throw new AuthorizationException("Null protocol not authorized");
      }
      Class<?> protocol = null;
      try {
        protocol = getProtocolClass(protocolName, getConf());
      } catch (ClassNotFoundException cfne) {
        throw new AuthorizationException("Unknown protocol: " + 
                                         protocolName);
      }
      serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
    }
  }
  
  /**
   * Get the port on which the IPC Server is listening for incoming connections.
   * This could be an ephemeral port too, in which case we return the real
   * port on which the Server has bound.
   * @return port on which IPC Server is listening
   */
  public int getPort() {
    return port;
  }
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return numConnections;
  }
  
  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }
  
  /**
   * The maximum size of the rpc call queue of this server.
   * @return The maximum size of the rpc call queue.
   */
  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  /**
   * The number of reader threads for this server.
   * @return The number of reader threads.
   */
  public int getNumReaders() {
    return readThreads;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be 
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.
  
  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large 
   * buffer.  
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private int channelWrite(WritableByteChannel channel, 
                           ByteBuffer buffer) throws IOException {
    
    int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                 channel.write(buffer) : channelIO(null, channel, buffer);
    if (count > 0) {
      rpcMetrics.incrSentBytes(count);
    }
    return count;
  }
  
  
  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * ByteBuffer increases. There should not be any performance degredation.
   * 
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private int channelRead(ReadableByteChannel channel, 
                          ByteBuffer buffer) throws IOException {
    
    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      rpcMetrics.incrReceivedBytes(count);
    }
    return count;
  }
  
  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   * 
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh, 
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {
    
    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;
    
    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);
        
        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf); 
        
        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);        
      }
    }

    int nBytes = initialRemaining - buf.remaining(); 
    return (nBytes > 0) ? nBytes : ret;
  }
}
