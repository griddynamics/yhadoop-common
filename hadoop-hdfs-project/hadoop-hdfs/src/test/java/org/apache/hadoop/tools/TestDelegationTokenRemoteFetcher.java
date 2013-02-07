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

package org.apache.hadoop.tools;

import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;

public class TestDelegationTokenRemoteFetcher {
  private URI uri;
  private static final int httpPort = 2001;
  private static final String EXP_DATE = "124123512361236";
  private static final String SERVICE_URL = "http://localhost:" + httpPort;
  private static String tokenFile = "http.file.dta";
  private static final Logger LOG = Logger
      .getLogger(TestDelegationTokenRemoteFetcher.class);
  private FileSystem fileSys;
  private Configuration conf;
  private ServerBootstrap bootstrap;

  @Before
  public void init() throws Exception {
    conf = new Configuration();
    fileSys = FileSystem.getLocal(conf);
    uri = new URI(SERVICE_URL);
  }
 
  /**
   * try to fetch token without http server with IOException
   */
  @Test
  public void testTokenFetchFail() {
    try {
      DelegationTokenFetcher.main(new String[] { "-webservice=" + SERVICE_URL,
          tokenFile });
    } catch (IOException ex) {
    } catch (Exception e) {
      fail("expectedTokenIsRetrievedFail ex error " + e);
    }
  }
  
  /**
   * try to fetch token without http server with IOException
   */
  @Test
  public void testTokenRenewFail() {
    try {
      DelegationTokenFetcher.renewDelegationToken(SERVICE_URL, 
          createToken(uri.toString()));
    } catch (IOException ex) {
    } catch (Exception e) {
      fail("expectedTokenIsRetrievedFail ex error " + e);
    }
  }     
  
  /**
   * try cancel token without http server with IOException
   */
  @Test
  public void expectedTokenCancelFail() {
    try {
      DelegationTokenFetcher.cancelDelegationToken(SERVICE_URL, 
          createToken(uri.toString()));
    } catch (IOException ex) {
    } catch (Exception e) {
      fail("expectedTokenIsRetrievedFail ex error " + e);
    }
  }
  
  /**
   * try fetch token and get http response with error
   */
  @Test  
  public void expectedTokenRenewErrorHttpResponse() {
    try {
      Token<DelegationTokenIdentifier> token = createToken(uri.toString());
      bootstrap = startHttpServer(httpPort, token, uri.toString());
      DelegationTokenFetcher.renewDelegationToken(SERVICE_URL + "/exception", 
          createToken(uri.toString()));
    } catch (IOException ex) {
    } catch (Exception e) {
      fail("expectedTokenIsRetrievedFail ex error " + e);
    }
  }
  
  /**
   *   
   *
   */
  @Test
  public void testCancelTokenFromHttp() throws IOException {
    Token<DelegationTokenIdentifier> token = createToken(uri.toString());
    bootstrap = startHttpServer(httpPort, token, uri.toString());
    DelegationTokenFetcher.cancelDelegationToken(SERVICE_URL, token);    
  }
  
  /**
   * Call renew token using http server return new expiration time
   */
  @Test
  public void testRenewTokenFromHttp() throws IOException {
    Token<DelegationTokenIdentifier> token = createToken(uri.toString());
    bootstrap = startHttpServer(httpPort, token, uri.toString());
    assertTrue("testRenewTokenFromHttp error",
        Long.valueOf(EXP_DATE) == DelegationTokenFetcher.renewDelegationToken(
            SERVICE_URL, token));
  }

  private static Token<DelegationTokenIdentifier> createToken(String serviceUri) {
    byte[] pw = "hadoop".getBytes();
    byte[] ident = new DelegationTokenIdentifier(new Text("owner"), new Text(
        "renewer"), new Text("realuser")).getBytes();
    Text service = new Text(serviceUri);
    return new Token<DelegationTokenIdentifier>(ident, pw,
        HftpFileSystem.TOKEN_KIND, service);
  }

  /**
   * Call fetch token using http server 
   */
  @Test
  public void expectedTokenIsRetrievedFromHttp() throws Exception {
    Token<DelegationTokenIdentifier> token = createToken(uri.toString());
    bootstrap = startHttpServer(httpPort, token, uri.toString());
    DelegationTokenFetcher.main(new String[] { "-webservice=" + SERVICE_URL,
        tokenFile });
    Path p = new Path(fileSys.getWorkingDirectory(), tokenFile);
    Credentials creds = Credentials.readTokenStorageFile(p, conf);
    Iterator<Token<?>> itr = creds.getAllTokens().iterator();
    assertTrue("token not exist error", itr.hasNext());
    Token<?> fetchedToken = itr.next();
    Assert.assertArrayEquals("token wrong identifier error",
        token.getIdentifier(), fetchedToken.getIdentifier());
    Assert.assertArrayEquals("token wrong password error",
        token.getPassword(), fetchedToken.getPassword());
  }

  interface Handler {
    void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException;
  }

  static class FetchHandler implements Handler {
    
    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      Credentials creds = new Credentials();
      creds.addToken(new Text(serviceUrl), token);
      DataOutputBuffer out = new DataOutputBuffer();
      creds.write(out);
      int fileLength = out.getData().length;
      ChannelBuffer cbuffer = ChannelBuffers.buffer(fileLength);
      cbuffer.writeBytes(out.getData());
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
          String.valueOf(fileLength));
      response.setContent(cbuffer);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }
  }

  static class RenewHandler implements Handler {
    
    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      byte[] bytes = EXP_DATE.getBytes();
      ChannelBuffer cbuffer = ChannelBuffers.buffer(bytes.length);
      cbuffer.writeBytes(bytes);
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
          String.valueOf(bytes.length));
      response.setContent(cbuffer);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }
  }
  
  static class ExceptionHandler implements Handler {

    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, 
          HttpResponseStatus.METHOD_NOT_ALLOWED);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }    
  }
  
  static class CancelHandler implements Handler {

    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }    
  }
  
  private static final class CredentialsLogicHandler extends
      SimpleChannelUpstreamHandler {

    private final Token<DelegationTokenIdentifier> token;
    private final String serviceUrl;
    private ImmutableMap<String, Handler> routes = ImmutableMap.of(
        "/exception", new ExceptionHandler(),
        "/cancelDelegationToken", new CancelHandler(),
        "/getDelegationToken", new FetchHandler() , 
        "/renewDelegationToken", new RenewHandler());

    public CredentialsLogicHandler(Token<DelegationTokenIdentifier> token,
        String serviceUrl) {
      this.token = token;
      this.serviceUrl = serviceUrl;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e)
        throws Exception {
      HttpRequest request = (HttpRequest) e.getMessage();
      if (request.getMethod() != GET) {
        return;
      }
      UnmodifiableIterator<Map.Entry<String, Handler>> iter = routes.entrySet()
          .iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Handler> entry = iter.next();
        if (request.getUri().contains(entry.getKey())) {
          Handler handler = entry.getValue();
          handler.handle(e.getChannel(), token, serviceUrl);
          return;
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();

      if (LOG.isDebugEnabled())
        LOG.debug(cause.getMessage());
      ch.close().addListener(ChannelFutureListener.CLOSE);
    }
  }

  private ServerBootstrap startHttpServer(int port,
      final Token<DelegationTokenIdentifier> token, final String url) {
    ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadExecutor()));

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new HttpRequestDecoder(),
            new HttpChunkAggregator(65536), new HttpResponseEncoder(),
            new CredentialsLogicHandler(token, url));
      }
    });
    bootstrap.bind(new InetSocketAddress("localhost", port));
    return bootstrap;
  }
  
  @After
  public void clean() throws IOException {
    if (fileSys != null)
      fileSys.delete(new Path(tokenFile), true);
    if (bootstrap != null)
      bootstrap.releaseExternalResources();
  }
  
}
