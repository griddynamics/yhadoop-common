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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static junit.framework.Assert.*;

import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test AmIpFilter. Requests to a no declared hosts should has way through proxy.
 * Another requests can be filtered with (without) user name. 
 * 
 */
public class TestAmFilter {

  private String proxyHost = "localhost";
  private String proxyUri = "http://bogus";
  private String doFilterRequest;
  private AmIpServletRequestWrapper servletWrapper;

  private class TestAmIpFilter extends AmIpFilter {

    private Set<String> proxyAddresses = null;

    protected Set<String> getProxyAddresses() {
      if (proxyAddresses == null) {
        proxyAddresses = new HashSet<String>();
      }
      proxyAddresses.add(proxyHost);
      return proxyAddresses;
    }
  }

  private static class DummyFilterConfig implements FilterConfig {
    final Map<String, String> map;

    DummyFilterConfig(Map<String, String> map) {
      this.map = map;
    }

    @Override
    public String getFilterName() {
      return "dummy";
    }

    @Override
    public String getInitParameter(String arg0) {
      return map.get(arg0);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return Collections.enumeration(map.keySet());
    }

    @Override
    public ServletContext getServletContext() {
      return null;
    }
  }

  @Test(timeout = 5000)
  public void filterNullCookies() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

    Mockito.when(request.getCookies()).thenReturn(null);
    Mockito.when(request.getRemoteAddr()).thenReturn(proxyHost);

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    final AtomicBoolean invoked = new AtomicBoolean();

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse) throws IOException, ServletException {
        invoked.set(true);
      }
    };

    Map<String, String> params = new HashMap<String, String>();
    params.put(AmIpFilter.PROXY_HOST, proxyHost);
    params.put(AmIpFilter.PROXY_URI_BASE, proxyUri);
    FilterConfig conf = new DummyFilterConfig(params);
    Filter filter = new TestAmIpFilter();
    filter.init(conf);
    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());
    filter.destroy();
  }
/**
 * Test AmIpFilter
 */
  @Test(timeout = 1000)
  public void testFilter() throws Exception {
    Map<String, String> params = new HashMap<String, String>();
    params.put(AmIpFilter.PROXY_HOST, proxyHost);
    params.put(AmIpFilter.PROXY_URI_BASE, proxyUri);
    FilterConfig config = new DummyFilterConfig(params);

    // dummy filter
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse) throws IOException, ServletException {
        doFilterRequest = servletRequest.getClass().getName();
        if (servletRequest instanceof AmIpServletRequestWrapper) {
          servletWrapper = (AmIpServletRequestWrapper) servletRequest;

        }
      }
    };
    AmIpFilter testFilter = new AmIpFilter();
    testFilter.init(config);

    MyHttpServletResponse response = new MyHttpServletResponse();
    // Test request should implements HttpServletRequest
    try {
      testFilter.doFilter(new MyServletRequest(), response, chain);
      fail();
    } catch (ServletException e) {
      assertEquals("This filter only works for HTTP/HTTPS", e.getMessage());
    }
    
    // request with HttpServletRequest
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn("redirect");
    Mockito.when(request.getRequestURI()).thenReturn("/redirect");
    testFilter.doFilter(request, response, chain);
    // address "redirect" is not in host list 
    assertEquals("http://bogus/redirect", response.getRedirect());
    // "127.0.0.1" contains in host list.   Without cookie 
    Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    testFilter.doFilter(request, response, chain);

    assertTrue(doFilterRequest
            .contains("javax.servlet.http.HttpServletRequest"));
    // cookie added
    Cookie[] cookies = new Cookie[1];
    cookies[0] = new Cookie(WebAppProxyServlet.PROXY_USER_COOKIE_NAME, "user");

    Mockito.when(request.getCookies()).thenReturn(cookies);
    testFilter.doFilter(request, response, chain);

    assertEquals(
        "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpServletRequestWrapper",
            doFilterRequest);
    // request contains principal from cookie
    assertEquals("user", servletWrapper.getUserPrincipal().getName());
    assertEquals("user", servletWrapper.getRemoteUser());
    assertFalse(servletWrapper.isUserInRole(""));

  }

  private class MyHttpServletResponse implements HttpServletResponse {
    String redirectLocation = "";
    public Collection<String> getHeaderNames(){
      return null;
    }

    @Override
    public String getCharacterEncoding() {
      return null;
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
      return null;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      return null;
    }

    @Override
    public void setCharacterEncoding(String charset) {

    }

    @Override
    public void setContentLength(int len) {

    }

    @Override
    public void setContentType(String type) {

    }

    @Override
    public void setBufferSize(int size) {

    }

    @Override
    public int getBufferSize() {
      return 0;
    }

    @Override
    public void flushBuffer() throws IOException {

    }

    @Override
    public void resetBuffer() {

    }

    @Override
    public boolean isCommitted() {
      return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public void setLocale(Locale loc) {

    }

    @Override
    public Locale getLocale() {
      return null;
    }

    @Override
    public void addCookie(Cookie cookie) {

    }

    @Override
    public boolean containsHeader(String name) {
      return false;
    }

    @Override
    public String encodeURL(String url) {
      return null;
    }

    @Override
    public String encodeRedirectURL(String url) {
      return url;
    }

    @Override
    public String encodeUrl(String url) {
      return null;
    }

    @Override
    public String encodeRedirectUrl(String url) {
      return null;
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {

    }

    @Override
    public void sendError(int sc) throws IOException {

    }

    public String getRedirect() {
      return redirectLocation;
    }

    @Override
    public void sendRedirect(String location) throws IOException {
      redirectLocation = location;
    }

    @Override
    public void setDateHeader(String name, long date) {

    }

    @Override
    public void addDateHeader(String name, long date) {

    }

    @Override
    public void setHeader(String name, String value) {

    }

    @Override
    public void addHeader(String name, String value) {

    }

    @Override
    public void setIntHeader(String name, int value) {

    }

    @Override
    public void addIntHeader(String name, int value) {

    }

    @Override
    public void setStatus(int sc) {

    }

    @Override
    public void setStatus(int sc, String sm) {

    }

    public int getStatus() {
      return 0;
    }

    public String getHeader(String name) {
      return null;
    }

    public Collection<String> getHeaders(String name) {
      return null;
    }

  }

  private class MyServletRequest implements ServletRequest {

    public AsyncContext startAsync(ServletRequest servletRequest,
                                   ServletResponse servletResponse)
            throws IllegalStateException{
      return null;
    }
    public AsyncContext startAsync()
            throws IllegalStateException{
      return null;
    }
    public boolean isAsyncStarted(){
      return false;
    }

    public boolean isAsyncSupported(){
      return false;
    }

    public AsyncContext getAsyncContext(){
      return null;
    }
    public DispatcherType getDispatcherType(){
      return null;
    }

    public ServletContext getServletContext(){
      return null;
    }
    @Override
    public Object getAttribute(String name) {
      return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getAttributeNames() {
      return null;
    }

    @Override
    public String getCharacterEncoding() {
      return null;
    }

    @Override
    public void setCharacterEncoding(String env)
        throws UnsupportedEncodingException {

    }

    @Override
    public int getContentLength() {
      return 0;
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
      return null;
    }

    @Override
    public String getParameter(String name) {
      return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getParameterNames() {
      return null;
    }

    @Override
    public String[] getParameterValues(String name) {
      return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map getParameterMap() {
      return null;
    }

    @Override
    public String getProtocol() {
      return null;
    }

    @Override
    public String getScheme() {
      return null;
    }

    @Override
    public String getServerName() {
      return null;
    }

    @Override
    public int getServerPort() {
      return 0;
    }

    @Override
    public BufferedReader getReader() throws IOException {
      return null;
    }

    @Override
    public String getRemoteAddr() {
      return null;
    }

    @Override
    public String getRemoteHost() {
      return null;
    }

    @Override
    public void setAttribute(String name, Object o) {

    }

    @Override
    public void removeAttribute(String name) {

    }

    @Override
    public Locale getLocale() {
      return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getLocales() {
      return null;
    }

    @Override
    public boolean isSecure() {
      return false;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
      return null;
    }

    @Override
    public String getRealPath(String path) {
      return null;
    }

    @Override
    public int getRemotePort() {
      return 0;
    }

    @Override
    public String getLocalName() {
      return null;
    }

    @Override
    public String getLocalAddr() {
      return null;
    }

    @Override
    public int getLocalPort() {
      return 0;
    }

  }
}
