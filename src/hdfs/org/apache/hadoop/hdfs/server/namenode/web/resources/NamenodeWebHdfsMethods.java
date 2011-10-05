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
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DstPathParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.sun.jersey.spi.container.ResourceFilters;

/** Web-hdfs NameNode implementation. */
@Path("")
@ResourceFilters(ParamFilter.class)
public class NamenodeWebHdfsMethods {
  public static final Log LOG = LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<String>(); 

  /** @return the remote client address. */
  public static String getRemoteAddress() {
    return REMOTE_ADDRESS.get();
  }

  private @Context ServletContext context;
  private @Context HttpServletRequest request;

  private static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset
      ) throws IOException {
    if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      final HdfsFileStatus status = namenode.getFileInfo(path);
      final long len = status.getLen();
      if (op == GetOpParam.Op.OPEN && (openOffset < 0L || openOffset >= len)) {
        throw new IOException("Offset=" + openOffset + " out of the range [0, "
          + len + "); " + op + ", path=" + path);
      }

      if (len > 0) {
        final long offset = op == GetOpParam.Op.OPEN? openOffset: len - 1;
        final LocatedBlocks locations = namenode.getBlockLocations(
            path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return JspHelper.bestNode(locations.get(0));
        }
      }
    } 

    return namenode.getNamesystem().getRandomDatanode();
  }

  private Token<? extends TokenIdentifier> generateDelegationToken(
      final NameNode namenode, final UserGroupInformation ugi,
      final String renewer) throws IOException {
    final Credentials c = DelegationTokenSecretManager.createCredentials(
        namenode, ugi, request.getUserPrincipal().getName());
    final Token<? extends TokenIdentifier> t = c.getAllTokens().iterator().next();
    t.setService(new Text(SecurityUtil.buildDTServiceName(
        NameNode.getUri(namenode.getNameNodeAddress()),
        NameNode.DEFAULT_PORT)));
    return t;
  }

  private URI redirectURI(final NameNode namenode,
      final UserGroupInformation ugi, final DelegationParam delegation,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn = chooseDatanode(namenode, path, op, openOffset);

    final String delegationQuery;
    if (!UserGroupInformation.isSecurityEnabled()) {
      //security disabled
      delegationQuery = "";
    } else if (delegation.getValue() != null) {
      //client has provided a token
      delegationQuery = "&" + delegation;
    } else {
      //generate a token
      final Token<? extends TokenIdentifier> t = generateDelegationToken(
          namenode, ugi, request.getUserPrincipal().getName());
      delegationQuery = "&" + new DelegationParam(t.encodeToUrlString());
    }
    final String query = op.toQueryString()
        + '&' + new UserParam(ugi) + delegationQuery
        + Param.toSortedString("&", parameters);
    final String uripath = "/" + WebHdfsFileSystem.PATH_PREFIX + path;

    final URI uri = new URI("http", null, dn.getHostName(), dn.getInfoPort(),
        uripath, query, null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response put(
      final InputStream in,
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DstPathParam.NAME) @DefaultValue(DstPathParam.DEFAULT)
          final DstPathParam dstPath,
      @QueryParam(OwnerParam.NAME) @DefaultValue(OwnerParam.DEFAULT)
          final OwnerParam owner,
      @QueryParam(GroupParam.NAME) @DefaultValue(GroupParam.DEFAULT)
          final GroupParam group,
      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize,
      @QueryParam(ModificationTimeParam.NAME) @DefaultValue(ModificationTimeParam.DEFAULT)
          final ModificationTimeParam modificationTime,
      @QueryParam(AccessTimeParam.NAME) @DefaultValue(AccessTimeParam.DEFAULT)
          final AccessTimeParam accessTime
      ) throws IOException, URISyntaxException, InterruptedException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ": " + path + ", ugi=" + ugi
          + Param.toSortedString(", ", dstPath, owner, group, permission,
              overwrite, bufferSize, replication, blockSize,
              modificationTime, accessTime));
    }

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

    final String fullpath = path.getAbsolutePath();
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case CREATE:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, fullpath,
          op.getValue(), -1L,
          permission, overwrite, bufferSize, replication, blockSize);
      return Response.temporaryRedirect(uri).build();
    } 
    case MKDIRS:
    {
      final boolean b = namenode.mkdirs(fullpath, permission.getFsPermission());
      final String js = JsonUtil.toJsonString(PutOpParam.Op.MKDIRS, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case RENAME:
    {
      final boolean b = namenode.rename(fullpath, dstPath.getValue());
      final String js = JsonUtil.toJsonString(PutOpParam.Op.RENAME, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETREPLICATION:
    {
      final boolean b = namenode.setReplication(fullpath, replication.getValue());
      final String js = JsonUtil.toJsonString(PutOpParam.Op.SETREPLICATION, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETOWNER:
    {
      namenode.setOwner(fullpath, owner.getValue(), group.getValue());
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    case SETPERMISSION:
    {
      namenode.setPermission(fullpath, permission.getFsPermission());
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    case SETTIMES:
    {
      namenode.setTimes(fullpath, modificationTime.getValue(), accessTime.getValue());
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response post(
      final InputStream in,
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException, InterruptedException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ": " + path + ", ugi=" + ugi
          + Param.toSortedString(", ", bufferSize));
    }

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

    final String fullpath = path.getAbsolutePath();
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case APPEND:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, fullpath,
          op.getValue(), -1L, bufferSize);
      return Response.temporaryRedirect(uri).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response root(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException, InterruptedException {
    return get(ugi, delegation, ROOT, op, offset, length, renewer, bufferSize);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException, InterruptedException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ": " + path + ", ugi=" + ugi
          + Param.toSortedString(", ", offset, length, renewer, bufferSize));
    }


    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final String fullpath = path.getAbsolutePath();

    switch(op.getValue()) {
    case OPEN:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, fullpath,
          op.getValue(), offset.getValue(), offset, length, bufferSize);
      return Response.temporaryRedirect(uri).build();
    }
    case GETFILEBLOCKLOCATIONS:
    {
      final long offsetValue = offset.getValue();
      final Long lengthValue = length.getValue();
      final LocatedBlocks locatedblocks = namenode.getBlockLocations(fullpath,
          offsetValue, lengthValue != null? lengthValue: offsetValue + 1);
      final String js = JsonUtil.toJsonString(locatedblocks);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILESTATUS:
    {
      final HdfsFileStatus status = namenode.getFileInfo(fullpath);
      final String js = JsonUtil.toJsonString(status);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS:
    {
      final StreamingOutput streaming = getListingStream(namenode, fullpath);
      return Response.ok(streaming).type(MediaType.APPLICATION_JSON).build();
    }
    case GETCONTENTSUMMARY:
    {
      final ContentSummary contentsummary = namenode.getContentSummary(fullpath);
      final String js = JsonUtil.toJsonString(contentsummary);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILECHECKSUM:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, fullpath,
          op.getValue(), -1L);
      return Response.temporaryRedirect(uri).build();
    }
    case GETDELEGATIONTOKEN:
    {
      final Token<? extends TokenIdentifier> token = generateDelegationToken(
          namenode, ugi, renewer.getValue());
      final String js = JsonUtil.toJsonString(token);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }    

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private static DirectoryListing getDirectoryListing(final NameNode np,
      final String p, byte[] startAfter) throws IOException {
    final DirectoryListing listing = np.getListing(p, startAfter);
    if (listing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    return listing;
  }
  
  private static StreamingOutput getListingStream(final NameNode np, 
      final String p) throws IOException {
    final DirectoryListing first = getDirectoryListing(np, p,
        HdfsFileStatus.EMPTY_NAME);

    return new StreamingOutput() {
      @Override
      public void write(final OutputStream outstream) throws IOException {
        final PrintStream out = new PrintStream(outstream);
        out.print('[');

        final HdfsFileStatus[] partial = first.getPartialListing();
        if (partial.length > 0) {
          out.print(JsonUtil.toJsonString(partial[0]));
        }
        for(int i = 1; i < partial.length; i++) {
          out.println(',');
          out.print(JsonUtil.toJsonString(partial[i]));
        }

        for(DirectoryListing curr = first; curr.hasMore(); ) { 
          curr = getDirectoryListing(np, p, curr.getLastName());
          for(HdfsFileStatus s : curr.getPartialListing()) {
            out.println(',');
            out.print(JsonUtil.toJsonString(s));
          }
        }
        
        out.println(']');
      }
    };
  }

  /** Handle HTTP DELETE request. */
  @DELETE
  @Path("{path:.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(
      @Context final UserGroupInformation ugi,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive
      ) throws IOException, InterruptedException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ": " + path + ", ugi=" + ugi
          + Param.toSortedString(", ", recursive));
    }

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

        final NameNode namenode = (NameNode)context.getAttribute("name.node");
        final String fullpath = path.getAbsolutePath();

        switch(op.getValue()) {
        case DELETE:
        {
          final boolean b = namenode.delete(fullpath, recursive.getValue());
          final String js = JsonUtil.toJsonString(DeleteOpParam.Op.DELETE, b);
          return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
        }
        default:
          throw new UnsupportedOperationException(op + " is not supported");
        }

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }
}
