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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.ConcatSourcesParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DestinationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeRpcAddressParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.hdfs.web.resources.RenameOptionSetParam;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.google.common.base.Charsets;
import com.sun.jersey.spi.container.ResourceFilters;

/** Web-hdfs NameNode implementation. */
@Path("")
@ResourceFilters(ParamFilter.class)
public class NamenodeWebHdfsMethods {
  public static final Log LOG = LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");
  
  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<String>(); 

  /** @return the remote client address. */
  public static String getRemoteAddress() {
    return REMOTE_ADDRESS.get();
  }

  public static InetAddress getRemoteIp() {
    try {
      return InetAddress.getByName(getRemoteAddress());
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Returns true if a WebHdfs request is in progress.  Akin to
   * {@link Server#isRpcInvocation()}.
   */
  public static boolean isWebHdfsInvocation() {
    return getRemoteAddress() != null;
  }

  private @Context ServletContext context;
  private @Context HttpServletRequest request;
  private @Context HttpServletResponse response;

  private void init(final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + ", " + username + ", " + doAsUser
          + Param.toSortedString(", ", parameters));
    }

    //clear content type
    response.setContentType(null);
  }

  static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, Configuration conf) throws IOException {
    final BlockManager bm = namenode.getNamesystem().getBlockManager();

    if (op == PutOpParam.Op.CREATE) {
      //choose a datanode near to client 
      final DatanodeDescriptor clientNode = bm.getDatanodeManager(
          ).getDatanodeByHost(getRemoteAddress());
      if (clientNode != null) {
        final DatanodeDescriptor[] datanodes = bm.getBlockPlacementPolicy()
            .chooseTarget(path, 1, clientNode,
                new ArrayList<DatanodeDescriptor>(), false, null, blocksize);
        if (datanodes.length > 0) {
          return datanodes[0];
        }
      }
    } else if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      //choose a datanode containing a replica 
      final NamenodeProtocols np = namenode.getRpcServer();
      final HdfsFileStatus status = np.getFileInfo(path);
      if (status == null) {
        throw new FileNotFoundException("File " + path + " not found.");
      }
      final long len = status.getLen();
      if (op == GetOpParam.Op.OPEN) {
        if (openOffset < 0L || (openOffset >= len && len > 0)) {
          throw new IOException("Offset=" + openOffset
              + " out of the range [0, " + len + "); " + op + ", path=" + path);
        }
      }

      if (len > 0) {
        final long offset = op == GetOpParam.Op.OPEN? openOffset: len - 1;
        final LocatedBlocks locations = np.getBlockLocations(path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return JspHelper.bestNode(locations.get(0).getLocations(), false, conf);
        }
      }
    } 

    return (DatanodeDescriptor)bm.getDatanodeManager().getNetworkTopology(
        ).chooseRandom(NodeBase.ROOT);
  }

  private Token<? extends TokenIdentifier> generateDelegationToken(
      final NameNode namenode, final UserGroupInformation ugi,
      final String renewer) throws IOException {
    final Credentials c = DelegationTokenSecretManager.createCredentials(
        namenode, ugi, renewer != null? renewer: ugi.getShortUserName());
    final Token<? extends TokenIdentifier> t = c.getAllTokens().iterator().next();
    t.setKind(WebHdfsFileSystem.TOKEN_KIND);
    return t;
  }

  private URI redirectURI(final NameNode namenode,
      final UserGroupInformation ugi, final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final Configuration conf = (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
    final DatanodeInfo dn = chooseDatanode(namenode, path, op, openOffset,
        blocksize, conf);

    final String delegationQuery;
    if (!UserGroupInformation.isSecurityEnabled()) {
      //security disabled
      delegationQuery = Param.toSortedString("&", doAsUser, username);
    } else if (delegation.getValue() != null) {
      //client has provided a token
      delegationQuery = "&" + delegation;
    } else {
      //generate a token
      final Token<? extends TokenIdentifier> t = generateDelegationToken(
          namenode, ugi, request.getUserPrincipal().getName());
      delegationQuery = "&" + new DelegationParam(t.encodeToUrlString());
    }
    final String query = op.toQueryString() + delegationQuery
        + "&" + new NamenodeRpcAddressParam(namenode)
        + Param.toSortedString("&", parameters);
    final String uripath = WebHdfsFileSystem.PATH_PREFIX + path;

    final URI uri = new URI("http", null, dn.getHostName(), dn.getInfoPort(),
        uripath, query, null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }

  /** Handle HTTP PUT request for the root. */
  @PUT
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response putRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DestinationParam.NAME) @DefaultValue(DestinationParam.DEFAULT)
          final DestinationParam destination,
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
          final AccessTimeParam accessTime,
      @QueryParam(RenameOptionSetParam.NAME) @DefaultValue(RenameOptionSetParam.DEFAULT)
          final RenameOptionSetParam renameOptions,
      @QueryParam(CreateParentParam.NAME) @DefaultValue(CreateParentParam.DEFAULT)
          final CreateParentParam createParent,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument
      ) throws IOException, InterruptedException {
    return put(ugi, delegation, username, doAsUser, ROOT, op, destination,
        owner, group, permission, overwrite, bufferSize, replication,
        blockSize, modificationTime, accessTime, renameOptions, createParent,
        delegationTokenArgument);
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response put(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DestinationParam.NAME) @DefaultValue(DestinationParam.DEFAULT)
          final DestinationParam destination,
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
          final AccessTimeParam accessTime,
      @QueryParam(RenameOptionSetParam.NAME) @DefaultValue(RenameOptionSetParam.DEFAULT)
          final RenameOptionSetParam renameOptions,
      @QueryParam(CreateParentParam.NAME) @DefaultValue(CreateParentParam.DEFAULT)
          final CreateParentParam createParent,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, destination, owner,
        group, permission, overwrite, bufferSize, replication, blockSize,
        modificationTime, accessTime, renameOptions, delegationTokenArgument);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {
          return put(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, destination, owner, group,
              permission, overwrite, bufferSize, replication, blockSize,
              modificationTime, accessTime, renameOptions, createParent,
              delegationTokenArgument);
        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private Response put(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final PutOpParam op,
      final DestinationParam destination,
      final OwnerParam owner,
      final GroupParam group,
      final PermissionParam permission,
      final OverwriteParam overwrite,
      final BufferSizeParam bufferSize,
      final ReplicationParam replication,
      final BlockSizeParam blockSize,
      final ModificationTimeParam modificationTime,
      final AccessTimeParam accessTime,
      final RenameOptionSetParam renameOptions,
      final CreateParentParam createParent,
      final TokenArgumentParam delegationTokenArgument
      ) throws IOException, URISyntaxException {

    final Configuration conf = (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final NamenodeProtocols np = namenode.getRpcServer();

    switch(op.getValue()) {
    case CREATE:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L, blockSize.getValue(conf),
          permission, overwrite, bufferSize, replication, blockSize);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    } 
    case MKDIRS:
    {
      final boolean b = np.mkdirs(fullpath, permission.getFsPermission(), true);
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CREATESYMLINK:
    {
      np.createSymlink(destination.getValue(), fullpath,
          PermissionParam.getDefaultFsPermission(), createParent.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case RENAME:
    {
      final EnumSet<Options.Rename> s = renameOptions.getValue();
      if (s.isEmpty()) {
        final boolean b = np.rename(fullpath, destination.getValue());
        final String js = JsonUtil.toJsonString("boolean", b);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      } else {
        np.rename2(fullpath, destination.getValue(),
            s.toArray(new Options.Rename[s.size()]));
        return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
      }
    }
    case SETREPLICATION:
    {
      final boolean b = np.setReplication(fullpath, replication.getValue(conf));
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETOWNER:
    {
      if (owner.getValue() == null && group.getValue() == null) {
        throw new IllegalArgumentException("Both owner and group are empty.");
      }

      np.setOwner(fullpath, owner.getValue(), group.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETPERMISSION:
    {
      np.setPermission(fullpath, permission.getFsPermission());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETTIMES:
    {
      np.setTimes(fullpath, modificationTime.getValue(), accessTime.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case RENEWDELEGATIONTOKEN:
    {
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      final long expiryTime = np.renewDelegationToken(token);
      final String js = JsonUtil.toJsonString("long", expiryTime);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CANCELDELEGATIONTOKEN:
    {
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      np.cancelDelegationToken(token);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP POST request for the root. */
  @POST
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response postRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(ConcatSourcesParam.NAME) @DefaultValue(ConcatSourcesParam.DEFAULT)
          final ConcatSourcesParam concatSrcs,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {
    return post(ugi, delegation, username, doAsUser, ROOT, op, concatSrcs, bufferSize);
  }

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response post(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(ConcatSourcesParam.NAME) @DefaultValue(ConcatSourcesParam.DEFAULT)
          final ConcatSourcesParam concatSrcs,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, concatSrcs, bufferSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {
          return post(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, concatSrcs, bufferSize);
        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private Response post(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final PostOpParam op,
      final ConcatSourcesParam concatSrcs,
      final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case APPEND:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L, -1L, bufferSize);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case CONCAT:
    {
      namenode.getRpcServer().concat(fullpath, concatSrcs.getAbsolutePaths());
      return Response.ok().build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response getRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
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
      ) throws IOException, InterruptedException {
    return get(ugi, delegation, username, doAsUser, ROOT, op,
        offset, length, renewer, bufferSize);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
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
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op,
        offset, length, renewer, bufferSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {
          return get(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, offset, length, renewer, bufferSize);
        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private Response get(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final GetOpParam op,
      final OffsetParam offset,
      final LengthParam length,
      final RenewerParam renewer,
      final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final NamenodeProtocols np = namenode.getRpcServer();

    switch(op.getValue()) {
    case OPEN:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), offset.getValue(), -1L, offset, length, bufferSize);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case GET_BLOCK_LOCATIONS:
    {
      final long offsetValue = offset.getValue();
      final Long lengthValue = length.getValue();
      final LocatedBlocks locatedblocks = np.getBlockLocations(fullpath,
          offsetValue, lengthValue != null? lengthValue: Long.MAX_VALUE);
      final String js = JsonUtil.toJsonString(locatedblocks);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILESTATUS:
    {
      final HdfsFileStatus status = np.getFileInfo(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status, true);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS:
    {
      final StreamingOutput streaming = getListingStream(np, fullpath);
      return Response.ok(streaming).type(MediaType.APPLICATION_JSON).build();
    }
    case GETCONTENTSUMMARY:
    {
      final ContentSummary contentsummary = np.getContentSummary(fullpath);
      final String js = JsonUtil.toJsonString(contentsummary);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILECHECKSUM:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L, -1L);
      return Response.temporaryRedirect(uri).type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case GETDELEGATIONTOKEN:
    {
      if (delegation.getValue() != null) {
        throw new IllegalArgumentException(delegation.getName()
            + " parameter is not null.");
      }
      final Token<? extends TokenIdentifier> token = generateDelegationToken(
          namenode, ugi, renewer.getValue());
      final String js = JsonUtil.toJsonString(token);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETHOMEDIRECTORY:
    {
      final String js = JsonUtil.toJsonString(
          org.apache.hadoop.fs.Path.class.getSimpleName(),
          WebHdfsFileSystem.getHomeDirectoryString(ugi));
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  private static DirectoryListing getDirectoryListing(final NamenodeProtocols np,
      final String p, byte[] startAfter) throws IOException {
    final DirectoryListing listing = np.getListing(p, startAfter, false);
    if (listing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    return listing;
  }
  
  private static StreamingOutput getListingStream(final NamenodeProtocols np, 
      final String p) throws IOException {
    // allows exceptions like FNF or ACE to prevent http response of 200 for
    // a failure since we can't (currently) return error responses in the
    // middle of a streaming operation
    final DirectoryListing firstDirList = getDirectoryListing(np, p,
        HdfsFileStatus.EMPTY_NAME);

    // must save ugi because the streaming object will be executed outside
    // the remote user's ugi
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new StreamingOutput() {
      @Override
      public void write(final OutputStream outstream) throws IOException {
        final PrintWriter out = new PrintWriter(new OutputStreamWriter(
            outstream, Charsets.UTF_8));
        out.println("{\"" + FileStatus.class.getSimpleName() + "es\":{\""
            + FileStatus.class.getSimpleName() + "\":[");

        try {
          // restore remote user's ugi
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
              long n = 0;
              for (DirectoryListing dirList = firstDirList; ;
                   dirList = getDirectoryListing(np, p, dirList.getLastName())
              ) {
                // send each segment of the directory listing
                for (HdfsFileStatus s : dirList.getPartialListing()) {
                  if (n++ > 0) {
                    out.println(',');
                  }
                  out.print(JsonUtil.toJsonString(s, false));
                }
                // stop if last segment
                if (!dirList.hasMore()) {
                  break;
                }
              }
              return null;
            }
          });
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        
        out.println();
        out.println("]}}");
        out.flush();
      }
    };
  }

  /** Handle HTTP DELETE request for the root. */
  @DELETE
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive
      ) throws IOException, InterruptedException {
    return delete(ugi, delegation, username, doAsUser, ROOT, op, recursive);
  }

  /** Handle HTTP DELETE request. */
  @DELETE
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, recursive);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {
          return delete(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, recursive);
        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private Response delete(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final DeleteOpParam op,
      final RecursiveParam recursive
      ) throws IOException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case DELETE:
    {
      final boolean b = namenode.getRpcServer().delete(fullpath, recursive.getValue());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }
}
