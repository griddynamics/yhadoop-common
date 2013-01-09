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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.vintPrefixed;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is a wrapper around connection to datanode
 * and understands checksum, offset etc.
 *
 * Terminology:
 * <dl>
 * <dt>block</dt>
 *   <dd>The hdfs block, typically large (~64MB).
 *   </dd>
 * <dt>chunk</dt>
 *   <dd>A block is divided into chunks, each comes with a checksum.
 *       We want transfers to be chunk-aligned, to be able to
 *       verify checksums.
 *   </dd>
 * <dt>packet</dt>
 *   <dd>A grouping of chunks used for transport. It contains a
 *       header, followed by checksum data, followed by real data.
 *   </dd>
 * </dl>
 * Please see DataNode for the RPC specification.
 *
 * This is a new implementation introduced in Hadoop 0.23 which
 * is more efficient and simpler than the older BlockReader
 * implementation. It should be renamed to RemoteBlockReader
 * once we are confident in it.
 */
@InterfaceAudience.Private
public class RemoteBlockReader2  implements BlockReader {

  static final Log LOG = LogFactory.getLog(RemoteBlockReader2.class);
  private final DatanodeID datanodeID;
  private final Peer peer;
  private DataChecksum checksum;
  
  private PacketReceiver packetReceiver = new PacketReceiver(true);
  private ByteBuffer curDataSlice = null;

  /** offset in block of the last chunk received */
  private long lastSeqNo = -1;

  /** offset in block where reader wants to actually read */
  private long startOffset;
  private final String filename;

  private final int bytesPerChecksum;
  private final int checksumSize;

  /**
   * The total number of bytes we need to transfer from the DN.
   * This is the amount that the user has requested plus some padding
   * at the beginning so that the read can begin on a chunk boundary.
   */
  private long bytesNeededToFinish;

  private final boolean verifyChecksum;

  private boolean sentStatusCode = false;
  
  byte[] skipBuf = null;
  ByteBuffer checksumBytes = null;
  /** Amount of unread data in the current received packet */
  int dataLeft = 0;
  
  @VisibleForTesting
  public Peer getPeer() {
    return peer;
  }
  
  @Override
  public synchronized int read(byte[] buf, int off, int len) 
                               throws IOException {

    if (curDataSlice == null || curDataSlice.remaining() == 0 && bytesNeededToFinish > 0) {
      readNextPacket();
    }
    if (curDataSlice.remaining() == 0) {
      // we're at EOF now
      return -1;
    }
    
    int nRead = Math.min(curDataSlice.remaining(), len);
    curDataSlice.get(buf, off, nRead);
    
    return nRead;
  }


  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (curDataSlice == null || curDataSlice.remaining() == 0 && bytesNeededToFinish > 0) {
      readNextPacket();
    }
    if (curDataSlice.remaining() == 0) {
      // we're at EOF now
      return -1;
    }

    int nRead = Math.min(curDataSlice.remaining(), buf.remaining());
    ByteBuffer writeSlice = curDataSlice.duplicate();
    writeSlice.limit(writeSlice.position() + nRead);
    buf.put(writeSlice);
    curDataSlice.position(writeSlice.position());

    return nRead;
  }

  private void readNextPacket() throws IOException {
    //Read packet headers.
    packetReceiver.receiveNextPacket(peer.getInputStreamChannel());

    PacketHeader curHeader = packetReceiver.getHeader();
    curDataSlice = packetReceiver.getDataSlice();
    assert curDataSlice.capacity() == curHeader.getDataLen();
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("DFSClient readNextPacket got header " + curHeader);
    }

    // Sanity check the lengths
    if (!curHeader.sanityCheck(lastSeqNo)) {
         throw new IOException("BlockReader: error in packet header " +
                               curHeader);
    }
    
    if (curHeader.getDataLen() > 0) {
      int chunks = 1 + (curHeader.getDataLen() - 1) / bytesPerChecksum;
      int checksumsLen = chunks * checksumSize;

      assert packetReceiver.getChecksumSlice().capacity() == checksumsLen :
        "checksum slice capacity=" + packetReceiver.getChecksumSlice().capacity() + 
          " checksumsLen=" + checksumsLen;
      
      lastSeqNo = curHeader.getSeqno();
      if (verifyChecksum && curDataSlice.remaining() > 0) {
        // N.B.: the checksum error offset reported here is actually
        // relative to the start of the block, not the start of the file.
        // This is slightly misleading, but preserves the behavior from
        // the older BlockReader.
        checksum.verifyChunkedSums(curDataSlice,
            packetReceiver.getChecksumSlice(),
            filename, curHeader.getOffsetInBlock());
      }
      bytesNeededToFinish -= curHeader.getDataLen();
    }    
    
    // First packet will include some data prior to the first byte
    // the user requested. Skip it.
    if (curHeader.getOffsetInBlock() < startOffset) {
      int newPos = (int) (startOffset - curHeader.getOffsetInBlock());
      curDataSlice.position(newPos);
    }

    // If we've now satisfied the whole client read, read one last packet
    // header, which should be empty
    if (bytesNeededToFinish <= 0) {
      readTrailingEmptyPacket();
      if (verifyChecksum) {
        sendReadResult(Status.CHECKSUM_OK);
      } else {
        sendReadResult(Status.SUCCESS);
      }
    }
  }
  
  @Override
  public synchronized long skip(long n) throws IOException {
    /* How can we make sure we don't throw a ChecksumException, at least
     * in majority of the cases?. This one throws. */  
    if ( skipBuf == null ) {
      skipBuf = new byte[bytesPerChecksum]; 
    }

    long nSkipped = 0;
    while ( nSkipped < n ) {
      int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
      int ret = read(skipBuf, 0, toSkip);
      if ( ret <= 0 ) {
        return nSkipped;
      }
      nSkipped += ret;
    }
    return nSkipped;
  }

  private void readTrailingEmptyPacket() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Reading empty packet at end of read");
    }
    
    packetReceiver.receiveNextPacket(peer.getInputStreamChannel());

    PacketHeader trailer = packetReceiver.getHeader();
    if (!trailer.isLastPacketInBlock() ||
       trailer.getDataLen() != 0) {
      throw new IOException("Expected empty end-of-read packet! Header: " +
                            trailer);
    }
  }

  protected RemoteBlockReader2(BlockReaderFactory.Params params, 
      DataChecksum checksum, long firstChunkOffset) {
    // Path is used only for printing block and file information in debug
    this.datanodeID = params.getDatanodeID();
    this.peer = params.getPeer();
    this.checksum = checksum;
    this.verifyChecksum = params.getVerifyChecksum();
    this.startOffset = Math.max( params.getStartOffset(), 0 );
    this.filename = params.getFile();

    // The total number of bytes that we need to transfer from the DN is
    // the amount that the user wants (bytesToRead), plus the padding at
    // the beginning in order to chunk-align. Note that the DN may elect
    // to send more than this amount if the read starts/ends mid-chunk.
    this.bytesNeededToFinish = params.getLen() + (startOffset - firstChunkOffset);
    bytesPerChecksum = this.checksum.getBytesPerChecksum();
    checksumSize = this.checksum.getChecksumSize();
  }


  @Override
  public synchronized void close(PeerCache peerCache) throws IOException {
    packetReceiver.close();
    
    startOffset = -1;
    checksum = null;
    if (peerCache != null && sentStatusCode) {
      peerCache.put(datanodeID, peer);
    } else {
      peer.close();
    }

    // in will be closed when its Socket is closed.
  }

  /**
   * When the reader reaches end of the read, it sends a status response
   * (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
   * closing our connection (which we will re-open), but won't affect
   * data correctness.
   */
  void sendReadResult(Status statusCode) {
    assert !sentStatusCode : "already sent status code to " + peer;
    try {
      writeReadResult(peer.getOutputStream(), statusCode);
      sentStatusCode = true;
    } catch (IOException e) {
      // It's ok not to be able to send this. But something is probably wrong.
      LOG.info("Could not send read status (" + statusCode + ") to datanode " +
               peer.getRemoteAddressString() + ": " + e.getMessage());
    }
  }

  /**
   * Serialize the actual read result on the wire.
   */
  static void writeReadResult(OutputStream out, Status statusCode)
      throws IOException {
    
    ClientReadStatusProto.newBuilder()
      .setStatus(statusCode)
      .build()
      .writeDelimitedTo(out);

    out.flush();
  }
  
  /**
   * File name to print when accessing a block directly (from servlets)
   * @param s Address of the block location
   * @param poolId Block pool ID of the block
   * @param blockId Block ID of the block
   * @return string that has a file name for debug purposes
   */
  public static String getFileName(final InetSocketAddress s,
      final String poolId, final long blockId) {
    return s.toString() + ":" + poolId + ":" + blockId;
  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return BlockReaderUtil.readAll(this, buf, offset, len);
  }

  @Override
  public void readFully(byte[] buf, int off, int len) throws IOException {
    BlockReaderUtil.readFully(this, buf, off, len);
  }
  
  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This method also sends the OP_READ_BLOCK request.
   *
   * @param sock  An established Socket to the DN. The BlockReader will not close it normally.
   *             This socket must have an associated Channel.
   * @param file  File location
   * @param block  The block object
   * @param blockToken  The block token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read
   * @param bufferSize  The IO buffer size (not the client buffer size)
   * @param verifyChecksum  Whether to verify checksum
   * @param clientName  Client name
   * @return New BlockReader instance, or null on error.
   */
  public static BlockReader newBlockReader(BlockReaderFactory.Params params)
                                     throws IOException {
    // in and out will be closed when sock is closed (by the caller)
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
          params.getPeer().getOutputStream()));
    new Sender(out).readBlock(params.getBlock(), params.getBlockToken(), 
        params.getClientName(), params.getStartOffset(), params.getLen());

    //
    // Get bytes in block
    //
    DataInputStream in = new DataInputStream(params.getPeer().getInputStream());

    BlockOpResponseProto status = BlockOpResponseProto.parseFrom(
        vintPrefixed(in));
    checkSuccess(status, params.getPeer(), params.getBlock(), params.getFile());
    ReadOpChecksumInfoProto checksumInfo =
      status.getReadOpChecksumInfo();
    DataChecksum checksum = DataTransferProtoUtil.fromProto(
        checksumInfo.getChecksum());
    //Warning when we get CHECKSUM_NULL?

    // Read the first chunk offset.
    long firstChunkOffset = checksumInfo.getChunkOffset();

    if ( firstChunkOffset < 0 || firstChunkOffset > params.getStartOffset() ||
        firstChunkOffset <= (params.getStartOffset() - checksum.getBytesPerChecksum())) {
      throw new IOException("BlockReader: error in first chunk offset (" +
                    firstChunkOffset + ") startOffset is " +
                    params.getStartOffset() + " for file " + params.getFile());
    }

    return new RemoteBlockReader2(params, checksum, firstChunkOffset);
  }

  static void checkSuccess(
      BlockOpResponseProto status, Peer peer,
      ExtendedBlock block, String file)
      throws IOException {
    if (status.getStatus() != Status.SUCCESS) {
      if (status.getStatus() == Status.ERROR_ACCESS_TOKEN) {
        throw new InvalidBlockTokenException(
            "Got access token error for OP_READ_BLOCK, self="
                + peer.getLocalAddressString() + ", remote="
                + peer.getRemoteAddressString() + ", for file " + file
                + ", for pool " + block.getBlockPoolId() + " block " 
                + block.getBlockId() + "_" + block.getGenerationStamp());
      } else {
        throw new IOException("Got error for OP_READ_BLOCK, self="
            + peer.getLocalAddressString() + ", remote="
            + peer.getRemoteAddressString() + ", for file " + file
            + ", for pool " + block.getBlockPoolId() + " block " 
            + block.getBlockId() + "_" + block.getGenerationStamp());
      }
    }
  }
}
