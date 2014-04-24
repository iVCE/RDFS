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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * 
 * The Client transfers data to/from datanode using a streaming protocol.
 *
 */
public interface DataTransferProtocol {
  
  
  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious. 
   *
   * Version 23:
   *        In the end of OP_READ_BLOCK stream, expect datanode to
   *        send flag indicating whether the block is finalized and
   *        its visible length.
   */
  public static final int DATA_TRANSFER_VERSION = 23;

  // the lowest version that added force sync field.
  static final int FORCESYNC_FIELD_VERSION = 20;

  // the lowest version that supports namespace federation.
  static final int FEDERATION_VERSION = 21;
  
  // the lowest version that supports scatter-gather
  static final int SCATTERGATHER_VERSION = 22;
  
  //the lowest version that supports sending block length in the end of stream
  static final int SEND_DATA_LEN_VERSION = 23;

  static final int BACKWARD_COMPATIBLE_VERSION = 23;

  // Processed at datanode stream-handler
  public static final byte OP_WRITE_BLOCK = (byte) 80;
  public static final byte OP_READ_BLOCK = (byte) 81;
  public static final byte OP_READ_METADATA = (byte) 82;
  public static final byte OP_REPLACE_BLOCK = (byte) 83;
  public static final byte OP_COPY_BLOCK = (byte) 84;
  public static final byte OP_BLOCK_CHECKSUM = (byte) 85;
  public static final byte OP_READ_BLOCK_ACCELERATOR = (byte) 86;
  //for tree-structured block recovery
  public static final byte OP_MERGE_BLOCK = (byte) 87; 
  
  public static final int OP_STATUS_SUCCESS = 0;  
  public static final int OP_STATUS_ERROR = 1;  
  public static final int OP_STATUS_ERROR_CHECKSUM = 2;  
  public static final int OP_STATUS_ERROR_INVALID = 3;  
  public static final int OP_STATUS_ERROR_EXISTS = 4;  
  public static final int OP_STATUS_CHECKSUM_OK = 5;  

  public static final int CLIENT_HEARTBEAT_VERSION = 19;

  public static class PipelineAck {
    private long seqno;
    private short replies[];
    final public static PipelineAck HEART_BEAT = new PipelineAck(-1, new short[0]);  
    public final static long UNKOWN_SEQNO = -2;

    /** default constructor **/
    public PipelineAck() {
    }

    /**
     * Constructor
     * @param seqno sequence number
     * @param replies an array of replies
     */
    public PipelineAck(long seqno, short[] replies) {
      this.seqno = seqno;
      this.replies = replies;
    }

    /**
     * Get the sequence number
     * @return the sequence number
     */
    public long getSeqno() {
      return seqno;
    }

    /**
     * Get the number of replies
     * @return the number of replies
     */
    public short getNumOfReplies() {
      return (short)replies.length;
    }

    /**
     * get the ith reply
     * @return the the ith reply
     */
    public short getReply(int i) {
      return replies[i];
    }

    /**
     * Check if this ack contains error status
     * @return true if all statuses are SUCCESS
     */
    public boolean isSuccess() {
      for (short reply : replies) {
        if (reply != OP_STATUS_SUCCESS) {
          return false;
        }
      }
      return true;
    }

    public void readFields(DataInput in, int numRepliesExpected) throws IOException {
      seqno = in.readLong();
      replies = new short[numRepliesExpected];
      int i=0;
      for (; i<numRepliesExpected; i++) {
        replies[i] = in.readShort();
        if (replies[i] != OP_STATUS_SUCCESS) {
          break;
        }
      }
      if (i < numRepliesExpected-1) {
        short[] newReplies = new short[i];
        System.arraycopy(replies, 0, newReplies, 0, i);
      }
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(seqno);
      for(short reply : replies) {
        out.writeShort(reply);
        if (reply != OP_STATUS_SUCCESS) {
          return;
        }
      }
    }

    @Override //Object
    public String toString() {
      StringBuilder ack = new StringBuilder("Replies for seqno ");
      ack.append( seqno ).append( " are" );
      for(short reply : replies) {
        ack.append(" ");
        if (reply == OP_STATUS_SUCCESS) {
          ack.append("SUCCESS");
        } else {
          ack.append("FAILED");
        }
      }
      return ack.toString();
    }
  }
}
