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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

/**
 * Static utility functions for serializing various pieces of data in the correct
 * format for the FSImage file.
 *
 * Some members are currently public for the benefit of the Offline Image Viewer
 * which is located outside of this package. These members should be made
 * package-protected when the OIV is refactored.
 */
public class FSImageSerialization {

  // Static-only class
  private FSImageSerialization() {}
  
  /**
   * In order to reduce allocation, we reuse some static objects. However, the methods
   * in this class should be thread-safe since image-saving is multithreaded, so 
   * we need to keep the static objects in a thread-local.
   */
  static private final ThreadLocal<TLData> TL_DATA =
    new ThreadLocal<TLData>() {
    @Override
    protected TLData initialValue() {
      return new TLData();
    }
  };

  /**
   * Simple container "struct" for threadlocal data.
   */
  @SuppressWarnings("deprecation")
  static private final class TLData {
    final UTF8 U_STR = new UTF8();
    final LongWritable U_LONG = new LongWritable();
    final FsPermission FILE_PERM = new FsPermission((short) 0);
  }

  // Helper function that reads in an INodeUnderConstruction
  // from the input stream
  //
  static INodeFileUnderConstruction readINodeUnderConstruction(
                            DataInputStream in) throws IOException {
    byte[] name = readBytes(in);
    String path = DFSUtil.bytes2String(name);
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    Block blk = new Block();
    for (int i = 0; i < numBlocks; i++) {
      blk.readFields(in);
      blocks[i] = new BlockInfo(blk, blockReplication);
    }
    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = readString(in);
    String clientMachine = readString(in);

    // These locations are not used at all
    int numLocs = in.readInt();
    DatanodeDescriptor[] locations = new DatanodeDescriptor[numLocs];
    for (int i = 0; i < numLocs; i++) {
      locations[i] = new DatanodeDescriptor();
      locations[i].readFields(in);
    }

    return new INodeFileUnderConstruction(name, 
                                          blockReplication, 
                                          modificationTime,
                                          preferredBlockSize,
                                          blocks,
                                          perm,
                                          clientName,
                                          clientMachine,
                                          null);
  }

  // Helper function that writes an INodeUnderConstruction
  // into the input stream
  //
  static void writeINodeUnderConstruction(DataOutputStream out,
                                           INodeFileUnderConstruction cons,
                                           String path) 
                                           throws IOException {
    writeString(path, out);
    out.writeShort(cons.getReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());
    int nrBlocks = cons.getBlocks().length;
    out.writeInt(nrBlocks);
    for (int i = 0; i < nrBlocks; i++) {
      cons.getBlocks()[i].write(out);
    }
    cons.getPermissionStatus().write(out);
    writeString(cons.getClientName(), out);
    writeString(cons.getClientMachine(), out);

    out.writeInt(0); //  do not store locations of last block
  }
  
  /*
   * Save one inode's attributes to the image.
   */
  static void saveINode2Image(INode node,
                                      DataOutputStream out) throws IOException {
    byte[] name = node.getLocalNameBytes();
    out.writeShort(name.length);
    out.write(name);
    FsPermission filePerm = TL_DATA.get().FILE_PERM;
    if (!node.isDirectory()) {  // write file inode
      INodeFile fileINode = (INodeFile)node;
      out.writeShort(fileINode.getReplication());
      out.writeLong(fileINode.getModificationTime());
      out.writeLong(fileINode.getAccessTime());
      out.writeLong(fileINode.getPreferredBlockSize());
      Block[] blocks = fileINode.getBlocks();
      out.writeInt(blocks.length);
      for (Block blk : blocks)
        blk.write(out);
      filePerm.fromShort(fileINode.getFsPermissionShort());
      PermissionStatus.write(out, fileINode.getUserName(),
                             fileINode.getGroupName(),
                             filePerm);
    } else {   // write directory inode
      out.writeShort(0);  // replication
      out.writeLong(node.getModificationTime());
      out.writeLong(0);   // access time
      out.writeLong(0);   // preferred block size
      out.writeInt(-1);    // # of blocks
      out.writeLong(node.getNsQuota());
      out.writeLong(node.getDsQuota());
      filePerm.fromShort(node.getFsPermissionShort());
      PermissionStatus.write(out, node.getUserName(),
                             node.getGroupName(),
                             filePerm);
    }
  }

  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
  @SuppressWarnings("deprecation")
  public static String readString(DataInputStream in) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return ustr.toString();
  }

  static String readString_EmptyAsNull(DataInputStream in) throws IOException {
    final String s = readString(in);
    return s.isEmpty()? null: s;
  }

  @SuppressWarnings("deprecation")
  static void writeString(String str, DataOutputStream out) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(str, true);
    ustr.write(out);
  }

  /** read the long value */
  static long readLong(DataInputStream in) throws IOException {
    LongWritable ustr = TL_DATA.get().U_LONG;
    ustr.readFields(in);
    return ustr.get();
  }

  /** write the long value */
  static void writeLong(long value, DataOutputStream out) throws IOException {
    LongWritable uLong = TL_DATA.get().U_LONG;
    uLong.set(value);
    uLong.write(out);
  }
  
  /** read the long value */
  static long readLongAsString(DataInputStream in) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return Long.parseLong(ustr.toString());
  }

  /** write the long value */
  static void writeLongAsString(long value, DataOutputStream out) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(toLogLong(value), true);
    ustr.write(out);
  }
  
  /** read the long value */
  static short readShortAsString(DataInputStream in) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return Short.parseShort(ustr.toString());
  }

  /** write the long value */
  static void writeShortAsString(short value, DataOutputStream out) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(toLogShort(value), true);
    ustr.write(out);
  }
  
  static private String toLogShort(short value) {
    return Short.toString(value);
  }
  
  static private String toLogLong(long value) {
    return Long.toString(value);
  }
  
  // Same comments apply for this method as for readString()
  @SuppressWarnings("deprecation")
  public static byte[] readBytes(DataInputStream in) throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    int len = ustr.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(ustr.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * @param in
   * @return the array each element of which is a byte[] representation 
   *            of a path component
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static byte[][] readPathComponents(DataInputStream in)
      throws IOException {
    UTF8 ustr = TL_DATA.get().U_STR;
    
    ustr.readFields(in);
    return DFSUtil.bytes2byteArray(ustr.getBytes(),
      ustr.getLength(), (byte) Path.SEPARATOR_CHAR);
  }

  /**
   * DatanodeImage is used to store persistent information
   * about datanodes into the fsImage.
   */
  static class DatanodeImage implements Writable {
    DatanodeDescriptor node = new DatanodeDescriptor();

    static void skipOne(DataInput in) throws IOException {
      DatanodeImage nodeImage = new DatanodeImage();
      nodeImage.readFields(in);
    }
    
    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     * Public method that serializes the information about a
     * Datanode to be stored in the fsImage.
     */
    public void write(DataOutput out) throws IOException {
      new DatanodeID(node).write(out);
      out.writeLong(node.getCapacity());
      out.writeLong(node.getRemaining());
      out.writeLong(node.getLastUpdate());
      out.writeInt(node.getXceiverCount());
    }

    /**
     * Public method that reads a serialized Datanode
     * from the fsImage.
     */
    public void readFields(DataInput in) throws IOException {
      DatanodeID id = new DatanodeID();
      id.readFields(in);
      long capacity = in.readLong();
      long remaining = in.readLong();
      long lastUpdate = in.readLong();
      int xceiverCount = in.readInt();

      // update the DatanodeDescriptor with the data we read in
      node.updateRegInfo(id);
      node.setStorageID(id.getStorageID());
      node.setCapacity(capacity);
      node.setRemaining(remaining);
      node.setLastUpdate(lastUpdate);
      node.setXceiverCount(xceiverCount);
    }
  }
}