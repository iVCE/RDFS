package org.apache.hadoop.hdfs.protocol;

import java.io.*;

import org.apache.hadoop.io.*;

/**
 * The header for the OP_WRITE_BLOCK datanode operation.
 */
public class MergeBlockHeader extends DataTransferHeader implements Writable {

	private int namespaceId;
	private long blockId;
	private long offsetInBlock;
	private long length;
	private boolean isRecovery;
	private long genStamp;
	private int level;

	public MergeBlockHeader(final int dataTransferVersion,
			final int namespaceId, final long blockId, 
			final long offsetInBlock, final long length, boolean isRecovery, 
			final long genStamp, final int level) {
		super(dataTransferVersion, DataTransferProtocol.OP_MERGE_BLOCK);
		set(namespaceId, blockId, offsetInBlock, length, isRecovery, genStamp, level);
	}

	public MergeBlockHeader(VersionAndOpcode versionAndOpcode) {
		super(versionAndOpcode);
	}

	public void set(int namespaceId, long blockId, long offsetInBlock, 
			long length, boolean isRecovery, long genStamp,int level) {
		this.namespaceId = namespaceId;
		this.blockId = blockId;
		this.offsetInBlock = offsetInBlock;
		this.length = length;
		this.isRecovery = isRecovery;
		this.genStamp = genStamp;
		this.level= level;
	}
	
	
	public void setNamespaceId(int namespaceId) {
		this.namespaceId = namespaceId;
	}

	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}

	public void setOffsetInBlock(long offsetInBlock) {
		this.offsetInBlock = offsetInBlock;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public void setGenStamp(long genStamp) {
		this.genStamp = genStamp;
	}

	public int getNamespaceId() {
		return namespaceId;
	}

	public long getBlockId() {
		return blockId;
	}
	
	public long getOffsetInBlock() {
		return offsetInBlock;
	}

	public long getLength() {
		return length;
	}

	public long getGenStamp() {
		return genStamp;
	}
	
	public int getLevel() {
		return level;
	}
	
	public void setLevel(int level) {
		this.level = level;
	}
	
	public boolean getIsRecovery() {
		return isRecovery;
	}

	public void setRecovery(boolean isRecovery) {
		this.isRecovery = isRecovery;
	}
	

	// ///////////////////////////////////
	// Writable
	// ///////////////////////////////////
	public void write(DataOutput out) throws IOException {
		if (getDataTransferVersion() >= DataTransferProtocol.FEDERATION_VERSION) {
			out.writeInt(namespaceId);
		}
		out.writeLong(blockId);
		out.writeLong(offsetInBlock);
		out.writeLong(length);
		out.writeBoolean(isRecovery);
		out.writeLong(genStamp);
		out.writeInt(level);
	}

	public void readFields(DataInput in) throws IOException {
		if (getDataTransferVersion() >= DataTransferProtocol.FEDERATION_VERSION) {
			namespaceId = in.readInt();
		}
		blockId = in.readLong();
		offsetInBlock = in.readLong();
		length = in.readLong();
		isRecovery = in.readBoolean();
		genStamp = in.readLong();
		level = in.readInt();
	}

	@Override
	public String toString() {
		return "MergeBlockHeader [namespaceId=" + namespaceId + ", blockId="
				+ blockId + ", offsetInBlock=" + offsetInBlock + ", length="
				+ length + ", isRecovery=" + isRecovery + ", genStamp="
				+ genStamp + ", level=" + level + "]";
	}

}
