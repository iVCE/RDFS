package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class RecoverTreeNodeElement implements Writable {
	private int namespaceId;
	private long blockId;
	private long genStamp;
	private int coefficient;
	private int stripId;

	public RecoverTreeNodeElement() {
	}

	public RecoverTreeNodeElement(int namespaceId, long blockId, 
			long genStamp, int coefficient, int stripId) {
		this.setNamespaceId(namespaceId);
		this.setBlockId(blockId);
		this.setGenStamp(genStamp);
		this.setCoefficient(coefficient);
		this.setStripId(stripId);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(namespaceId);
		out.writeLong(blockId);
		out.writeLong(genStamp);
		out.writeInt(coefficient);
		out.writeInt(stripId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.namespaceId = in.readInt();
		this.blockId = in.readLong();
		this.genStamp = in.readLong();
		this.coefficient = in.readInt();
		this.stripId = in.readInt();
	}
	
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecoverTreeNodeElement other = (RecoverTreeNodeElement) obj;
		if (blockId != other.blockId)
			return false;
		if (coefficient != other.coefficient)
			return false;
		if (genStamp != other.genStamp)
			return false;
		if (namespaceId != other.namespaceId)
			return false;
		return true;
	}

	public int getNamespaceId() {
		return namespaceId;
	}

	public void setNamespaceId(int namespaceId) {
		this.namespaceId = namespaceId;
	}

	public long getBlockId() {
		return blockId;
	}

	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}

	public long getGenStamp() {
		return genStamp;
	}

	public void setGenStamp(long genStamp) {
		this.genStamp = genStamp;
	}

	public int getCoefficient() {
		return coefficient;
	}

	public void setCoefficient(int coefficient) {
		this.coefficient = coefficient;
	}

	@Override
	public String toString() {
		return "RecoverTreeNodeElement [namespaceId=" + namespaceId
				+ ", blockId=" + blockId + ", genStamp=" + genStamp
				+ ", coefficient=" + coefficient + "]";
	}

	public int getStripId() {
		return stripId;
	}

	public void setStripId(int stripId) {
		this.stripId = stripId;
	}
}