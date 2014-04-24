package org.apache.hadoop.hdfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

public class DataTransferPacket {
	// the length of data and checksum in this packet
	public int dataLength;
	public long offsetInBlock;
	public long sequenceNumber;
	private byte booleanFields;
	public byte[] buffer;
	public String errMessage; //just for debug
	
	public DataTransferPacket(int bufferSize) {
		buffer = new byte[bufferSize];
		reset();
	}
	
	public void readHeader(DataInputStream in) throws IOException {
		dataLength = in.readInt();
		if (dataLength > 0) {
			offsetInBlock = in.readLong();
			sequenceNumber = in.readLong();
			booleanFields = in.readByte();
		}
	}
	
	public void read(DataInputStream in) throws IOException {
		readHeader(in);
		if (dataLength > 0) {
			try {
				IOUtils.readFully(in, buffer, 0, dataLength);
			} catch (ArrayIndexOutOfBoundsException e) {
				throw new ArrayIndexOutOfBoundsException("buffer.length = "
						+ buffer.length + ", dataLength = "
						+ dataLength);
			}
		} else if (dataLength < 0) {
			errMessage = Text.readString(in);
		}
	}
	
	public void writeHeader(DataOutputStream out) throws IOException {
		out.writeInt(dataLength);
		if (dataLength > 0) {
			out.writeLong(offsetInBlock);
			out.writeLong(sequenceNumber);
			out.writeByte(booleanFields);
		}
	}
	
	public void write(DataOutputStream out) throws IOException {
		writeHeader(out);
		if (dataLength > 0)
			out.write(buffer, 0, dataLength);
		else if (dataLength < 0)
			Text.writeString(out, errMessage);
	}
	
	public void reset() {
		// TODO Auto-generated method stub
		dataLength = 0;
		offsetInBlock  = 0;
		sequenceNumber = 0;
		booleanFields = 0x00;
		Arrays.fill(buffer, (byte)0);
		errMessage = null;
	}

	private static byte lastPacketInBlockMask = 0x01;
	private static byte forceSyncMask = 0x02;
	
	final public boolean isLastPacketInBlock() {
		return (((booleanFields & lastPacketInBlockMask) == 0) ? false : true);
	}
	
	final public void setLastPacketInBlock(boolean value) {
		if(value) {
			booleanFields |= lastPacketInBlockMask;
		} else {
			booleanFields &= (~lastPacketInBlockMask);
		}
	}
	
	final public boolean isForceSync() {
		return (((booleanFields & forceSyncMask) == 0) ? false : true);
	}
	
	final public void setForceSync(boolean value) {
		if(value) {
			booleanFields |= forceSyncMask;
		} else {
			booleanFields &= (~forceSyncMask);
		}
	}

	@Override
	public String toString() {
		return "DataTransferPacket dataLength=[" + dataLength
				+ "], offsetInBlock=[" + offsetInBlock + "], sequenceNumber=["
				+ sequenceNumber + "], booleanFields=[" + booleanFields + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + booleanFields;
		result = prime * result + dataLength;
		result = prime * result
				+ (int) (offsetInBlock ^ (offsetInBlock >>> 32));
		result = prime * result
				+ (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataTransferPacket other = (DataTransferPacket) obj;
		if (booleanFields != other.booleanFields)
			return false;
		if (dataLength != other.dataLength)
			return false;
		if (offsetInBlock != other.offsetInBlock)
			return false;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		return true;
	}
}
