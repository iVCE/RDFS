package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.hdfs.DataTransferPacket;
import org.apache.hadoop.hdfs.GaloisField;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

public class BlockXCodingSender implements java.io.Closeable, FSConstants {

	static {
		Configuration.addDefaultResource("raid-site.xml");
	}
	private Block block; // the block to read from
	private InputStream blockIn; // data stream
	private DataInputStream checksumIn; // checksum datastream
	private DataChecksum checksum; // checksum stream
	private long offset; // starting position to read
	private long endOffset; // ending position
	private long blockLength; // block visible length
	private int bytesPerChecksum; // chunk size
	private int checksumSize; // checksum size
	
	private long seqno; // sequence number of packet
	private int packetSize = 4096;
	
	private boolean noData = false;
	private boolean blockReadFully; // set when the whole block is read
	
	private boolean verifyChecksum; // if true, check is verified while
									// reading
	private boolean corruptChecksumOk; // if need to verify checksum
	private boolean ignoreChecksum;
	
	private int coefficient = 1;
	private GaloisField GF = null;
	
	private Configuration conf;

	public static final Log LOG = DataNode.LOG;
	
	public BlockXCodingSender(int namespaceId, Block block, long startOffset,
			long length, boolean ignoreChecksum, boolean corruptChecksumOk,
			boolean verifyChecksum, DataNode datanode, int coefficient) 
					throws IOException {
		
		long blockLength = datanode.data.getVisibleLength(namespaceId, block);
		this.ignoreChecksum = ignoreChecksum;

		InputStreamFactory streamFactory = new BlockInputStreamFactory(
				namespaceId, block, datanode.data);
		DataInputStream metadataIn = null;
		if (!ignoreChecksum) {
			try {
				metadataIn = new DataInputStream(
						new BufferedInputStream(
								datanode.data.getMetaDataInputStream(
										namespaceId, block), BUFFER_SIZE));
			} catch (IOException e) {
				if (!corruptChecksumOk
						|| datanode.data.metaFileExists(namespaceId, block)) {
					throw e;
				}
				metadataIn = null;
			}
		}
		initialize(namespaceId, block, blockLength, startOffset, length,
				corruptChecksumOk, verifyChecksum,
				metadataIn, streamFactory);
		this.coefficient = coefficient;
		this.GF = GaloisField.getInstance();
	}

	private void initialize(int namespaceId, Block block, long blockLength,
			long startOffset, long length, boolean corruptChecksumOk, 
			boolean verifyChecksum, DataInputStream metadataIn,
			InputStreamFactory streamFactory)
			throws IOException {
		try {
			this.block = block;
			this.corruptChecksumOk = corruptChecksumOk;
			this.verifyChecksum = verifyChecksum;
			this.blockLength = blockLength;
			
			this.conf = new Configuration();
			this.packetSize = conf.getInt("raid.blockreconstruct.packetsize", 4096);
			if (!corruptChecksumOk || metadataIn != null) {
				this.checksumIn = metadataIn;

				// read and handle the common header here. For now just a
				// version
				BlockMetadataHeader header = BlockMetadataHeader
						.readHeader(checksumIn);
				short version = header.getVersion();

				if (version != FSDataset.METADATA_VERSION) {
					LOG.warn("NTar:Wrong version (" + version
							+ ") for metadata file for " + block
							+ " ignoring ...");
				}
				checksum = header.getChecksum();
			} else {
				if (!ignoreChecksum) {
					LOG.warn("NTar:Could not find metadata file for " + block);
				}
				// This only decides the buffer size. Use BUFFER_SIZE?
				checksum = DataChecksum.newDataChecksum(
						DataChecksum.CHECKSUM_CRC32, 512);
			}

			/*
			 * If bytesPerChecksum is very large, then the metadata file is
			 * mostly corrupted. For now just truncate bytesPerchecksum to
			 * blockLength.
			 */
			bytesPerChecksum = checksum.getBytesPerChecksum();
			if (bytesPerChecksum > 10 * 1024 * 1024
					&& bytesPerChecksum > blockLength) {
				checksum = DataChecksum.newDataChecksum(
						checksum.getChecksumType(),
						Math.max((int) blockLength, 10 * 1024 * 1024));
				bytesPerChecksum = checksum.getBytesPerChecksum();
			}
			checksumSize = checksum.getChecksumSize();

			if (length < 0  || length > blockLength) {
				length = blockLength;
			}

			endOffset = blockLength;
			if (startOffset < 0 || startOffset >= endOffset) {
				//String msg = " Offset " + startOffset + " and length " + length
				//		+ " don't match block " + block + " ( blockLen "
				//		+ endOffset + " )";
				//LOG.error("NTar : BlockXCodingSender: " + msg);
				noData = true;
				return;
			}

			offset = (startOffset - (startOffset % bytesPerChecksum));
			if (length >= 0) {
				// Make sure endOffset points to end of a checksumed chunk.
				long tmpLen = startOffset + length;
				if (tmpLen % bytesPerChecksum != 0) {
					tmpLen += (bytesPerChecksum - tmpLen % bytesPerChecksum);
				}
				if (tmpLen < endOffset) {
					endOffset = tmpLen;
				}
			}

			// seek to the right offsets
			if (offset > 0) {
				long checksumSkip = (offset / bytesPerChecksum) * checksumSize;
				// note blockInStream is seeked when created below
				if (checksumSkip > 0 && checksumIn != null) {
					// Should we use seek() for checksum file as well?
					IOUtils.skipFully(checksumIn, checksumSkip);
				}
			}
			seqno = 0;

			blockIn = streamFactory.createStream(offset);
		} catch (IOException ioe) {
			IOUtils.closeStream(this);
			IOUtils.closeStream(blockIn);
			throw ioe;
		}
	}

	/**
	 * close opened files.
	 */
	public void close() throws IOException {
		IOException ioe = null;
		// close checksum file
		if (checksumIn != null) {
			try {
				checksumIn.close();
			} catch (IOException e) {
				ioe = e;
			}
			checksumIn = null;
		}
		// close data file
		if (blockIn != null) {
			try {
				blockIn.close();
			} catch (IOException e) {
				ioe = e;
			}
			blockIn = null;
		}
		// throw IOException if there is any
		if (ioe != null) {
			throw ioe;
		}
	}

	/**
	 * Converts an IOExcpetion (not subclasses) to SocketException. This is
	 * typically done to indicate to upper layers that the error was a socket
	 * error rather than often more serious exceptions like disk errors.
	 */
	protected static IOException ioeToSocketException(IOException ioe) {
		if (ioe.getClass().equals(IOException.class)) {
			// "se" could be a new class in stead of SocketException.
			IOException se = new SocketException("Original Exception : " + ioe);
			se.initCause(ioe);
			/*
			 * Change the stacktrace so that original trace is not truncated
			 * when printed.
			 */
			se.setStackTrace(ioe.getStackTrace());
			return se;
		}
		// otherwise just return the same exception.
		return ioe;
	}

	/**
	 * sendBlock() is used to read block and its metadata and stream the data to
	 * either a client or to another datanode.
	 * 
	 * @param out
	 *            stream to which the block is written to
	 * @param baseStream
	 *            optional. if non-null, <code>out</code> is assumed to be a
	 *            wrapper over this stream. This enables optimizations for
	 *            sending the data, e.g.
	 *            {@link SocketOutputStream#transferToFully(FileChannel, long, int)}
	 *            .
	 * @param throttler
	 *            for sending data.
	 * @return total bytes reads, including crc.
	 */
	public long sendBlock(DataOutputStream out, OutputStream baseStream,
			DataTransferThrottler throttler) throws IOException {
		return sendBlock(out, baseStream, throttler, null);
	}

	/**
	 * sendBlock() is used to read block and its metadata and stream the data to
	 * either a client or to another datanode.
	 * 
	 * @param out
	 *            stream to which the block is written to
	 * @param baseStream
	 *            optional. if non-null, <code>out</code> is assumed to be a
	 *            wrapper over this stream. This enables optimizations for
	 *            sending the data, e.g.
	 *            {@link SocketOutputStream#transferToFully(FileChannel, long, int)}
	 *            .
	 * @param throttler
	 *            for sending data.
	 * @param progress
	 *            for signaling progress.
	 * @return total bytes reads, including crc.
	 */
	public long sendBlock(DataOutputStream out, OutputStream baseStream,
			DataTransferThrottler throttler, Progressable progress)
			throws IOException {
		if (out == null) {
			throw new IOException("out stream is null");
		}

		long initialOffset = offset;
		long totalRead = 0;
		long totalToRead = endOffset - initialOffset;

		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {	
			if (noData) {
				out.writeInt(0); // mark the end of block
				out.flush();
				return 0;
			}

			int maxChunksPerPacket = Math.max(1, this.packetSize / bytesPerChecksum);
			int pktSize = bytesPerChecksum * maxChunksPerPacket;
			BlockingQueue<DataTransferPacket> readQueue = new ArrayBlockingQueue<DataTransferPacket>(3);
			BlockingQueue<DataTransferPacket> xcodeQueue = new ArrayBlockingQueue<DataTransferPacket>(3);
			executor.execute(new PacketReader(readQueue, maxChunksPerPacket, pktSize));
			executor.execute(new PacketXCoder(readQueue, xcodeQueue, totalToRead));

			while (totalRead < totalToRead) {
				DataTransferPacket packet = xcodeQueue.take();
				try {
					packet.write(out);
				} catch (IOException ioe) {
					LOG.error("NTar:BlockXCodingSender.sendBlock: " 
							+ "exception occured when writing packet:" + packet
							+ "\n" + StringUtils.stringifyException(ioe));
					break;
				}
				if (packet.dataLength > 0) {
					totalRead += packet.dataLength;
					if (throttler != null) { // rebalancing so throttle
						throttler.throttle(packet.dataLength);
					}
				} else {
					break;
				}
				if (progress != null) {
					progress.progress();
				}	
			}
			
			if(totalRead >= totalToRead) {
				out.writeInt(0); // mark the end of block
				out.flush();
			}
		} catch (RuntimeException e) {
			LOG.error("NTar: unexpected exception during sending block", e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("NTar : BlockXCodingSender.sendBlock interrupted.");
		} finally {
			blockReadFully = (initialOffset == 0 && offset >= blockLength);
			executor.shutdownNow();
			close();
		}

		return totalRead;
	}

	boolean isBlockReadFully() {
		return blockReadFully;
	}

	public static interface InputStreamFactory {
		public InputStream createStream(long offset) throws IOException;
	}

	private static class BlockInputStreamFactory implements InputStreamFactory {
		private final int namespaceId;
		private final Block block;
		private final FSDatasetInterface data;

		private BlockInputStreamFactory(int namespaceId, Block block,
				FSDatasetInterface data) {
			this.namespaceId = namespaceId;
			this.block = block;
			this.data = data;
		}

		@Override
		public InputStream createStream(long offset) throws IOException {
			return data.getBlockInputStream(namespaceId, block, offset);
		}
	}
	
	class PacketReader implements Runnable {
		BlockingQueue<DataTransferPacket> readQueue;
		int bufferSize;
		int maxChunks;
		
		int bufferLen = 0;
		int bufferPos = 0;
		int localBufferSize;
		byte[] localBuffer;
		
		public PacketReader(BlockingQueue<DataTransferPacket> readQueue, int maxChunks, int bufferSize) {
			this.readQueue = readQueue;
			this.bufferSize = bufferSize;
			this.maxChunks = maxChunks;
			localBufferSize = conf.getInt("raid.blockreconstruct.xcoder.buffersize", (256 << 10));
			LOG.info("NTar : packetReader : localBuffserSize = " + localBufferSize);
			localBuffer = new byte[localBufferSize];
		}
		
		public String toString() {
			return "Packet Reader of BlockXCodingSender";
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			boolean running = true;
			int packetLength = bytesPerChecksum * maxChunks;
			try {
				while(running && !Thread.interrupted()) {
					DataTransferPacket packet = new DataTransferPacket(bufferSize);
					// Sends multiple chunks in one packet with a single write().
					int dataLength = (int) Math.min(endOffset - offset, (long)packetLength);

					if (dataLength <= 0) {
						running = false;
						break;
					}
					
					packet.offsetInBlock = offset;
					packet.sequenceNumber = seqno;
					packet.setLastPacketInBlock((offset + dataLength >= endOffset) ? true : false);
					packet.dataLength = dataLength;

					int numRead = 0;
					int off = 0;
					if (bufferPos < bufferLen) {
						int numBytesToCopy = Math.min(dataLength - numRead, bufferLen - bufferPos);
						System.arraycopy(localBuffer, bufferPos, packet.buffer, off, numBytesToCopy);
						numRead += numBytesToCopy;	
						bufferPos += numBytesToCopy;
						off += numBytesToCopy;
					}
					
					while (numRead < dataLength) {
						int toRead = Math.min(localBufferSize, (int)(endOffset - offset));
						int numChunks = (toRead + bytesPerChecksum - 1) / bytesPerChecksum;
						int checksumLen = numChunks * checksumSize;
						byte[] checksumBuffer = new byte[checksumLen];
						
						if (checksumSize > 0 && checksumIn != null) {
							try {
								checksumIn.readFully(checksumBuffer, 0, checksumLen);
							} catch (IOException e) {
								LOG.error("NTar: " + this + " : Could not read checksum for data"
										+ " at offset " + offset + " for block " + block
										+ " got : " + StringUtils.stringifyException(e));
								if (corruptChecksumOk) {
									// Just fill the array with zeros.
									Arrays.fill(checksumBuffer, 0, checksumLen, (byte) 0);
								} else {
									packet.reset();
									packet.dataLength = -1;
									packet.errMessage = this + ":could not read checksum for data"
											+ " at offset " + offset + " for block " + block
											+ " got : " + StringUtils.stringifyException(e);
									running = false;
									break;
								}
							}
						}

						if (toRead > 0) {
							try {
								IOUtils.readFully(blockIn, localBuffer, 0, toRead);
								bufferPos = 0;
								bufferLen = toRead;
							} catch (IOException e) {
								LOG.error("NTar: " + this + " : exception occurred during read packet: "
										+ StringUtils.stringifyException(e));
								packet.reset();
								packet.dataLength = -1;
								packet.errMessage =  this + ":Exception occurred during read packet: "
										+ StringUtils.stringifyException(e);
								running = false;
								break;
							}
						}

						if (verifyChecksum) {
							int dOff = 0;
							int cOff = 0;
							int dLeft = toRead;

							for (int i = 0; i < numChunks; i++) {
								checksum.reset();
								int dLen = Math.min(dLeft, bytesPerChecksum);
								checksum.update(localBuffer, dOff, dLen);
								if (!checksum.compare(checksumBuffer, cOff)) {
									LOG.error("NTar: " + this + " : Checksum failed at " + (offset + dataLength - dLeft));
									running = false;
									packet.reset();
									packet.dataLength = -1;
									packet.errMessage = this + ":checksum failed at " + (offset + dataLength - dLeft);
									break;
								}
								dLeft -= dLen;
								dOff += dLen;
								cOff += checksumSize;
							}
							if (packet.dataLength < 0)
								break;
						}
						
						int numBytesToCopy = Math.min(dataLength - numRead, bufferLen - bufferPos);
						System.arraycopy(localBuffer, bufferPos, packet.buffer, off, numBytesToCopy);
						numRead += numBytesToCopy;	
						bufferPos += numBytesToCopy;
						off += numBytesToCopy;	
					}
					if (packet.dataLength >= 0) {
						offset += dataLength;
						seqno++;
						
						if (packet.dataLength > 0 && packet.dataLength < packetLength) {
							Arrays.fill(packet.buffer, packet.dataLength, 
									packetLength, (byte)0);
							packet.dataLength = packetLength;
						}
					}

					readQueue.put(packet);
					
				}
			} catch (InterruptedException e) {
				//LOG.error("NTar: " + this + " : interrupted: " 
				//		+ StringUtils.stringifyException(e));
			} 
		}
	}
	
	class PacketXCoder implements Runnable {
		BlockingQueue<DataTransferPacket> readQueue;
		BlockingQueue<DataTransferPacket> xcodeQueue;
		long totalToXCode;
		
		public PacketXCoder(BlockingQueue<DataTransferPacket> readQueue, 
				BlockingQueue<DataTransferPacket> xcodeQueue, long totalToXCode) {
			this.readQueue = readQueue;
			this.xcodeQueue = xcodeQueue;
			this.totalToXCode = totalToXCode;
		}
		
		public String toString() {
			return "Packet XCoder of BlockXCodingSender";
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			boolean running = true;
			long totalXCode = 0;
			try {
				while(running && !Thread.interrupted()) {
					DataTransferPacket packet = readQueue.take();
					if (packet.dataLength > 0) {
						xCodeData(packet.buffer, 0, packet.dataLength);
						totalXCode += packet.dataLength;
					} else {
						running = false;
					}
					
					if (totalXCode >= totalToXCode)
						running = false;
					
					xcodeQueue.put(packet);
				}
			} catch (InterruptedException e) {
				//LOG.error("NTar: " + this + " : interrupted: " + StringUtils.stringifyException(e));
			} 
		}
	}
	
	private void xCodeData(byte[] data, int offset, int length) {

		if (coefficient == 0) {
			Arrays.fill(data, offset, length, (byte) (0 & 0x000000FF));
			return;
		} else if (coefficient == 1) {
			return;
		}

		for (int i = 0; i < length; i++)
			data[offset + i] = (byte) (GF.multiply(coefficient, data[offset + i] & 0x000000FF) & 0x000000FF);
	}

}
