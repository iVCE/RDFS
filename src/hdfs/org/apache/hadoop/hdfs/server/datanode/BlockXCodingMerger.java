package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.DataTransferPacket;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ChecksumUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;

/**
 * A class that merges a block and writes to its own disk, or transfer to its
 * parent site. If a throttler is provided, streaming throttling is also supported.
 **/
abstract public class BlockXCodingMerger implements java.io.Closeable, FSConstants {
	static {
		Configuration.addDefaultResource("raid-site.xml");
	}
	protected static final Log LOG = DataNode.LOG;
	static final Log ClientTraceLog = DataNode.ClientTraceLog;

	protected Block block; 	// the block that is being reconstructed
	protected int namespaceId; // the namespace the block being reconstructed belongs to
	protected DataInputStream[] childInputStreams = null; // from where data are read
	protected DataChecksum checksum;   // the checksum of data read
	protected int bytesPerChecksum = 0;
	protected int checksumSize = 0;
	protected long offsetInBlock;
	protected long length;
	protected final String[] childAddrs;
	protected final String myAddr;
	private DataTransferThrottler throttler;
	protected int mergerLevel; // the level of merger in the recovery tree
							   // the level of root is 0
	int packetSize = 4096;

	public BlockXCodingMerger(Block block, int namespaceId,
			DataInputStream[] childInputStreams, long offsetInBlock,
			long length, String[] childAddrs, String myAddr,
			DataTransferThrottler throttler,
			int mergerLevel) throws IOException{
		super();
		this.block = block;
		this.namespaceId = namespaceId;
		this.childInputStreams = childInputStreams;
		this.offsetInBlock = offsetInBlock;
		this.length = length;
		this.childAddrs = childAddrs;
		this.myAddr = myAddr;
		this.throttler = throttler;
		this.mergerLevel = mergerLevel;
		Configuration conf = new Configuration();
		this.packetSize = conf.getInt("raid.blockreconstruct.packetsize", 4096);
		this.bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
		this.checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
				bytesPerChecksum, new PureJavaCrc32());
		this.checksumSize = checksum.getChecksumSize();
	}
	
	/**
	 * Write the data packet merged from child sites somewhere
	 * 
	 * @param buffer
	 * 		the ByteBuffer of packet merged
	 * @param packetInfo
	 * 		the information of the packet merged
	 * @throws IOException
	 */
	abstract void writePacket(DataTransferPacket packet) throws IOException;
	abstract void finalizeWrite() throws IOException;
	abstract void handleException(Exception e) throws IOException;


	class PacketReader implements Runnable {
		int id;
		int bufferSize;
		DataInputStream inputStream;
		BlockingQueue<DataTransferPacket> readQueue;
		
		PacketReader(DataInputStream inputStream, int bufferSize,
				BlockingQueue<DataTransferPacket> readQueue, int id){
			this.inputStream = inputStream;
			this.readQueue = readQueue;
			this.id = id;
			this.bufferSize = bufferSize;
		}
		
		public void run() {
			boolean running = true;
			try {
				while(running && !Thread.interrupted()) {
					DataTransferPacket packet = new DataTransferPacket(bufferSize);
					try {
						packet.read(inputStream);
					} catch (IOException e) {
						packet.reset();
						packet.dataLength = -1;
						LOG.error("NTar: " + this + " : Exception occured during reading packet:" 
								+ StringUtils.stringifyException(e));
						packet.errMessage = this + " : Exception occured during reading packet:" 
								+ StringUtils.stringifyException(e);
					}
					readQueue.put(packet);
					if(packet.dataLength <= 0)
						running = false;
				}
			} catch (InterruptedException e) {
				//LOG.error("NTar: " + this + " : interrupted:" + StringUtils.stringifyException(e));	
			}
		}
		
		public String toString() {
			return "Packet Reader [" + id + "] of BlockXCodingMerger";
		}
	}
	
	class PacketMerger implements Runnable {
		int bufferSize;
		List<BlockingQueue<DataTransferPacket>> readQueues;
		BlockingQueue<DataTransferPacket> mergeQueue;
		
		PacketMerger(List<BlockingQueue<DataTransferPacket>> readQueues, int bufferSize,
				BlockingQueue<DataTransferPacket> mergeQueue) {
			this.readQueues =  new ArrayList<BlockingQueue<DataTransferPacket>>(readQueues.size());
			this.readQueues.addAll(readQueues);
			this.mergeQueue = mergeQueue;
			this.bufferSize = bufferSize;
		}
		
		@Override
		public void run() {
			boolean running = true;
			List<BlockingQueue<DataTransferPacket>> finishedQueue = 
					new LinkedList<BlockingQueue<DataTransferPacket>>();
			try {
				while (running && !Thread.interrupted()) {
					DataTransferPacket mergedPacket = null;
					
					for (BlockingQueue<DataTransferPacket> queue : readQueues) {
						DataTransferPacket packet = queue.take();
						if (packet.dataLength < 0) {
							mergedPacket = packet;
							break;
						}
						if (packet.dataLength == 0) {
							finishedQueue.add(queue);
						}
						
						if (mergedPacket == null) {
							mergedPacket = packet;
							continue;
						}
						
						if ((packet.dataLength > 0) && (mergedPacket.dataLength > 0)
								&& (mergedPacket.offsetInBlock != packet.offsetInBlock)) {
							String errMessage = this + ":offsetInBlock does not match "
									+ mergedPacket.offsetInBlock + " vs " + packet.offsetInBlock;
							mergedPacket.reset();
							mergedPacket.dataLength = -1;
							mergedPacket.errMessage = errMessage;
							break;
						}
						if (mergedPacket.dataLength < packet.dataLength) {
							DataTransferPacket tempPacket = mergedPacket;
							mergedPacket = packet;
							packet = tempPacket;
						}

						byte[] packetBuffer = packet.buffer;
						byte[] mergedPacketBuffer = mergedPacket.buffer;
						for (int j = 0; j < mergedPacket.dataLength; j++) {
							mergedPacketBuffer[j] ^= packetBuffer[j];
						}
					}
					
					for (BlockingQueue<DataTransferPacket> queue : finishedQueue) {
						readQueues.remove(queue);
					}
					mergeQueue.put(mergedPacket);
					if (mergedPacket.dataLength <= 0) {
						running = false;
					}
				}
				
			} catch (InterruptedException e) {
				LOG.info("NTar: Packet merger interrupted.");
			}
		}
		
		public String toString() {
			return "Packet Merger";
		}
	}
	
	public long mergeBlock() throws IOException {
		long totalMergedSize = 0;
		ExecutorService executor = Executors.newFixedThreadPool(childInputStreams.length + 1);
		try {
			List<BlockingQueue<DataTransferPacket>> readQueues 
				= new ArrayList<BlockingQueue<DataTransferPacket>>(childInputStreams.length);
			for(int i = 0; i < childInputStreams.length; i++) {
				BlockingQueue<DataTransferPacket> readQueue = new ArrayBlockingQueue<DataTransferPacket>(3);
				readQueues.add(readQueue);
				executor.execute(new PacketReader(childInputStreams[i], packetSize, readQueue, i));
			}
			BlockingQueue<DataTransferPacket> mergeQueue = new ArrayBlockingQueue<DataTransferPacket>(3);
			executor.execute(new PacketMerger(readQueues, packetSize, mergeQueue));
			
			boolean running = true;
			while (running) {
				DataTransferPacket packet = mergeQueue.take();
				if (packet.dataLength <= 0)
					running = false;
				if (packet.dataLength < 0) {
					throw new IOException("Error packet.dataLength[" + packet.dataLength
							+ "], at offsetInBlock[" + this.offsetInBlock + "], packet.errMessage" 
							+ packet.errMessage);
				}
				if(packet.dataLength > 0) {
					writePacket(packet);
					totalMergedSize += packet.dataLength;
					continue;
				} else {
					finalizeWrite();
				}
			}
		} catch (IOException ioe) {
			LOG.error("NTar: Exception in mergeBlock for block " + block + " :\n"
					+ StringUtils.stringifyException(ioe));
			IOUtils.closeStream(this);
			handleException(ioe);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			handleException(e);
		} finally {
			executor.shutdownNow();
		}
		return totalMergedSize;
	}
	
	public static class InternalBlockXCodingMerger extends BlockXCodingMerger {
		private String parentAddr;
		private DataOutputStream parentOut;
		
		public InternalBlockXCodingMerger(Block block, int namespaceId,
				DataInputStream[] childInputStreams, long offsetInBlock,
				long length, String[] childAddrs, String myAddr,
				DataTransferThrottler throttler,
				int mergerLevel, String parentAddr,
				DataOutputStream parentOut) throws IOException {
			super(block, namespaceId, childInputStreams, offsetInBlock, length,
					childAddrs, myAddr, throttler,
					mergerLevel);
			this.parentAddr = parentAddr;
			this.parentOut = parentOut;
		}
		
		@Override
		void writePacket(DataTransferPacket packet)
				throws IOException {
		    if (parentOut != null) {
		      try {
		        packet.write(parentOut);
		      } catch (IOException e) {
		        LOG.error("NTar: Exception in mergeBlock for block" + block 
		        		+ " whend sending data to parent: \n" 
		        		+ StringUtils.stringifyException(e));
		        throw e;
		      }
		    }
		}

		@Override
		void finalizeWrite() throws IOException {
			// TODO Auto-generated method stub
			parentOut.writeInt(0);
			parentOut.flush();
		}

		@Override
		void handleException(Exception e) throws IOException {
			// TODO Auto-generated method stub
			parentOut.writeInt(-1);
			parentOut.flush();
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
	
	public static class BufferBlockXCodingMerger extends BlockXCodingMerger {
		private byte[] buffer;
		private int offsetInBuffer;
		private long currentOffsetInBlock;
		private int writtenLength = 0;
		public BufferBlockXCodingMerger(Block block, int namespaceId,
				DataInputStream[] childInputStreams, long offsetInBlock,
				long length, String[] childAddrs, String myAddr,
				DataTransferThrottler throttler,int mergerLevel,
				byte[] buffer, int offsetInBuffer) throws IOException {
			super(block, namespaceId, childInputStreams, offsetInBlock, length,
					childAddrs, myAddr, throttler, mergerLevel);
			this.buffer = buffer;
			this.offsetInBuffer = offsetInBuffer;
			this.currentOffsetInBlock = offsetInBlock;
		}
		
		@Override
		void writePacket(DataTransferPacket packet) {
			if (packet.dataLength > 0) {
				int dataLength = packet.dataLength;
				int alienOff = (int) (currentOffsetInBlock - packet.offsetInBlock);
				
				if (alienOff > 0) {
					dataLength -= alienOff;
				} else if (alienOff < 0)
					throw new ArrayIndexOutOfBoundsException("packet.dataLength = [" + packet.dataLength
							+ "], packet.offsetInBlock = [" + packet.offsetInBlock
							+ "], currentOffsetInBlock = [" + currentOffsetInBlock + "]");

				dataLength = (int) Math.min(dataLength, this.length - this.writtenLength);
				System.arraycopy(packet.buffer, alienOff, buffer, offsetInBuffer, dataLength);
				
				this.writtenLength += dataLength;
			    offsetInBuffer += dataLength;
			    currentOffsetInBlock += dataLength;
			}
		}

		@Override
		void finalizeWrite() throws IOException {
			// TODO Auto-generated method stub
		}

		@Override
		void handleException(Exception e) throws IOException {
			// TODO Auto-generated method stub
			throw new IOException(e);
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
	}

	public static class RootBlockXCodingMerger extends BlockXCodingMerger {
		private boolean finalized;
		final private boolean isRecovery; 
		private OutputStream out = null;  // to block file at local disk
		private OutputStream cout = null; // output stream for checksum file
		private DataOutputStream checksumOut = null; // to crc file at local disk
		
		private FSDataset.BlockWriteStreams streams = null;
		private ReplicaBeingWritten replicaBeingWritten = null;
		private Checksum partialCrc = null;
		private DataNode datanode;
		private boolean writtenHeader = false;
		private long flushNumber = 0;
		
		private long writtenLength = 0;
		
		public RootBlockXCodingMerger(Block block, int namespaceId,
				DataInputStream[] childInputStreams, long offsetInBlock,
				long length, boolean isRecovery, String[] childAddrs, String myAddr,
				DataTransferThrottler throttler, DataNode datanode, int mergerLevel) 
						throws IOException {
			super(block, namespaceId, childInputStreams, offsetInBlock, length,
					childAddrs, myAddr, throttler, mergerLevel);
			this.datanode = datanode;
			this.isRecovery = isRecovery;
			try {
				// Open local disk out
				streams = datanode.data.writeToBlock(namespaceId, this.block,
						this.isRecovery, false);
				replicaBeingWritten = datanode.data.getReplicaBeingWritten(
						namespaceId, this.block);
				this.finalized = false;
				if (streams != null) {
					this.out = streams.dataOut;
					this.cout = streams.checksumOut;
					this.checksumOut = new DataOutputStream(
							new BufferedOutputStream(streams.checksumOut,
									SMALL_BUFFER_SIZE));
					// If this block is for appends, then remove it from
					// periodic validation.
					if (datanode.blockScanner != null && isRecovery) {
						datanode.blockScanner.deleteBlock(namespaceId, block);
					}
				}
			} catch (BlockAlreadyExistsException bae) {
				throw bae;
			} catch (IOException ioe) {
				IOUtils.closeStream(this);
				cleanupBlock();

				// check if there is a disk error
				IOException cause = FSDataset.getCauseIfDiskError(ioe);
				LOG.warn("NTar:IOException in RootBlockXCodingMerger constructor. "
						+ "Cause is " + cause);
				
				if (cause != null) { // possible disk error
					ioe = cause;
					datanode.checkDiskError(ioe); // may throw an exception here
				}
				throw ioe;
			}
		}

		/**
		 * close files.
		 */
		 public void close() throws IOException {
		    IOException ioe = null;
		    // close checksum file
		    try {
		      if (checksumOut != null) {
		        try {
		          checksumOut.flush();
		          if (datanode.syncOnClose && (cout instanceof FileOutputStream)) {
		            ((FileOutputStream)cout).getChannel().force(true);
		          }
		        } finally {
		          checksumOut.close();          
		          checksumOut = null;
		        }
		      }
		    } catch(IOException e) {
		      ioe = e;
		    }
		    // close block file
		    try {
		      if (out != null) {
		        try {
		          out.flush();
		          if (datanode.syncOnClose && (out instanceof FileOutputStream)) {
		            ((FileOutputStream)out).getChannel().force(true);
		          }
		        } finally {
		          out.close();
		          out = null;
		        }
		      }
		    } catch (IOException e) {
		      ioe = e;
		    }
		    
		    // disk check
		    // We don't check disk for ClosedChannelException as close() can be
		    // called twice and it is possible that out.close() throws.
		    // No need to check or recheck disk then.
		    if (ioe != null) {
		      if (!(ioe instanceof ClosedChannelException)) {
		        datanode.checkDiskError(ioe);
		      }
		      throw ioe;
		    }
		  }

		/**
		 * Flush the data and checksum data out to the stream. Please call sync to
		 * make sure to write the data in to disk
		 * 
		 * @throws IOException
		 */
		void flush(boolean forceSync) throws IOException {
			flushNumber += 1;
			if (flushNumber % 20 != 0)
				return;
			
			if (checksumOut != null) {
				checksumOut.flush();
				if (forceSync && (cout instanceof FileOutputStream)) {
					((FileOutputStream) cout).getChannel().force(true);
				}
			}
			if (out != null) {
				out.flush();
				if (forceSync && (out instanceof FileOutputStream)) {
					((FileOutputStream) out).getChannel().force(true);
				}
			}
		}

		void writeChecksumHeader(DataChecksum checksum)
				throws IOException {
			// write data chunk header
			if (!finalized) {
				BlockMetadataHeader.writeHeader(checksumOut, checksum);
			}
		}
		
		void finalizeWrite() throws IOException {
			// close the block/crc files
			close();

			// Finalize the block. 
			block.setNumBytes(offsetInBlock);
			datanode.data.finalizeBlock(namespaceId, block);
			datanode.myMetrics.blocksWritten.inc();
			
			// if this write is for a lost block then confirm block. 
			if (!isRecovery) {
				datanode.notifyNamenodeReceivedBlock(namespaceId, block, null);
			} 
			
			if (datanode.blockScanner != null) {
				datanode.blockScanner.addBlock(namespaceId, block);
			}
		}
		
		/**
		 * Cleanup a partial block
		 * */
		private void cleanupBlock() throws IOException {
			datanode.data.unfinalizeBlock(namespaceId, block);
		}

		/**
		 * Sets the file pointer in the local block file to the specified value.
		 */
		private void setBlockPosition(long offsetInBlock) throws IOException {
			if (finalized) {
				if (!isRecovery) {
					throw new IOException("Merge to offset " + offsetInBlock
							+ " of block " + block + " that is already finalized.");
				}
				if (offsetInBlock > datanode.data.getFinalizedBlockLength(
						namespaceId, block)) {
					throw new IOException("Merge to offset " + offsetInBlock
							+ " of block " + block
							+ " that is already finalized and is of size "
							+ datanode.data.getFinalizedBlockLength(namespaceId, block));
				}
				return;
			}

			if (datanode.data.getChannelPosition(namespaceId, block, streams) 
					== offsetInBlock) {
				return; // nothing to do
			}
			long offsetInChecksum = BlockMetadataHeader.getHeaderSize()
					+ offsetInBlock / bytesPerChecksum * checksumSize;
			if (out != null) {
				out.flush();
			}
			if (checksumOut != null) {
				checksumOut.flush();
			}

			// If this is a partial chunk, then read in pre-existing checksum
			if (offsetInBlock % bytesPerChecksum != 0) {
				LOG.info("NTar:setBlockPosition trying to set position to "
						+ offsetInBlock + " for block " + block
						+ " which is not a multiple of bytesPerChecksum "
						+ bytesPerChecksum);
				computePartialChunkCrc(offsetInBlock, offsetInChecksum,
						bytesPerChecksum);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("NTar:Changing block file offset of block "
						+ block
						+ " from "
						+ datanode.data.getChannelPosition(namespaceId, block,
								streams) + " to " + offsetInBlock
						+ " meta file offset to " + offsetInChecksum);
			}

			// set the position of the block file
			datanode.data.setChannelPosition(namespaceId, block, streams,
					offsetInBlock, offsetInChecksum);
		}

		/**
		 * reads in the partial crc chunk and computes checksum of pre-existing data
		 * in partial chunk.
		 */
		private void computePartialChunkCrc(long blkoff, long ckoff,
				int bytesPerChecksum) throws IOException {

			// find offset of the beginning of partial chunk.
			//
			int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
			int checksumSize = checksum.getChecksumSize();
			blkoff = blkoff - sizePartialChunk;

			// create an input stream from the block file
			// and read in partial crc chunk into temporary buffer
			byte[] buf = new byte[sizePartialChunk];
			byte[] crcbuf = new byte[checksumSize];
			FSDataset.BlockInputStreams instr = null;
			try {
				instr = datanode.data.getTmpInputStreams(namespaceId, block,
						blkoff, ckoff);
				IOUtils.readFully(instr.dataIn, buf, 0, sizePartialChunk);

				// open meta file and read in crc value computer earlier
				IOUtils.readFully(instr.checksumIn, crcbuf, 0, crcbuf.length);
			} finally {
				IOUtils.closeStream(instr);
			}

			// compute crc of partial chunk from data read in the block file.
			partialCrc = new CRC32();
			partialCrc.update(buf, 0, sizePartialChunk);

			// paranoia! verify that the pre-computed crc matches what we
			// recalculated just now
			if (partialCrc.getValue() != FSInputChecker.checksum2long(crcbuf)) {
				String msg = "Partial CRC " + partialCrc.getValue()
						+ " does not match value computed the "
						+ " last time file was closed "
						+ FSInputChecker.checksum2long(crcbuf);
				throw new IOException(msg);
			}
		}

		@Override
		void writePacket(DataTransferPacket packet) throws IOException {
			
			if (!writtenHeader) {
				writeChecksumHeader(checksum);
				writtenHeader = true;
			}
			
		    boolean forceSync = packet.isForceSync();
		    int dataLength = packet.dataLength;
		    
		    if (dataLength <= 0) {
		      LOG.warn("NTar: writePacket: Receiving empty packet:" + 
		    		  packet + " for block " + block);
		    } else {
		      dataLength = (int) Math.min(dataLength, this.length - this.writtenLength);
		      setBlockPosition(offsetInBlock);  // adjust file position
		      offsetInBlock += dataLength;
		      this.writtenLength += dataLength;

		      int checksumLength = (dataLength + bytesPerChecksum -1)/bytesPerChecksum 
		    		  * checksumSize;
		     
		      byte[] pktBuf = new byte[checksumLength + dataLength];
		      Arrays.fill(pktBuf, (byte)0);
		      System.arraycopy(packet.buffer, 0, pktBuf, 
		    		  checksumLength, dataLength);
		      ChecksumUtil.updateChunkChecksum(pktBuf, 0,
		    		  checksumLength, dataLength, checksum);
		      
		      try {
		        if (!finalized) {
		          long writeStartTime = System.currentTimeMillis();
		          //finally write to the disk :
		          out.write(pktBuf, checksumLength, dataLength);

		          // If this is a partial chunk, then verify that this is the only
		          // chunk in the packet. Calculate new crc for this chunk.
		          if (partialCrc != null) {
		            if (dataLength > bytesPerChecksum) {
		              throw new IOException("Got wrong length during mergeBlock(" + 
		                                    block + ") from " + childAddrs + " " +
		                                    "A packet can have only one partial chunk."+
		                                    " len = " + dataLength + 
		                                    " bytesPerChecksum " + bytesPerChecksum);
		            }
		            partialCrc.update(pktBuf, checksumLength, dataLength);
		            byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
		            checksumOut.write(buf);
		            LOG.debug("Writing out partial crc for data len " + dataLength);
		            partialCrc = null;
		          } else {
		            checksumOut.write(pktBuf, 0, checksumLength);
		          }
		          datanode.myMetrics.bytesWritten.inc(dataLength);

		          flush(forceSync);

		          this.replicaBeingWritten.setBytesOnDisk(offsetInBlock);
		          // Record time taken to write packet
		          long writePacketDuration = System.currentTimeMillis() - writeStartTime;
		          datanode.myMetrics.writePacketLatency.inc(writePacketDuration);
		        }
		      } catch (ClosedByInterruptException cix) {
		        LOG.warn( "NTar: Thread interrupted when flushing bytes to disk."
		        		+ "Might cause inconsistent sates", cix);
		        throw cix;
		      } catch (InterruptedIOException iix) {
		        LOG.warn(
		            "NTar: InterruptedIOException when flushing bytes to disk."
		        		+ "Might cause inconsistent sates", iix);
		        throw iix;
		      } catch (IOException iex) {
		        datanode.checkDiskError(iex);
		        throw iex;
		      }
		    }
		  }

		@Override
		void handleException(Exception e) throws IOException {
			// TODO Auto-generated method stub
			cleanupBlock();
		}
	}
}







