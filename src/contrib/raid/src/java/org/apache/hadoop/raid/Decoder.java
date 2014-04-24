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

package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.net.NetUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.StripeReader.LocationPair;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MinimumSpanningTree;
import org.apache.hadoop.hdfs.RecoverTreeNode;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.MergeBlockHeader;
import org.apache.hadoop.hdfs.protocol.VersionAndOpcode;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockXCodingMerger;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Counter;
import org.apache.hadoop.raid.RaidNode.LOGTYPES;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.zip.CRC32;

/**
 * Represents a generic decoder that can be used to read a file with corrupt
 * blocks by using the parity file.
 */
public class Decoder {
	public static final Log LOG = LogFactory
			.getLog("org.apache.hadoop.raid.Decoder");
	private final Log DECODER_METRICS_LOG = LogFactory.getLog("RaidMetrics");
	public static final int DEFAULT_PARALLELISM = 4;
	protected Configuration conf;
	protected int parallelism;
	protected Codec codec;
	protected ErasureCode code;
	protected Random rand;
	protected int bufSize;
	protected byte[][] readBufs;
	protected byte[][] writeBufs;
	private int numMissingBlocksInStripe;
	private long numReadBytes;

	/**
	 * added by jason
	 */
	public static final int DEFAULT_DECODING_THREADNUM = 5;
	protected int decodingThreadNum;				
	public static final boolean DEFAULT_DECODING_IFPARALLELISM = true;
	protected boolean ifDecodingParallelism;	
	/**
	 * added by jason ended
	 */
	
	public Decoder(Configuration conf, Codec codec) {
		this.conf = conf;
		this.parallelism = conf.getInt("raid.encoder.parallelism",
				DEFAULT_PARALLELISM);
		this.codec = codec;
		this.code = codec.createErasureCode(conf);
		this.rand = new Random();
		this.bufSize = conf.getInt("raid.decoder.bufsize", 1024 * 1024);
		// this.writeBufs = new byte[codec.parityLength][];
		this.readBufs = new byte[codec.parityLength + codec.stripeLength][];
		
		/**
		 * added by jason
		 */
		// allocateBuffers();
		// writeBufs will be allocated when the writeBufs.length is known and
		// writeBufs array can be initialized.
		this.decodingThreadNum = (int) conf.getInt("raid.decoder.threadnum", DEFAULT_DECODING_THREADNUM);
		this.ifDecodingParallelism = conf.getBoolean("raid.decoder.ifparallelism", DEFAULT_DECODING_IFPARALLELISM);
		code.initThreadPool(decodingThreadNum);
		/**
		 * added by jason ended
		 */
	}

	public int getNumMissingBlocksInStripe() {
		return numMissingBlocksInStripe;
	}

	public long getNumReadBytes() {
		return numReadBytes;
	}

	private void allocateBuffers() {
		for (int i = 0; i < writeBufs.length; i++) {
			writeBufs[i] = new byte[bufSize];
		}
	}

	private void configureBuffers(long blockSize) {
		if ((long) bufSize > blockSize) {
			bufSize = (int) blockSize;
			allocateBuffers();
		} else if (blockSize % bufSize != 0) {
			bufSize = (int) (blockSize / 256L); // heuristic.
			if (bufSize == 0) {
				bufSize = 1024;
			}
			bufSize = Math.min(bufSize, 1024 * 1024);
			allocateBuffers();
		}
	}

	public void recoverParityBlockToFile(FileSystem srcFs, Path srcPath,
			FileSystem parityFs, Path parityPath, long blockSize,
			long blockOffset, File localBlockFile, Context context)
			throws IOException, InterruptedException {
		OutputStream out = new FileOutputStream(localBlockFile);
		fixErasedBlock(srcFs, srcPath, parityFs, parityPath, false, blockSize,
				blockOffset, blockSize, false, out, context, false);
		out.close();
	}

	/**
	 * Recovers a corrupt block to local file.
	 * 
	 * @param srcFs
	 *            The filesystem containing the source file.
	 * @param srcPath
	 *            The damaged source file.
	 * @param parityPath
	 *            The filesystem containing the parity file. This could be
	 *            different from srcFs in case the parity file is part of a HAR
	 *            archive.
	 * @param parityFile
	 *            The parity file.
	 * @param blockSize
	 *            The block size of the file.
	 * @param blockOffset
	 *            Known location of error in the source file. There could be
	 *            additional errors in the source file that are discovered
	 *            during the decode process.
	 * @param localBlockFile
	 *            The file to write the block to.
	 * @param limit
	 *            The maximum number of bytes to be written out. This is to
	 *            prevent writing beyond the end of the file.
	 * @param reporter
	 *            A mechanism to report progress.
	 */
	public void recoverBlockToFile(FileSystem srcFs, Path srcPath,
			FileSystem parityFs, Path parityPath, long blockSize,
			long blockOffset, File localBlockFile, long limit, Context context)
			throws IOException, InterruptedException {
		OutputStream out = new FileOutputStream(localBlockFile);
		fixErasedBlock(srcFs, srcPath, parityFs, parityPath, true, blockSize,
				blockOffset, limit, false, out, context, false);
		out.close();
	}

	/**
	 * Return the old code id to construct a old decoder
	 */
	private String getOldCodeId(Path srcFile) throws IOException {
		if (codec.id.equals("xor") || codec.id.equals("rs")) {
			return codec.id;
		} else {
			// Search for xor/rs parity files
			if (ParityFilePair.getParityFile(Codec.getCodec("xor"), srcFile,
					this.conf) != null)
				return "xor";
			if (ParityFilePair.getParityFile(Codec.getCodec("rs"), srcFile,
					this.conf) != null)
				return "rs";
		}
		return null;
	}

	DecoderInputStream generateAlternateStream(FileSystem srcFs, Path srcFile,
			FileSystem parityFs, Path parityFile, long blockSize,
			long errorOffset, long limit, Context context) {
		configureBuffers(blockSize);
		Progressable reporter = context;
		if (reporter == null) {
			reporter = RaidUtils.NULL_PROGRESSABLE;
		}

		DecoderInputStream decoderInputStream = null;
		
		if(codec.id.equals("crs") || codec.id.equals("lrc")) {
			decoderInputStream = new CRSDecoderInputStream(
					reporter, limit, blockSize, errorOffset, srcFs, srcFile,
					parityFs, parityFile);
		} else {
			decoderInputStream = new DecoderInputStream(
					reporter, limit, blockSize, errorOffset, srcFs, srcFile,
					parityFs, parityFile);
		}
		
		/*
		decoderInputStream = new DecoderInputStream(
				reporter, limit, blockSize, errorOffset, srcFs, srcFile,
				parityFs, parityFile);
		*/
		return decoderInputStream;
	}

	/**
	 * Having buffers of the right size is extremely important. If the the
	 * buffer size is not a divisor of the block size, we may end up reading
	 * across block boundaries.
	 * 
	 * If codec's simulateBlockFix is true, we use the old code to fix blocks
	 * and verify the new code's result is the same as the old one.
	 */
	void fixErasedBlock(FileSystem srcFs, Path srcFile, FileSystem parityFs,
			Path parityFile, boolean fixSource, long blockSize,
			long errorOffset, long limit, boolean partial, OutputStream out,
			Context context, boolean skipVerify) throws IOException,
			InterruptedException {
		configureBuffers(blockSize);
		Progressable reporter = context;
		if (reporter == null) {
			reporter = RaidUtils.NULL_PROGRESSABLE;
		}

		LOG.info("Code: " + this.codec.id + " simulation: "
				+ this.codec.simulateBlockFix);
		if (this.codec.simulateBlockFix) {
			String oldId = getOldCodeId(srcFile);
			if (oldId == null) {
				// Couldn't find old codec for block fixing, throw exception
				// instead
				throw new IOException("Couldn't find old parity files for "
						+ srcFile + ". Won't reconstruct the block since code "
						+ this.codec.id + " is still under test");
			}
			if (partial) {
				throw new IOException(
						"Couldn't reconstruct the partial data because "
								+ "old decoders don't support it");
			}
			Decoder decoder = (oldId.equals("xor")) ? new XORDecoder(conf)
					: new ReedSolomonDecoder(conf);
			CRC32 newCRC = null;
			long newLen = 0;
			if (!skipVerify) {
				newCRC = new CRC32();
				newLen = this.fixErasedBlockImpl(srcFs, srcFile, parityFs,
						parityFile, fixSource, blockSize, errorOffset, limit,
						partial, null, reporter, newCRC);
			}
			CRC32 oldCRC = skipVerify ? null : new CRC32();
			long oldLen = decoder.fixErasedBlockImpl(srcFs, srcFile, parityFs,
					parityFile, fixSource, blockSize, errorOffset, limit,
					partial, out, reporter, oldCRC);

			if (!skipVerify) {
				if (newCRC.getValue() != oldCRC.getValue() || newLen != oldLen) {
					LOG.error(" New code "
							+ codec.id
							+ " produces different data from old code "
							+ oldId
							+ " during fixing "
							+ (fixSource ? srcFile.toString() : parityFile
									.toString()) + " (offset=" + errorOffset
							+ ", limit=" + limit + ")" + " checksum:"
							+ newCRC.getValue() + ", " + oldCRC.getValue()
							+ " len:" + newLen + ", " + oldLen);
					if (context != null) {
						context.getCounter(Counter.BLOCK_FIX_SIMULATION_FAILED)
								.increment(1L);
						String outkey;
						if (fixSource) {
							outkey = srcFile.toString();
						} else {
							outkey = parityFile.toString();
						}
						String outval = "simulation_failed";
						context.write(new Text(outkey), new Text(outval));
					}
				} else {
					LOG.info(" New code "
							+ codec.id
							+ " produces the same data with old code "
							+ oldId
							+ " during fixing "
							+ (fixSource ? srcFile.toString() : parityFile
									.toString()) + " (offset=" + errorOffset
							+ ", limit=" + limit + ")");
					if (context != null) {
						context.getCounter(
								Counter.BLOCK_FIX_SIMULATION_SUCCEEDED)
								.increment(1L);
					}
				}
			}
		} else {
			fixErasedBlockImpl(srcFs, srcFile, parityFs, parityFile, fixSource,
					blockSize, errorOffset, limit, partial, out, reporter, null);
		}
	}

	long fixErasedBlockImpl(FileSystem srcFs, Path srcFile,
			FileSystem parityFs, Path parityFile, boolean fixSource,
			long blockSize, long errorOffset, long limit, boolean partial,
			OutputStream out, Progressable reporter, CRC32 crc)
			throws IOException {

		long startTime = System.currentTimeMillis();
		if (crc != null) {
			crc.reset();
		}
		int blockIdx = (int) (errorOffset / blockSize);
		LocationPair lp = null;
		int erasedLocationToFix;
		if (fixSource) {
			lp = StripeReader.getBlockLocation(codec, srcFs, srcFile, blockIdx,
					conf);
			erasedLocationToFix = codec.parityLength + lp.getBlockIdxInStripe();
		} else {
			lp = StripeReader.getParityBlockLocation(codec, blockIdx);
			erasedLocationToFix = lp.getBlockIdxInStripe();
		}

		FileStatus srcStat = srcFs.getFileStatus(srcFile);
		FileStatus parityStat = parityFs.getFileStatus(parityFile);

		InputStream[] inputs = null;
		List<Integer> erasedLocations = new ArrayList<Integer>();
		// Start off with one erased location.
		erasedLocations.add(erasedLocationToFix);
		List<Integer> locationsToRead = new ArrayList<Integer>(
				codec.parityLength + codec.stripeLength);

		int boundedBufferCapacity = 2;
		ParallelStreamReader parallelReader = null;
		LOG.info("Need to write " + limit + " bytes for erased location index "
				+ erasedLocationToFix);

		long startOffsetInBlock = 0;
		if (partial) {
			startOffsetInBlock = errorOffset % blockSize;
		}

		// will be resized later
		int[] erasedLocationsArray = new int[0];
		int[] locationsToReadArray = new int[0];
		int[] locationsNotToReadArray = new int[0];

		try {
			numReadBytes = 0;
			long written;
			// Loop while the number of written bytes is less than the max.
			for (written = 0; written < limit;) {
				try {
					if (parallelReader == null) {
						long offsetInBlock = written + startOffsetInBlock;
						StripeReader sReader = StripeReader.getStripeReader(
								codec, conf, blockSize, srcFs,
								lp.getStripeIdx(), srcStat);
						inputs = sReader.buildInputs(srcFs, srcFile, srcStat,
								parityFs, parityFile, parityStat,
								lp.getStripeIdx(), offsetInBlock,
								erasedLocations, locationsToRead, code);

						/*
						 * locationsToRead have now been populated and
						 * erasedLocations might have been updated with more
						 * erased locations.
						 */
						LOG.info("Erased locations: "
								+ erasedLocations.toString()
								+ "\nLocations to Read for repair:"
								+ locationsToRead.toString());

						/*
						 * Initialize erasedLocationsArray with erasedLocations.
						 */
						int i = 0;
						erasedLocationsArray = new int[erasedLocations.size()];
						for (int loc = 0; loc < codec.stripeLength
								+ codec.parityLength; loc++) {
							if (erasedLocations.indexOf(loc) >= 0) {
								erasedLocationsArray[i] = loc;
								i++;
							}
						}

						/*
						 * Initialize locationsToReadArray with locationsToRead.
						 */
						i = 0;
						locationsToReadArray = new int[locationsToRead.size()];
						for (int loc = 0; loc < codec.stripeLength
								+ codec.parityLength; loc++) {
							if (locationsToRead.indexOf(loc) >= 0) {
								locationsToReadArray[i] = loc;
								i++;
							}
						}

						i = 0;
						locationsNotToReadArray = new int[codec.stripeLength
								+ codec.parityLength - locationsToRead.size()];

						for (int loc = 0; loc < codec.stripeLength
								+ codec.parityLength; loc++) {
							if (locationsToRead.indexOf(loc) == -1
									|| erasedLocations.indexOf(loc) != -1) {
								locationsNotToReadArray[i] = loc;
								i++;
							}
						}

						this.writeBufs = new byte[erasedLocations.size()][];
						allocateBuffers();

						assert (parallelReader == null);
						parallelReader = new ParallelStreamReader(reporter,
								inputs, (int) Math.min(bufSize, limit),
								parallelism, boundedBufferCapacity, Math.min(
										limit, blockSize));
						parallelReader.start();
					}
					ParallelStreamReader.ReadResult readResult = readFromInputs(
							erasedLocations, limit, reporter, parallelReader);

					/*
					code.decodeBulk(readResult.readBufs, writeBufs,
							erasedLocationsArray, locationsToReadArray,
							locationsNotToReadArray);
					*/
					/**
					 * added by jason
					 */
					if(!ifDecodingParallelism) {
						
						code.decodeBulk(readResult.readBufs, writeBufs,
						erasedLocationsArray, locationsToReadArray,
						locationsNotToReadArray);
						
					}else if(ifDecodingParallelism) {

						LOG.info("execute the parallel decoding with thread num " + decodingThreadNum);
						code.decodeBulkParallel(readResult.readBufs, writeBufs,
								erasedLocationsArray, locationsToReadArray,
								locationsNotToReadArray, decodingThreadNum);
						
					}
					
					/**
					 * added by jason ended
					 */

					// get the number of bytes read through hdfs.
					for (int readNum : readResult.numRead) {
						numReadBytes += readNum;
					}

					int toWrite = (int) Math.min((long) bufSize, limit
							- written);
					for (int i = 0; i < erasedLocationsArray.length; i++) {
						if (erasedLocationsArray[i] == erasedLocationToFix) {
							if (out != null)
								out.write(writeBufs[i], 0, toWrite);
							if (crc != null) {
								crc.update(writeBufs[i], 0, toWrite);
							}
							written += toWrite;
							break;
						}
					}
				} catch (IOException e) {
					if (e instanceof TooManyErasedLocations) {
						logRaidReconstructionMetrics("FAILURE", 0, codec,
								System.currentTimeMillis() - startTime,
								erasedLocations.size(), numReadBytes, srcFile,
								errorOffset, LOGTYPES.OFFLINE_RECONSTRUCTION,
								srcFs);
						throw e;
					}
					// Re-create inputs from the new erased locations.
					if (parallelReader != null) {
						parallelReader.shutdown();
						parallelReader = null;
					}
					RaidUtils.closeStreams(inputs);
				}
			}
			logRaidReconstructionMetrics("SUCCESS", written, codec,
					System.currentTimeMillis() - startTime,
					erasedLocations.size(), numReadBytes, srcFile, errorOffset,
					LOGTYPES.OFFLINE_RECONSTRUCTION, srcFs);
			return written;
		} finally {
			numMissingBlocksInStripe = erasedLocations.size();
			if (parallelReader != null) {
				parallelReader.shutdown();
			}
			RaidUtils.closeStreams(inputs);
		}
	}

	ParallelStreamReader.ReadResult readFromInputs(
			List<Integer> erasedLocations, long limit, Progressable reporter,
			ParallelStreamReader parallelReader) throws IOException {
		ParallelStreamReader.ReadResult readResult;
		try {
			readResult = parallelReader.getReadResult();
		} catch (InterruptedException e) {
			throw new IOException("Interrupted while waiting for read result");
		}

		IOException exceptionToThrow = null;
		// Process io errors, we can tolerate upto codec.parityLength errors.
		for (int i = 0; i < readResult.ioExceptions.length; i++) {
			IOException e = readResult.ioExceptions[i];
			if (e == null) {
				continue;
			}
			if (e instanceof BlockMissingException) {
				LOG.warn("Encountered BlockMissingException in stream " + i);
			} else if (e instanceof ChecksumException) {
				LOG.warn("Encountered ChecksumException in stream " + i);
			} else {
				throw e;
			}
			int newErasedLocation = i;
			erasedLocations.add(newErasedLocation);
			exceptionToThrow = e;
		}
		if (exceptionToThrow != null) {
			throw exceptionToThrow;
		}
		return readResult;
	}

	public void logRaidReconstructionMetrics(String result, long bytes,
			Codec codec, long delay, int numMissingBlocks, long numReadBytes,
			Path srcFile, long errorOffset, LOGTYPES type, FileSystem fs) {

		try {
			JSONObject json = new JSONObject();
			json.put("result", result);
			json.put("constructedbytes", bytes);
			json.put("code", codec.id);
			json.put("delay", delay);
			json.put("missingblocks", numMissingBlocks);
			json.put("readbytes", numReadBytes);
			json.put("file", srcFile.toString());
			json.put("offset", errorOffset);
			json.put("type", type.name());
			json.put("cluster", fs.getUri().getAuthority());
			DECODER_METRICS_LOG.info(json.toString());

		} catch (JSONException e) {
			LOG.warn(
					"Exception when logging the Raid metrics: "
							+ e.getMessage(), e);
		}
	}

	public class DecoderInputStream extends InputStream {

		private long limit;
		private ParallelStreamReader parallelReader = null;
		private byte[] buffer;
		private long bufferLen;
		private int position;
		private long streamOffset = 0;

		private final Progressable reporter;
		private InputStream[] inputs;
		private final int boundedBufferCapacity = 2;

		private final long blockSize;
		private final long errorOffset;
		private long startOffsetInBlock;

		private final FileSystem srcFs;
		private final Path srcFile;
		private final FileSystem parityFs;
		private final Path parityFile;

		private int blockIdx;
		private int erasedLocationToFix;
		private LocationPair locationPair;

		private long currentOffset;
		private long dfsNumRead = 0;

		private final List<Integer> locationsToRead = new ArrayList<Integer>();
		private final List<Integer> erasedLocations = new ArrayList<Integer>();
		int[] erasedLocationsArray;
		int[] locationsToReadArray;
		int[] locationsNotToReadArray;

		public DecoderInputStream(final Progressable reporter,
				final long limit, final long blockSize, final long errorOffset,
				final FileSystem srcFs, final Path srcFile,
				final FileSystem parityFs, final Path parityFile) {

			this.reporter = reporter;
			this.limit = limit;

			this.blockSize = blockSize;
			this.errorOffset = errorOffset;

			this.srcFile = srcFile;
			this.srcFs = srcFs;
			this.parityFile = parityFile;
			this.parityFs = parityFs;

			this.blockIdx = (int) (errorOffset / blockSize);
			this.startOffsetInBlock = errorOffset % blockSize;
			this.currentOffset = errorOffset;
		}

		public long getCurrentOffset() {
			return currentOffset;
		}

		public long getAvailable() {
			return limit - streamOffset;
		}

		/**
		 * Will init the required objects, start the parallel reader, and put
		 * the decoding result in buffer in this method.
		 * 
		 * @throws IOException
		 */
		private void init() throws IOException {
			if (streamOffset >= limit) {
				buffer = null;
				return;
			}

			if (null == locationPair) {
				locationPair = StripeReader.getBlockLocation(codec, srcFs,
						srcFile, blockIdx, conf);
				erasedLocationToFix = codec.parityLength
						+ locationPair.getBlockIdxInStripe();
				erasedLocations.add(erasedLocationToFix);
			}

			if (null == parallelReader) {

				long offsetInBlock = streamOffset + startOffsetInBlock;
				FileStatus srcStat = srcFs.getFileStatus(srcFile);
				FileStatus parityStat = parityFs.getFileStatus(parityFile);
				StripeReader sReader = StripeReader.getStripeReader(codec,
						conf, blockSize, srcFs, locationPair.getStripeIdx(),
						srcStat);

				inputs = sReader.buildInputs(srcFs, srcFile, srcStat, parityFs,
						parityFile, parityStat, locationPair.getStripeIdx(),
						offsetInBlock, erasedLocations, locationsToRead, code);

				/*
				 * locationsToRead have now been populated and erasedLocations
				 * might have been updated with more erased locations.
				 */
				LOG.info("Erased locations: " + erasedLocations.toString()
						+ "\nLocations to Read for repair:"
						+ locationsToRead.toString());

				/*
				 * Initialize erasedLocationsArray with the erasedLocations.
				 */
				int i = 0;
				erasedLocationsArray = new int[erasedLocations.size()];
				for (int loc = 0; loc < codec.stripeLength + codec.parityLength; loc++) {
					if (erasedLocations.indexOf(loc) >= 0) {
						erasedLocationsArray[i] = loc;
						i++;
					}
				}
				/*
				 * Initialize locationsToReadArray with the locationsToRead.
				 */
				i = 0;
				locationsToReadArray = new int[locationsToRead.size()];
				for (int loc = 0; loc < codec.stripeLength + codec.parityLength; loc++) {
					if (locationsToRead.indexOf(loc) >= 0) {
						locationsToReadArray[i] = loc;
						i++;
					}
				}

				/*
				 * Initialize locationsNotToReadArray with the locations that
				 * are either erased or not supposed to be read.
				 */
				i = 0;
				locationsNotToReadArray = new int[codec.stripeLength
						+ codec.parityLength - locationsToRead.size()];

				for (int loc = 0; loc < codec.stripeLength + codec.parityLength; loc++) {
					if (locationsToRead.indexOf(loc) == -1
							|| erasedLocations.indexOf(loc) != -1) {
						locationsNotToReadArray[i] = loc;
						i++;
					}
				}

				writeBufs = new byte[erasedLocations.size()][];
				allocateBuffers();

				assert (parallelReader == null);
				parallelReader = new ParallelStreamReader(reporter, inputs,
						(int) Math.min(bufSize, limit), parallelism,
						boundedBufferCapacity, limit);
				parallelReader.start();
			}

			if (null != buffer && position == bufferLen) {
				buffer = null;
			}

			if (null == buffer) {
				ParallelStreamReader.ReadResult readResult = readFromInputs(
						erasedLocations, limit, reporter, parallelReader);

				// get the number of bytes read through hdfs.
				for (int readNum : readResult.numRead) {
					dfsNumRead += readNum;
				}
				/*
				code.decodeBulk(readResult.readBufs, writeBufs,
						erasedLocationsArray, locationsToReadArray,
						locationsNotToReadArray);
						*/
				/**
				 * added by jason
				 */
				if(!ifDecodingParallelism) {
					
					code.decodeBulk(readResult.readBufs, writeBufs,
					erasedLocationsArray, locationsToReadArray,
					locationsNotToReadArray);
					
				}else if(ifDecodingParallelism) {
					
					code.decodeBulkParallel(readResult.readBufs, writeBufs,
							erasedLocationsArray, locationsToReadArray,
							locationsNotToReadArray, decodingThreadNum);
					
				}
				
				/**
				 * added by jason ended
				 */

				for (int i = 0; i < erasedLocationsArray.length; i++) {
					if (erasedLocationsArray[i] == erasedLocationToFix) {
						buffer = writeBufs[i];
						bufferLen = Math.min(bufSize, limit - streamOffset);
						position = 0;
						break;
					}
				}
			}
		}

		/**
		 * make sure we have the correct recovered data in the buffer.
		 * 
		 * @throws IOException
		 */
		private void checkBuffer() throws IOException {
			while (streamOffset <= limit) {
				try {
					init();
					break;
				} catch (IOException e) {
					if (e instanceof TooManyErasedLocations) {
						throw e;
					}
					// Re-create inputs from the new erased locations.
					if (parallelReader != null) {
						parallelReader.shutdown();
						parallelReader = null;
					}
					if (inputs != null) {
						RaidUtils.closeStreams(inputs);
					}
				}
			}
		}

		@Override
		public int read() throws IOException {

			checkBuffer();
			if (null == buffer) {
				return -1;
			}

			int result = buffer[position] & 0xff;
			position++;
			streamOffset++;
			currentOffset++;

			return result;
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			long startTime = System.currentTimeMillis();
			dfsNumRead = 0;

			if (b == null) {
				throw new NullPointerException();
			} else if (off < 0 || len < 0 || len > b.length - off) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return 0;
			}

			int numRead = 0;
			while (numRead < len) {
				try {
					checkBuffer();
				} catch (IOException e) {
					long delay = System.currentTimeMillis() - startTime;
					logRaidReconstructionMetrics("FAILURE", 0, codec, delay,
							erasedLocations.size(), dfsNumRead, srcFile,
							errorOffset, LOGTYPES.ONLINE_RECONSTRUCTION, srcFs);
					throw e;
				}

				if (null == buffer) {
					if (numRead > 0) {
						logRaidReconstructionMetrics("SUCCESS", (int) numRead,
								codec, System.currentTimeMillis() - startTime,
								erasedLocations.size(), dfsNumRead, srcFile,
								errorOffset, LOGTYPES.ONLINE_RECONSTRUCTION,
								srcFs);
						return (int) numRead;
					}
					return -1;
				}

				int numBytesToCopy = (int) Math.min(bufferLen - position, len
						- numRead);
				System.arraycopy(buffer, position, b, off, numBytesToCopy);
				position += numBytesToCopy;
				currentOffset += numBytesToCopy;
				streamOffset += numBytesToCopy;
				off += numBytesToCopy;
				numRead += numBytesToCopy;
			}

			if (numRead > 0) {
				logRaidReconstructionMetrics("SUCCESS", numRead, codec,
						System.currentTimeMillis() - startTime,
						erasedLocations.size(), dfsNumRead, srcFile,
						errorOffset, LOGTYPES.ONLINE_RECONSTRUCTION, srcFs);
			}
			return (int) numRead;
		}

		@Override
		public void close() throws IOException {
			if (parallelReader != null) {
				parallelReader.shutdown();
				parallelReader = null;
			}
			if (inputs != null) {
				RaidUtils.closeStreams(inputs);
			}
			super.close();
		}
	}
	
	public class CRSDecoderInputStream extends DecoderInputStream {

		private long limit;
		private byte[] buffer;
		private int localBufferSize;
		private long bufferLen = 0;
		private int position =  0;
		private long streamOffset = 0;

		private final Progressable reporter;

		private final long blockSize;
		private long startOffsetInBlock;

		private final DistributedFileSystem srcFs;
		private final Path srcFile;
		private final DistributedFileSystem parityFs;
		private final Path parityFile;

		private int blockIndex;
		private int erasedLocationToFix;
		private Stripe erasedStripe;
		private MergeBlockHeader header;
		private RecoverTreeNode[] nodes;
		private String structure;
		private boolean inited = false;
		private long currentOffset;

		public CRSDecoderInputStream(final Progressable reporter,
				final long limit, final long blockSize, final long errorOffset,
				final FileSystem srcFs, final Path srcFile,
				final FileSystem parityFs, final Path parityFile) {
			super(reporter,limit, blockSize, errorOffset, srcFs, srcFile,
				parityFs, parityFile);
			this.reporter = reporter;
			this.limit = limit;

			this.blockSize = blockSize;

			this.srcFile = srcFile;
			this.srcFs = (DistributedFileSystem)srcFs;
			this.parityFile = parityFile;
			this.parityFs = (DistributedFileSystem)parityFs;

			this.blockIndex = (int) (errorOffset / blockSize);
			this.startOffsetInBlock = errorOffset % blockSize;
			this.currentOffset = errorOffset;
			this.erasedLocationToFix = blockIndex % codec.stripeLength;
			this.structure = conf.get("raid.degradedread.recover.structure", "line");
			localBufferSize = conf.getInt("raid.degradedread.merger.buffersize", 
					(1 << 20));
			this.buffer = new byte[this.localBufferSize];
			
			LOG.info("blockSize = [" + blockSize + "], limit = ["
					+ limit + "], errorOffset = [" + errorOffset 
					+ "], blockIndex = [" + blockIndex + "], structure = [" 
					+ structure + "], erasedLocationToFix = ["
					+ erasedLocationToFix + "], localBufferSize = ["
					+ localBufferSize + "]");
			
			try {
				init();
				inited = true;
			} catch (IOException ioe) {
				inited = false;
				LOG.error("NTar : CRSDecoderInputStream inition failed !" + ioe);	
			}
		}

		public long getCurrentOffset() {
			return currentOffset;
		}

		public long getAvailable() {
			return limit - streamOffset;
		}

		void init() throws IOException {
			//long startTime = System.currentTimeMillis();
			
			erasedStripe = getErasedStripe(srcFs, srcFile,
					parityFs, parityFile, codec, blockIndex);
			ErasureCode ec = null;
			if (codec.id.equals("crs"))
				ec = new CauchyRSCode();
			else if (codec.id.equals("lrc"))
				ec = new LocallyRepairableCode();
			ec.init(codec);
			LocatedBlockWithMetaInfo[] blocks = erasedStripe.getBlocks();
			CandidateLocations candidate = ec.getCandidateLocations(erasedStripe, 
					erasedLocationToFix);
			if (candidate == null) {
				throw new TooManyErasedLocations("to many erasures");
			}
			int[] candidateLocations = candidate.locations;
			int minNum = candidate.minNum;
			DatanodeInfo root = srcFs.getClient().getDatanodeInfo();
			String rootHost = root.name;
			DatanodeInfo[] datanodeInfos = BlockReconstructor.CorruptBlockReconstructor.
					getCandidateDatanodeInfos(erasedStripe, candidateLocations, root);
			nodes = BlockReconstructor.CorruptBlockReconstructor.
					getCandidateNodes(erasedStripe, candidateLocations, rootHost);
			if (nodes == null)
				throw new IOException("nodes is null");
			int[][] distances = BlockReconstructor.CorruptBlockReconstructor.
					getRealDistances(datanodeInfos);
			MinimumSpanningTree.Result result;
			if(structure.equals("tree")) {
				result = MinimumSpanningTree.chooseAndBuildTree(nodes, 
						distances, 0, minNum);
			} else if(structure.equals("line")){
				result = MinimumSpanningTree.chooseAndBuildLine(nodes, 
						distances, 0, minNum);
			} else {
				result = MinimumSpanningTree.chooseAndBuildStar(nodes, 
						distances, 0, minNum);
			}
			
			int[] choosed = result.chosed;
			if (choosed == null)
				throw new IOException("choosed is null");
			int[] locationsToUse = ec.getLocationsToUse(erasedStripe, nodes, 
					choosed, erasedLocationToFix);
			int[] recoverVector = ec.getRecoverVector(locationsToUse, erasedLocationToFix);
			
			if (recoverVector == null)
				throw new IOException("recoverVector is null");
			
			for (int j = 0; j < choosed.length; j++) {
				nodes[choosed[j]].getElement().setCoefficient(recoverVector[j]);
			}
			LocatedBlockWithMetaInfo block = blocks[erasedLocationToFix];
			
			header = new MergeBlockHeader(new VersionAndOpcode(
					DataTransferProtocol.DATA_TRANSFER_VERSION, 
					DataTransferProtocol.OP_MERGE_BLOCK));
			header.setLevel(1);
			header.setRecovery(true);
			header.setNamespaceId(block.getNamespaceID());
			header.setBlockId(block.getBlock().getBlockId());
			header.setGenStamp(block.getBlock().getGenerationStamp());
		}
		
		private long recoverRead(long offsetInBlock, int length, byte[] buffer, 
				int offsetInBuffer) throws IOException 
		{
			if (!inited) {
				init();
				inited = true;
			}
	
			header.setOffsetInBlock(offsetInBlock);
			header.setLength(length);
			RecoverTreeNode treeNode = nodes[0];
			int childrenNumber = treeNode.getChildrenNumber();
			RecoverTreeNode[] childNodes = new RecoverTreeNode[childrenNumber];
			for (int i = 0; i < childrenNumber; i++) {
				childNodes[i] = (RecoverTreeNode) treeNode.getChild(i);
			}
			
			List<String> childAddrs = new LinkedList<String>();
			List<Socket> childSockets = new LinkedList<Socket>();
			List<DataOutputStream> childOutputs = new LinkedList<DataOutputStream>();
			List<DataInputStream> childInputs = new LinkedList<DataInputStream>();

			long read = 0;
			try {
				String childAddr;
				InetSocketAddress child;
				Socket childSocket = null;
				DataOutputStream childOutput;
				DataInputStream childInput;

				// Open network connected to children sites
				for (int i = 0; i < childrenNumber; i++) {

					// Connect to child site
					childAddr = childNodes[i].getHostName();
					childAddrs.add(childAddr);
					child = NetUtils.createSocketAddr(childAddr);
					childSocket = new Socket();

					/**
					 * To Do: We should multiply timeout according to children
					 * number of subnodes. I'm not sure the current way is OK.
					 */
					int timeoutValue = HdfsConstants.READ_TIMEOUT
							+ (HdfsConstants.READ_TIMEOUT_EXTENSION * 10);
					int writeTimeout = HdfsConstants.WRITE_TIMEOUT
							+ (HdfsConstants.WRITE_TIMEOUT_EXTENSION);
					NetUtils.connect(childSocket, child, timeoutValue);
					childSockets.add(childSocket);
					childSocket.setSoTimeout(timeoutValue);
					childSocket.setSendBufferSize(FSConstants.DEFAULT_DATA_SOCKET_SIZE);
					childOutput = new DataOutputStream(new BufferedOutputStream(
							NetUtils.getOutputStream(childSocket, writeTimeout),
							FSConstants.SMALL_BUFFER_SIZE));
					childOutputs.add(childOutput);
					childInput = new DataInputStream(NetUtils.getInputStream(childSocket));
					childInputs.add(childInput);

					header.writeVersionAndOpCode(childOutput);
					header.write(childOutput);
					childNodes[i].write(childOutput);
					childOutput.flush();
				}
				
				Block block = new Block(header.getBlockId(), 0, header.getGenStamp());
				BlockXCodingMerger merger = 
						new BlockXCodingMerger.BufferBlockXCodingMerger(block, header.getNamespaceId(),
							childInputs.toArray(new DataInputStream[0]),
							header.getOffsetInBlock(), header.getLength(),
							childAddrs.toArray(new String[0]), 
							childSocket.getLocalSocketAddress().toString(), null,
							0, buffer, offsetInBuffer);
			
				// Now, start the merging process
				read = merger.mergeBlock();

			} catch (IOException ioe) {
				throw ioe;
			} finally {
				// close all opened streams
				DataOutputStream[] outputs = childOutputs.toArray(new DataOutputStream[0]);
				for (int i = 0; i < outputs.length; i++)
					IOUtils.closeStream(outputs[i]);
				DataInputStream[] inputs = childInputs.toArray(new DataInputStream[0]);
				for (int i = 0; i < inputs.length; i++)
					IOUtils.closeStream(inputs[i]);
				Socket[] sockets = childSockets.toArray(new Socket[0]);
				for (int i = 0; i < sockets.length; i++)
					IOUtils.closeSocket(sockets[i]);
			}
			return read;
		}
		

		@Override
		public int read() throws IOException {
			if (streamOffset >= limit)
				return -1;
			
			if (position >= bufferLen) {
				int toRead = (int) Math.min(limit - streamOffset,
						localBufferSize);
				bufferLen = recoverRead(startOffsetInBlock + streamOffset, 
						toRead, buffer, 0);
				position = 0;
			}
			
			if (bufferLen > 0) {
				int result = buffer[position] & 0xff;
				position++;
				streamOffset++;
				currentOffset++;

				return result;
			}
			
			return -1;
		}

		@Override
		public int read(byte[] b) throws IOException {	
			if (streamOffset >= limit)
				return -1;
			
			return read(b, 0, b.length);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if(streamOffset >= limit)
				return -1;
			
			if (b == null) {
				throw new NullPointerException();
			} else if (off < 0 || len < 0 || len > (b.length - off)) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return 0;
			}
			
			len = (int) Math.min(len, limit - streamOffset);
			
			int numRead = 0;
			if(position < bufferLen) {
				int numBytesToCopy = (int) Math.min(bufferLen - position, len
						- numRead);
				System.arraycopy(buffer, position, b, off, numBytesToCopy);
				off += numBytesToCopy;
				numRead += numBytesToCopy;
				position += numBytesToCopy;
			}
			
			while (numRead < len) {
				int numBytesToRead = Math.min(this.localBufferSize, 
						(int)(limit - streamOffset) - numRead);
				position = 0;
				bufferLen = recoverRead(streamOffset + startOffsetInBlock + numRead, 
						numBytesToRead, buffer, position);
				int numBytesToCopy = (int) Math.min(bufferLen - position, len
						- numRead);
				System.arraycopy(buffer, position, b, off, numBytesToCopy);
				off += numBytesToCopy;
				numRead += numBytesToCopy;
				position += numBytesToCopy;
			}

			currentOffset += numRead;
			streamOffset += numRead;
			

			return numRead;
		}

		@Override
		public void close() throws IOException {
			super.close();
		}
		
		boolean isBlockCorrupt(LocatedBlock block) {
			if (block.isCorrupt()
					|| (block.getLocations().length == 0 && block.getBlockSize() > 0)) {
				return true;
			}
			
			return false;
		}
		
		boolean isBlockDecom(LocatedBlock block) {
			// Copy this block iff all good copies are being decommissioned
			boolean allDecommissioning = true;
			for (DatanodeInfo i : block.getLocations()) {
				allDecommissioning &= i.isDecommissionInProgress();
			}
			if (allDecommissioning) {
				return true;
			}
			return false;
		}
		
		public void processBlock(Stripe stripe, LocatedBlock block, int index) {
			// TODO Auto-generated method stub
			if(isBlockCorrupt(block)){
				stripe.addToReconstructe(index);
			} else if(isBlockDecom(block)) {
				stripe.addNotToRead(index);
			}
		}
		
		/**
		 * Get the stripe which contains the block to be reconstructed
		 * @param srcFs
		 * @param srcUriPath
		 * @param parityFs
		 * @param parityUriPath
		 * @param codec
		 * @param blockIndex
		 * @return
		 * 		stripe
		 * @throws IOException
		 */
		Stripe getErasedStripe(DistributedFileSystem srcFs, Path srcPath, 
				DistributedFileSystem parityFs, Path parityPath, 
				Codec codec, int blockIndex) 
						throws IOException {
			
			VersionedLocatedBlocks srcLocatedBlocks;
			VersionedLocatedBlocks parityLocatedBlocks;
			int srcNamespaceId = 0;
			int parityNamespaceId = 0;
			int srcMethodFingerprint = 0;
			int parityMethodFingerprint = 0;
			FileStatus srcStat = srcFs.getFileStatus(srcPath);
			FileStatus parityStat = parityFs.getFileStatus(parityPath);
			String srcFile = srcPath.toUri().getPath();
			String parityFile = parityPath.toUri().getPath();
			if (DFSClient
					.isMetaInfoSuppoted(srcFs.getClient().namenodeProtocolProxy)) {
				LocatedBlocksWithMetaInfo lbksm = srcFs.getClient().namenode
						.openAndFetchMetaInfo(srcFile, 0, srcStat.getLen());
				srcNamespaceId = lbksm.getNamespaceID();
				srcLocatedBlocks = lbksm;
				srcMethodFingerprint = lbksm.getMethodFingerPrint();
				srcFs.getClient().getNewNameNodeIfNeeded(srcMethodFingerprint);
			} else {
				srcLocatedBlocks = srcFs.getClient().namenode.open(srcFile, 0,
						srcStat.getLen());
			}
			
			if (DFSClient
					.isMetaInfoSuppoted(parityFs.getClient().namenodeProtocolProxy)) {
				LocatedBlocksWithMetaInfo lbksm = parityFs.getClient().namenode
						.openAndFetchMetaInfo(parityFile, 0, parityStat.getLen());
				parityNamespaceId = lbksm.getNamespaceID();
				parityLocatedBlocks = lbksm;
				parityMethodFingerprint = lbksm.getMethodFingerPrint();
				parityFs.getClient().getNewNameNodeIfNeeded(parityMethodFingerprint);
			} else {
				parityLocatedBlocks = parityFs.getClient().namenode.open(parityFile, 0,
						parityStat.getLen());
			}
			
			final int srcDataTransferVersion = srcLocatedBlocks.getDataProtocolVersion();
			final int parityDataTransferVersion = parityLocatedBlocks.getDataProtocolVersion();
			final int stripeLen = codec.stripeLength;
			final int parityLen = codec.parityLength;
			
			LocatedBlock[] srcBlocks = 
					srcLocatedBlocks.getLocatedBlocks().toArray(new LocatedBlock[0]);
			final int srcBlockNumber = srcBlocks.length;
			LocatedBlock[] parityBlocks =
					parityLocatedBlocks.getLocatedBlocks().toArray(new LocatedBlock[0]);
			final int parityBlockNumber = parityBlocks.length;
			
			final int stripeNumber = (int)((srcBlockNumber + stripeLen -1)/stripeLen);
			final int stripeIndex = (int)(blockIndex/stripeLen);
			
			if(parityBlockNumber !=  (stripeNumber * parityLen)) {
				throw new IOException("The number of blocks in pariyfile: " + parityFile
						+ "does not match that in srcfile" + srcFile); 
			}
			
			Stripe erasedStripe = new Stripe(stripeLen, parityLen);
			
			int offset = stripeIndex * stripeLen;
			int len = Math.min(stripeLen, srcBlockNumber - offset);
			for(int i = 0; i < len; i++) {
				LocatedBlock block = srcBlocks[offset + i];
				erasedStripe.addDataBlock(new LocatedBlockWithMetaInfo(block.getBlock(), 
						block.getLocations(), block.getStartOffset(),
						srcDataTransferVersion, srcNamespaceId, srcMethodFingerprint));
				processBlock(erasedStripe, block, i);
			}
				
			offset = stripeIndex * parityLen;
			len = Math.min(parityLen, parityBlockNumber - offset);	
			for(int i = 0; i < len; i++) {
				LocatedBlock block = parityBlocks[offset+i];
				erasedStripe.addParityBlock(new LocatedBlockWithMetaInfo(block.getBlock(), 
						block.getLocations(), block.getStartOffset(),
						parityDataTransferVersion, parityNamespaceId, parityMethodFingerprint));
				processBlock(erasedStripe, block, stripeLen + i);
			}
			int erasedIndex = blockIndex % stripeLen;
			erasedStripe.addToReconstructe(erasedIndex);
			LocatedBlock[] blocks = erasedStripe.getBlocks();
			DatanodeInfo[] locations = blocks[erasedIndex].getLocations();
			if(locations.length > 0) {
				String erasedHost = locations[0].getName();
				for(int i = erasedStripe.getDataSize() - 1; i >= 0; i--) {
					locations = blocks[i].getLocations();
						if(locations.length > 0) {
							String host = locations[0].getName();
							if(host.equals(erasedHost))
								erasedStripe.addToReconstructe(i);
						}
				}
				for(int i = erasedStripe.getParitySize() - 1; i >= 0; i--) {
					locations = blocks[i + stripeLen].getLocations();
					if(locations.length > 0) {
						String host = locations[0].getName();
						if(host.equals(erasedHost))
							erasedStripe.addToReconstructe(i + stripeLen);
					}
				}
			}
			return erasedStripe;
		}
	}
}
