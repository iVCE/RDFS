package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MinimumSpanningTree;
import org.apache.hadoop.hdfs.MinimumSpanningTree.Result;
import org.apache.hadoop.hdfs.RecoverTreeNode;
import org.apache.hadoop.hdfs.RecoverTreeNodeElement;
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
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 * this class implements the actual reconstructing functionality we keep this in
 * a separate class so that the distributed block fixer can use it
 */
public abstract class BlockReconstructor extends Configured {

	public static final Log LOG = LogFactory.getLog(BlockReconstructor.class);
	
	public static ExecutorService executor = Executors.newCachedThreadPool();
	
	public Lock dataLock = null;
	public long totalTime = 0;
	public int blocksFixed = 0;
	public int allWeight = 0;
	public long startTime = 0;
	public boolean isFirstTime = true;
	
	public Lock nodesLock = null;
	public Lock pathsLock = null;
	public Map<String, Integer> involedNodes = null;
	
	public BlockingQueue<Integer> flyingCount = null; 	  // number of blocks being fixed
	public BlockingQueue<PathInfo> pendingFiles = null;   // files needed to be fixed
	public BlockingQueue<RepairTask> pendingTasks = null;
	public HashMap<PathInfo, Integer> flyingFiles = null;
	
	public String structure;
	public int maxParallsim;
	public float maxRepairThroughput;
	
	public Configuration conf;
	
	boolean running = true;
	
	public void setRunning(boolean value) {
		running = value;
	}

	BlockReconstructor(Configuration conf) throws IOException {
		super(conf);
		this.conf = conf;
		
		this.structure = conf.get("raid.blockreconstruct.structure", "tree");
		this.maxParallsim = conf.getInt("raid.blockreconstruct.maxparallsim", 3);
		this.maxRepairThroughput = conf.getFloat("raid.blockreconstruct.maxrepairthroughput", -1);
		LOG.info("Performance: structure = [" + this.structure  
				+ "], maxParallsim = [" + this.maxParallsim 
				+ "], maxRepariThroughput = [" + this.maxRepairThroughput + "]");
		
		dataLock = new ReentrantLock();
		nodesLock = new ReentrantLock();
		pathsLock = new ReentrantLock();
		involedNodes = new HashMap<String, Integer>();
		flyingCount = new ArrayBlockingQueue<Integer>(maxParallsim);
		pendingFiles = new LinkedBlockingQueue<PathInfo>();
		pendingTasks = new LinkedBlockingQueue<RepairTask>();
		flyingFiles = new HashMap<PathInfo, Integer>();
		executor.execute(new RepairTaskPlanner(10000));
		executor.execute(new RepairTaskScheduler());
	}
	
	/**
	 * Is the path a parity file of a given Codec?
	 */
	boolean isParityFile(Path p, Codec c) {
		return isParityFile(p.toUri().getPath(), c);
	}

	boolean isParityFile(String pathStr, Codec c) {
		if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
			return false;
		}
		return pathStr.startsWith(c.getParityPrefix());
	}
	
	class PathInfo{
		Path srcPath;
		Path parityPath;
		Codec codec;
		
		public PathInfo(Path srcPath, Path parityPath, Codec codec) {
			super();
			this.srcPath = srcPath;
			this.parityPath = parityPath;
			this.codec = codec;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result
					+ ((parityPath == null) ? 0 : parityPath.hashCode());
			result = prime * result
					+ ((srcPath == null) ? 0 : srcPath.hashCode());
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
			PathInfo other = (PathInfo) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (parityPath == null) {
				if (other.parityPath != null)
					return false;
			} else if (! (parityPath.toString().startsWith(other.parityPath.toString()) 
					|| other.parityPath.toString().startsWith(parityPath.toString())))
				return false;
			if (srcPath == null) {
				if (other.srcPath != null)
					return false;
			} else if (! (srcPath.toString().startsWith(other.srcPath.toString()) 
					|| other.srcPath.toString().startsWith(srcPath.toString())))
				return false;
			return true;
		}

		private BlockReconstructor getOuterType() {
			return BlockReconstructor.this;
		}
	}
	
	class RepairTask {
		public RepairTask(PathInfo pathInfo, Result result, RecoverTreeNode root,
				RecoverTreeNode[] nodes, MergeBlockHeader header) {
			super();
			this.pathInfo = pathInfo;
			this.result = result;
			this.root = root;
			this.nodes = nodes;
			this.header = header;
		}
		
		PathInfo pathInfo;
		Result result;
		RecoverTreeNode root;
		RecoverTreeNode[] nodes;
		MergeBlockHeader header;
	}
	
	class RepairTaskPlanner implements Runnable {
		
		long fileRecheckInterval;
		RepairTaskPlanner(long fileRecheckInterval) {
			this.fileRecheckInterval = fileRecheckInterval;
		}
		
		@Override
		public void run() {
			try {
				Thread.sleep(5*1000);
				doPlan();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}
		
		void doPlan() throws IOException, InterruptedException {
			boolean paused = false;
			while (running) {
				if(!pendingFiles.isEmpty()) {
					try {
						pathsLock.lock();
						PathInfo pathInfo = pendingFiles.poll();
						int number = planRepairTasks(pathInfo);
						if (number > 0) {
							if (flyingFiles.containsKey(pathInfo)) {
								int value = flyingFiles.get(pathInfo);
								flyingFiles.put(pathInfo, value + number);
							} else {
								flyingFiles.put(pathInfo, number);
							}
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						pathsLock.unlock();
					}
				} else { // If there are no corrupt files, retry after some time.
					//just for performance testing
					if(!isFirstTime && !paused) {
						paused = true;
					}
					
					Thread.sleep(fileRecheckInterval);
				}
			}	
		}
		
		int planRepairTasks(PathInfo pathInfo) throws IOException, InterruptedException {
			Path srcPath = pathInfo.srcPath;
			Path parityPath = pathInfo.parityPath;
			Codec codec = pathInfo.codec;
			
			DistributedFileSystem srcFs = getDFS(srcPath);
			DistributedFileSystem parityFs = getDFS(parityPath);
			String srcUriPath = srcPath.toUri().getPath();
			String parityUriPath = parityPath.toUri().getPath();
			FileStatus srcStat = srcFs.getFileStatus(srcPath);
			FileStatus parityStat = parityFs.getFileStatus(parityPath);

			// Second, get stripes that contain erased block
			List<Stripe> erasedStripeList = getErasedStripes(srcFs, srcUriPath,
					srcStat, parityFs, parityUriPath, parityStat, codec);

			if (erasedStripeList == null) {
				LOG.error("NTar: Error occurred when getting erased stripes for source file:"
						+ srcPath + " and it's parity file:" + parityPath);
				return 0;
			}

			if (erasedStripeList.size() == 0) {
				LOG.info("NTar: There is no stripe about source file: " + srcPath
						+ " that contain blocks needing to be reconstructed. ignoring ...");
				return 0;
			}
			
			ErasureCode ec = null;
			if (codec.id.equals("crs"))
				ec = new CauchyRSCode();
			else if (codec.id.equals("lrc"))
				ec = new LocallyRepairableCode();
			ec.init(codec);
			int number = 0;
			for (Stripe stripe : erasedStripeList) {
				int[] erasedLocations = stripe.getErasures();
				for(int i = 0; i < 1; i++) {
					RepairTask task = planRepairTask(pathInfo, stripe, erasedLocations[i], ec);
					if(task != null) {
						if(isFirstTime) {
							startTime = System.currentTimeMillis();
							isFirstTime = false;
						}
						number = number + 1;
						pendingTasks.put(task);
					}
				}
			}
			return number;
		}
	}
	
	class RepairTaskScheduler implements Runnable {
		@Override
		public void run() {
			try {
				Thread.sleep(10*1000);
				schedule();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}
		
		void schedule() throws InterruptedException, IOException {
			boolean paused = false;
			float throughput = 0;
			while (running) {
				if (maxRepairThroughput > 0 ) {
					long time = 0;
					while(true) {
						time = System.currentTimeMillis() - startTime;
						throughput = blocksFixed*1000/(float)time;
						if (throughput <= maxRepairThroughput)
							break;
						else
							Thread.sleep(100);
					}
				}
				flyingCount.put(0);
				RepairTask task = chooseTask();
				if(task != null) {
					nodesLock.lock();
					String name = task.root.getHostName();
					int value = 0;
					if(involedNodes.containsKey(name)) {
						value = involedNodes.get(name);
						involedNodes.put(name, value + 1);
					} else {
						involedNodes.put(name, 1);
					}
					RecoverTreeNode[] nodes = task.nodes;
					int[] chosed = task.result.chosed;
					for(int i = 0; i < chosed.length; i++) {
						name = nodes[chosed[i]].getHostName();
						if(involedNodes.containsKey(name)) {
							value = involedNodes.get(name);
							involedNodes.put(name, value + 1);
						} else {
							involedNodes.put(name, 1);
						}
					}
					nodesLock.unlock();
					executor.execute(new BlockFixer(task));
				} else {
					flyingCount.poll();
					if(!paused) {
						LOG.info("NTar : RepairTaskScheduler paused !");
						paused = true;
					}
					Thread.sleep(1000);
				}
			}	
		}
		
		RepairTask chooseTask() throws InterruptedException {
			int size = pendingTasks.size();
			if(size == 0)
				return null;
			return pendingTasks.poll();
		}
		
	}

	/**
	 * Fix a file, report progress.
	 * 
	 * @return true if file was reconstructed, false if no reconstruction was
	 *         necessary or possible.
	 */
	public boolean reconstructFile(Path srcPath, Context context) throws IOException,
			InterruptedException {
		Progressable progress = context;
		if (progress == null) {
			progress = RaidUtils.NULL_PROGRESSABLE;
		}

		if (RaidNode.isParityHarPartFile(srcPath)) {
			return processParityHarPartFile(srcPath, progress);
		}

		// Reconstruct parity file
		for (Codec codec : Codec.getCodecs()) {
			if (isParityFile(srcPath, codec)) {
				if(codec.id.equals("crs") || codec.id.equals("lrc"))
					return processCrsFile(srcPath, false, codec);
				return processParityFile(srcPath,
						new Decoder(getConf(), codec), context);
			}
		}

		// Reconstruct source file
		for (Codec codec : Codec.getCodecs()) {
			ParityFilePair ppair = ParityFilePair.getParityFile(codec, srcPath,
					getConf());
			if (ppair != null) {
				if(codec.id.equals("crs") || codec.id.equals("lrc")) 
					return processCrsFile(srcPath, true, codec);
				Decoder decoder = new Decoder(getConf(), codec);
				return processFile(srcPath, ppair, decoder, context);
			}
		}

		// there was nothing to do
		LOG.warn("Could not find parity file for source file " + srcPath
				+ ", ignoring...");
		return false;
	}
	
	boolean processCrsFile(Path path, boolean isSource, Codec codec) throws IOException {
		Path srcPath, parityPath;
		// First, get the source file and parity file pair
		if (isSource) {
			srcPath = path;
			ParityFilePair ppair = ParityFilePair.getParityFile(codec, srcPath,
					getConf());
			if (ppair == null) {
				LOG.error("NTar: Could not construct ParityFilePairy for source file"
					+ srcPath + ", ignoring...");
				return false;
			}
			parityPath = ppair.getPath();
		} else {
			parityPath = path;
			srcPath = sourcePathFromParityPath(parityPath);
			if (srcPath == null) {
				LOG.info("NTar: Could not get regular file corresponding to parity file "
						+ parityPath + ", ignoring...");
				return false;
			}
		}
		
		PathInfo pathInfo = new PathInfo(srcPath, parityPath, codec);
		pathsLock.lock();
		if (!pendingFiles.contains(pathInfo) 
				&& !flyingFiles.containsKey(pathInfo)) {
			pendingFiles.add(pathInfo);
		}
		pathsLock.unlock();
		
		return true;
	}
	
	class BlockFixer implements Runnable {
		RepairTask task;
		
		public BlockFixer(RepairTask task) {
			super();
			this.task = task;
		}

		@Override
		public void run() {
			boolean status = executeRepairTask(task.root, task.header);
			String name = null;
			int value = 0;
			nodesLock.lock();
			name = task.root.getHostName();
			value = involedNodes.get(name);
			if(value == 1)
				involedNodes.remove(name);
			else {
				involedNodes.put(name, value -1);
			}
			RecoverTreeNode[] nodes = task.nodes;
			int[] chosed = task.result.chosed;
			for(int i = 0; i < chosed.length; i++) {
				name = nodes[chosed[i]].getHostName();
				value = involedNodes.get(name);
				if(value == 1)
					involedNodes.remove(name);
				else {
					involedNodes.put(name, value - 1);
				}
			}
			nodesLock.unlock();
			pathsLock.lock();
			value = flyingFiles.get(task.pathInfo);
			if (value == 1)
				flyingFiles.remove(task.pathInfo);
			else {
				flyingFiles.put(task.pathInfo, value - 1);
			}
			pathsLock.unlock();
			flyingCount.poll();
			if (status) {
				Result result = task.result;
				long endTime = System.currentTimeMillis();
				long tTime = endTime - startTime;
				long tBlocks;
				int tWeight;
				dataLock.lock();
				blocksFixed++;
				allWeight += result.totalWeight;
				tBlocks = blocksFixed;
				tWeight = allWeight;
				dataLock.unlock();
				
				if (tBlocks % 100 == 0) {
					float seconds = (float)tTime/1000;
					LOG.info("Performance: blocksFixed = " + tBlocks
							+ " avageTime = " + seconds/tBlocks + " seconds"
							+ " avageWeight = " + (float)tWeight/tBlocks);
						
				}
			}
		}
	}
	
	RepairTask planRepairTask(PathInfo pathInfo, Stripe stripe, int erasedLocation, 
			ErasureCode ec) throws IOException {
		
		MergeBlockHeader header = new MergeBlockHeader(new VersionAndOpcode(
				DataTransferProtocol.DATA_TRANSFER_VERSION, 
				DataTransferProtocol.OP_MERGE_BLOCK));
		header.setLevel(0);
		header.setRecovery(false);
		LocatedBlockWithMetaInfo[] blocks = stripe.getBlocks();
		CandidateLocations candidate = ec.getCandidateLocations(stripe, erasedLocation);
		if (candidate == null) {
			LOG.warn("NTar : There are not enougth live blocks to reconstrunct lost block !");
			return null;
		}
		int[] candidateLocations = candidate.locations;
		int minNum = candidate.minNum;
		DatanodeInfo root = chooseRootDatanodeInfo(stripe, erasedLocation);
		String rootHost = root.name;
		DatanodeInfo[] datanodeInfos = getCandidateDatanodeInfos(stripe, candidateLocations, root);
		RecoverTreeNode[] nodes = getCandidateNodes(stripe, candidateLocations, rootHost);
		int[][] distances = getRealDistances(datanodeInfos);
		MinimumSpanningTree.Result result;
		int[] choosed;
		if(structure.equals("tree")) {
			result = MinimumSpanningTree.chooseAndBuildTree(nodes, distances, 0, minNum);
		} else if(structure.equals("line")){
			result = MinimumSpanningTree.chooseAndBuildLine(nodes, distances, 0, minNum);
		} else {
			result = MinimumSpanningTree.chooseAndBuildStar(nodes, distances, 0, minNum);
		}
		if (result == null)
			return null;
		choosed = result.chosed;
		int[] locationsToUse = ec.getLocationsToUse(stripe, nodes, choosed, erasedLocation);
		int[] recoverVector = ec.getRecoverVector(locationsToUse, erasedLocation);
		for(int j = 0; j < choosed.length; j++) {
			nodes[choosed[j]].getElement().setCoefficient(recoverVector[j]);
		}
		LocatedBlockWithMetaInfo block = blocks[erasedLocation];
		header.setNamespaceId(block.getNamespaceID());
		header.setBlockId(block.getBlock().getBlockId());
		header.setGenStamp(block.getBlock().getGenerationStamp());
		header.setOffsetInBlock(0);
		header.setLength(block.getBlockSize());
		return new RepairTask(pathInfo, result, nodes[0], nodes, header);
	}
	
	/**
	 * Get the stripes that contains blocks that need to be reconstructed
	 * @param srcFs
	 * @param srcUriPath
	 * @param srcStat
	 * @param parityFs
	 * @param parityUriPath
	 * @param parityStat
	 * @param codec
	 * @return
	 * 		stripes
	 * @throws IOException
	 */
	public List<Stripe> getErasedStripes(DistributedFileSystem srcFs,
			String srcUriPath, FileStatus srcStat,
			DistributedFileSystem parityFs, String parityUriPath,
			FileStatus parityStat, Codec codec) throws IOException {
		
		List<Stripe> erasedStripes = new LinkedList<Stripe>();
		VersionedLocatedBlocks srcLocatedBlocks;
		VersionedLocatedBlocks parityLocatedBlocks;
		int srcNamespaceId = 0;
		int parityNamespaceId = 0;
		int srcMethodFingerprint = 0;
		int parityMethodFingerprint = 0;
		if (DFSClient
				.isMetaInfoSuppoted(srcFs.getClient().namenodeProtocolProxy)) {
			LocatedBlocksWithMetaInfo lbksm = srcFs.getClient().namenode
					.openAndFetchMetaInfo(srcUriPath, 0, srcStat.getLen());
			srcNamespaceId = lbksm.getNamespaceID();
			srcLocatedBlocks = lbksm;
			srcMethodFingerprint = lbksm.getMethodFingerPrint();
			srcFs.getClient().getNewNameNodeIfNeeded(srcMethodFingerprint);
		} else {
			srcLocatedBlocks = srcFs.getClient().namenode.open(srcUriPath, 0,
					srcStat.getLen());
		}
		
		if (DFSClient
				.isMetaInfoSuppoted(parityFs.getClient().namenodeProtocolProxy)) {
			LocatedBlocksWithMetaInfo lbksm = parityFs.getClient().namenode
					.openAndFetchMetaInfo(parityUriPath, 0, parityStat.getLen());
			parityNamespaceId = lbksm.getNamespaceID();
			parityLocatedBlocks = lbksm;
			parityMethodFingerprint = lbksm.getMethodFingerPrint();
			parityFs.getClient().getNewNameNodeIfNeeded(parityMethodFingerprint);
		} else {
			parityLocatedBlocks = parityFs.getClient().namenode.open(parityUriPath, 0,
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
		
		final int stripeNumber =(int)((srcBlockNumber + stripeLen -1)/stripeLen);
		
		if(parityBlockNumber !=  (stripeNumber * parityLen)) {
			LOG.error("NTar : The number of blocks in pariyfile: " + parityUriPath
					+ "does not match that in srcfile" + srcUriPath); 
			return null;
		}
		
		for(int i = 0; i < stripeNumber; i++) {
			Stripe stripe = new Stripe(stripeLen, parityLen);
			int offset = i * stripeLen;
			int len = Math.min(stripeLen, srcBlockNumber - offset);
			for(int j = 0; j < len; j++) {
				LocatedBlock block = srcBlocks[offset+j];
				stripe.addDataBlock(new LocatedBlockWithMetaInfo(block.getBlock(), 
						block.getLocations(), block.getStartOffset(),
						srcDataTransferVersion, srcNamespaceId, srcMethodFingerprint));
				processBlock(stripe, block, j);
			}
			
			offset = i * parityLen;
			len = Math.min(parityLen, parityBlockNumber - offset);
			
			for(int j = 0; j < len; j++) {
				LocatedBlock block = parityBlocks[offset+j];
				stripe.addParityBlock(new LocatedBlockWithMetaInfo(block.getBlock(), 
						block.getLocations(), block.getStartOffset(),
						parityDataTransferVersion, parityNamespaceId, parityMethodFingerprint));
				processBlock(stripe, block, stripeLen + j);
			}
			
			if(stripe.hasErasures())
				erasedStripes.add(stripe);
		}
		return erasedStripes;
	}
	
	static public RecoverTreeNode[] getCandidateNodes(Stripe stripe,
			int[] candidateLocations, String rootHost) {
		RecoverTreeNode[] nodes = new RecoverTreeNode[candidateLocations.length + 1];
		nodes[0] = new RecoverTreeNode(rootHost, null);
		LocatedBlockWithMetaInfo[]  blocks = stripe.getBlocks();
		for(int i = 0; i < candidateLocations.length; i++) {
			LocatedBlockWithMetaInfo block = blocks[candidateLocations[i]];
			RecoverTreeNodeElement element = new RecoverTreeNodeElement(
					block.getNamespaceID(), block.getBlock().getBlockId(),
					block.getBlock().getGenerationStamp(), 0, candidateLocations[i]);
			nodes[i + 1] = new RecoverTreeNode(block.getLocations()[0].name, element);
		}
		return nodes;
	}
	
	static public DatanodeInfo[] getCandidateDatanodeInfos(Stripe stripe,
			int[] candidateLocations, DatanodeInfo root) {
		DatanodeInfo[] nodes = new DatanodeInfo[candidateLocations.length + 1];
		nodes[0] = root;
		LocatedBlockWithMetaInfo[]  blocks = stripe.getBlocks();
		for(int i = 0; i < candidateLocations.length; i++) {
			LocatedBlockWithMetaInfo block = blocks[candidateLocations[i]];
			nodes[i + 1] = block.getLocations()[0];
		}
		return nodes;
	}

	/**
	 * Choose the root node of regeneration tree, AKA the newcomer.
	 * The current method is very simple, we just try to avoid the 
	 * nodes that the other blocks in the stripe locate at.
	 * 
	 * @param stripe
	 * @param erasedLocation
	 * @return
	 *        the newcomer
	 * @throws IOException
	 */
	private DatanodeInfo chooseRootDatanodeInfo(Stripe stripe, int erasedLocation) 			
			throws IOException {
		// TODO Auto-generated method stub
		List<DatanodeInfo> nodesToAvoid = new LinkedList<DatanodeInfo>();
		final int dataSize = stripe.getDataSize();
		final int paritySize = stripe.getParitySize();
		
		for(int i = 0; i < dataSize; i++) {
			DatanodeInfo[] nodes = stripe.getDataBlock(i).getLocations();
			for(int j = 0; j < nodes.length; j++) {
				if(!nodesToAvoid.contains(nodes[j]))
					nodesToAvoid.add(nodes[j]);
			}
		}
		
		for(int i = 0; i < paritySize; i++) {
			DatanodeInfo[] nodes = stripe.getParityBlock(i).getLocations();
			for(int j = 0; j < nodes.length; j++) {
				if(!nodesToAvoid.contains(nodes[j]))
					nodesToAvoid.add(nodes[j]);
			}
		}
	
		return chooseDatanodeInfo(nodesToAvoid.toArray(new DatanodeInfo[0]));
	}
	
	/**
	 * Return the distances among an array of nodes
	 * @param nodes
	 *        the nodes array
	 * @return
	 *        an distance matrix, distance of nodes[i][j] is put in
	 *        element indexed by [i][j]
	 */
	static public int[][] getRealDistances(DatanodeInfo[] nodes) {
		// TODO Auto-generated method stub
		int[][] distances = new int[nodes.length][nodes.length];
		NetworkTopology netTopology = new NetworkTopology();
		for(int i = 0; i < distances.length; i++)
			netTopology.add(nodes[i]);
		for (int i = 0; i < distances.length; i++) {
			for (int j = 0; j < i; j++) {
				distances[i][j] = netTopology.getDistance(nodes[i], nodes[j]);
				distances[j][i] = distances[i][j];
			}
			distances[i][i] = 0;
		}

		return distances;
	}

	String arrayToString(int[] locationsToUse) {
		StringBuilder result = new StringBuilder();
		for(int i = 0; i < locationsToUse.length - 1; i++) {
			result.append(locationsToUse[i]);
			result.append(", ");
		}
		result.append(locationsToUse[locationsToUse.length -1]);
		return result.toString();
	}
	
	/**
	 * submit a tree-structured block recovery job to the root node,
	 * and wait for it to complete.
	 * 
	 * @param treeRoot
	 *        the newcomer
	 * @param header
	 *        recovery job header
	 * @return
	 *        true if succeeds, otherwise false
	 */
	boolean executeRepairTask(RecoverTreeNode treeRoot, 
			MergeBlockHeader header) {

		String rootHost = treeRoot.getHostName();
		Socket rootSocket = null;
		DataOutputStream rootOutput = null;
		DataInputStream rootInput = null;
		boolean returnvalue = false;
		
		try{
			InetSocketAddress rootAddr = NetUtils.createSocketAddr(rootHost);
			rootSocket = new Socket();

			/**
			 * To Do: We should multiply timeout according to children
			 * number of subnodes. I'm not sure the current way is OK.
			 */
			int timeoutValue = HdfsConstants.READ_TIMEOUT 
					+ HdfsConstants.READ_TIMEOUT_EXTENSION * 10;
			int writeTimeout = HdfsConstants.WRITE_TIMEOUT 
					+ HdfsConstants.WRITE_TIMEOUT_EXTENSION;
			NetUtils.connect(rootSocket, rootAddr, timeoutValue);
			rootSocket.setSoTimeout(timeoutValue);
			rootSocket.setSendBufferSize(FSConstants.DEFAULT_DATA_SOCKET_SIZE);
			rootOutput = new DataOutputStream(new BufferedOutputStream(
					NetUtils.getOutputStream(rootSocket, writeTimeout),
					FSConstants.SMALL_BUFFER_SIZE));
			rootInput = new DataInputStream(NetUtils.getInputStream(rootSocket));
			
			header.writeVersionAndOpCode(rootOutput);
			header.write(rootOutput);
			treeRoot.write(rootOutput);
			rootOutput.flush();
					
			// wait for the reconstruction to complete
			int status = rootInput.readInt();
			if(status < 0) {
				throw new IOException("Root node: " + rootHost 
						+ " return error status  during reconstructing block.");
			}
				
			returnvalue = true;
		} catch (IOException ioe) {
			LOG.error("NTar: executeTreeReconstructJob: error occurred during reconstructing block: " + ioe);
			returnvalue = false;
		} finally {
			IOUtils.closeStream(rootOutput);
			IOUtils.closeStream(rootInput);
			IOUtils.closeSocket(rootSocket);
		}
		return returnvalue;
	}

	/**
	 * Sorts source files ahead of parity files.
	 */
	void sortLostFiles(List<String> files) {
		// TODO: We should first fix the files that lose more blocks
		Comparator<String> comp = new Comparator<String>() {
			public int compare(String p1, String p2) {
				Codec c1 = null;
				Codec c2 = null;
				for (Codec codec : Codec.getCodecs()) {
					if (isParityFile(p1, codec)) {
						c1 = codec;
					} else if (isParityFile(p2, codec)) {
						c2 = codec;
					}
				}
				if (c1 == null && c2 == null) {
					return 0; // both are source files
				}

				if (c1 == null && c2 != null) {
					return -1; // only p1 is a source file
				}
				if (c2 == null && c1 != null) {
					return 1; // only p2 is a source file
				}
				return c2.priority - c1.priority; // descending order
			}
		};
		Collections.sort(files, comp);
	}

	/**
	 * Returns a DistributedFileSystem hosting the path supplied.
	 */
	protected DistributedFileSystem getDFS(Path p) throws IOException {
		return (DistributedFileSystem) p.getFileSystem(getConf());	
	}

	/**
	 * Reads through a source file reconstructing lost blocks on the way.
	 * 
	 * @param srcPath
	 *            Path identifying the lost file.
	 * @throws IOException
	 * @return true if file was reconstructed, false if no reconstruction was
	 *         necessary or possible.
	 */
	boolean processFile(Path srcPath, ParityFilePair parityPair,
			Decoder decoder, Context context) throws IOException,
			InterruptedException {
		//LOG.info("Processing file " + srcPath);
		Progressable progress = context;
		if (progress == null) {
			progress = RaidUtils.NULL_PROGRESSABLE;
		}

		DistributedFileSystem srcFs = getDFS(srcPath);
		FileStatus srcStat = srcFs.getFileStatus(srcPath);
		long blockSize = srcStat.getBlockSize();
		long srcFileSize = srcStat.getLen();
		String uriPath = srcPath.toUri().getPath();

		List<LocatedBlockWithMetaInfo> lostBlocks = lostBlocksInFile(srcFs,
				uriPath, srcStat);
		if (lostBlocks.size() == 0) {
			LOG.warn("Couldn't find any lost blocks in file " + srcPath
					+ ", ignoring...");
			return false;
		}
		
		for (LocatedBlockWithMetaInfo lb : lostBlocks) {
			Block lostBlock = lb.getBlock();
			long lostBlockOffset = lb.getStartOffset();

			final long blockContentsSize = Math.min(blockSize, srcFileSize
					- lostBlockOffset);
			File localBlockFile = File.createTempFile(lostBlock.getBlockName(),
					".tmp");
			localBlockFile.deleteOnExit();

			try {
				decoder.recoverBlockToFile(srcFs, srcPath,
						parityPair.getFileSystem(), parityPair.getPath(),
						blockSize, lostBlockOffset, localBlockFile,
						blockContentsSize, context);

				// Now that we have recovered the file block locally, send it.
				String datanode = chooseDatanode(lb.getLocations());
				computeMetadataAndSendReconstructedBlock(datanode,
						localBlockFile, lostBlock, blockContentsSize,
						lb.getDataProtocolVersion(), lb.getNamespaceID(),
						progress);

			} finally {
				localBlockFile.delete();
			}
			progress.progress();
		}

		return true;
	}

	/**
	 * Reads through a parity file, reconstructing lost blocks on the way. This
	 * function uses the corresponding source file to regenerate parity file
	 * blocks.
	 * 
	 * @return true if file was reconstructed, false if no reconstruction was
	 *         necessary or possible.
	 */
	boolean processParityFile(Path parityPath, Decoder decoder, Context context)
			throws IOException, InterruptedException {

		Progressable progress = context;
		if (progress == null) {
			progress = RaidUtils.NULL_PROGRESSABLE;
		}

		Path srcPath = sourcePathFromParityPath(parityPath);
		if (srcPath == null) {
			LOG.warn("Could not get regular file corresponding to parity file "
					+ parityPath + ", ignoring...");
			return false;
		}

		DistributedFileSystem parityFs = getDFS(parityPath);
		DistributedFileSystem srcFs = getDFS(srcPath);
		FileStatus parityStat = parityFs.getFileStatus(parityPath);
		long blockSize = parityStat.getBlockSize();
		FileStatus srcStat = srcFs.getFileStatus(srcPath);
		
		// Check timestamp.
		if (srcStat.getModificationTime() != parityStat.getModificationTime()) {
			LOG.warn("Mismatching timestamp for " + srcPath + " and "
					+ parityPath + ", ignoring...");
			return false;
		}

		String uriPath = parityPath.toUri().getPath();
		List<LocatedBlockWithMetaInfo> lostBlocks = lostBlocksInFile(parityFs,
				uriPath, parityStat);
		if (lostBlocks.size() == 0) {
			LOG.warn("Couldn't find any lost blocks in parity file "
					+ parityPath + ", ignoring...");
			return false;
		}
		for (LocatedBlockWithMetaInfo lb : lostBlocks) {
			Block lostBlock = lb.getBlock();
			long lostBlockOffset = lb.getStartOffset();

			File localBlockFile = File.createTempFile(lostBlock.getBlockName(),
					".tmp");
			localBlockFile.deleteOnExit();

			try {
				decoder.recoverParityBlockToFile(srcFs, srcPath, parityFs,
						parityPath, blockSize, lostBlockOffset, localBlockFile,
						context);

				// Now that we have recovered the parity file block locally,
				// send it.
				String datanode = chooseDatanode(lb.getLocations());
				computeMetadataAndSendReconstructedBlock(datanode,
						localBlockFile, lostBlock, blockSize,
						lb.getDataProtocolVersion(), lb.getNamespaceID(),
						progress);
			} finally {
				localBlockFile.delete();
			}
			progress.progress();
		}

		return true;
	}

	/**
	 * Reads through a parity HAR part file, reconstructing lost blocks on the
	 * way. A HAR block can contain many file blocks, as long as the HAR part
	 * file block size is a multiple of the file block size.
	 * 
	 * @return true if file was reconstructed, false if no reconstruction was
	 *         necessary or possible.
	 */
	boolean processParityHarPartFile(Path partFile, Progressable progress)
			throws IOException {
		LOG.info("Processing parity HAR file " + partFile);
		// Get some basic information.
		DistributedFileSystem dfs = getDFS(partFile);
		FileStatus partFileStat = dfs.getFileStatus(partFile);
		long partFileBlockSize = partFileStat.getBlockSize();
		LOG.info(partFile + " has block size " + partFileBlockSize);

		// Find the path to the index file.
		// Parity file HARs are only one level deep, so the index files is at
		// the same level as the part file.
		// Parses through the HAR index file.
		HarIndex harIndex = HarIndex.getHarIndex(dfs, partFile);
		String uriPath = partFile.toUri().getPath();
		int numBlocksReconstructed = 0;
		List<LocatedBlockWithMetaInfo> lostBlocks = lostBlocksInFile(dfs,
				uriPath, partFileStat);
		if (lostBlocks.size() == 0) {
			LOG.warn("Couldn't find any lost blocks in HAR file " + partFile
					+ ", ignoring...");
			return false;
		}
		for (LocatedBlockWithMetaInfo lb : lostBlocks) {
			Block lostBlock = lb.getBlock();
			long lostBlockOffset = lb.getStartOffset();

			File localBlockFile = File.createTempFile(lostBlock.getBlockName(),
					".tmp");
			localBlockFile.deleteOnExit();

			try {
				processParityHarPartBlock(dfs, partFile, lostBlock,
						lostBlockOffset, partFileStat, harIndex,
						localBlockFile, progress);

				// Now that we have recovered the part file block locally, send
				// it.
				String datanode = chooseDatanode(lb.getLocations());
				computeMetadataAndSendReconstructedBlock(datanode,
						localBlockFile, lostBlock, localBlockFile.length(),
						lb.getDataProtocolVersion(), lb.getNamespaceID(),
						progress);

				numBlocksReconstructed++;
			} finally {
				localBlockFile.delete();
			}
			progress.progress();
		}

		return true;
	}

	/**
	 * This reconstructs a single part file block by recovering in sequence each
	 * parity block in the part file block.
	 */
	private void processParityHarPartBlock(FileSystem dfs, Path partFile,
			Block block, long blockOffset, FileStatus partFileStat,
			HarIndex harIndex, File localBlockFile, Progressable progress)
			throws IOException {
		String partName = partFile.toUri().getPath(); // Temporarily.
		partName = partName.substring(1 + partName.lastIndexOf(Path.SEPARATOR));

		OutputStream out = new FileOutputStream(localBlockFile);

		try {
			// A HAR part file block could map to several parity files.
			// We need to use all of them to recover this block.
			final long blockEnd = Math.min(
					blockOffset + partFileStat.getBlockSize(),
					partFileStat.getLen());
			for (long offset = blockOffset; offset < blockEnd;) {
				HarIndex.IndexEntry entry = harIndex
						.findEntry(partName, offset);
				if (entry == null) {
					String msg = "Lost index file has no matching index entry for "
							+ partName + ":" + offset;
					LOG.warn(msg);
					throw new IOException(msg);
				}
				Path parityFile = new Path(entry.fileName);
				Encoder encoder = null;
				for (Codec codec : Codec.getCodecs()) {
					if (isParityFile(parityFile, codec)) {
						encoder = new Encoder(getConf(), codec);
					}
				}
				if (encoder == null) {
					String msg = "Could not figure out codec correctly for "
							+ parityFile;
					LOG.warn(msg);
					throw new IOException(msg);
				}
				Path srcFile = sourcePathFromParityPath(parityFile);
				FileStatus srcStat = dfs.getFileStatus(srcFile);
				if (srcStat.getModificationTime() != entry.mtime) {
					String msg = "Modification times of " + parityFile
							+ " and " + srcFile + " do not match.";
					LOG.warn(msg);
					throw new IOException(msg);
				}
				long lostOffsetInParity = offset - entry.startOffset;
				LOG.info(partFile + ":" + offset + " maps to " + parityFile
						+ ":" + lostOffsetInParity
						+ " and will be recovered from " + srcFile);
				encoder.recoverParityBlockToStream(dfs, srcStat,
						srcStat.getBlockSize(), parityFile, lostOffsetInParity,
						out, progress);
				// Finished recovery of one parity block. Since a parity block
				// has the same size as a source block, we can move offset by
				// source
				// block size.
				offset += srcStat.getBlockSize();
				LOG.info("Recovered " + srcStat.getBlockSize()
						+ " part file bytes ");
				if (offset > blockEnd) {
					String msg = "Recovered block spills across part file blocks. Cannot continue";
					throw new IOException(msg);
				}
				progress.progress();
			}
		} finally {
			out.close();
		}
	}

	/**
	 * Choose a datanode (hostname:portnumber). The datanode is chosen at random
	 * from the live datanodes.
	 * 
	 * @param locationsToAvoid
	 *            locations to avoid.
	 * @return A string in the format name:port.
	 * @throws IOException
	 */
	private String chooseDatanode(DatanodeInfo[] locationsToAvoid)
			throws IOException {
		DistributedFileSystem dfs = getDFS(new Path("/"));
		DatanodeInfo[] live = dfs.getClient().datanodeReport(
				DatanodeReportType.LIVE);
	
		Random rand = new Random();
		String chosen = null;
		int maxAttempts = 1000;
		for (int i = 0; i < maxAttempts && chosen == null; i++) {
			int idx = rand.nextInt(live.length);
			chosen = live[idx].name;
			for (DatanodeInfo avoid : locationsToAvoid) {
				if (chosen.equals(avoid.name)) {
					//LOG.info("Avoiding " + avoid.name);
					chosen = null;
					break;
				}
			}
		}
		if (chosen == null) {
			throw new IOException("Could not choose datanode");
		}
		return chosen;
	}
	
	private DatanodeInfo chooseDatanodeInfo(DatanodeInfo[] locationsToAvoid)
			throws IOException {
		DistributedFileSystem dfs = getDFS(new Path("/"));
		DatanodeInfo[] live = dfs.getClient().datanodeReport(
				DatanodeReportType.LIVE);
	
		Random rand = new Random();
		DatanodeInfo chosen = null;
		int maxAttempts = 1000;
		for (int i = 0; i < maxAttempts && chosen == null; i++) {
			int idx = rand.nextInt(live.length);
			chosen = live[idx];
			for (DatanodeInfo avoid : locationsToAvoid) {
				if (chosen.name.equals(avoid.name)) {
					chosen = null;
					break;
				}
			}
		}
		if (chosen == null) {
			throw new IOException("Could not choose datanode");
		}
		return chosen;
	}

	/**
	 * Reads data from the data stream provided and computes metadata.
	 */
	DataInputStream computeMetadata(Configuration conf, InputStream dataStream)
			throws IOException {
		ByteArrayOutputStream mdOutBase = new ByteArrayOutputStream(1024 * 1024);
		DataOutputStream mdOut = new DataOutputStream(mdOutBase);

		// First, write out the version.
		mdOut.writeShort(FSDataset.METADATA_VERSION);

		// Create a summer and write out its header.
		int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
		DataChecksum sum = DataChecksum.newDataChecksum(
				DataChecksum.CHECKSUM_CRC32, bytesPerChecksum);
		sum.writeHeader(mdOut);

		// Buffer to read in a chunk of data.
		byte[] buf = new byte[bytesPerChecksum];
		// Buffer to store the checksum bytes.
		byte[] chk = new byte[sum.getChecksumSize()];

		// Read data till we reach the end of the input stream.
		int bytesSinceFlush = 0;
		while (true) {
			// Read some bytes.
			int bytesRead = dataStream.read(buf, bytesSinceFlush,
					bytesPerChecksum - bytesSinceFlush);
			if (bytesRead == -1) {
				if (bytesSinceFlush > 0) {
					boolean reset = true;
					sum.writeValue(chk, 0, reset); // This also resets the sum.
					// Write the checksum to the stream.
					mdOut.write(chk, 0, chk.length);
					bytesSinceFlush = 0;
				}
				break;
			}
			// Update the checksum.
			sum.update(buf, bytesSinceFlush, bytesRead);
			bytesSinceFlush += bytesRead;

			// Flush the checksum if necessary.
			if (bytesSinceFlush == bytesPerChecksum) {
				boolean reset = true;
				sum.writeValue(chk, 0, reset); // This also resets the sum.
				// Write the checksum to the stream.
				mdOut.write(chk, 0, chk.length);
				bytesSinceFlush = 0;
			}
		}

		byte[] mdBytes = mdOutBase.toByteArray();
		return new DataInputStream(new ByteArrayInputStream(mdBytes));
	}

	private void computeMetadataAndSendReconstructedBlock(String datanode,
			File localBlockFile, Block block, long blockSize,
			int dataTransferVersion, int namespaceId, Progressable progress)
			throws IOException {

		LOG.info("Computing metdata");
		InputStream blockContents = null;
		DataInputStream blockMetadata = null;
		try {
			blockContents = new FileInputStream(localBlockFile);
			blockMetadata = computeMetadata(getConf(), blockContents);
			blockContents.close();
			progress.progress();
			// Reopen
			blockContents = new FileInputStream(localBlockFile);
			sendReconstructedBlock(datanode, blockContents, blockMetadata,
					block, blockSize, dataTransferVersion, namespaceId,
					progress);
		} finally {
			if (blockContents != null) {
				blockContents.close();
				blockContents = null;
			}
			if (blockMetadata != null) {
				blockMetadata.close();
				blockMetadata = null;
			}
		}
	}

	/**
	 * Send a generated block to a datanode.
	 * 
	 * @param datanode
	 *            Chosen datanode name in host:port form.
	 * @param blockContents
	 *            Stream with the block contents.
	 * @param block
	 *            Block object identifying the block to be sent.
	 * @param blockSize
	 *            size of the block.
	 * @param dataTransferVersion
	 *            the data transfer version
	 * @param namespaceId
	 *            namespace id the block belongs to
	 * @throws IOException
	 */
	private void sendReconstructedBlock(String datanode,
			final InputStream blockContents, DataInputStream metadataIn,
			Block block, long blockSize, int dataTransferVersion,
			int namespaceId, Progressable progress) throws IOException {
		InetSocketAddress target = NetUtils.createSocketAddr(datanode);
		Socket sock = SocketChannel.open().socket();

		int readTimeout = getConf().getInt(
				BlockIntegrityMonitor.BLOCKFIX_READ_TIMEOUT,
				HdfsConstants.READ_TIMEOUT);
		NetUtils.connect(sock, target, readTimeout);
		sock.setSoTimeout(readTimeout);

		int writeTimeout = getConf().getInt(
				BlockIntegrityMonitor.BLOCKFIX_WRITE_TIMEOUT,
				HdfsConstants.WRITE_TIMEOUT);

		OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				baseStream, FSConstants.SMALL_BUFFER_SIZE));

		boolean corruptChecksumOk = false;
		boolean chunkOffsetOK = false;
		boolean verifyChecksum = true;
		boolean transferToAllowed = false;

		try {
			LOG.info("Sending block " + block + " from "
					+ sock.getLocalSocketAddress().toString() + " to "
					+ sock.getRemoteSocketAddress().toString());
			BlockSender blockSender = new BlockSender(namespaceId, block,
					blockSize, 0, blockSize, corruptChecksumOk, chunkOffsetOK,
					verifyChecksum, transferToAllowed, metadataIn,
					new BlockSender.InputStreamFactory() {
						@Override
						public InputStream createStream(long offset)
								throws IOException {
							// we are passing 0 as the offset above,
							// so we can safely ignore
							// the offset passed
							return blockContents;
						}
					});

			// Header info
			out.writeShort(dataTransferVersion);
			out.writeByte(DataTransferProtocol.OP_WRITE_BLOCK);
			if (dataTransferVersion >= DataTransferProtocol.FEDERATION_VERSION) {
				out.writeInt(namespaceId);
			}
			out.writeLong(block.getBlockId());
			out.writeLong(block.getGenerationStamp());
			out.writeInt(0); // no pipelining
			out.writeBoolean(false); // not part of recovery
			Text.writeString(out, ""); // client
			out.writeBoolean(true); // sending src node information
			DatanodeInfo srcNode = new DatanodeInfo();
			srcNode.write(out); // Write src node DatanodeInfo
			// write targets
			out.writeInt(0); // num targets
			// send data & checksum
			blockSender.sendBlock(out, baseStream, null, progress);

			LOG.info("Sent block " + block + " to " + datanode);
		} finally {
			sock.close();
			out.close();
		}
	}

	/**
	 * returns the source file corresponding to a parity file
	 */
	Path sourcePathFromParityPath(Path parityPath) {
		String parityPathStr = parityPath.toUri().getPath();
		for (Codec codec : Codec.getCodecs()) {
			String prefix = codec.getParityPrefix();
			if (parityPathStr.startsWith(prefix)) {
				// Remove the prefix to get the source file.
				String src = parityPathStr.replaceFirst(prefix, "/");
				return new Path(src);
			}
		}
		return null;
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
	

	/**
	 * Returns the lost blocks in a file.
	 */
	abstract List<LocatedBlockWithMetaInfo> lostBlocksInFile(
			DistributedFileSystem fs, String uriPath, FileStatus stat)
			throws IOException;
	

	abstract public void processBlock(Stripe stipe, LocatedBlock block, int index);
	
	/**
	 * This class implements corrupt block fixing functionality.
	 */
	public static class CorruptBlockReconstructor extends BlockReconstructor {

		public CorruptBlockReconstructor(Configuration conf) throws IOException {
			super(conf);
		}

		List<LocatedBlockWithMetaInfo> lostBlocksInFile(
				DistributedFileSystem fs, String uriPath, FileStatus stat)
				throws IOException {

			List<LocatedBlockWithMetaInfo> corrupt = 
					new LinkedList<LocatedBlockWithMetaInfo>();
			VersionedLocatedBlocks locatedBlocks;
			int namespaceId = 0;
			int methodFingerprint = 0;
			if (DFSClient
					.isMetaInfoSuppoted(fs.getClient().namenodeProtocolProxy)) {
				LocatedBlocksWithMetaInfo lbksm = fs.getClient().namenode
						.openAndFetchMetaInfo(uriPath, 0, stat.getLen());
				namespaceId = lbksm.getNamespaceID();
				locatedBlocks = lbksm;
				methodFingerprint = lbksm.getMethodFingerPrint();
				fs.getClient().getNewNameNodeIfNeeded(methodFingerprint);
			} else {
				locatedBlocks = fs.getClient().namenode.open(uriPath, 0,
						stat.getLen());
			}
			final int dataTransferVersion = locatedBlocks
					.getDataProtocolVersion();
			for (LocatedBlock b : locatedBlocks.getLocatedBlocks()) {
				if (b.isCorrupt()
						|| (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
					corrupt.add(new LocatedBlockWithMetaInfo(b.getBlock(), b
							.getLocations(), b.getStartOffset(),
							dataTransferVersion, namespaceId, methodFingerprint));
				}
			}
			return corrupt;
		}

		@Override
		public void processBlock(Stripe stripe, LocatedBlock block, int index) {
			// TODO Auto-generated method stub
			if(isBlockCorrupt(block)){
				stripe.addToReconstructe(index);
			} else if(isBlockDecom(block)) {
				stripe.addNotToRead(index);
			}
		}
	}

	/**
	 * This class implements decommissioning block copying functionality.
	 */
	public static class DecommissioningBlockReconstructor extends
			BlockReconstructor {

		public DecommissioningBlockReconstructor(Configuration conf)
				throws IOException {
			super(conf);
		}

		List<LocatedBlockWithMetaInfo> lostBlocksInFile(
				DistributedFileSystem fs, String uriPath, FileStatus stat)
				throws IOException {

			List<LocatedBlockWithMetaInfo> decommissioning = new LinkedList<LocatedBlockWithMetaInfo>();
			VersionedLocatedBlocks locatedBlocks;
			int namespaceId = 0;
			int methodFingerprint = 0;
			if (DFSClient
					.isMetaInfoSuppoted(fs.getClient().namenodeProtocolProxy)) {
				LocatedBlocksWithMetaInfo lbksm = fs.getClient().namenode
						.openAndFetchMetaInfo(uriPath, 0, stat.getLen());
				namespaceId = lbksm.getNamespaceID();
				locatedBlocks = lbksm;
				methodFingerprint = lbksm.getMethodFingerPrint();
				fs.getClient().getNewNameNodeIfNeeded(methodFingerprint);
			} else {
				locatedBlocks = fs.getClient().namenode.open(uriPath, 0,
						stat.getLen());
			}
			final int dataTransferVersion = locatedBlocks
					.getDataProtocolVersion();

			for (LocatedBlock b : locatedBlocks.getLocatedBlocks()) {
				if (b.isCorrupt()
						|| (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
					// If corrupt, this block is the responsibility of the
					// CorruptBlockReconstructor
					continue;
				}

				// Copy this block iff all good copies are being decommissioned
				boolean allDecommissioning = true;
				for (DatanodeInfo i : b.getLocations()) {
					allDecommissioning &= i.isDecommissionInProgress();
				}
				if (allDecommissioning) {
					decommissioning
							.add(new LocatedBlockWithMetaInfo(b.getBlock(), b
									.getLocations(), b.getStartOffset(),
									dataTransferVersion, namespaceId,
									methodFingerprint));
				}
			}
			return decommissioning;
		}

		@Override
		public void processBlock(Stripe stripe, LocatedBlock block, int index) {
			// TODO Auto-generated method stub
			if(isBlockCorrupt(block)){
				stripe.addNotToRead(index);
			} else if(isBlockDecom(block)) {
				stripe.addToReconstructe(index);
			}
		} 
	}

}
