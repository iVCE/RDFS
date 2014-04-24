package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.*;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.RaidConfigurationException;
import org.apache.hadoop.util.HostsFileReader;
import org.xml.sax.SAXException;

public class BlockPlacementPolicyRaidBalance extends BlockPlacementPolicyRaid {
	static final Log LOG = LogFactory
			.getLog(BlockPlacementPolicyRaidBalance.class);

	private org.apache.hadoop.raid.ConfigManager policyConfig;
	
	/** {@inheritDoc} */
	@Override
	public void initialize(Configuration conf,  FSClusterStats stats,
	                       NetworkTopology clusterMap, HostsFileReader hostsReader,
	                       DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem namesystem) {
		super.initialize(conf, stats, clusterMap, 
				hostsReader, dnsToSwitchMapping, namesystem);
		conf.addResource("raid-default.xml");
		conf.addResource("raid-site.xml");
		LOG.info("balance");
		try {
			this.policyConfig = new org.apache.hadoop.raid.ConfigManager(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RaidConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	/*
	@Override
	public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
			DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
			long blocksize) {
		return chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, null,
				blocksize);
	}

	@Override
	public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
			DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
			List<Node> exlcNodes, long blocksize) {
		try {
			FileInfo info = getFileInfo(srcPath);
			if (LOG.isDebugEnabled()) {
				LOG.debug("FileType:" + srcPath + " " + info.type.name());
			}
			if (info.type == FileType.NOT_RAID) {
				return super.chooseTarget(srcPath, numOfReplicas, writer,
						chosenNodes, exlcNodes, blocksize);
			}
			ArrayList<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
			HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
			if (exlcNodes != null) {
				for (Node node : exlcNodes) {
					excludedNodes.put(node, node);
				}
			}
			for (Node node : chosenNodes) {
				excludedNodes.put(node, node);
			}
			chooseRandom(numOfReplicas, "/", excludedNodes, blocksize, 1,
					results);
			return results.toArray(new DatanodeDescriptor[results.size()]);
		} catch (Exception e) {
			FSNamesystem.LOG
					.debug("Error happend when choosing datanode to write:"
							+ StringUtils.stringifyException(e));
			return super.chooseTarget(srcPath, numOfReplicas, writer,
					chosenNodes, blocksize);
		}
	}
	*/ 
	/** {@inheritDoc} */
	@Override
	public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
			Block block, short replicationFactor,
			Collection<DatanodeDescriptor> first,
			Collection<DatanodeDescriptor> second) {

		//LOG.info("Balance: replicationFacotr = " + replicationFactor
		//		+ " first:");
		for (DatanodeDescriptor dn : first) {
			LOG.info(dn.getName());
		}
		//LOG.info("Balance: second:");
		for (DatanodeDescriptor dn : second) {
			LOG.info(dn.getName());
		}

		try {
			DatanodeDescriptor chosenNode = null;
			// Count the number of replicas on each node and rack
			String path = cachedFullPathNames.get(inode);
			//LOG.info("Balance: path = " + path);
			FileInfo info = getFileInfo(path);
			if (info.type == FileType.NOT_RAID) {
				return super.chooseReplicaToDelete(inode, block,
						replicationFactor, first, second);
			}
			
			List<DatanodeDescriptor> all = new LinkedList<DatanodeDescriptor>();
			all.addAll(first);
			all.addAll(second);
			final Map<String, Integer> nodeCount = countDataNodes(
					all, false);
			final Map<String, Integer> rackCount = countDataNodes(
					all, true);
			
			int nodeMax = Collections.max(nodeCount.values());
			int rackMax = Collections.max(rackCount.values());
			//LOG.info("Balance: nodeMax = " + nodeMax + ", rackMax = " + rackMax);
			if(nodeMax > 1 || rackMax > 1) {
				NodeComparator comparator =
					      new NodeComparator(nodeCount, rackCount);
				chosenNode = Collections.max(all, comparator);
				if(chosenNode != null) {
					//LOG.info("Balance: chosenNode = " + chosenNode.getName());
					return chosenNode;
				}
			}
			return super.chooseReplicaToDelete(inode, block, replicationFactor,
					first, second);
		} catch (Exception e) {
			LOG.debug("Failed to choose the correct replica to delete", e);
			return super.chooseReplicaToDelete(inode, block, replicationFactor,
					first, second);
		}
	}

	private Map<String, Integer> countDataNodes(
			List<DatanodeDescriptor> all, boolean doRackCount) {
		// TODO Auto-generated method stub
		Map<String, Integer> result = new HashMap<String, Integer>();
		for (DatanodeDescriptor dn : all) {
			String name = doRackCount ? dn.getParent().getName(): dn.getName();
			if (result.containsKey(name)) {
				int value = result.get(name) + 1;
				result.put(name, value);
			} else {
				result.put(name, 1);
			}
		}
		return result;
	}

	/**
	 * Return raid information about a file, for example if this file is the
	 * source file, parity file, or not raid
	 * 
	 * @param path
	 *            file name
	 * @return raid information
	 * @throws IOException
	 */
	@Override
	protected FileInfo getFileInfo(String path) throws IOException {
		for (org.apache.hadoop.raid.protocol.PolicyInfo policy : policyConfig
				.getAllPolicies()) {
			LOG.info("placement:srcPath:" + policy.getSrcPath()
					+ ", extenededSrcPath:" + policy.getSrcPathExpanded()
					+ ", codecID:" + policy.getCodecId());
		}
		LOG.info("placement:path:" + path);
		for (Codec c : Codec.getCodecs()) {
			if (path.startsWith(c.tmpHarDirectory + Path.SEPARATOR)) {
				return new FileInfo(FileType.HAR_TEMP_PARITY, c);
			}
			if (path.startsWith(c.tmpParityDirectory + Path.SEPARATOR)) {
				return new FileInfo(FileType.TEMP_PARITY, c);
			}
			if (path.startsWith(c.parityDirectory + Path.SEPARATOR)) {
				return new FileInfo(FileType.PARITY, c);
			}
		}
		return new FileInfo(FileType.SOURCE, Codec.getCodec("crs"));
	}

	
	/**
	 * Count how many companion blocks are on each datanode or the each rack
	 * 
	 * @param companionBlocks
	 *            a collection of all the companion blocks
	 * @param doRackCount
	 *            count the companion blocks on the racks of datanodes
	 * @param result
	 *            the map from node name to the number of companion blocks
	 */
	static Map<String, Integer> countCompanionBlocks(
			Collection<LocatedBlock> companionBlocks, boolean doRackCount) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		for (LocatedBlock block : companionBlocks) {
			for (DatanodeInfo d : block.getLocations()) {
				String name = doRackCount ? d.getParent().getName() : d
						.getName();
				if (result.containsKey(name)) {
					int count = result.get(name) + 1;
					result.put(name, count);
				} else {
					result.put(name, 1);
				}
			}
		}
		return result;
	}
}