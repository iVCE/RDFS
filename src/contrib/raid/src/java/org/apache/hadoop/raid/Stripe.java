package org.apache.hadoop.raid;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;

/**
 * Collection of blocks in a stripe with their locations, and information
 * about how to reconstructed lost blocks (if any) in this stripe. <br>
 * <br>
 * Data blocks and parity blocks are sequentially numbered from 0 to
 * (stripeLen+parityLen+1) consecutively, with data blocks in the first part
 * 
 * @author star
 * 
 */
public class Stripe {
	final private int stripeLen; // data blocks number in a code stripe
	private int dataSize; // data blocks number present,
						  // note that a stripe doesn't need to be full
	final private int parityLen; // data blocks number in a code stripe
	private int paritySize; // parity blocks number in a code stripe
	private List<Integer> toReconstructList; // indexes of blocks need to be reconstruct
	private List<Integer> notToReadList; // indexes of blocks 
										 // that should not be read when reconstructing
	private LocatedBlockWithMetaInfo[] blocks; // blocks present in the stripe, 
											   // data blocks is put in the first part
	
	public Stripe(int stripeLen, int parityLen) {
		super();
		this.stripeLen = stripeLen;
		this.parityLen = parityLen;
		this.dataSize =  0;
		this.paritySize = 0; 
		this.toReconstructList = null; 
		this.notToReadList = null;
		this.blocks = new LocatedBlockWithMetaInfo[this.stripeLen + this.parityLen];
	}
	
	/**
	 * Add block that need to be reconstruct
	 * @param index
	 * 		the index of block needed to be reconstructed
	 * @return
	 */
	final public boolean addToReconstructe(int index) {
		if(toReconstructList != null 
				&& toReconstructList.size() >= (stripeLen+parityLen))
			return false;
		if(toReconstructList == null)
			toReconstructList = new LinkedList<Integer>();
		if(toReconstructList.contains(index)) {
			return true;
		} else {
			return toReconstructList.add(index);
		}
	}
	
	/**
	 * Add block that should not to be read when reconstructing
	 * @param index
	 * 		the index of block that should not to be read
	 * @return
	 */
	final public boolean addNotToRead(int index) {
		if(notToReadList != null 
				&& notToReadList.size() >= (stripeLen+parityLen))
			return false;
		if(notToReadList == null)
			notToReadList = new LinkedList<Integer>();
		if(notToReadList.contains(index)) {
			return true;
		} else {
			return notToReadList.add(index);
		}
	}
	
	/**
	 * Add data block into stripe.
	 * 
	 * @param block
	 * 		the block to add
	 * @return
	 * 		true is returned if operation succeed, 
	 * 		otherwise false is returned
	 */
	final public boolean addDataBlock(LocatedBlockWithMetaInfo block) {
		if(dataSize >= stripeLen)
			return false;
		blocks[dataSize] = block;
		dataSize++;
		return true;
	}
	
	/**
	 * Get a data block. 
	 * The data blocks are numbered from 0 to [dataSize-1]
	 * @param index
	 * 		the number of data block, which should be less than dataSize
	 * @return
	 * 		the block(when succeed) or null(when index out of bound)
	 */
	final public LocatedBlockWithMetaInfo getDataBlock(int index) {
		if(index < dataSize) 
			return blocks[index];
		
		return null;
	}
	
	/**
	 * Add parity block into stripe.
	 * 
	 * @param block
	 * 		the block to add
	 * @return
	 * 		true is returned if operation succeed, 
	 * 		otherwise false is returned
	 */
	final public boolean addParityBlock(LocatedBlockWithMetaInfo block) {
		if(paritySize >= parityLen)
			return false;
		blocks[paritySize + stripeLen] = block;
		paritySize++;
		return true;
	}
	
	/**
	 * Get a parity block. 
	 * The parity blocks are numbered from 0 to [paritySize-1]
	 * @param index
	 * 		the number of parity block, which should be less than paritySize
	 * @return
	 * 		the block(when succeed) or null(when index out of bound)
	 */
	final public LocatedBlockWithMetaInfo getParityBlock(int index) {
		if(index < paritySize)
			return blocks[stripeLen + index];
		
		return null;
	}
	
	/**
	 * Does there exists block(s) present need to be reconstructed
	 * 
	 * @return
	 */
	final public boolean hasErasures() {
		if(toReconstructList != null && toReconstructList.size() > 0)
			return true;
		else
			return false;
	}
	
	/**
	 * Return the indexes of blocks that need to be reconstructed
	 */
	public int[] getErasures() {
		if(hasErasures()) {
			int erasureNumber = toReconstructList.size();
			int[] erasures = new int[erasureNumber];
			Integer[] elements = toReconstructList.toArray(new Integer[0]);
			for(int i = 0; i < erasureNumber; i++)
				erasures[i] = elements[i].intValue();
			
			return erasures;
		}
		
		return new int[0];
	}
	
	/**
	 * Return the indexes of blocks that should not to be read when reconstructing
	 * 
	 * @return
	 */
	public int[] getNotToRead() {
		if(notToReadList != null && notToReadList.size() > 0) {
			int notToReadNumber = notToReadList.size();
			int[] notToRead = new int[notToReadNumber];
			Integer[] elements = notToReadList.toArray(new Integer[0]);
			for(int i = 0; i < notToReadNumber; i++)
				notToRead[i] = elements[i].intValue();
			
			return notToRead;
		}
		
		return new int[0];
	}

	
	final public int getStripeLen() {
		return stripeLen;
	}

	final public int getParityLen() {
		return parityLen;
	}

	final public int getDataSize() {
		return dataSize;
	}

	final public int getParitySize() {
		return paritySize;
	}

	final public LocatedBlockWithMetaInfo[] getBlocks() {
		return blocks;
	}

}
