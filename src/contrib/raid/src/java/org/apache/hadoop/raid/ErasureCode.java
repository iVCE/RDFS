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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.RecoverTreeNode;

public abstract class ErasureCode {
	/**
	 * added by jason
	 */
	//Thread for parallel processing
	//pool of threads, static
	static ExecutorService readPool = Executors.newFixedThreadPool(30);
	
	Semaphore slots;
	
	 /**
	  * init the pool, after set the threads
	  */
	 public void initThreadPool(int codingThreadNum){
		this.slots = new Semaphore(codingThreadNum);	
	 }
	 
	 /**
	  * added by jason ended
	  */
  /**
   * Encodes the given message.
   * 
   * @param message
   *          The data of the message. The data is present in the least
   *          significant bits of each int. The number of data bits is
   *          symbolSize(). The number of elements of message is stripeSize().
   * @param parity
   *          (out) The information is present in the least significant bits of
   *          each int. The number of parity bits is symbolSize(). The number of
   *          elements in the code is paritySize().
   */
  public abstract void encode(int[] message, int[] parity);
  
  
  /**
   * added by jason
   */
  /**
   * Encodes the given message.
   * 
   * @param message
   * 			The data of the message. The data is present in the least
   *          significant bits of each int. The number of data bits is
   *          symbolSize(). The number of elements of message is stripeSize().
   * @param parity
   * 			(out) The information is present in the least significant bits of
   *          each int. The number of parity bits is symbolSize(). The number of
   *          elements in the code is paritySize().
   * @param dataBuffer
   * 			dataBuffer for encoding
   */
  public abstract void encode(int[] message, int[] parity, int[] dataBuffer);
  /**
   * added by jason ended
  */
  

  /**
   * Generates missing portions of data.
   * 
   * @param data
   *          The message and parity. The parity should be placed in the first
   *          part of the array. In each integer, the relevant portion is
   *          present in the least significant bits of each int. The number of
   *          elements in data is stripeSize() + paritySize().
   * @param erasedLocations
   *          The indexes in data which are to be fixed.
   *          All indexes not included in erasedLocations are considered
   *          available to the decode operation.
   * @param erasedValues
   *          (out)The decoded values corresponding to erasedLocations.
   */
  public abstract void decode(int[] data, int[] erasedLocations,
      int[] erasedValues);

  /**
   * Generates missing portions of data.
   * 
   * @param data
   *          The message and parity. The parity should be placed in the first
   *          part of the array. In each integer, the relevant portion is
   *          present in the least significant bits of each int. The number of
   *          elements in data is stripeSize() + paritySize().
   * @param erasedLocations
   *          The indexes in data which are to be fixed.
   * @param erasedValues
   *          (out)The decoded values corresponding to erasedLocations.
   * @param locationsToRead
   *          The indexes in data which can be used to fix the erasedLocations.
   * @param locationsNotToRead
   *          The indexes in data which cannot be used in the decode process.
   */
  public abstract void decode(int[] data, int[] erasedLocations,
      int[] erasedValues, int[] locationsToRead, int[] locationsNotToRead);
 
  /**
   *  added by jason
   */
  
  /**
   * Generates missing portions of data.
   * @param data
   *          The message and parity. The parity should be placed in the first
   *          part of the array. In each integer, the relevant portion is
   *          present in the least significant bits of each int. The number of
   *          elements in data is stripeSize() + paritySize().
   * @param erasedLocations
   *          The indexes in data which are to be fixed.
   * @param erasedValues
   *          (out)The decoded values corresponding to erasedLocations.
   * @param locationsToRead
   *          The indexes in data which can be used to fix the erasedLocations.
   * @param locationsNotToRead
   *          The indexes in data which cannot be used in the decode process.
   */
  public abstract void decodeParallel(int[] data, int[] erasedLocations,
      int[] erasedValues, int[] locationsToRead, int[] locationsNotToRead);
  /**
   * added by jason ended
   */

  /**
   * Figure out which locations need to be read to decode erased locations. The
   * locations are specified as integers in the range [ 0, stripeSize() +
   * paritySize() ). Values in the range [ 0, paritySize() ) represent parity
   * data. Values in the range [ paritySize(), paritySize() + stripeSize() )
   * represent message data.
   * 
   * @param erasedLocations
   *          The erased locations.
   * @return The locations to read.
   */
  public List<Integer> locationsToReadForDecode(List<Integer> erasedLocations)
      throws TooManyErasedLocations {
    List<Integer> locationsToRead = new ArrayList<Integer>(stripeSize());
    int limit = stripeSize() + paritySize();
    // Loop through all possible locations in the stripe.
    for (int loc = limit - 1; loc >= 0; loc--) {
      // Is the location good.
      if (erasedLocations.indexOf(loc) == -1) {
        locationsToRead.add(loc);
        if (stripeSize() == locationsToRead.size()) {
          break;
        }
      }
    }
    // If we are are not able to fill up the locationsToRead list,
    // we did not find enough good locations. Throw TooManyErasedLocations.
    if (locationsToRead.size() != stripeSize()) {
      String locationsStr = "";
      for (Integer erasedLocation : erasedLocations) {
        locationsStr += " " + erasedLocation;
      }
      throw new TooManyErasedLocations("Locations " + locationsStr);
    }
    return locationsToRead;
  }

  /**
   * The number of elements in the message.
   */
  public abstract int stripeSize();

  /**
   * The number of elements in the code.
   */
  public abstract int paritySize();

  /**
   * Initialize code parameters
   */
  public abstract void init(Codec codec);
  

  public abstract int symbolSize();

  /**
   * This method would be overridden in the subclass, 
   * so that the subclass will have its own encodeBulk behavior. 
   */
  public void encodeBulk(byte[][] inputs, byte[][] outputs) {
    final int stripeSize = stripeSize();
    final int paritySize = paritySize();
    assert (stripeSize == inputs.length);
    assert (paritySize == outputs.length);
    int[] data = new int[stripeSize];
    int[] code = new int[paritySize];

    for (int j = 0; j < outputs[0].length; j++) {
      for (int i = 0; i < paritySize; i++) {
        code[i] = 0;
      }
      for (int i = 0; i < stripeSize; i++) {
        data[i] = inputs[i][j] & 0x000000FF;
      }
      encode(data, code);
      for (int i = 0; i < paritySize; i++) {
        outputs[i][j] = (byte) code[i];
      }
    }
  }

  /**
   * This method would be overridden in the subclass, 
   * so that the subclass will have its own encodeBulk behavior. 
   * added by jason
   */
  public void encodeBulkParallel(final byte[][] inputs, final byte[][] outputs, int codingThreadNum) {
	  final int stripeSize = stripeSize();
	    final int paritySize = paritySize();
	    assert (stripeSize == inputs.length);
	    assert (paritySize == outputs.length);
	   
	    if(codingThreadNum<=1){
	    	encodeBulk(inputs,outputs);
	    	return;
	    }
	    final int totalThreadLocal=outputs[0].length;
	    //real 
	    final int UpperRange=totalThreadLocal/codingThreadNum +1;
	    
	    boolean acquired = false;
	    
	    for(int idx=0;idx<codingThreadNum;){
	    	final int idexThread=idx;
	    	//slot
	    	try{
	    		
	    		acquired = slots.tryAcquire(1, 10, TimeUnit.SECONDS);
	    		
	    	}catch(Exception e) {
	    		
	    		e.printStackTrace();
	    		
	    	}
	    	
	    	if (acquired) {
	    		readPool.execute(new Runnable(){
					public void run() {

						int[] data = new int[stripeSize];
					    int[] code = new int[paritySize]; 
						for(int j=Math.min(idexThread*UpperRange,totalThreadLocal);
								j<Math.min((idexThread+1)*UpperRange,totalThreadLocal);j++){
						
						
					    for (int i = 0; i < paritySize; i++) {
					        code[i] = 0;
					      }
					      //input
					      
					      for (int i = 0; i < stripeSize; i++) {
					        data[i] = inputs[i][j] & 0x000000FF;
					      }
					   		
					      int[] dataBuff = new int[paritySize + stripeSize];
					      encode(data, code, dataBuff);

					      for (int i = 0; i < paritySize; i++) {
					        outputs[i][j] = (byte) code[i];
					      }
					      }
						//processTime[0]+=System.currentTimeMillis()-time1;
					      //LOG.info("Complete writing a coded bulk: "+j);
						// }
					     //release 	
					    data = null;
					    code = null;
					    slots.release();
					}	 
				 });
	    		//increase thread id
	    		idx++;
	    	}
	    }
	    // All read operations have been submitted to the readPool.
	    // Now wait for the operations to finish and release the semaphore.
	   try {
		   
		    slots.acquire(codingThreadNum);
		   
	   }catch(Exception e) {
		   
		   e.printStackTrace();
		   
	   }

	    slots.release(codingThreadNum);
	    
  }
  
  /**
   * added by jason ended
   */
  
  /**
   * This method would be overridden in the subclass, 
   * so that the subclass will have its own decodeBulk behavior. 
   */
  public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
      int[] erasedLocations, int[] locationsToRead, int[] locationsNotToRead) {
    int[] tmpInput = new int[readBufs.length];
    int[] tmpOutput = new int[erasedLocations.length];

    int numBytes = readBufs[0].length;
    for (int idx = 0; idx < numBytes; idx++) {
      for (int i = 0; i < tmpOutput.length; i++) {
        tmpOutput[i] = 0;
      }
      for (int i = 0; i < tmpInput.length; i++) {
        tmpInput[i] = readBufs[i][idx] & 0x000000FF;
      }
      decode(tmpInput, erasedLocations, tmpOutput, locationsToRead,
          locationsNotToRead);
      for (int i = 0; i < tmpOutput.length; i++) {
        writeBufs[i][idx] = (byte) tmpOutput[i];
      }
    }
  }
  
  /**
   * This method would be overridden in the subclass, 
   * so that the subclass will have its own decodeBulk behavior. 
   * added by jason
   */
  public void decodeBulkParallel(final byte[][] readBufs, final byte[][] writeBufs,
      final int[] erasedLocations, final int[] locationsToRead, final int[] locationsNotToRead, int decodingThreadNum) {

	  final int totalThreadLocal=readBufs[0].length;	
	    //single thread
	    if(decodingThreadNum<=1){
	    	decodeBulk(readBufs,writeBufs,erasedLocations,locationsToRead,locationsNotToRead);
	    	return;
	    }
	     
	    final int UpperRange=totalThreadLocal/decodingThreadNum +1;	  
	    
	    try{
	    
	    for(int idx=0;idx<decodingThreadNum;){
	    	final int idexThread=idx;
	    	//slot
	    	boolean acquired = slots.tryAcquire(1, 10, TimeUnit.SECONDS);
	    	if (acquired) {
	    		readPool.execute(new Runnable(){
					public void run() {
						
						int[] tmpInput = new int[readBufs.length];
						int[] tmpOutput = new int[erasedLocations.length];	
			    //inner cycle
				for(int idx=Math.min(idexThread*UpperRange,totalThreadLocal);
						idx<Math.min((idexThread+1)*UpperRange,totalThreadLocal);idx++){
					//run
					for (int i = 0; i < tmpOutput.length; i++) {
				        tmpOutput[i] = 0;
				      }
				      for (int i = 0; i < tmpInput.length; i++) {
				        tmpInput[i] = readBufs[i][idx] & 0x000000FF;
				      }
				     	        				  
				      
//				      System.out.println("to execute the reconstruction");
				      //decode to temporary array

				      decodeParallel(tmpInput, erasedLocations, tmpOutput, locationsToRead,
					          locationsNotToRead);
				      //save to global data structure				     
				      for (int i = 0; i < tmpOutput.length; i++) {
				        writeBufs[i][idx] = (byte) tmpOutput[i];
				      }
				      
					
				}//end cycle
				tmpInput=null;
				tmpOutput=null;
				
						//realse
					    slots.release();
					}	 
				 });
	    		//increase thread id
	    		idx++;
	    	}
	    }
	    }catch(Exception e) {
	    	
	    	e.printStackTrace();
	    	
	    }
	    // All read operations have been submitted to the readPool.
	    // Now wait for the operations to finish and release the semaphore.
	    
	    while (true) {
	        boolean acquired = false;
			try {
				acquired = slots.tryAcquire(decodingThreadNum, 10, TimeUnit.SECONDS);
		        if (acquired) {
			          slots.release(decodingThreadNum);
			          break;
			        }
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	      }
  }
  
  /**
   * added by jason ended
   */
  
	/**
	 * Get the locations to use when reconstruct block numbered locationToReconstruct.
	 * At the moment we just try to use blocks from distinct nodes, as the current
	 * block placement policy is likely to put more than one blocks from same stripe
	 * in to same site[not implemented yet].
	 * To Do: The current implementation is very naive. We should implement more 
	 * sophisticated method. For example, there may be various choices with various
	 * cost, we should choose one with least cost.
	 * @param stripe
	 * 		the stripe that contains the block to reconstruct
	 * @param locationToReconstruct
	 * 		the location in stripe of block to reconstruct
	 * @return
	 *     an array of locations
	 */
	 abstract public CandidateLocations getCandidateLocations(Stripe stripe, 
			 int locationToReconstruct);
	
	abstract public int[] getLocationsToUse(Stripe stripe, RecoverTreeNode[] nodes, 
			int[] choosed, int locationToReconstruct);
	
	/**
	 * Given locations of data available and locations of erasures,
	 * return the vector to recover these erasures
	 * The locations of first original data symbol to last parity symbol are
	 * 0, 1, 2, 3, ... (stripeSize+paritySize-1) in order
	 * 
	 * @param dataLocations
	 * 		The locations of data available, which can be original data or parity.
	 * @param erasedLocation
	 *      The location of erasure to recover, which can be original data or parity.
	 * @return
	 *      The vector to recover from the data in give dataLocations.
	 */	
	abstract int[] getRecoverVector(int[] dataLocations, int locationToReconstruct);

}
