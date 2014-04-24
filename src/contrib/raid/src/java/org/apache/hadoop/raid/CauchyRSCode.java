package org.apache.hadoop.raid;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.RecoverTreeNode;

public class CauchyRSCode extends ErasureCode {
	public static final Log LOG = LogFactory.getLog(CauchyRSCode.class);

	private int stripeSize;
	private int paritySize;
	private GaloisField GF = GaloisField.getInstance();
	private int[][] codingMatrix;
	private int[][] identityMatrix;
	
	public CauchyRSCode() {	
	}
	
	@Override
	public void init(Codec codec) {
		// TODO Auto-generated method stub
		init(codec.stripeLength, codec.parityLength);
		/*
	    LOG.info("Initialized " + CauchyRSCode.class +
	             " stripeLength:" + codec.stripeLength +
	             " parityLength:" + codec.parityLength);
	             */
	}
	
	private void init(int stripeSize, int paritySize) {
		assert (stripeSize + paritySize < GF.getFieldSize());
		this.stripeSize = stripeSize;
		this.paritySize = paritySize;
		this.codingMatrix = getCodingMatrix();
		this.identityMatrix = generateIdentityMatrix(this.stripeSize);	
	}
	
	@Override
	/**
	 * added by jason
	 */
	public void encode(int[] message, int[] parity, int[] dataBuffer) {
		// TODO Auto-generated method stub
		assert(message.length == stripeSize && parity.length == paritySize);
		
		int tmp;
		for(int i = 0; i < paritySize; i++) {
			tmp = 0;
			for(int j = 0; j < stripeSize; j++) {
				tmp ^= GF.multiply(codingMatrix[i][j], message[j]);
			}
			parity[i] = tmp;
		}
	}
	
	/**
	 * added by jason ended
	 */
	
	@Override
	public void encode(int[] message, int[] parity) {
		assert(message.length == stripeSize && parity.length == paritySize);
		
		int tmp;
		for(int i = 0; i < paritySize; i++) {
			tmp = 0;
			for(int j = 0; j < stripeSize; j++) {
				tmp ^= GF.multiply(codingMatrix[i][j], message[j]);
			}
			parity[i] = tmp;
		}
	}

	@Override
	public void decode(int[] data, int[] erasedLocations, int[] erasedValues) {

		if (erasedLocations.length == 0) {
			return;
		}
		assert (erasedLocations.length == erasedValues.length);
		
		int[] locationsToUse = getLocationsToUse(erasedLocations, stripeSize);
		int[] dataToUse = getDataToUse(data, locationsToUse);
		
		decodeInternal(dataToUse, transformLocationsToInternal(locationsToUse), 
				transformLocationsToInternal(erasedLocations), erasedValues);

	}
	
	@Override
	/**
	 * added by jason
	 */
	public void decodeParallel(int[] data, int[] erasedLocations,
			int[] erasedValues, int[] locationsToRead, int[] locationsNotToRead) {
		// TODO Auto-generated method stub
		
//		System.out.println("in the cauchy RS Code");
		
		int[] locationsToUse = getLocationsToUse(erasedLocations, locationsToRead, 
				locationsNotToRead, stripeSize);
		int[] dataToUse = getDataToUse(data, locationsToUse);
		
		decodeInternal(dataToUse, transformLocationsToInternal(locationsToUse), 
				transformLocationsToInternal(erasedLocations), erasedValues);
		
	}
	/**
	 * added by jason ended
	 */
	
	
	@Override
	public void decode(int[] data, int[] erasedLocations, int[] erasedValues,
			int[] locationsToRead, int[] locationsNotToRead) {
		
		int[] locationsToUse = getLocationsToUse(erasedLocations, locationsToRead, 
				locationsNotToRead, stripeSize);
		int[] dataToUse = getDataToUse(data, locationsToUse);
		
		decodeInternal(dataToUse, transformLocationsToInternal(locationsToUse), 
				transformLocationsToInternal(erasedLocations), erasedValues);
	}
	
	@Override
	public void encodeBulk(byte[][] message, byte[][] parity) {
		assert (message.length == stripeSize && parity.length == paritySize);
        
		int tmp, length;
		length = message[0].length;
		for (int i = 0; i < paritySize; i++) {
			for (int j = 0; j < length; j++) {
				tmp = 0;
				for (int k = 0; k < stripeSize; k++) {
					tmp ^= GF.multiply(codingMatrix[i][k],
							message[k][j] & 0x000000FF);
				}
				parity[i][j] = (byte) (0x000000FF & tmp);
			}
		}
	}
	
	public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
			int[] erasedLocation) {
		if (erasedLocation.length == 0) {
			return;
		}

		int[] locationsToUse = getLocationsToUse(erasedLocation, stripeSize);
		byte[][] dataToUse = getDataToUse(readBufs, locationsToUse);
		
		decodeInternal(dataToUse, transformLocationsToInternal(locationsToUse), 
				transformLocationsToInternal(erasedLocation), writeBufs, writeBufs[0].length);
	}
	  
	@Override
	public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
			int[] erasedLocations, int[] locationsToRead,
			int[] locationsNotToRead) {
		int[] locationsToUse = getLocationsToUse(erasedLocations, locationsToRead,
				locationsNotToRead, stripeSize);
		byte[][] dataToUse = getDataToUse(readBufs, locationsToUse);
		
		decodeInternal(dataToUse, 
				transformLocationsToInternal(locationsToUse), 
				transformLocationsToInternal(erasedLocations), 
				writeBufs, readBufs[0].length);
	}
	
	/**
	 * Recover the given erasures from given data,
	 * and put results in erasedValues
	 * 
	 * @param data
	 * 		The data, original data or parity, 
	 * 		which can recover erasures in erasuredLocations.
	 *      For RS code, there should be stripeSize of data
	 *        
	 * @param dataLocations
	 * 		The locations of each data in parameter data. 
	 *      The locations of original data and parity data are numbered:
	 *      0, 1, 2, ... (stripeSize + paritySize-1)
	 * @param erasedLocations
	 *      The locations of erasures to recover. For RS code,
	 *      there should be less than pairtySize erasures.
	 * @param erasedValues
	 *      The buffer where recovered data will be put in
	 */		
	private void decodeInternal(int[] data, int[] dataLocations,
			int[] erasedLocations, int[] erasedValues) {
		if (erasedLocations.length == 0) {
			return;
		}

		assert (erasedLocations.length == erasedValues.length);

		int tmp;
		int[][] recoverVectors = getRecoverVectors(dataLocations,
				erasedLocations);
		for (int i = 0; i < erasedLocations.length; i++) {
			tmp = 0;
			for (int k = 0; k < stripeSize; k++) {
				tmp ^= GF.multiply(data[k],
						recoverVectors[i][k]);
			}
			erasedValues[i] = tmp;
		}
	}
	
	/**
	 * Recover the given erasures from given data,
	 * and put results in erasedValues
	 * 
	 * @param data
	 * 		The data, original data or parity, 
	 * 		which can recover erasures in erasuredLocations.
	 *      For RS code, there should be stripeSize of data
	 *        
	 * @param dataLocations
	 * 		The locations of each data in parameter data. 
	 *      The locations of original data and parity data are numbered:
	 *      0, 1, 2, ... (stripeSize + paritySize-1)
	 * @param erasedLocations
	 *      The locations of erasures to recover. For RS code,
	 *      there should be less than pairtySize erasures.
	 * @param erasedValues
	 *      The buffer where recovered data will be put in
	 * @param dataSize
	 * 		The size of data in each buffer
	 */		
	private void decodeInternal(byte[][] data, int[] dataLocations,
			int[] erasedLocations, byte[][] erasedValues, int dataSize) {
		if (erasedLocations.length == 0) {
			return;
		}

		assert (erasedLocations.length == erasedValues.length);

		int tmp;
		int[][] recoverVectors = getRecoverVectors(dataLocations,
				erasedLocations);
		for (int i = 0; i < erasedLocations.length; i++) {
			for (int j = 0; j < dataSize; j++) {
				tmp = 0;
				for (int k = 0; k < stripeSize; k++)
					tmp ^= GF.multiply(data[k][j] & 0x000000FF,
							recoverVectors[i][k]);
				erasedValues[i][j] = (byte) (tmp & 0x000000FF);
			}
		}
	}
	
	/**
	 * Given locations of data available and locations of erasures,
	 * return the vectors to recover these erasures
	 * The locations of first original data symbol to last parity symbol are
	 * 0, 1, 2, 3, ... (stripeSize+paritySize-1) in order
	 * 
	 * @param dataLocations
	 * 		The locations of data available, which can be original data or parity.
	 * 		For RS code, stripeSize of data is enough to recover any erasure, so
	 *      ensure dataLocations contain stripeSize of locations
	 * @param erasedLocations
	 *      The locations of erasures to recovery, which can be original data or parity.
	 *      For RS code, erasedLocations should be less than paritySize
	 * @return
	 *      The vectors for each erasure to recover from the data in give dataLocations.
	 *      The i row of return is the vector for i erasure in erasedLocation
	 */	
	public int[][] getRecoverVectors(int[] dataLocations, int[] erasedLocations) {
		if (erasedLocations.length == 0 || dataLocations.length != stripeSize)
			return null;

		int[][] recoverVectors = new int[erasedLocations.length][];
		int[][] decodingMatrix = new int[stripeSize][];
		int[][] inverse;
		int location;

		// Constitute the matrix which generate data in dataLocations
		for (int i = 0; i < stripeSize; i++) {
			location = dataLocations[i];
			decodingMatrix[i] = ((location < stripeSize) ? identityMatrix[location]
					: codingMatrix[location - stripeSize]);
		}

		// Compute the matrix which can generate the original data 
		// from data in dataLocations
		inverse = invertMatrix(decodingMatrix);

		// Generate the vector of each erasure. For a data erasure i row of inverse
		// is its vector, for a parity erasure we need do a little more work
		for (int i = 0; i < erasedLocations.length; i++) {
			location = erasedLocations[i];
			recoverVectors[i] = ((location < stripeSize) ? inverse[location]
					: multiplyMatrix(codingMatrix[location - stripeSize],
							inverse));
		}

		return recoverVectors;
	}
	
	public int[] getRecoverVector(int[] dataLocations, int locationToReconstruct) {
		if (dataLocations.length != stripeSize)
			return null;

		int[] recoverVector = null;
		int[][] decodingMatrix = new int[stripeSize][];
		int[][] inverse;

		int location;
		// Constitute the matrix which generate data in dataLocations
		for (int i = 0; i < stripeSize; i++) {
			location = dataLocations[i];
			decodingMatrix[i] = ((location < stripeSize) ? identityMatrix[location]
					: codingMatrix[location - stripeSize]);
		}

		// Compute the matrix which can generate the original data
		// from data in dataLocations
		inverse = invertMatrix(decodingMatrix);

		// Generate the vector of erasure. For a data erasure i row of
		// inverse is its vector, for a parity erasure we need do a little more work
		recoverVector = ((locationToReconstruct < stripeSize) ? inverse[locationToReconstruct]
				: multiplyMatrix(codingMatrix[locationToReconstruct
						- stripeSize], inverse));

		return recoverVector;
	}
	
	/////////////////////////////////////////////////////////////////////////
	// The following four methods is used to get the real data and their locations,
	// which will actually be used to decode erasures.
	/////////////////////////////////////////////////////////////////////////

	private int[] getLocationsToUse(int[] erasedLocations, int size) {
		assert (size <= (stripeSize + paritySize - erasedLocations.length));
		int[] locationsToUse = new int[size];
		int[] locations = new int[stripeSize + paritySize];
		
		Arrays.fill(locations, 1);
		for(int i = 0; i < erasedLocations.length; i++)
			locations[erasedLocations[i]] = 0;
		
		for(int i = 0, j = 0; (i < (stripeSize + paritySize)) && (j < size); i++) {
			if(locations[i] == 1) {
				locationsToUse[j] = i;
				j++;
			}
		}
		
		return locationsToUse;
	}
	
	public int[] getLocationsToUse(int[] erasedLocations, int[] locationsToRead, 
			int[] locationsNotToRead, int size) {

		int[] locationsToUse = new int[size];
		int[] locations = new int[stripeSize + paritySize];
		
		Arrays.fill(locations, 0);
		for(int i = 0; i < locationsToRead.length; i++)
			locations[locationsToRead[i]] = 1;
		for(int i = 0; i < erasedLocations.length; i++)
			locations[erasedLocations[i]] = 0;
		for(int i = 0; i < locationsNotToRead.length; i++)
			locations[locationsNotToRead[i]] = 0;
		
		for(int i = 0, j = 0; (i < (stripeSize + paritySize)) && (j < size); i++) {
			if(locations[i] == 1) {
				locationsToUse[j] = i;
				j++;
			}
		}
		
		return locationsToUse;
	}
	
	private int[] getDataToUse(int[] data, int[] locationsToUse) {
		
		int[] dataToUse = new int[locationsToUse.length];
		
		for(int i = 0; i < dataToUse.length; i++) {
				dataToUse[i] = data[locationsToUse[i]];
		}
		
		return dataToUse;
	}
	
	private byte[][] getDataToUse(byte[][] data, int[] locationsToUse) {
		
		byte[][] dataToUse = new byte[locationsToUse.length][];
		
		for(int i = 0; i < dataToUse.length; i++) {
				dataToUse[i] = data[locationsToUse[i]];
		}
		
		return dataToUse;
	}
	/////////////////////////////////////////////////////////////////////////////
	
	/**
	 * In ErasureCode class, the parity is placed in the first place.
	 * However, inside this class, the data is placed in the first place.
	 * So we need to map the out side locations to internal ones.
	 * This is not the ideal way, but this is the simple one, we don't
	 * want to modify, debug and test either of them.
	 * We may modify this later.
	 */
	private int[] transformLocationsToInternal(int[] locations) {
		
		int[] internalLocations = new int[locations.length];
		
		for(int i = 0; i < locations.length; i++) {
			int tmpLocation = locations[i];
			if (tmpLocation < 0) {
				return null;
			} else if (tmpLocation < paritySize) {
				internalLocations[i] = tmpLocation + stripeSize;
				continue;
			} else if(tmpLocation < (stripeSize + paritySize)) {
				internalLocations[i] = tmpLocation - paritySize;
				continue;
			} else {
				return null;
			}		
		}
		
		return internalLocations;
	}
	
	
	/**
	 * Generate the Coding Matrix
	 */
	private int[][] getCodingMatrix() {
		int[][] cauchyDM;
		int i, j;

		cauchyDM = new int[paritySize][stripeSize];

		for (i = 0; i < paritySize; i++) {
			for (j = 0; j < stripeSize; j++) {
				cauchyDM[i][j] = GF.divide(1, (i ^ (paritySize + j)));
			}
		}
		return cauchyDM;
	}

	/**
	 * Swap row i and row j of matrix
	 * 
	 * @param matrix
	 * @param i
	 * @param j
	 */
	final private void swap(int[][] matrix, int i, int j) {
		int[] temp;
		temp = matrix[i];
		matrix[i] = matrix[j];
		matrix[j] = temp;
	}

	/**
	 * Compute X*A It should be ensured that the number of A's rows equals the
	 * size of X, there is no check in this method
	 * 
	 * @param X
	 * @param A
	 * @return X*A
	 */
	private int[] multiplyMatrix(int[] X, int[][] A) {

		int rows = A.length;
		int cols = A[0].length;
		int[] results = new int[cols];

		for (int i = 0; i < cols; i++) {
			results[i] = 0;
			for (int j = 0; j < rows; j++) {
				results[i] ^= GF.multiply(A[j][i], X[j]);
			}
		}

		return results;
	}

	private int[][] generateIdentityMatrix(int n) {
		int[][] identityMatrix = new int[n][n];

		for (int i = 0; i < n; i++)
			for (int j = 0; j < n; j++)
				identityMatrix[i][j] = ((i == j) ? 1 : 0);

		return identityMatrix;
	}

	/**
	 * Invert square matrix A by Gaussian elimination
	 * 
	 * @param A A must be a square matrix and invertible
	 * @return If matrix is invertible, return its inverse; otherwise, return
	 *         null
	 */
	private int[][] invertMatrix(int[][] A) {
		int i, j, k, n, tmp;
		int[][] inverse, matrixCopy;

		n = A.length;
		matrixCopy = new int[n][n];

		inverse = generateIdentityMatrix(n);

		// We would not like to modify square matrix A,
		// so copy it firstly
		for (i = 0; i < n; i++)
			System.arraycopy(A[i], 0, matrixCopy[i], 0, n);

		// First -- convert it into upper triangular
		for (i = 0; i < n; i++) {

			// Swap rows if we have a zero [i,i] element.
			// If we can't swap, then the matrix was not invertible
			if (matrixCopy[i][i] == 0) {
				for (j = i + 1; j < n && matrixCopy[j][i] == 0; j++)
					;
				if (j == n) 
					return null;
	
				swap(matrixCopy, i, j);
				swap(inverse, i, j);
			}

			// Divide i row by element [i,i] to transform element [i,i] to 1
			tmp = matrixCopy[i][i];
			for (j = i; j < n; j++)
				matrixCopy[i][j] = GF.divide(matrixCopy[i][j], tmp);
			for (j = 0; j < n; j++)
				inverse[i][j] = GF.divide(inverse[i][j], tmp);

			// Now for each j > i, add matrix[i]*matrix[j][i] to matrix[j]
			for (j = i + 1; j < n; j++) {
				if (matrixCopy[j][i] != 0) {
					tmp = matrixCopy[j][i];
					for (k = i; k < n; k++)
						matrixCopy[j][k] ^= GF.multiply(matrixCopy[i][k], tmp);
					for (k = 0; k < n; k++)
						inverse[j][k] ^= GF.multiply(inverse[i][k], tmp);
				}
			}
		}

		// Now the matrix is upper triangular. Start at the top and multiply
		// down. Note that this is the last step and these operations on 
		// matrixCopy has no effects on the result, so it's OK to neglect 
		// matrixCopy
		for (i = n - 1; i > 0; i--) {
			for (j = i - 1; j >= 0; j--) {
				tmp = matrixCopy[j][i];
				for (k = n - 1; k >= 0; k--)
					inverse[j][k] ^= GF.multiply(inverse[i][k], tmp);
			}
		}
		return inverse;
	}
	
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
	public CandidateLocations getCandidateLocations(Stripe stripe, int locationToReconstruct) {
		// TODO Auto-generated method stub
		final int stripeLen = stripe.getStripeLen();
		final int dataSize = stripe.getDataSize();
		final int parityLen = stripe.getParityLen();
		final int paritySize = stripe.getParitySize();
		int[] erasedLocations = stripe.getErasures();
		int[] notToReadLocations = stripe.getNotToRead();
		int[] locations = new int[stripeLen + parityLen];
		Arrays.fill(locations, 1);
		for(int i = 0; i < erasedLocations.length; i++)
			locations[erasedLocations[i]] = 0;
		for(int i = 0; i < notToReadLocations.length; i++)
			locations[notToReadLocations[i]] = 0;
		
		List<Integer> candidatesList = new LinkedList<Integer>();
		for(int i = 0; i < (stripeLen + paritySize); i++) {
			if((i < dataSize) || (i >= stripeLen)) {
				if(locations[i] == 1) {
					candidatesList.add(new Integer(i));
				}
			}
		}
		
		if (candidatesList.size() < dataSize)
			return null;
		
		int[] candidates = new int[candidatesList.size()];
		int index = 0;
		for(Integer e : candidatesList) {
			candidates[index] = e.intValue();
			index = index + 1;
		}
		
		return new CandidateLocations(candidates, dataSize);
	}
	
	public int[] getLocationsToUse(Stripe stripe, RecoverTreeNode[] nodes, 
			int[] choosed, int locationToReconstruct) {
		// TODO Auto-generated method stub
		final int stripeLen = stripe.getStripeLen();
		int[] locationsToUse = new int[stripeLen];
		int choosedLen = choosed.length;
		for(int i  = 0; i < choosedLen; i++) {
			locationsToUse[i] = nodes[choosed[i]].getElement().getStripId();
		}
		
		for(int i = stripeLen - choosedLen - 1; i >= 0; i--) {
			locationsToUse[choosedLen + i] = choosedLen + i;
		}
		
		return locationsToUse;
	}

	@Override
	public int stripeSize() {
		// TODO Auto-generated method stub
		return this.stripeSize;
	}

	@Override
	public int paritySize() {
		// TODO Auto-generated method stub
		return this.paritySize;
	}

	@Override
	public int symbolSize() {
		// TODO Auto-generated method stub
		return 8;
	}

}
