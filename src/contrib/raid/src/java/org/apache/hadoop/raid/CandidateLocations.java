package org.apache.hadoop.raid;


public class CandidateLocations {
	public int[] locations;
	public int minNum;

	public CandidateLocations(int[] locations, int minNum) {
		super();
		this.locations = locations;
		this.minNum = minNum;
	}
}