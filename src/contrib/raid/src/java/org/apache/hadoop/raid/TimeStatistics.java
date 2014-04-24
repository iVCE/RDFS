package org.apache.hadoop.raid;

import org.apache.commons.logging.*;

public class TimeStatistics {
	private static long totalTime = 0;
	private static long communicateTime = 0;
	private static long computeTime = 0;
	private static long readTime = 0;
	private static long writeTime = 0;
	private static long copyTime = 0;

	public static void clear() {
		totalTime = 0;
		communicateTime = 0;
		computeTime = 0;
		readTime = 0;
		writeTime = 0;
		copyTime = 0;
	}

	public static void addCommunicateTime(long time) {
		communicateTime += time;
	}

	public static void addComputeTime(long time) {
		computeTime += time;
	}

	public static void addReadTime(long time) {
		readTime += time;
	}

	public static void addWriteTime(long time) {
		writeTime += time;
	}

	public static void addCopyTime(long time) {
		copyTime += time;
	}

	public static void setTotalTime(long time) {
		totalTime = time;
	}

	public static void print(String file, String erasureCode, Log LOG) {
		LOG.info("TIMESTATISTICS file" + file + " erasureCode: " + erasureCode);
		LOG.info("TIMESTATISTICS total\tcommu\tread\tcompu\twrite\tcopy");
		LOG.info("TIMESTATISTICS " + (float) totalTime / 1000 + "\t"
				+ (float) communicateTime / 1000 + "\t" 
				+ (float) readTime / 1000 + "\t" 
				+ (float) computeTime / 1000 + "\t" 
				+ (float) writeTime / 1000 + "\t" 
				+ (float) copyTime / 1000);
	}
}
