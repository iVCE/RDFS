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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Daemon;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Implementation of {@link RaidNode} that uses map reduce jobs to raid files.
 */
public class DistRaidNode extends RaidNode {

	public static final Log LOG = LogFactory.getLog(DistRaidNode.class);

	/** Daemon thread to monitor raid job progress */
	JobMonitor jobMonitor = null;
	Daemon jobMonitorThread = null;
	JobScheduler jobScheduler = null;
	Daemon jobSchedulerThread = null;
	Map<PolicyInfo, Map<String, FileStatus>> pendingFiles =  null; 
    int maxRunningJobNum = 100;
	public DistRaidNode(Configuration conf) throws IOException {
		super(conf);
		pendingFiles =  new HashMap<PolicyInfo, Map<String, FileStatus>>();
		this.jobMonitor = new JobMonitor(conf);
		this.jobMonitorThread = new Daemon(this.jobMonitor);
		this.jobScheduler = new JobScheduler();
		this.jobSchedulerThread = new Daemon(this.jobScheduler);
		this.jobMonitorThread.start();
		this.jobSchedulerThread.start();
		LOG.info("created");
	}

	
	class JobScheduler implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				while (jobMonitor.runningJobsCount() < maxRunningJobNum) {
					boolean newJob = false;
					Set<PolicyInfo> keys = pendingFiles.keySet();
					for (PolicyInfo info : keys) {
						Map<String, FileStatus> files = pendingFiles.get(info);
						Set<String> names = files.keySet();
						for(String name : names) {
							FileStatus fstatus = files.get(name);
							if (fstatus != null) {
								DistRaid dr = new DistRaid(conf);
								List<FileStatus> paths = new ArrayList<FileStatus>();
								paths.add(fstatus);
								dr.addRaidPaths(info, paths);
								boolean started;
								try {
									started = dr.startDistRaid();
									if (started) {
										newJob = true;
										files.remove(name);
										jobMonitor.monitorJob(info.getName(), dr);
									}
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							
							if (newJob)
								break;
						}
						
						if (newJob)
							break;
					}
					
					if (newJob)
						continue;
					else 
						break;
					
				} 
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * {@inheritDocs}
	 */
	@Override
	public void join() {
		super.join();
		try {
			if (jobMonitorThread != null)
				jobMonitorThread.join();
			if (jobSchedulerThread != null)
				jobSchedulerThread.join();
		} catch (InterruptedException ie) {
			// do nothing
		}
	}

	/**
	 * {@inheritDocs}
	 */
	@Override
	public void stop() {
		if (stopRequested) {
			return;
		}
		super.stop();
		if (jobMonitor != null)
			jobMonitor.running = false;
		if (jobMonitorThread != null)
			jobMonitorThread.interrupt();
		if (jobSchedulerThread != null)
			jobSchedulerThread.interrupt();
	}

	/**
	 * {@inheritDocs}
	 */
	@Override
	void raidFiles(PolicyInfo info, List<FileStatus> paths) throws IOException {
		raidFiles(conf, jobMonitor, info, paths);
	}

	void raidFiles(Configuration conf, JobMonitor jobMonitor,
			PolicyInfo info, List<FileStatus> paths) throws IOException {
		Map<String, FileStatus> files = this.pendingFiles.get(info);
		if (files == null) {
			files = new HashMap<String, FileStatus>();
			this.pendingFiles.put(info, files);
		}
		for (FileStatus path: paths) {
			String name = path.getPath().toString();
			files.put(name, path);
		}
		
	}

	/**
	 * {@inheritDocs}
	 */
	@Override
	int getRunningJobsForPolicy(String policyName) {
		return jobMonitor.runningJobsCount(policyName);
	}

	@Override
	public String raidJobsHtmlTable(JobMonitor.STATUS st) {
		return jobMonitor.toHtml(st);
	}
}
