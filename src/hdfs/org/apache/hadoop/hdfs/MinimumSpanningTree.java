package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;


public class MinimumSpanningTree implements Writable {
	
	static public int DISCONNECTED = java.lang.Integer.MAX_VALUE;
	public static final Log LOG = LogFactory.getLog(MinimumSpanningTree.class);
	abstract static public class TreeNode implements Writable {
		abstract public int getChildrenNumber();
		abstract public String getHostName();
		abstract public TreeNode getChild(int index);
		abstract public boolean addChild(TreeNode child);
		abstract public boolean isLeaf();
		abstract public void write(DataOutput out) throws IOException;
		abstract public void readFields(DataInput in) throws IOException;
		abstract public boolean equals(Object obj);
		abstract public void dropChildren();
	}
	
	public static class Result {
		public int totalWeight;
		public int[] chosed;
		
		public Result() {
			totalWeight = 0;
			chosed = null;
		}
		
		public Result(int totalWeight, int[] chosed) {
			this.totalWeight = totalWeight;
			this.chosed = chosed;
		}
	}
	private TreeNode root;
	
	public MinimumSpanningTree(TreeNode root) {
		this.root = root;
	}

	public TreeNode getRoot() {
		return root;
	}
	
 	@Override
	public void write(DataOutput out) throws IOException {
		root.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		root.readFields(in);	
	}
	
	static private class Vertex implements Comparable<Vertex> {
		public int id;
		public int value;
		public int to;
		public Vertex(int id, int to, int value) {
			this.id = id;
			this.value = value;
			this.to = to;
		}
		
		@Override
		public int compareTo(Vertex o) {
			if (value > o.value) {
				return +1;
			} else if(value < o.value) {
				return -1;
			} else {
				return 0;
			}
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Vertex other = (Vertex) obj;
			if (id != other.id)
				return false;
			if (to != other.to)
				return false;
			if (value != other.value)
				return false;
			return true;
		}
	}
	
	static public MinimumSpanningTree build(TreeNode[] nodes, int[][] distances, int root) {
		MinimumSpanningTree mst = new MinimumSpanningTree(nodes[root]);
		PriorityQueue<Vertex> costList = new PriorityQueue<Vertex>();
		
		for(int i = 0; i < distances.length; i++) {
			if(i != root)
				costList.add(new Vertex(i, root, distances[i][root]));
		}
		
		while(!costList.isEmpty()) {
			Vertex next = costList.poll();
			nodes[next.to].addChild(nodes[next.id]);
			Vertex[] remains = costList.toArray(new Vertex[0]);
			for(int i = 0; i<remains.length; i++) {
				if(distances[remains[i].id][next.id] <= remains[i].value) {
					costList.remove(remains[i]);
					remains[i].to = next.id;
					remains[i].value = distances[remains[i].id][next.id];
					costList.add(remains[i]);
				}
			}
		}
		return mst;	
	}
	
	static public Result chooseAndBuildTree(TreeNode[] nodes, int[][] distances, 
			int root, int nodeNumber) {
		PriorityQueue<Vertex> costList = new PriorityQueue<Vertex>();
		int[] locationsChoosed = new int[nodeNumber];	
		int totalWeight = 0;
		for(int i = 0; i < distances.length; i++) {
			if(i != root)
				costList.add(new Vertex(i, root, distances[i][root]));
		}
		int number = 0;	
		while((number < nodeNumber) && (!costList.isEmpty())) {
			Vertex next = costList.poll();
			nodes[next.to].addChild(nodes[next.id]);
			totalWeight += next.value;
			//LOG.info("NTar: add " + nodes[next.id] + " as child of " + nodes[next.to]);
			locationsChoosed[number] = next.id;
			number = number + 1;
			Vertex[] remains = costList.toArray(new Vertex[0]);
			for(int i = 0; i<remains.length; i++) {
				if(distances[remains[i].id][next.id] < remains[i].value) {
					costList.remove(remains[i]);
					remains[i].to = next.id;
					remains[i].value = distances[remains[i].id][next.id];
					costList.add(remains[i]);
				}
			}
		}
		
		return new Result(totalWeight, locationsChoosed);
	}
	
	static public Result chooseAndBuildLine(TreeNode[] nodes, int[][] distances, 
			int root, int nodeNumber) {
		PriorityQueue<Vertex> costList = new PriorityQueue<Vertex>();
		int[] locationsChoosed = new int[nodeNumber];	
		Arrays.fill(locationsChoosed, -1);
		int totalWeight = 0;

		for (int i = 0; i < distances.length; i++) {
			if (i != root)
				costList.add(new Vertex(i, root, distances[i][root]));
		}
		int number = 0;	
		while ((number < nodeNumber) && (!costList.isEmpty())) {
			Vertex next = costList.poll();
			nodes[next.to].addChild(nodes[next.id]);
			totalWeight += next.value;
			//LOG.info("NTar: add " + nodes[next.id] + " as child of " + nodes[next.to]);
			locationsChoosed[number] = next.id;
			number = number + 1;
			Vertex[] remains = costList.toArray(new Vertex[0]);
			for (int i = 0; i<remains.length; i++) {
				costList.remove(remains[i]);
				remains[i].to = next.id;
				remains[i].value = distances[remains[i].id][next.id];
				costList.add(remains[i]);
			}
		}
		return new Result(totalWeight, locationsChoosed);
	}
	
	static public Result chooseAndBuildStar(TreeNode[] nodes, int[][] distances, 
			int root, int nodeNumber) {
		PriorityQueue<Vertex> costList = new PriorityQueue<Vertex>();
		int[] locationsChoosed = new int[nodeNumber];
		int totalWeight = 0;
		for(int i = 0; i < distances.length; i++) {
			if(i != root)
				costList.add(new Vertex(i, root, distances[i][root]));
		}
		int number = 0;	
		while((number < nodeNumber) && (!costList.isEmpty())) {
			Vertex next = costList.poll();
			totalWeight += next.value;
			nodes[next.to].addChild(nodes[next.id]);
			//LOG.info("TREE: add " + nodes[next.id] + " as child of " + nodes[next.to]);
			locationsChoosed[number] = next.id;
			number = number + 1;
		}
		
		return new Result(totalWeight, locationsChoosed);
	}
}

