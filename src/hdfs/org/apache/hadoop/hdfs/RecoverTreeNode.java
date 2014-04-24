package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hdfs.MinimumSpanningTree.TreeNode;
import org.apache.hadoop.io.Text;

public class RecoverTreeNode extends TreeNode {

	private String hostName;
	private RecoverTreeNodeElement element;
	private List<TreeNode> children;
	
	public RecoverTreeNode(String hostName, RecoverTreeNodeElement element) {
		super();
		this.hostName = hostName;
		this.element = element;
	}

	public RecoverTreeNode() {
		super();
		element = null;
		children = null;
	}
	
	public RecoverTreeNode(TreeNode[] children) {
		super();
		element = null;
		this.children = new LinkedList<TreeNode>();
		for(int i = 0; i < children.length; i++) {
			this.children.add(children[i]);
		}
	}
	
	@Override
	public int getChildrenNumber() {
		if(children != null)
			return children.size();
		else
			return 0;
	}
	
	@Override
	public TreeNode getChild(int index) {
		if(children != null)
			return children.toArray(new TreeNode[0])[index];
		else
			return null;
	}
	
	@Override
	public boolean addChild(TreeNode child) {
		if(children == null)
			children = new LinkedList<TreeNode>();
		
		return this.children.add(child);
	}
	
	@Override
	public boolean isLeaf() {
		if(children != null && children.size() > 0)
			return false;
		else
			return true;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, hostName);
		if (element != null) {
			out.writeBoolean(true);
			element.write(out);
		} else {
			out.writeBoolean(false);
		}

		int childrenNumber = 0;
		TreeNode[] children = null;

		if (this.children != null) {
			children = this.children.toArray(new TreeNode[0]);
			childrenNumber = children.length;
		}

		out.writeInt(childrenNumber);
		for (int i = 0; i < childrenNumber; i++) {
			TreeNode node = children[i];
			node.write(out);
		}

	}
	
	public void readFields(DataInput in) throws IOException {
		hostName = Text.readString(in);
		if(in.readBoolean()) {
			element = new RecoverTreeNodeElement();
			element.readFields(in);
		}
		
		int childrenNumber = in.readInt();
		if(childrenNumber > 0) {
			children = new LinkedList<TreeNode>();
			for(int i = 0; i < childrenNumber; i++) {
				TreeNode child = new RecoverTreeNode();
				child.readFields(in);
				this.addChild(child);
			}
		}
		else {
			children = null;
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
		RecoverTreeNode other = (RecoverTreeNode) obj;
		if (children == null) {
			if (other.children != null)
				return false;
		} else if (!children.equals(other.children))
			return false;
		if (element == null) {
			if (other.element != null)
				return false;
		} else if (!element.equals(other.element))
			return false;
		if (hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!hostName.equals(other.hostName))
			return false;
		return true;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public RecoverTreeNodeElement getElement() {
		return element;
	}

	public void setElement(RecoverTreeNodeElement element) {
		this.element = element;
	}

	@Override
	public String toString() {
		return "RecoverTreeNode [hostName=" + hostName + ", has element = " 
				+ ((element == null)? false : true) +"]" + "child number: "
				+ this.getChildrenNumber();
	}

	@Override
	public void dropChildren() {
		// TODO Auto-generated method stub
		children.clear();
	}
	
}
