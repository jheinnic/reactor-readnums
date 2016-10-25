package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import com.google.common.base.Preconditions;


public class ExclusionTrie
{
	public static final byte BYTE_0 = 48;
	/**
	 * A node represents the set content at a particular digit position.
	 * 
	 * Nodes have four variants: empty, complete, partial, and sparse.
	 *
	 * The variants can be broken down in terms of three key traits.
	 * 
	 * The first trait is whether the node is at a leaf or internal level of the trie. Absent and present nodes describe
	 * the leaf states where children are no longer a possibility. The former case is used when the parent is The second
	 * simpler trait is whether its children are explicitly allocated or implied without allocation. A node's children
	 * can only be explicit when they are all in the same state. Homogenous nodes are empty or complete, and heterogenous
	 * nodes are partial or sparse.
	 * 
	 * The final trait determines whether the existence of a child node indicates presence or exclusion in the content
	 * set. This distinction is fairly obvious for differentiating the two homogenous variants--empty nodes exclude the
	 * existence of an children, whereas complete nodes include the existence of all implied children.
	 * 
	 * The root node of the trie begins as homogenous exclusion, but very quickly changes to express heterogenous
	 * inclusion. The homogenous children of a hetereogenous node can always be either complete or empty, but there is a
	 * constraint restricting heterogenous children. The heterogenous children of an partial (inclusive) node may either
	 * be partial or sparse, but the heterogenous children of a sparse node may only be other sparse nodes. The reason
	 * has to to with the inability of an exclusive node to express the path information to locate such nodes if they
	 * were permitted to exist.
	 * 
	 * In summary using the terminology introduced above: -- An empty node implies absolute exclusion of all set members
	 * rooted at its position within the trie. -- A complete node implies absolute inclusion of all set members rooted at
	 * its position within the trie. -- A partial node explicitly declares children
	 * 
	 * @author jheinnic
	 *
	 */
	abstract static class Node
	{
		final short nodeLevel;
		final int maxPresentLeaves;


		protected Node(short nodeLevel) {
			this.nodeLevel = nodeLevel;
			this.maxPresentLeaves = 10 ^ nodeLevel;
		}


		short nodeLevel()
		{
			return this.nodeLevel;
		}


		int maxLeafCount()
		{
			return this.maxPresentLeaves;
		}


		abstract boolean hasChildren();


		abstract int actualLeafCount();


		abstract boolean isLeaf();


		abstract InnerNode asInner();


		abstract LeafNode asLeaf();
	}


	abstract static class LeafNode
	extends Node
	{
		public LeafNode( short nodeLevel )
		{
			super(nodeLevel);
		}


		@Override
		boolean hasChildren()
		{
			return false;
		}


		@Override
		boolean isLeaf()
		{
			return true;
		}


		@Override
		InnerNode asInner()
		{
			throw new IllegalStateException("Not an inner node");
		}


		@Override
		LeafNode asLeaf()
		{
			return this;
		}

		abstract boolean isPresent();


		abstract boolean isAbsent();
	}


	/**
	 * A node is
	 * 
	 * @author jheinnic
	 *
	 */
	static class CompleteNode
	extends LeafNode
	{
		CompleteNode( short nodeLevel ) {
			super(nodeLevel);
		}


		@Override
		boolean isPresent()
		{
			return true;
		}


		@Override
		boolean isAbsent()
		{
			return false;
		}


		@Override
		int actualLeafCount()
		{
			return this.maxLeafCount();
		}
	}


	static class EmptyNode
	extends LeafNode
	{
		public EmptyNode( final short nodeLevel )
		{
			super(nodeLevel);
		}


		@Override
		boolean isPresent()
		{
			return false;
		}


		@Override
		boolean isAbsent()
		{
			return true;
		}


		@Override
		int actualLeafCount()
		{
			return 0;
		}
	}


	static class InnerNode
	extends Node
	{
		final Node[] children = new Node[10];
		int actualPresentLeaves;


		InnerNode( short expandForLevel, EmptyNode baseFlyweight )
		{
			super(expandForLevel);

			if (expandForLevel == 1) {
				for (int ii = 0; ii < 10; ii++) {
					children[ii] = baseFlyweight;
				}
			} else {
				final short nextLevel = (short) (expandForLevel - 1);
				for (int ii = 0; ii < 10; ii++) {
					children[ii] = new InnerNode(nextLevel, baseFlyweight);
				}
			}

			actualPresentLeaves = 0;
		}

		@Override
		boolean hasChildren()
		{
			return true;
		}


		@Override
		boolean isLeaf()
		{
			return false;
		}


		@Override
		InnerNode asInner()
		{
			return this;
		}


		@Override
		LeafNode asLeaf()
		{
			throw new IllegalStateException("Not a leaf node");
		}


		@Override
		int maxLeafCount()
		{
			return this.maxPresentLeaves;
		}


		@Override
		int actualLeafCount()
		{
			return this.actualPresentLeaves;
		}

		boolean hasChildren(byte childByte) {
			return children[childByte - BYTE_0].hasChildren();
		}


		boolean isLeaf(byte childByte)
		{
			return children[childByte - BYTE_0].isLeaf();
		}


		Node getChildNode(byte childByte)
		{
			return children[childByte - BYTE_0];
		}


		LeafNode getLeafChildNode(byte childByte)
		{
			assert children[childByte - BYTE_0].isLeaf();
			return (LeafNode) children[childByte - BYTE_0];
		}


		InnerNode getInnerChildNode(byte childByte)
		{
			assert children[childByte - BYTE_0].hasChildren();
			return (InnerNode) children[childByte - BYTE_0];
		}


		boolean doUniqueCheck(byte[] msgBuf)
		{
			final boolean retVal;
			final Node nextChild =
				children[msgBuf[this.nodeLevel - BYTE_0]];

			if (nextChild.isLeaf()) {
				if (nextChild.asLeaf().isAbsent()) {
					// TODO: Replace previously empty child with partial subtree.
					retVal = true;
					this.actualPresentLeaves += 1;
				} else {
					retVal = false;
				}
			} else {
				final InnerNode innerChildNode = nextChild.asInner();
				if (innerChildNode.doUniqueCheck(msgBuf)) {
					retVal = true;
					this.actualPresentLeaves += 1;
					if ((this.actualPresentLeaves < this.maxPresentLeaves) &&
						(innerChildNode.actualLeafCount() >= innerChildNode.maxLeafCount()))
					{
						// TODO: Replace full child of incomplete parent with completion flyweight.
					}
				} else {
					retVal = false;
				}
			}

			return retVal;
		}
	}

	private final InnerNode[] rootNodes;
	private final EmptyNode[] emptyFlyweights;
	private final CompleteNode[] completeFlyweights;


	ExclusionTrie( short partitionCount, short levelCount )
	{
		Preconditions.checkArgument(partitionCount > 0);
		Preconditions.checkArgument(levelCount > 0);

		this.rootNodes = new InnerNode[partitionCount];
		this.completeFlyweights = new CompleteNode[levelCount];
		this.emptyFlyweights = new EmptyNode[levelCount];

		for (short ii = 0; ii < levelCount; ii++) {
			this.emptyFlyweights[ii] = new EmptyNode(ii);
			this.completeFlyweights[ii] = new CompleteNode(ii);
		}
		for (short ii = 0; ii < partitionCount; ii++) {
			this.rootNodes[ii] = new InnerNode((short) (levelCount - 1), this.emptyFlyweights[0]);
		}
	}


	public static ExclusionTrie getExpandedTrie( short partitionCount, short levelCount ) {
		return new ExclusionTrie(partitionCount, levelCount);
	}


	public boolean insertIfUnique(byte[] msgBuf, short suffix)
	{
		assert suffix >= 0;
		assert suffix < this.rootNodes.length;
		assert msgBuf.length == 9;

		final InnerNode rootNode = this.rootNodes[suffix];
		return rootNode.doUniqueCheck(msgBuf);
	}
}
