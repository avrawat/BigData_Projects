package pagerank;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {
	private static final long serialVersionUID = 1L;
	private int numParts;

	public CustomPartitioner(int i) {
		numParts = i;
	}

	@Override
	public int numPartitions() {
		return numParts;
	}

	@Override
	public int getPartition(Object key) {
		// partition w.r.t the keys
		// 17 - number of partitions
		return ((Integer.parseInt((String) key)) / 10000) / 6;

	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CustomPartitioner) {
			CustomPartitioner partitionerObject = (CustomPartitioner) obj;
			if (partitionerObject.numParts == this.numParts)
				return true;
		}

		return false;
	}
}
