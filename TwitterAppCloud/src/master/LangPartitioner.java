package master;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class LangPartitioner implements Partitioner{

	public LangPartitioner (VerifiableProperties props) {
	}
	
	/**
	 * Implementation of custom partition publishing based on language.
	 * @see kafka.producer.Partitioner#partition(java.lang.Object, int)
	 */
	@Override
	public int partition(Object key, int numPartitions) {
        return Math.abs(((String) key).hashCode()) % numPartitions;
	}

}
