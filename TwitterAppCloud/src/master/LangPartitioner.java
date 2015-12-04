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
    public int partition(Object key, int numPartitions) {
    	/**
    	 * TODO: We don't receive languages as a parameter of the app.
    	 * We receive languages as a parameter of Storm topology. 
    	 * Think of how to partition!!!
    	 */
       return 0;
    }
}
