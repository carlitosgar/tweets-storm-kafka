package master2015;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import master2015.spout.LogBolt;
import master2015.spout.TweetKafkaSpout;

public class Top3App {
	
	public static final int RANK_NUMBER = 3;
	
	public static final String STREAM_TOTALS_SPOUT_TO_RANK = "totalstorank";
	public static final String STREAM_SUBRANK_TO_RANK = "subranktorank";
	public static final String STREAM_RANK_TO_LOGERS = "ranktologers";

	public Top3App() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws InterruptedException {
		TweetKafkaSpout spout = new TweetKafkaSpout("twitterStream", "localhost:2181");
		TopologyBuilder builder = new TopologyBuilder();  
		builder.setSpout("tweets",spout.getSpout());    
		builder.setBolt("logger", new LogBolt())
		        .shuffleGrouping("tweets");
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("test", conf, builder.createTopology());
	    Thread.sleep((long) 10000);
	    cluster.killTopology("test");
	    cluster.shutdown();
	}

}
