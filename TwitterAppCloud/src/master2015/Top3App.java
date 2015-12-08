package master2015;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import master2015.bolt.LangBolt;
import master2015.bolt.LogBolt;
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
		
		if(args.length < 1) {
			System.err.println("Invalid parameters");
			return;
		}
		
		//Get list of languages
		List<String> languages = new ArrayList<String>();
		String[] langs = args[0].split(",");
		for(String lang:langs){
			languages.add(lang);
		}
		
		//Build topology.
		TweetKafkaSpout spout = new TweetKafkaSpout("twitterStream", "localhost:2181");
		TopologyBuilder builder = new TopologyBuilder();
		//Kafka tweet's consumer.
		builder.setSpout("tweets",spout.getSpout());
		//Language filter bolt.
		builder.setBolt("langMapper", new LangBolt(languages))
			.shuffleGrouping("tweets");
		for(String lang:languages){
			builder.setBolt("logger_"+lang, new LogBolt(lang))
	        	.shuffleGrouping("langMapper", lang);
		}
		//Config topology.
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("test", conf, builder.createTopology());
	    //Set topology life-cycle.
	    Thread.sleep((long) 10000);
	    cluster.killTopology("test");
	    cluster.shutdown();
	}

}
