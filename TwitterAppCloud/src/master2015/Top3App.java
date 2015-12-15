package master2015;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import master2015.bolt.HashtagSplitBolt;
import master2015.bolt.LangBolt;
import master2015.bolt.LogBolt;
import master2015.bolt.SubRankBolt;
import master2015.spout.TweetKafkaSpout;
import master2015.structures.TimeWindow;

public class Top3App {
	
	public static final int RANK_NUMBER = 3;
	
	public static final String STREAM_TWEETS_SPOUT = "tweets";
	public static final String STREAM_LANG_MAPPER_BOLT = "langMapper";
	public static final String STREAM_TOTALS_SPOUT_TO_RANK = "totalstorank";
	public static final String STREAM_SUBRANK_TO_RANK = "subranktorank";
	public static final String STREAM_RANK_TO_LOGERS = "ranktologers";
	public static final int LANG_MAPPER_PARALLELISM = 2;
	public static final int HASHTAG_SPLIT_PARALLELISM = 2;
	public static final int SUBRANK_PARALLELISM = 2;

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
		
		//Config time window.
		String[] twParams = args[2].split(",");
		TimeWindow.configTimeWindow(Integer.parseInt(twParams[0]), Integer.parseInt(twParams[1]));
		
		//Build topology.
		TweetKafkaSpout spout = new TweetKafkaSpout("twitterStream", args[1]);
		TopologyBuilder builder = new TopologyBuilder();
		
		//Kafka tweet's consumer.
		builder.setSpout(Top3App.STREAM_TWEETS_SPOUT,spout.getSpout());
		
		//Language subStreams
		builder.setBolt(Top3App.STREAM_LANG_MAPPER_BOLT, new LangBolt(languages),
			Top3App.LANG_MAPPER_PARALLELISM)
			.shuffleGrouping(Top3App.STREAM_TWEETS_SPOUT);
		
		for(String lang:languages){
			//Hashtag Splitter 
			builder.setBolt("htSplitter_"+lang, new HashtagSplitBolt(),Top3App.HASHTAG_SPLIT_PARALLELISM)
	        	.shuffleGrouping("langMapper", lang);
			//Subrank
			builder.setBolt("subRank_"+lang, new SubRankBolt(),Top3App.SUBRANK_PARALLELISM)
				.fieldsGrouping("htSplitter_"+lang, new Fields("hashtag"));
			builder.setBolt("printer_"+lang, new LogBolt())
				.shuffleGrouping("subRank_"+lang);
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
