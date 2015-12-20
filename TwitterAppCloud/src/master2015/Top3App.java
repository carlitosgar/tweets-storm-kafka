package master2015;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import master2015.bolt.FileLogBolt;
import master2015.bolt.FinalRankBolt;
import master2015.bolt.HashtagSplitBolt;
import master2015.bolt.LangBolt;
import master2015.bolt.LogBolt;
import master2015.bolt.SubRankBolt;
import master2015.bolt.TimeWindowManagerBolt;
import master2015.spout.TweetKafkaSpout;
import master2015.structures.TimeWindow;
import master2015.structures.tuple.RankTupleValues;

public class Top3App {
	
	public static final int RANK_NUMBER = 3;
	
	public static final String TWEETS_SPOUT = "tweets";
	public static final String LANG_FILTER_BOLT = "langFilter";
	public static final String HASHTAG_SPLIT_BOLT = "htSplitter";
	public static final String SUBRANK_BOLT = "subRank";
	public static final String FINAL_RANK_BOLT = "finalRank";
	public static final String FILE_LOGER_BOLT = "fileLoger";
	public static final String LOGER_BOLT = "printer";
	public static final String TIME_MANAGER_BOLT = "timemanager";
	
	public static final String STREAM_TOTALS_SPOUT_TO_RANK = "totalstorank";
	public static final String STREAM_SUBRANK_TO_RANK = "subranktorank";
	public static final String STREAM_RANK_TO_LOGERS = "ranktologers";
	public static final String STREAM_MANAGER_TO_RANK = "managertorank";
	
	public static final int LANG_FILTER_PARALLELISM = 1;
	public static final int HASHTAG_SPLIT_PARALLELISM = 1;
	public static final int SUBRANK_PARALLELISM = 1;
	public static final int FINAL_RANK_PARALLELISM = 1; 
	public static final int FILE_LOGER_PARALLELISM = 1; //Should not be greater than the number of languages

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
		String[] langs;
		if(args[0].contains(",")){
			langs = args[0].split(",");
			for(String lang:langs){
				languages.add(lang);
			}
		}else{
			languages.add(args[0]);
		}
		
		//Config time window.
		String[] twParams = args[2].split(",");
		TimeWindow.configTimeWindow(Integer.parseInt(twParams[0]), Integer.parseInt(twParams[1]));

		//Build topology.
		TweetKafkaSpout spout = new TweetKafkaSpout("twitterStream", args[1]);
		TopologyBuilder builder = new TopologyBuilder();
		
		//Kafka tweet's consumer.
		builder.setSpout(TWEETS_SPOUT,spout.getSpout());
		
		//Language filter.
		builder.setBolt(LANG_FILTER_BOLT, new LangBolt(languages),LANG_FILTER_PARALLELISM)
    		.shuffleGrouping(TWEETS_SPOUT);

		//Hashtag Splitter 
		builder.setBolt(HASHTAG_SPLIT_BOLT, new HashtagSplitBolt(),HASHTAG_SPLIT_PARALLELISM)
        	.shuffleGrouping(LANG_FILTER_BOLT);

		//Time manager 
		builder.setBolt(TIME_MANAGER_BOLT, new TimeWindowManagerBolt())
        	.shuffleGrouping(HASHTAG_SPLIT_BOLT);
		
		//Subrank
		builder.setBolt(SUBRANK_BOLT, new SubRankBolt())
			.fieldsGrouping(TIME_MANAGER_BOLT, new Fields("language","hashtag"));

		/*//Final Rank
		builder.setBolt(FINAL_RANK_BOLT, new FinalRankBolt(),FINAL_RANK_PARALLELISM)
			.globalGrouping(SUBRANK_BOLT, STREAM_SUBRANK_TO_RANK)
			.globalGrouping(TIME_MANAGER_BOLT, STREAM_MANAGER_TO_RANK);
		
		//File loger
		builder.setBolt(FILE_LOGER_BOLT, new FileLogBolt(), FILE_LOGER_PARALLELISM)
			.fieldsGrouping(FINAL_RANK_BOLT, STREAM_RANK_TO_LOGERS, new Fields(RankTupleValues.FIELD_LANGUAGE));
				
		//Printer.
		builder.setBolt("printer", new LogBolt())
			.shuffleGrouping(SUBRANK_BOLT);*/

		//Config topology.
		Config conf = new Config();
		conf.setNumAckers(0);
		LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("test", conf, builder.createTopology());
	    //Set topology life-cycle.
	    Thread.sleep((long) 10000);
	    cluster.killTopology("test");
	    cluster.shutdown();
	}

}
