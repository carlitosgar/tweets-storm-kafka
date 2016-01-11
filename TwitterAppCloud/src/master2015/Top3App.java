package master2015;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import master2015.bolt.FileLogBolt;
import master2015.bolt.FinalRankBolt;
import master2015.bolt.HashtagSplitBolt;
import master2015.bolt.LangBolt;
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
	
	public static final String STREAM_SUBRANK_TO_RANK = "subranktorank";
	public static final String STREAM_RANK_TO_LOGERS = "ranktologers";
	public static final String STREAM_MANAGER_TO_RANK = "managertorank";
	public static final String STREAM_MANAGER_TO_SUBRANK = "managertosubrank";
	public static final String STREAM_MANAGER_BROADCAST_BLANK = "managerbroadcastblank";
	
	public static final int KAFKA_SPOUT_PARALLELISM = 4; // Parallelism <= N_Partitions.
	public static final int LANG_FILTER_PARALLELISM = 4;
	public static final int HASHTAG_SPLIT_PARALLELISM = 4;
	public static final int TIME_MANAGER_PARALLELISM = 4;
	public static final int SUBRANK_PARALLELISM = 4;
	public static final int FINAL_RANK_PARALLELISM = 1; 
	public static final int FILE_LOGER_PARALLELISM = 1; //Should not be greater than the number of languages
	
	public static String LOG_PATH = "";

	public Top3App() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws InterruptedException {
		
		if(args.length < 5) {
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
		} else {
			languages.add(args[0]);
		}
		
		//Topology name
		String topologyName = args[3];
		
		//Set the log path
		LOG_PATH = (args[4].endsWith(File.separator) ? args[4] : args[4] + File.separator);
		File path = new File(LOG_PATH);
		if(!path.exists() || !path.isDirectory() || !path.canWrite()) {
			System.err.println("Log directory is not writable");
			return;
		}
		
		//Config time window.
		String[] twParams = args[2].split(",");
		TimeWindow.configTimeWindow(Integer.parseInt(twParams[0]), Integer.parseInt(twParams[1]));

		//Build topology.
		TweetKafkaSpout spout = new TweetKafkaSpout("twitterStream", args[1]);
		TopologyBuilder builder = new TopologyBuilder();
		
		//Kafka tweet's consumer.
		builder.setSpout(TWEETS_SPOUT,spout.getSpout(),KAFKA_SPOUT_PARALLELISM);		
		//Language filter.
		builder.setBolt(LANG_FILTER_BOLT, new LangBolt(languages),LANG_FILTER_PARALLELISM)
    		.fieldsGrouping(TWEETS_SPOUT,new Fields("language"));

		//Hashtag Splitter 
		builder.setBolt(HASHTAG_SPLIT_BOLT, new HashtagSplitBolt(),HASHTAG_SPLIT_PARALLELISM)
        	.fieldsGrouping(LANG_FILTER_BOLT,new Fields("language"));

		//Time manager 
		builder.setBolt(TIME_MANAGER_BOLT, new TimeWindowManagerBolt(),TIME_MANAGER_PARALLELISM)
        	.fieldsGrouping(HASHTAG_SPLIT_BOLT, new Fields("language"));
		
		//Subrank
		builder.setBolt(SUBRANK_BOLT, new SubRankBolt(),SUBRANK_PARALLELISM)
			.fieldsGrouping(TIME_MANAGER_BOLT, STREAM_MANAGER_TO_SUBRANK, new Fields("language","hashtag"))
			.allGrouping(TIME_MANAGER_BOLT, STREAM_MANAGER_BROADCAST_BLANK);

		//Final Rank
		builder.setBolt(FINAL_RANK_BOLT, new FinalRankBolt(),FINAL_RANK_PARALLELISM)
			.globalGrouping(SUBRANK_BOLT, STREAM_SUBRANK_TO_RANK)
			.globalGrouping(TIME_MANAGER_BOLT, STREAM_MANAGER_TO_RANK);
		
		//File loger
		builder.setBolt(FILE_LOGER_BOLT, new FileLogBolt(), FILE_LOGER_PARALLELISM)
			.fieldsGrouping(FINAL_RANK_BOLT, STREAM_RANK_TO_LOGERS, new Fields(RankTupleValues.FIELD_LANGUAGE));

		//Config topology.
		Config conf = new Config();
		conf.setNumAckers(0);
		conf.setNumWorkers(2);
		
		System.out.print("Sending topology...");
		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
			System.out.println(" Done!");
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
	    
		/*
		LocalCluster cluster = new LocalCluster();
		System.out.print("Sending topology...");
	    cluster.submitTopology(topologyName, conf, builder.createTopology());
	    System.out.println(" Done!");
	    */
	    
	    //Set topology life-cycle.
	    //Thread.sleep((long)10000);
	    //cluster.killTopology(topologyName);
	    //cluster.shutdown();
	}

}
