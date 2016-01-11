package master2015.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import master2015.Top3App;
import master2015.structures.HashtagRank;
import master2015.structures.HashtagRankEntry;
import master2015.structures.TimeWindow;
import master2015.structures.tuple.RankTupleValues;
import master2015.structures.tuple.SubRankTupleValues;
import master2015.structures.tuple.TotalTupleValues;

public class FinalRankBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2801948582197519691L;
	
	private OutputCollector collector;
	
	/**
	 * HashMap that contains timeWindows as keys, and the total of tweets in that time window as values.
	 */
	private HashMap<TimeWindow, Integer> totals;
	
	/**
	 * HashMap that contains timeWindows as keys, and the current count of received tweets in 
	 * that time window.
	 */
	private HashMap<TimeWindow, Integer> counts;
	
	/**
	 * HashMap that contains all the ranks that are pending to be logged because not all the SubRanks have
	 * been received
	 */
	private HashMap<TimeWindow, HashtagRank> pendingRanks;
	

	public FinalRankBolt() {
		super();
	}

	@Override
	public void execute(Tuple input) {
		
		switch(input.getSourceStreamId()) {
		
		case Top3App.STREAM_MANAGER_TO_RANK:
			processTotalTuple(input);
			break;
			
		case Top3App.STREAM_SUBRANK_TO_RANK:
			processSubRankTuple(input);
			break;
			
		default:
			System.err.println("Unknown tuple");
			break;
		
		}

	}
	
	private void processTotalTuple(Tuple input) {
		
		TotalTupleValues tupleVals = TotalTupleValues.fromTuple(input);
		
		if(tupleVals != null) {
			
			TimeWindow timeWindow = tupleVals.getTimeWindow();
			Integer total = tupleVals.getTotal();
			System.out.println("Received total count for window " + timeWindow + ": " + total);
			// Set the total of tweets in that time window
			this.totals.put(timeWindow, total);
			
			// If all the tweets have been processed, finalize
			if(this.counts.containsKey(timeWindow) && this.counts.get(timeWindow).equals(total)) {
				finalizeTimeWindow(timeWindow, this.pendingRanks.get(timeWindow));
			}
		}
		
	}
	
	private void processSubRankTuple(Tuple input) {
		
		SubRankTupleValues tupleVals = SubRankTupleValues.fromTuple(input);
		
		// Add to the pendingRank or update its values and send in case all the tweets have been processed
		if(tupleVals != null) {
			
			TimeWindow timeWindow = tupleVals.getTimeWindow();
			List<HashtagRankEntry> subRank = tupleVals.getRank();
			int tweetsInSubRank = tupleVals.getRankContentTweetCount();
			int newCount;
			System.out.println("[FinalRank] Received tuple " + timeWindow + " with count " + tweetsInSubRank);
			
			HashtagRank rank = this.pendingRanks.get(timeWindow);
			
			if(rank != null) { //Updating
				
				//Update the count of tweets for the time window
				newCount = this.counts.get(timeWindow) + tweetsInSubRank;
				
				
			} else { //Adding for first time
				
				rank = new HashtagRank();
				
				//Add the subrank to the final rank
				this.pendingRanks.put(timeWindow, rank);
				
				//Set the count value to the number of tweets inside the subrank
				newCount = tweetsInSubRank;
			}
			
			//Update the final rank with the entries of the subrank
			for(HashtagRankEntry entry : subRank) {
				rank.add(entry);
			}
			System.out.println("[FinalRank] Count: " + newCount);
			//Set the count value to the number of tweets inside the subrank
			this.counts.put(timeWindow, newCount);
			
			// If all the tweets have been processed, log the time window top and clean
			if(this.totals.containsKey(timeWindow) && this.totals.get(timeWindow).equals(newCount)) {
				finalizeTimeWindow(timeWindow, rank);				
			}
			
		}
		
	}
	
	private void finalizeTimeWindow(TimeWindow timeWindow, HashtagRank rank) {
		
		// Emit to logers
		this.collector.emit(Top3App.STREAM_RANK_TO_LOGERS, 
				new RankTupleValues(timeWindow, rank.getBestN(Top3App.RANK_NUMBER)));
		
		// Clean all the data that does not need to be stored after logging
		this.counts.remove(timeWindow);
		this.totals.remove(timeWindow);
		this.pendingRanks.remove(timeWindow);
		System.out.println("[FinalRank] Finalized window " + timeWindow);
	}

	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.totals = new HashMap<TimeWindow, Integer>();
		this.counts = new HashMap<TimeWindow, Integer>();
		this.pendingRanks = new HashMap<TimeWindow, HashtagRank>();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Top3App.STREAM_RANK_TO_LOGERS, RankTupleValues.getFields());
	}

}
