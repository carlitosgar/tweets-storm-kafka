package master2015.bolt;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import master2015.structures.HashtagRank;
import master2015.structures.HashtagRankEntry;
import master2015.structures.SubRankTupleValues;
import master2015.structures.TimeWindow;

public class FinalRankBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2801948582197519691L;
	
	/**
	 * SortedMap that contains timeWindows as keys, and the total of tweets in that time window as values.
	 */
	private SortedMap<TimeWindow, Integer> totals = new TreeMap<TimeWindow, Integer>();
	
	/**
	 * SortedMap that contains timeWindows as keys, and the current count of received tweets in 
	 * that time window.
	 */
	private SortedMap<TimeWindow, Integer> counts = new TreeMap<TimeWindow, Integer>();
	
	/**
	 * SortedMap that contains all the ranks that are pending to be logged because not all the SubRanks have
	 * been received
	 */
	private SortedMap<TimeWindow, HashtagRank> pendingRanks = new TreeMap<TimeWindow, HashtagRank>();
	

	public FinalRankBolt() {
		super();
	}

	@Override
	public void execute(Tuple input) {
		
		//TODO: process totals tuples
		//processTotalTuple(Tuple input);
		
		processSubRankTuple(input);

	}
	
	private void processTotalTuple(Tuple input) {
		// TODO
	}
	
	private void processSubRankTuple(Tuple input) {
		
		SubRankTupleValues tupleVals = SubRankTupleValues.fromTuple(input);
		
		// Add to the pendingRank or update its values and send in case all the tweets have been processed
		if(tupleVals != null) {
			
			TimeWindow timeWindow = tupleVals.getTimeWindow();
			List<HashtagRankEntry> subRank = tupleVals.getRank();
			int tweetsInSubRank = tupleVals.getRankContentTweetCount();
			int newCount;
			
			
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
			
			//Set the count value to the number of tweets inside the subrank
			this.counts.put(timeWindow, newCount);
			
			// If all the tweets have been processed, log the time window top and clean
			if(newCount == this.totals.get(timeWindow)) {
				finalizeTimeWindow(timeWindow, rank);				
			}
			
		}
		
	}
	
	private void finalizeTimeWindow(TimeWindow timeWindow, HashtagRank rank) {
		
		// TODO: log
		
		// Clean all the data that does not need to be stored after logging
		this.counts.remove(timeWindow);
		this.totals.remove(timeWindow);
		this.pendingRanks.remove(timeWindow);
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
