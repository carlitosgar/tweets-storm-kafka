package master2015.bolt;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import master.structures.Tweet;
import master2015.Top3App;
import master2015.structures.HashtagRank;
import master2015.structures.HashtagRankEntry;
import master2015.structures.TimeWindow;
import master2015.structures.tuple.SubRankTupleValues;

public class SubRankBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3586105017027068572L;

	private TimeWindow tw;
	private OutputCollector collector;
	private HashMap<String, HashtagRankEntry> subRank;
	private int totalTweets;
	//private HashtagRank subRank;
	
	public SubRankBolt(){
		//this.subRank = new HashtagRank();
		this.subRank = new HashMap<String, HashtagRankEntry>();
		this.totalTweets = 0;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timewindow","subrank","count"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String lang = (String) input.getValueByField("language");
		String ht = (String) input.getValueByField("hashtag");
		Long ts = (Long) input.getValueByField("timestamp");
		HashtagRankEntry entry = new HashtagRankEntry(lang, ht, 1);
		
		if(this.tw == null) {
			this.tw = new TimeWindow(lang, ts);
		}
		
		if(this.tw.isNewWindow(ts)){
			this.collector.emit(new SubRankTupleValues(this.tw.copy(), this.getBestN(3), this.totalTweets));
			this.tw.updateWindow(ts);
			this.subRank = new HashMap<String, HashtagRankEntry>(); //Substitute by remove method.
			this.totalTweets = 1;
			this.updateSubRank(entry);
		}
		else{
			this.totalTweets++;
			this.updateSubRank(entry);
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {		
	}
	
	private List<HashtagRankEntry> getBestN(int n) {
		
		List<HashtagRankEntry> entries = new LinkedList<HashtagRankEntry>(this.subRank.values());
		
		// Sort the entries
		Collections.sort(entries, Collections.reverseOrder());
		
		// Return the top n elements
		return entries.subList(0, (n < this.subRank.size() ? n : this.subRank.size()));
		
	}
	
	private void updateSubRank(HashtagRankEntry entry) {
		HashtagRankEntry previous = this.subRank.put(entry.hashtag, entry);
		if(previous != null) {
			entry.count += previous.count;
		}
	}

}
