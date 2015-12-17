package master2015.bolt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

	private OutputCollector collector;
	private HashMap<String, HashtagRank> subRanks;
	private HashMap<String, TimeWindow> timeWindows;
	
	public SubRankBolt(){
		this.subRanks = new HashMap<String, HashtagRank>();
		this.timeWindows = new HashMap<String,TimeWindow>();
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
		HashtagRankEntry newEntry = new HashtagRankEntry(lang, ht, 1);
		TimeWindow tw = TimeWindow.getTimeWindow(lang, ts);
		TimeWindow timeWindow = this.timeWindows.put(lang, tw);
		
		if(timeWindow != null && !timeWindow.equals(tw)){
			this.emitSubRankAndUpdate(lang, timeWindow, newEntry);
		} else {
			this.updateRanking(lang, newEntry);
		}
	}
	
	private void updateRanking(String lang, HashtagRankEntry entry){
		HashtagRank subRank = this.subRanks.get(lang);
		if(subRank == null){
			HashtagRank newSubRank = new HashtagRank();
			newSubRank.add(entry);
			this.subRanks.put(lang, newSubRank);
		} else{
			subRank.add(entry);
		}
	}
	
	private void emitSubRankAndUpdate(String lang, TimeWindow tw, HashtagRankEntry entry){
		HashtagRank subRank = this.subRanks.get(lang);
		SubRankTupleValues tuple = new SubRankTupleValues(
				tw,
				subRank.getBestN(Top3App.RANK_NUMBER),
				this.totalTweetsProcessed(subRank));
		this.collector.emit(tuple);
		subRank.clear();
		this.updateRanking(lang, entry);
	}
	
	private int totalTweetsProcessed(HashtagRank subRank){
		int count = 0;
		Iterator<HashtagRankEntry> it = subRank.getAll().iterator();
		while(it.hasNext()){
			HashtagRankEntry entry = it.next();
			count += entry.count;
		}
		return count;
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {		
	}
}
