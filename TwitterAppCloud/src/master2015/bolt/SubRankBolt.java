package master2015.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
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
		declarer.declareStream(Top3App.STREAM_SUBRANK_TO_RANK, new Fields("timewindow","subrank","count"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String lang = (String) input.getValueByField("language");
		TimeWindow tw = (TimeWindow) input.getValueByField("timewindow");
		String ht = (String) input.getValueByField("hashtag");;
		HashtagRankEntry newEntry;
		//Check blank tuple.
		if(ht == null){
			this.emitSubRank(lang);
		} else {
			newEntry = new HashtagRankEntry(lang, ht, 1);
			this.timeWindows.put(lang, tw);
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
	
	private void emitSubRank(String lang){
		HashtagRank subRank = this.subRanks.get(lang);
		TimeWindow tw = this.timeWindows.get(lang);
		SubRankTupleValues tuple = new SubRankTupleValues(
				tw,
				subRank.getBestN(Top3App.RANK_NUMBER),
				this.totalTweetsProcessed(subRank));
		System.out.println(tw.getLanguage()+", "+tw.getTimestamp());
		System.out.println(tuple);
		this.collector.emit(Top3App.STREAM_SUBRANK_TO_RANK, tuple);
		subRank.clear();
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
