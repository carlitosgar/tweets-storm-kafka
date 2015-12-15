package master2015.bolt;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import master2015.structures.HashtagRankEntry;
import master2015.structures.TimeWindow;
import master2015.structures.tuple.SubRankTupleValues;

public class LogBolt implements IBasicBolt{

	private static final ObjectMapper objectMapper = new ObjectMapper();
	private int logID;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {	
		SubRankTupleValues tupleVals = SubRankTupleValues.fromTuple(tuple);
		TimeWindow timeWindow = tupleVals.getTimeWindow();
		List<HashtagRankEntry> subRank = tupleVals.getRank();
		int i = 1;
		System.out.println("TimeWindow: " + timeWindow.getLanguage() + " - " + timeWindow.getTimestamp()
		+ " - " + tupleVals.getRankContentTweetCount());
		for(HashtagRankEntry entry : subRank){
			System.out.println("Top "+i+": "+entry.hashtag+ " "+entry.count+ " "+entry.language);
			i++;
		}
		//System.out.println(timeWindow.getTimestamp());
	}

	@Override
	public void prepare(Map map, TopologyContext ctx) {
		this.logID = ctx.getThisTaskId();
	}

}
