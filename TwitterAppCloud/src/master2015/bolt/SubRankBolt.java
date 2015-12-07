package master2015.bolt;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import master.structures.Tweet;
import master2015.Top3App;
import master2015.structures.HashtagRank;
import master2015.structures.HashtagRankEntry;

public class SubRankBolt extends BaseRichBolt {

	private OutputCollector collector;
	private HashtagRank subRank = new HashtagRank();
	//private static final ObjectMapper objectMapper = new ObjectMapper();
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("language"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
	}

	@Override
	public void execute(Tuple input) {
		/*Tweet tweet = null;
		try {
			tweet = objectMapper.readValue(input.getValue(0).toString(), Tweet.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		HashtagRankEntry entry = new HashtagRankEntry(tweet.getLanguage(), tweet.getLanguage(), 0);
		subRank.add(entry);*/
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
