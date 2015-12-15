package master2015.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagSplitBolt extends BaseRichBolt{

	private static final long serialVersionUID = -2842847942631968729L;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		List<String> hashtags = (List<String>) input.getValueByField("hashtags");
		for(String ht : hashtags){
			collector.emit(new Values(
					input.getValueByField("language"),
					ht,
					input.getValueByField("timestamp")));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("language","hashtag","timestamp"));	
	}

}
