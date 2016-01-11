package master2015.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class LangBolt extends BaseRichBolt implements Serializable{
	
	private static final long serialVersionUID = 9129598163544824838L;
	
	/**
	 * List of languages that define the different streams that generates
	 * this bolt.
	 */
	private List<String> languages;
	private OutputCollector collector;
	
	public LangBolt(List<String> languages){
		this.languages = languages;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		
		if(this.languages.contains(input.getValueByField("language")) || input.getValueByField("language") == null) {
			this.collector.emit(input.getValues());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("language","hashtags","timestamp"));
	}

}
