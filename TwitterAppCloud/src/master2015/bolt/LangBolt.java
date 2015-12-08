package master2015.bolt;

import java.util.List;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class LangBolt extends BaseBasicBolt{
	
	/**
	 * List of languages that define the different streams that generates
	 * this bolt.
	 */
	private List<String> languages;
	
	public LangBolt(List<String> languages){
		this.languages = languages;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(this.languages.contains(input.getValueByField("language"))) {
			collector.emit(input.getStringByField("language"), input.getValues());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(String lang: this.languages){
			declarer.declareStream(lang, new Fields("language","hashtags","timestamp"));
		}
	}

}
