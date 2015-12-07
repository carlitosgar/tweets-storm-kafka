package master2015.spout;

import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class LogBolt implements IBasicBolt{

	private static final ObjectMapper objectMapper = new ObjectMapper();
	
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

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("Values: " + tuple.getValues() + ", Fields: "+tuple.getFields());
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {		
	}

}
