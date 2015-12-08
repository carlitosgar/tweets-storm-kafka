package master2015.bolt;

import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class LogBolt implements IBasicBolt{

	private static final ObjectMapper objectMapper = new ObjectMapper();
	private String logID;
	
	public LogBolt(String id){
		this.logID = id;
	}
	
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
		System.out.println("Values: " + tuple.getValues() + " - Fields: "+tuple.getFields() 
		+ " - ID: "+this.logID);
	}

	@Override
	public void prepare(Map map, TopologyContext ctx) {
	}

}
