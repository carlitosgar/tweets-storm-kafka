package master2015.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import master2015.structures.TimeWindow;

public class TimeWindowManagerBolt extends BaseRichBolt{

	private static final long serialVersionUID = -1078494623581520582L;
	private HashMap<Long, HashMap<String,Queue<Values>>> timeWindowsTuples;
	private OutputCollector collector;
	private Long window;
	
	public TimeWindowManagerBolt(){
		this.timeWindowsTuples = new HashMap<Long, HashMap<String,Queue<Values>>>();
		this.window = 0L;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
	}

	@Override
	public void execute(Tuple input) {
		String lang = (String) input.getValueByField("language");
		String ht = (String) input.getValueByField("hashtag");
		Long timestamp = (Long) input.getValueByField("timestamp");
		
		// Generate possible time windows.
		List<TimeWindow> tws = TimeWindow.getAllTimeWindow(lang, timestamp);
		Long tsWindow = tws.get(0).getTimestamp();
		TimeWindow tw;
		Values tuple;
		if (this.isFirstWindow()){ // First tuple
			this.window = tsWindow;
		} else if (!this.isSameWindow(tsWindow)){
			this.emitTimeWindowTuples(tsWindow,lang);
			this.window = tsWindow;
		}
		// Process all time-windows for tuple.
		Iterator<TimeWindow> it = tws.iterator();
		while(it.hasNext()){
			tw = it.next();
			tsWindow = tw.getTimestamp();
			tuple = new Values(lang,ht,tw);
			if (this.isSameWindow(tsWindow)){
				this.collector.emit(tuple);
			} else {
				this.keepFutureTuple(tsWindow,lang, tuple);
			}
		}	
	}
	
	private boolean isFirstWindow(){
		return this.window.equals(0L);
	}
	
	private boolean isSameWindow(Long ts){
		return ts.equals(this.window);
	}
	
	private void emitTimeWindowTuples(Long ts, String lang){
		HashMap<String,Queue<Values>> langQueues = this.timeWindowsTuples.get(ts);
		if (langQueues != null){
			Queue<Values> queue = langQueues.get(lang);
			while(queue != null && !queue.isEmpty()){
				this.collector.emit(queue.poll());
			}
			langQueues.remove(lang);
			if (langQueues.isEmpty()){
				this.timeWindowsTuples.remove(ts);
			}
		}
	}
	
	private void keepFutureTuple(Long ts, String lang,Values tuple){
		HashMap<String,Queue<Values>> langQueues = this.timeWindowsTuples.get(ts);
		if(langQueues == null){
			langQueues = new HashMap<String,Queue<Values>>();
			this.timeWindowsTuples.put(ts, langQueues);
		}
		Queue<Values> queue = langQueues.get(lang);
		if(queue == null){
			queue = new LinkedList<Values>();
			queue.add(tuple);
			langQueues.put(lang, queue);
		} else {
			queue.add(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("language","hashtag","timewindow"));
	}

}
