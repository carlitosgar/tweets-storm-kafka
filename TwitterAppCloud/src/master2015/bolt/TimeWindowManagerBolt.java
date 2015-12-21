package master2015.bolt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import master2015.Top3App;
import master2015.structures.TimeWindow;
import master2015.structures.tuple.TotalTupleValues;

public class TimeWindowManagerBolt extends BaseRichBolt{

	private static final long serialVersionUID = -1078494623581520582L;
	private TreeMap<Long, Queue<Values>> timeWindowsTuples;
	
	/**
	 * This hashmap contains the list of unique TimeWindows received per window timestamp
	 */
	private HashMap<Long, HashSet<TimeWindow>> uniqueTimeWindows = new HashMap<Long, HashSet<TimeWindow>>();
	
	/**
	 * This hashmap contains the count of tuples per language for the current window
	 */
	private HashMap<TimeWindow, Integer> tuplesCount = new HashMap<TimeWindow, Integer>();
	
	private OutputCollector collector;
	private Long window;
	
	public TimeWindowManagerBolt(){
		this.timeWindowsTuples = new TreeMap<Long, Queue<Values>>();
		this.window = 0L;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
	}

	@Override
	public void execute(Tuple input) {
		
		if(this.isBlankTuple(input)) {
			processBlankTuple();
		} else {
			processHashtagTuple(input);
		}

	}
	
	private boolean isBlankTuple(Tuple input) {
		return input.getValueByField("language") == null;
	}
	
	private void processBlankTuple() {
		System.out.println("Blank tuple!!");
		
		//Send all pending tuples and count per each timestamp
		Iterator<Long> timeIt = this.timeWindowsTuples.keySet().iterator();
		while(timeIt.hasNext()) {
			
			Long time = timeIt.next();
			
			this.sendBlankTuplesForTimestamp(time);
			this.emitTimeWindowTuples(time);
			this.sendAllCountsOfTimestamp(time);

			//Remove the element from the timeWindowsTuples tree
			timeIt.remove();
			
		}
		
	}
	
	private void processHashtagTuple(Tuple input) {
		
		String lang = (String) input.getValueByField("language");
		String ht = (String) input.getValueByField("hashtag");
		Long timestamp = (Long) input.getValueByField("timestamp");
		
		// Generate possible time windows.
		List<TimeWindow> tws = TimeWindow.getAllTimeWindow(lang, timestamp);
		Long tsWindow = tws.get(0).getTimestamp();
		
		if (this.isFirstWindow()){ // First tuple
			this.window = tsWindow;
			
		} else if (!this.isSameWindow(tsWindow)){ // Change of time window
						
			//Send blank tuples for all the languages of the old timestamp
			this.sendBlankTuplesForTimestamp(this.window);
			
			// Send totals for all the different TimeWindows of the old timestamp
			this.sendAllCountsOfTimestamp(this.window);
			
			// Send tuples from previous timestamps
			NavigableMap<Long, Queue<Values>> previous = this.timeWindowsTuples.headMap(tsWindow, false);
			Iterator<Long> timeIt = previous.keySet().iterator();
			while(timeIt.hasNext()) {
				
				Long time = timeIt.next();
				
				// Emit all the queued tuples for the new window
				this.emitTimeWindowTuples(time);
				
				// Send blank tuples
				this.sendBlankTuplesForTimestamp(time);
				
				// Send totals for all the different TimeWindows of the old timestamp
				this.sendAllCountsOfTimestamp(time);

				//Remove the element from the timeWindowsTuples tree
				timeIt.remove();
				
			}
			
			// Emit all the queued tuples for the new window
			this.emitTimeWindowTuples(tsWindow);

			// Set the new window
			this.window = tsWindow;
			
		}
		
		// Process all time-windows for tuple.
		for(TimeWindow tw : tws) {
			tsWindow = tw.getTimestamp();
			Values tuple = new Values(lang, ht, tw);
			
			//Increment the tuple count of that TimeWindow
			incrementTimeWindowCount(tw);
			
			if (this.isSameWindow(tsWindow)){
				System.out.println("Emit!!! : " + tuple + ", " + tsWindow);
				this.collector.emit(Top3App.STREAM_MANAGER_TO_SUBRANK, tuple);
			} else {
				this.keepFutureTuple(tsWindow, lang, tuple);
			}
		}
		
	}
	
	private boolean isFirstWindow(){
		return this.window.equals(0L);
	}
	
	private boolean isSameWindow(Long ts){
		return ts.equals(this.window);
	}
	
	/**
	 * Note: this method removes the content of uniqueTimeWindows.
	 * @param timestamp
	 */
	private void sendAllCountsOfTimestamp(Long timestamp) {
		for(TimeWindow timeWindow: this.uniqueTimeWindows.get(timestamp)) {
			this.sendTimeWindowCount(timeWindow);
		}
		this.uniqueTimeWindows.remove(timestamp);
	}
	
	private void sendBlankTuplesForTimestamp(Long timestamp) {
		for(TimeWindow timeWindow: this.uniqueTimeWindows.get(timestamp)) {
			System.out.println("Emit Blank!!! : " + timeWindow );
			this.emitBlankTuple(timeWindow);
		}
	}
	
	private void emitBlankTuple(TimeWindow timeWindow) {
		this.collector.emit(Top3App.STREAM_MANAGER_TO_SUBRANK,new Values(timeWindow.getLanguage(),null,timeWindow));
	}
	
	/**
	 * This method sends all the tuples queued for a given window timestamp
	 * @param ts Window timestamp
	 */
	private void emitTimeWindowTuples(Long ts){
		Queue<Values> queue = this.timeWindowsTuples.get(ts);
		if (queue != null) {
			
			for(Values values : queue) {
				System.out.println("EmitAll!!! : " + values + ", " + ts);
				this.collector.emit(Top3App.STREAM_MANAGER_TO_SUBRANK,values);
			}
			
			// Free memory
			queue.clear();
			
		}
	}
	
	private void keepFutureTuple(Long ts, String lang,Values tuple){
		System.out.println("Keep!!! : " + tuple + ", " + ts);
		Queue<Values> queue = this.timeWindowsTuples.get(ts);
		if(queue == null){
			queue = new LinkedList<Values>();
			this.timeWindowsTuples.put(ts, queue);
		}
		queue.add(tuple);
	}
	
	private void incrementTimeWindowCount(TimeWindow tw) {
		Integer count = this.tuplesCount.get(tw);
		
		if(count != null) {
			this.tuplesCount.put(tw, count + 1);
			
		} else { // Is a new TimeWindow
			
			HashSet<TimeWindow> windowSet;
			
			// If it is the first TimeWindow for that timestamp
			if((windowSet = this.uniqueTimeWindows.get(tw.getTimestamp())) == null) {
				windowSet = new HashSet<TimeWindow>();
				this.uniqueTimeWindows.put(tw.getTimestamp(), windowSet);
			}
			
			// Add it to the list of TimeWindows for that timestamp
			if(!windowSet.contains(tw)) {
				windowSet.add(tw);
			}
			
			// Initialize the count
			this.tuplesCount.put(tw, 1);
		}
		
		
	}
	
	private void sendTimeWindowCount(TimeWindow tw) {
		Integer count = this.tuplesCount.remove(tw);
		if(count != null) {
			System.out.println("Sent total count for window " + tw + ": " + count);
			this.collector.emit(Top3App.STREAM_MANAGER_TO_RANK, new TotalTupleValues(tw, count));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Top3App.STREAM_MANAGER_TO_RANK, new Fields("timewindow","count"));
		declarer.declareStream(Top3App.STREAM_MANAGER_TO_SUBRANK, new Fields("language","hashtag","timewindow"));
	}

}
