package master2015.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
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
	private TreeMap<Long, HashMap<String, Queue<Values>>> timeWindowsTuples;
	
	/**
	 * This hashmap contains the list of unique TimeWindows received per window timestamp
	 */
	private HashMap<Long, HashSet<TimeWindow>> uniqueTimeWindows = new HashMap<Long, HashSet<TimeWindow>>();
	
	/**
	 * This hashmap contains the count of tuples per language for the current window
	 */
	private HashMap<TimeWindow, Integer> tuplesCount = new HashMap<TimeWindow, Integer>();
	
	/**
	 * 
	 */
	private HashMap<String, Long> timestampsPerLanguage = new HashMap<String, Long>();
	
	private OutputCollector collector;
	
	public TimeWindowManagerBolt(){
		this.timeWindowsTuples = new TreeMap<Long, HashMap<String, Queue<Values>>>();
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
	}

	@Override
	public void execute(Tuple input) {
		System.out.println(input);
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
		
		for(Entry<String, Long> entry : this.timestampsPerLanguage.entrySet()) {
			TimeWindow tw = new TimeWindow(entry.getKey(), entry.getValue());
			
			// Send blank tuples for the current window
			this.sendBlankTupleForTimeWindow(tw);

			// Send the count of the current window
			this.sendCountOfTimeWindow(tw);
			
			this.removeTuplesOfTimeWindow(tw, null);
			
			
		}
		
		
		//Send all pending tuples and count per each timestamp of the next windows that are still queued
		Iterator<Long> timeIt = this.timeWindowsTuples.keySet().iterator();
		while(timeIt.hasNext()) {
			
			Long time = timeIt.next();
			
			this.sendBlankTuplesForTimestamp(time);
			this.emitTimestampTuples(time);
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
		TimeWindow timeWindow = tws.get(0);
		Long tsWindow = timeWindow.getTimestamp();
		
		Long window = this.timestampsPerLanguage.get(lang);
		
		if (window == null){ // First tuple
			this.timestampsPerLanguage.put(lang, tsWindow);
			window = tsWindow;
			
		} else if (window < tsWindow){// Change of time window
						
			//Send blank tuples for all the languages of the old timestamp
			this.sendBlankTupleForTimeWindow(timeWindow);
			
			// Send totals for all the different TimeWindows of the old timestamp
			this.sendCountOfTimeWindow(timeWindow);
			
			this.removeTuplesOfTimeWindow(timeWindow, null);
			
			// Send tuples from previous timestamps
			NavigableMap<Long, HashMap<String, Queue<Values>>> previous = this.timeWindowsTuples.headMap(tsWindow, false);
			Iterator<Long> timeIt = previous.keySet().iterator();
			while(timeIt.hasNext()) {
				
				Long time = timeIt.next();
				TimeWindow tw = new TimeWindow(lang, time);
				
				// Emit all the queued tuples for the new window
				this.emitTimeWindowTuples(tw);
				
				// Send blank tuples
				this.sendBlankTupleForTimeWindow(tw);
				
				// Send totals for all the different TimeWindows of the old timestamp
				this.sendCountOfTimeWindow(tw);

				//Remove the element from the timeWindowsTuples tree
				this.removeTuplesOfTimeWindow(tw, timeIt);
				
				
			}
			
			// Emit all the queued tuples for the new window
			this.emitTimeWindowTuples(timeWindow);

			// Set the new window
			this.timestampsPerLanguage.put(lang, tsWindow);
			window = tsWindow;
			
		}
		
		// Process all time-windows for tuple.
		for(TimeWindow tw : tws) {
			tsWindow = tw.getTimestamp();
			Values tuple = new Values(lang, ht, tw);
			
			//Increment the tuple count of that TimeWindow
			incrementTimeWindowCount(tw);
			
			if (window.equals(tsWindow)){
				System.out.println("Emit!!! : " + tuple + ", " + tsWindow);
				this.collector.emit(Top3App.STREAM_MANAGER_TO_SUBRANK, tuple);
			} else if(window < tsWindow) {
				this.keepFutureTuple(tsWindow, lang, tuple);
			}
		}
		
	}
	
	
	private void removeTuplesOfTimeWindow(TimeWindow tw, Iterator it) {
		HashMap<String, Queue<Values>> twtuples = this.timeWindowsTuples.get(tw.getTimestamp());
		twtuples.remove(tw.getLanguage());
		if(twtuples.size() == 0) {
			if(it != null) {
				it.remove();
			} else {
				this.timeWindowsTuples.remove(tw.getTimestamp());
			}
			
		}
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
	
	private void sendCountOfTimeWindow(TimeWindow timeWindow) {
		
		Long timestamp = timeWindow.getTimestamp();
		this.sendTimeWindowCount(timeWindow);
		
		Set<TimeWindow> setTimeWindows = this.uniqueTimeWindows.get(timestamp);
		setTimeWindows.remove(timeWindow);
		
		if(setTimeWindows.size() == 0) {
			this.uniqueTimeWindows.remove(timestamp);
		}
		
	}
	
	private void sendBlankTuplesForTimestamp(Long timestamp) {
		for(TimeWindow timeWindow: this.uniqueTimeWindows.get(timestamp)) {
			this.sendBlankTupleForTimeWindow(timeWindow);
		}
	}
	
	private void sendBlankTupleForTimeWindow(TimeWindow timeWindow) {
		System.out.println("Emit Blank!!! : " + timeWindow );
		this.emitBlankTuple(timeWindow);
	}
	
	private void emitBlankTuple(TimeWindow timeWindow) {
		this.collector.emit(Top3App.STREAM_MANAGER_TO_SUBRANK,new Values(timeWindow.getLanguage(),null,timeWindow));
	}
	
	/**
	 * This method sends all the tuples queued for a given window timestamp
	 * @param ts Window timestamp
	 */
	private void emitTimeWindowTuples(TimeWindow tw) {
		HashMap<String, Queue<Values>> queues = this.timeWindowsTuples.get(tw.getTimestamp());
		
		if(queues != null) {
			
			Queue<Values> queue = queues.get(tw.getLanguage());
			
			if (queue != null) {
				
				for(Values values : queue) {
					System.out.println("EmitAll!!! : " + values + ", " + tw);
					this.collector.emit(Top3App.STREAM_MANAGER_TO_SUBRANK,values);
				}
				
				// Free memory
				queue.clear();	
			}
				
		}
	}
	
	private void emitTimestampTuples(Long ts){
		HashMap<String, Queue<Values>> queues = this.timeWindowsTuples.get(ts);
		
		if(queues != null) {
			for(String lang : queues.keySet()) {
			
				this.emitTimeWindowTuples(new TimeWindow(lang, ts));
				
			}
		}
		
	}
	
	private void keepFutureTuple(Long ts, String lang,Values tuple){
		System.out.println("Keep!!! : " + tuple + ", " + ts);
		
		HashMap<String, Queue<Values>> queues = this.timeWindowsTuples.get(ts);
		if(queues == null){
			queues = new HashMap<String, Queue<Values>>();
			this.timeWindowsTuples.put(ts, queues);
		}
		
		Queue<Values> queue = queues.get(lang);
		if(queue == null) {
			queue = new LinkedList<Values>();
			queues.put(lang, queue);
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
