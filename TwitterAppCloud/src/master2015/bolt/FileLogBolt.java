package master2015.bolt;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import master2015.Top3App;
import master2015.structures.HashtagRankEntry;
import master2015.structures.TimeWindow;
import master2015.structures.tuple.RankTupleValues;

public class FileLogBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = -8612023062638735516L;
	private HashMap<String, PrintWriter> writers;
	private String path;
	

	public FileLogBolt(String path) {
		super();
		this.path = path;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.writers = new HashMap<String, PrintWriter>();
		return;
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("[FileLogBolt] Received tuple " + input);
		RankTupleValues tupleVals = RankTupleValues.fromTuple(input);
		
		if(tupleVals != null) {
			
			String lang = tupleVals.getTimeWindow().getLanguage();
			
			PrintWriter writer = writers.get(lang);
			
			if(writer == null) {
				try {
					writer = new PrintWriter(this.path + lang +"_02.log", "UTF-8");
				} catch (FileNotFoundException | UnsupportedEncodingException e) {
					e.printStackTrace();
					return;
				}
				writers.put(lang, writer);
			}
			
			writer.println(getLogTextFromTuple(tupleVals));
			writer.flush();
			
			
		} else {
			System.err.println("Unknown tuple");
		}

	}
	
	/**
	 * This methods generates the log string
	 * @param tupleVals
	 * @return
	 */
	private String getLogTextFromTuple(RankTupleValues tupleVals) {
		Long windowID = (tupleVals.getTimeWindow().getTimestamp()  
				- TimeWindow.getSize() + TimeWindow.getAdvance()) * 1000;
		String s = windowID + "," + tupleVals.getTimeWindow().getLanguage();
		
		List<HashtagRankEntry> rank = tupleVals.getRank();
		
		for(int i = 0; i < Top3App.RANK_NUMBER; ++i) {
			if(i < rank.size()) {
				s += "," + rank.get(i).hashtag + "," + rank.get(i).count;
			} else {
				s += ",null,0";
			}
		}
		
		return s;
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
		
		// Close all the writers
		for(PrintWriter writer : writers.values()) {
			writer.close();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		return;
	}

}
