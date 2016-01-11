package master2015.structures.tuple;

import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import master2015.structures.HashtagRankEntry;
import master2015.structures.TimeWindow;

public class RankTupleValues extends Values {

	private static final long serialVersionUID = -274169347158612458L;
	public static final String FIELD_TIME_WINDOW = "timewindow";
	public static final String FIELD_RANK = "rank";
	public static final String FIELD_LANGUAGE = "language";


	/**
	 * Structure to send information from the FinalRank bolt to log bolts.
	 * @param timeWindow The timewindow this rank corresponds to
	 * @param rank List<HashtagRankEntry> object
	 */
	public RankTupleValues(TimeWindow timeWindow, List<HashtagRankEntry> rank) {
		super(timeWindow, rank, timeWindow.getLanguage());
	}
	
	/**
	 * Obtain an object of this class from a tuple.
	 * @param tuple
	 * @return The object. Null if the tuple does not contain a RankTupleValues object.
	 */
	public static RankTupleValues fromTuple(Tuple tuple) {
		System.out.println("El print del asco:" + tuple.getValues());
		if(tuple != null && tuple.getValues() != null && tuple.getValues().size() == 3) {
			List<Object> l = tuple.getValues();
			return new RankTupleValues((TimeWindow)l.get(0), (List<HashtagRankEntry>)l.get(1));
		} else {
			return null;
		}
	}
	
	public TimeWindow getTimeWindow() {
		return (TimeWindow) this.get(0);
	}
	
	@SuppressWarnings("unchecked")
	public List<HashtagRankEntry> getRank() {
		return (List<HashtagRankEntry>) this.get(1);
	}
	
	public String getLanguage() {
		return (String) this.get(2);
	}
	
	/**
	 * Get the fields of the tuple.
	 * @return
	 */
	public static Fields getFields() {
		return new Fields(FIELD_TIME_WINDOW, FIELD_RANK, FIELD_LANGUAGE);
	}
	

}
