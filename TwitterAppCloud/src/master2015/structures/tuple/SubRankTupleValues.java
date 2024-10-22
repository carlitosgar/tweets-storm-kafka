package master2015.structures.tuple;

import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import master2015.structures.HashtagRankEntry;
import master2015.structures.TimeWindow;

public class SubRankTupleValues extends Values {

	private static final long serialVersionUID = 2352991485250200838L;

	/**
	 * Structure to send information from the SubRank bolt to the FinalRank bolt.
	 * @param timeWindow The timewindow this rank corresponds to
	 * @param rank List<HashtagRankEntry> object
	 * @param contentTweetCount Number of tweets contained inside the rank (it could be calculated in the 
	 * 			destination bolt, but this is added for performance reasons)
	 */
	public SubRankTupleValues(TimeWindow timeWindow, List<HashtagRankEntry> rank, int contentTweetCount) {
		super(timeWindow, rank, contentTweetCount);
	}
	
	/**
	 * Obtain an object of this class from a tuple.
	 * @param tuple
	 * @return The object. Null if the tuple does not contain a SubRankTupleValues object.
	 */
	public static SubRankTupleValues fromTuple(Tuple tuple) {
		if(tuple != null && tuple.getValues() != null && tuple.getValues().size() == 3) {
			List<Object> l = tuple.getValues();
			return new SubRankTupleValues((TimeWindow)l.get(0), (List<HashtagRankEntry>)l.get(1), (int)l.get(2));
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
	
	public int getRankContentTweetCount() {
		return (int) this.get(2);
	}

}
