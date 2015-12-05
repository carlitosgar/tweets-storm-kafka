package master2015.structures.tuple;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import master2015.structures.TimeWindow;

public class TotalTupleValues extends Values {
	
	private static final long serialVersionUID = -4129638758188108878L;

	public static final String FIELD_TIME_WINDOW = "timewindow";
	public static final String FIELD_TOTAL = "total";

	public TotalTupleValues(TimeWindow timeWindow, Integer total) {
		super(timeWindow, total);
	}

	/**
	 * Obtain an object of this class from a tuple.
	 * @param tuple
	 * @return The object. Null if the tuple does not contain an object of this class.
	 */
	public static TotalTupleValues fromTuple(Tuple tuple) {
		if(tuple.getValues() instanceof TotalTupleValues) {
			return (TotalTupleValues) tuple.getValues();
		} else {
			return null;
		}
	}
	
	public TimeWindow getTimeWindow() {
		return (TimeWindow) this.get(0);
	}
	
	public Integer getTotal() {
		return (Integer) this.get(1);
	}
	
	/**
	 * Get the fields of the tuple.
	 * @return
	 */
	public static Fields getFields() {
		return new Fields(FIELD_TIME_WINDOW, FIELD_TOTAL);
	}

}
