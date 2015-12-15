package master2015.structures;

public class TimeWindow implements Comparable<TimeWindow> {

	private static int size;
	private static int advance;
	private static final Object setStartTsLock = new Object();
	private static Long startTs;
	private Long lowWindowTs;
	private Long topWindowTs;
	private int window;
	private String language;
	private Long timestamp;
	
	public TimeWindow(String language, Long timestamp) {
		this.setTimeWindow(language,timestamp);
	}
	
	/**
	* Copy constructor.
	* @param other
	*/
	public TimeWindow(TimeWindow other){
		this.language = other.language;
		this.timestamp = other.timestamp;
	}
	
	/**
	 * Defensive copy.
	 * @return itself
	 */
	public TimeWindow copy(){
		return new TimeWindow(this);
	}

	public static void configTimeWindow(int size, int advance){
		TimeWindow.size = size;
		TimeWindow.advance = advance;
	}
	
	private void setTimeWindow(String language,Long timestamp){
		Long tsInSec = timestamp / 1000;
		if (startTs == null){
			TimeWindow.setStartTs(tsInSec);
		}
		if (this.lowWindowTs == null){
			this.lowWindowTs = startTs;
			this.topWindowTs = this.lowWindowTs + size;
		}
		this.language = language;
		this.timestamp = (long) this.window * advance + size;
	}

    private static void setStartTs(Long timestamp) {
        synchronized (setStartTsLock) {
        	startTs = startTs == null ? timestamp : startTs;
        }
    }
	
	public void updateWindow(Long ts){
		this.lowWindowTs += advance;
		this.topWindowTs += advance;
		this.window++;
		this.setTimeWindow(this.language, ts);
	}
	
	public boolean isNewWindow(Long timestamp){
		return (this.topWindowTs > 0) && (timestamp / 1000 > this.topWindowTs);
	}
	
	public String getLanguage() {
		return language;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	@Override
	public int compareTo(TimeWindow o) {
		return this.timestamp.compareTo(o.getTimestamp());
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(!obj.getClass().equals(this.getClass())) {
			return false;
		}
		
		TimeWindow o = (TimeWindow) obj;
		
		return this.language.equals(o.getLanguage()) && this.timestamp.equals(o.getTimestamp());
	}
}
