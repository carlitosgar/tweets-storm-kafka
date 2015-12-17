package master2015.structures;

public class TimeWindow implements Comparable<TimeWindow> {

	private static int size;
	private static int advance;
	private String language;
	private Long timestamp; //Time in seconds
	
	public TimeWindow(String language, Long timestamp) {
		this.language = language;
		this.timestamp = timestamp;
	}
	
	public static void configTimeWindow(int size, int advance){
		TimeWindow.size = size;
		TimeWindow.advance = advance;
	}
	
	public static TimeWindow getTimeWindow(String language, Long timestamp){
		Long currentTs = timestamp / 1000L;
		Long window = (long) TimeWindow.size;
		while(!(currentTs < window)){
			window += TimeWindow.advance;
		}
		return new TimeWindow(language,window);
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
