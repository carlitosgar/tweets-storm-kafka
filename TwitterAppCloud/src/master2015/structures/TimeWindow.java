package master2015.structures;

public class TimeWindow implements Comparable<TimeWindow> {

	private String language;
	private Long timestamp;
	
	public TimeWindow(String language, Long timestamp) {
		this.language = language;
		this.timestamp = timestamp;
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
