package master2015.structures;

public class TimeWindow implements Comparable<TimeWindow> {

	private String language;
	private Long timeWindow;
	
	public TimeWindow(String language, Long timeWindow) {
		this.language = language;
		this.timeWindow = timeWindow;
	}
	
	public String getLanguage() {
		return language;
	}

	public Long getTimeWindow() {
		return timeWindow;
	}

	@Override
	public int compareTo(TimeWindow o) {
		return this.timeWindow.compareTo(o.getTimeWindow());
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(!obj.getClass().equals(this.getClass())) {
			return false;
		}
		
		TimeWindow o = (TimeWindow) obj;
		
		return this.language.equals(o.getLanguage()) && this.timeWindow.equals(o.getTimeWindow());
	}

}
