package master;

import java.util.List;

public class Tweet {
	
	private Long timestamp;
	private List<String> hashtags;
	private String language;

	public Tweet(String timestamp, List<String> hashtags, String language) {
		this.timestamp = new Long(timestamp);
		this.hashtags = hashtags;
		this.language = language;
	}
	
	public Long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = new Long(timestamp);
	}
	
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	public List<String> getHashtags() {
		return hashtags;
	}
	
	public void setHashtags(List<String> hashtags) {
		this.hashtags = hashtags;
	}
	
	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "[" + this.language + "]" + this.timestamp + " " + this.hashtags.toString();
	}
	
	

}
