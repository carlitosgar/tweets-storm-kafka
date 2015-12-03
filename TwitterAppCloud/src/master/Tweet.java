package master;

import java.util.List;

public class Tweet {
	
	private String created_at;
	private List<String> hashtags;
	private String language;

	public Tweet(String created_at, List<String> hashtags, String language) {
		this.created_at = created_at;
		this.hashtags = hashtags;
		this.language = language;
	}
	
	public String getCreated_at() {
		return created_at;
	}
	
	public void setCreated_at(String created_at) {
		this.created_at = created_at;
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
		return this.created_at + " " + this.hashtags.toString();
	}
	
	

}
