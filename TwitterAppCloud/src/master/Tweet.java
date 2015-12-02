package master;

import java.util.List;

public class Tweet {
	
	private String created_at;
	private List<String> hashtags;
	
	
	public Tweet(String created_at, List<String> hashtags) {
		this.created_at = created_at;
		this.hashtags = hashtags;
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
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.created_at + " " + this.hashtags.toString();
	}
	
	

}
