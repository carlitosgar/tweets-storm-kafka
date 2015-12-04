package master.structures;

public class HashtagRankEntry implements Comparable<HashtagRankEntry>{
	
	public String language;
	public String hashtag;
	public int count;
	
	
	public HashtagRankEntry(String language, String hashtag, int count) {
		this.language = language;
		this.hashtag = hashtag;
		this.count = count;
	}

	@Override
	public int compareTo(HashtagRankEntry o) {
		if(this.count != o.count) {
			return this.count - o.count;
		} else {
			return this.hashtag.compareTo(o.hashtag);
		}
	}

}
