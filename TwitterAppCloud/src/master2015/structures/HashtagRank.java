package master2015.structures;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class HashtagRank implements Serializable{
	
	private static final long serialVersionUID = 1103609228922852174L;
	private HashMap<String, HashtagRankEntry> hashMap;

	public HashtagRank() {
		this.hashMap = new HashMap<String, HashtagRankEntry>();
	}
	
	/**
	 * Adds an entry. Is the entry already exists, increments its count.
	 * Carefull, it modifies the entries!!
	 * @param entry
	 */
	public void add(HashtagRankEntry entry) {
		HashtagRankEntry previous = this.hashMap.put(entry.hashtag, entry);
		
		if(previous != null) {
			entry.count += previous.count;
		}
		
	}
	
	/**
	 * Gets an ordered list with the top n elements.
	 * @param n Maximum of top elements to return. If n >= size, then all the elements will be returned.
	 * @return Ordered list with the top n elements
	 */
	public List<HashtagRankEntry> getBestN(int n) {
		
		List<HashtagRankEntry> entries = new LinkedList<HashtagRankEntry>(this.hashMap.values());
		List<HashtagRankEntry> ret = new LinkedList<HashtagRankEntry>();
		// Sort the entries
		Collections.sort(entries);
		
		// Return the top n elements
		for(HashtagRankEntry ent : entries.subList(0, (n < this.size() ? n : this.size()))) {
			ret.add(ent);
		}
		
		return ret;
		
	}
	
	/**
	 * Gets an ordered list with all the elements.
	 * @return
	 */
	public List<HashtagRankEntry> getAll() {
		return this.getBestN(this.size());
	}
	
	/**
	 * Clear the ranking.
	 */
	public void clear(){
		this.hashMap.clear();
	}
	
	public int size() {
		return this.hashMap.size();
	}
	
	

}
