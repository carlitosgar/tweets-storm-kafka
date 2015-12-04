package master.structures;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class HashtagRank {
	
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
		
		// Sort the entries
		Collections.sort(entries, Collections.reverseOrder());
		
		// Return the top n elements
		return entries.subList(0, (n < this.size() ? n : this.size()));
		
	}
	
	public int size() {
		return this.hashMap.size();
	}
	
	

}
