package master.structures;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class FinalRank {
	
	private HashMap<String, HashtagRankEntry> hashMap;

	public FinalRank() {
		
		this.hashMap = new HashMap<String, HashtagRankEntry>();
	}
	
	/**
	 * Adds an entry. Is the entry already exists, increments its count.
	 * @param entry
	 */
	public void add(HashtagRankEntry entry) {
		
		HashtagRankEntry previous = this.hashMap.put(entry.hashtag, entry);
		
		if(previous != null) {
			entry.count += previous.count;
		}
		
	}
	
	/**
	 * Gets a list with the top n elements.
	 * @param n
	 * @return
	 */
	public List<HashtagRankEntry> getBestN(int n) {
		
		List<HashtagRankEntry> entries = new LinkedList<HashtagRankEntry>(this.hashMap.values());
		
		// Sort the entries
		Collections.sort(entries);
		
		// Return the top n elements
		return entries.subList(0, n);
		
	}
	
	

}
