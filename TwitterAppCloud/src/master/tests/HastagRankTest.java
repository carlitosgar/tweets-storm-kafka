package master.tests;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import master.structures.HashtagRank;
import master.structures.HashtagRankEntry;

public class HastagRankTest {
	
	protected HashtagRankEntry entry0;
	protected HashtagRankEntry entry1;
	protected HashtagRankEntry entry2;
	protected HashtagRankEntry entry3;
	protected HashtagRankEntry entry4;
	protected HashtagRankEntry entry5;
	protected HashtagRankEntry entry6;

	@Before
	public void setUp() throws Exception {
		
		entry0 = new HashtagRankEntry("es", "hashtag1", 10);
		entry1 = new HashtagRankEntry("es", "hashtag2", 20);
		entry2 = new HashtagRankEntry("es", "hashtag3", 21);
		entry3 = new HashtagRankEntry("es", "hashtag4", 22);
		entry4 = new HashtagRankEntry("es", "hashtag1", 5);
		entry5 = new HashtagRankEntry("es", "hashtag2", 7);
		entry6 = new HashtagRankEntry("es", "hashtag2", 3);
		
	}
	
	@Test
	public void AddAlreadyOrderedAndUniqueAndAfew() {
		
		HashtagRank rank = new HashtagRank();
		rank.add(entry3);
		rank.add(entry2);
		rank.add(entry1);
		rank.add(entry0);
		
		List<HashtagRankEntry> bests = rank.getBestN(2);
		
		assertTrue(bests.get(0).equals(new HashtagRankEntry("es", "hashtag4", 22)));
		assertTrue(bests.get(1).equals(new HashtagRankEntry("es", "hashtag3", 21)));
	}

	@Test
	public void AddAlreadyOrderedAndUnique() {
		
		HashtagRank rank = new HashtagRank();
		rank.add(entry3);
		rank.add(entry2);
		rank.add(entry1);
		rank.add(entry0);
		
		List<HashtagRankEntry> bests = rank.getBestN(4);
		
		assertTrue(bests.get(0).equals(new HashtagRankEntry("es", "hashtag4", 22)));
		assertTrue(bests.get(1).equals(new HashtagRankEntry("es", "hashtag3", 21)));
		assertTrue(bests.get(2).equals(new HashtagRankEntry("es", "hashtag2", 20)));
		assertTrue(bests.get(3).equals(new HashtagRankEntry("es", "hashtag1", 10)));
	}
	
	@Test
	public void AddShuffledAndUnique() {
		
		HashtagRank rank = new HashtagRank();
		rank.add(entry1);
		rank.add(entry0);
		rank.add(entry3);
		rank.add(entry2);
		
		List<HashtagRankEntry> bests = rank.getBestN(4);
		
		assertTrue(bests.get(0).equals(new HashtagRankEntry("es", "hashtag4", 22)));
		assertTrue(bests.get(1).equals(new HashtagRankEntry("es", "hashtag3", 21)));
		assertTrue(bests.get(2).equals(new HashtagRankEntry("es", "hashtag2", 20)));
		assertTrue(bests.get(3).equals(new HashtagRankEntry("es", "hashtag1", 10)));
	}
	
	@Test
	public void AddShuffledAndRepeated() {
		
		HashtagRank rank = new HashtagRank();
		rank.add(entry1);
		rank.add(entry0);
		rank.add(entry3);
		rank.add(entry2);
		rank.add(entry6);
		rank.add(entry4);
		rank.add(entry5);
		
		List<HashtagRankEntry> bests = rank.getBestN(4);
		
		assertTrue(bests.get(0).equals(new HashtagRankEntry("es", "hashtag2", 30)));
		assertTrue(bests.get(1).equals(new HashtagRankEntry("es", "hashtag4", 22)));
		assertTrue(bests.get(2).equals(new HashtagRankEntry("es", "hashtag3", 21)));
		assertTrue(bests.get(3).equals(new HashtagRankEntry("es", "hashtag1", 15)));
	}

}
