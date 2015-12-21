package master2015.tests;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import master2015.structures.TimeWindow;

public class TimeWindowTest {
	
	protected String lang;
	protected TimeWindow tw1;
	protected TimeWindow tw2;
	protected TimeWindow tw3;
	protected TimeWindow twN;
	protected TimeWindow twN1;
	protected TimeWindow tws1;
	protected TimeWindow tws2;
	protected List<TimeWindow> tws;
	protected List<TimeWindow> twsA;
	
	@Before
	public void setUp() throws Exception {
		
		lang = "es";
		tw1 = new TimeWindow(lang, 10L);
		tw2 = new TimeWindow(lang, 20L);
		tw3 = new TimeWindow(lang, 30L);
		twN = new TimeWindow(lang, 40L);
		twN1 = new TimeWindow(lang, 37L);
		tws1 = new TimeWindow(lang, 5L);
		tws2 = new TimeWindow(lang, 8L);
		tws = new ArrayList<TimeWindow>();
		tws.add(tws1);
		tws.add(tws2);
		twsA = new ArrayList<TimeWindow>();
		twsA.add(tws2);
	}
	
	/*@Test
	public void getFirstTimeWindow0() {
		
		int advance = 3;
		int size = 5;
		Long ts = 4000L;
		Long twFinal = 5L;
		Long tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getFirstTimeWindowTime(ts);
		assertTrue(twFinal.equals(tw));		
	}*/
	
	@Test
	public void TimeWindow_equals() {
		TimeWindow tw1 = new TimeWindow("es", 1450526000L);
		TimeWindow tw2 = new TimeWindow("es", 1450526000L);
		assertEquals(tw1, tw2);
	}
	
	@Test
	public void TimeWindow_not_equals() {
		TimeWindow tw1 = new TimeWindow("es", 1450526000L);
		TimeWindow tw2 = new TimeWindow("en", 1450526000L);
		assertNotEquals(tw1, tw2);
	}
	
	@Test
	public void TimeWindow_should_retrieve_from_hashmap() {
		TimeWindow tw1 = new TimeWindow("es", 1450526000L);
		HashMap<TimeWindow, Integer> m = new HashMap<TimeWindow, Integer>();
		m.put(tw1, 1);
		assertTrue(m.containsKey(tw1));
		assertNotNull(m.get(tw1));
	}
	
	@Test
	public void TimeWindow_should_not_retrieve_from_hashmap() {
		TimeWindow tw1 = new TimeWindow("es", 1450526000L);
		TimeWindow tw2 = new TimeWindow("en", 1450526000L);
		HashMap<TimeWindow, Integer> m = new HashMap<TimeWindow, Integer>();
		m.put(tw1, 1);
		assertFalse(m.containsKey(tw2));
		assertNull(m.get(tw2));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_FirstTs() {
		int advance = 10;
		int size = 10;
		Long ts = 5L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, ts);
		assertTrue(tw1.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_TwoTsSameWindow() {
		int advance = 10;
		int size = 10;
		Long ts1 = 8000L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, ts1);
		assertTrue(tw1.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_TwoTsDiffWindow() {
		int advance = 10;
		int size = 10;
		Long ts1 = 16000L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, ts1);
		assertTrue(tw2.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_TsEqualWindow() {
		int advance = 10;
		int size = 10;
		Long ts1 = 20000L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, ts1);
		assertTrue(tw3.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_AdvancedWindow() {
		int advance = 10;
		int size = 10;
		Long tsN = 35777L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, tsN);
		assertTrue(twN.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_DiffSizeAdvance_FirstBelongingWindow() {
		int advance = 3;
		int size = 10;
		Long tsN1 = 35777L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, tsN1);
		assertTrue(twN1.equals(tw));
	}
	
	@Test
	public void GetAllTimeWindow() {
		int advance = 3;
		int size = 5;
		Long ts = 4000L;
		List<TimeWindow> tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getAllTimeWindow(lang, ts);
		assertTrue(tw.size() == tws.size());
		for (int i=0; i<tw.size(); i++){
			assertTrue(tw.get(i).equals(tws.get(i)));
		}
	}
	
	@Test
	public void GetAllTimeWindow_TsOnWindowLimit() {
		int advance = 3;
		int size = 5;
		Long ts = 3000L;
		List<TimeWindow> tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getAllTimeWindow(lang, ts);
		assertTrue(tw.size() == tws.size());
		for (int i=0; i<tw.size(); i++){
			assertTrue(tw.get(i).equals(tws.get(i)));
		}
	}
	
	@Test
	public void GetAllTimeWindow_TsInAdvancesGap() {
		int advance = 3;
		int size = 5;
		Long ts = 5999L;
		List<TimeWindow> tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getAllTimeWindow(lang, ts);
		assertTrue(tw.size() == twsA.size());
		for (int i=0; i<tw.size(); i++){
			assertTrue(tw.get(i).equals(twsA.get(i)));
		}
	}
	
	@Test
	public void getFirstTimeWindowTime_random_same() {
		
		int advance = 10;
		int size = 10;
		TimeWindow.configTimeWindow(size, advance);
		
		testRandomTimestamps(advance, size, 100, 14503);
		
	}
	
	@Test
	public void getFirstTimeWindowTime_random_multiples() {
		
		int size = 10;
		int advance = 5;
		
		TimeWindow.configTimeWindow(size, advance);
		
		testRandomTimestamps(advance, size, 100, 14503);
		
	}
	
	@Test
	public void getFirstTimeWindowTime_random_not_multiples() {
		
		int size = 97;
		int advance = 13;
		
		TimeWindow.configTimeWindow(size, advance);
		
		testRandomTimestamps(advance, size, 100, 145037);
		
	}
	
	@Test
	public void getFirstTimeWindowTime_random_random() {
		
		int size, advance;
		for(int x = 0; x < 1000; ++x) {
			
			size = (int) (Math.random() * 100000);
			advance = (int) (Math.random() * size);
			
			TimeWindow.configTimeWindow(size, advance);
			
			testRandomTimestamps(advance, size, 100, 1450379);
		}
		
	}
	
	@Test
	public void getFirstTimeWindowTime_linear() {
		
		int size, advance;
		for(size = 1; size < 300; ++size) {
			for(advance = 1; advance <= size; advance++) {
				
				TimeWindow.configTimeWindow(size, advance);
				testRandomTimestamps(advance, size, 100, 30000);
				
			}
		}
	
	}
	
	@Test
	public void getFirstTimeWindowTime_linear_primes() {
		
		int size, advance;
		int total = 25;
		int[] primes = getNPrimeNumbers(total);
		for(int x = 1; x < total; ++x) {
			for(int y = 1; y <= x; y++) {
				
				size = primes[x];
				advance = primes[y];

				TimeWindow.configTimeWindow(size, advance);
				testRandomTimestamps(advance, size, 100, 1000000);
				
			}
		}
	
	}
	
	@Test(timeout=20)
	public void getFirstTimeWindowTime_performance() {
		
		int size = 97;
		int advance = 13;
		
		TimeWindow.configTimeWindow(size, advance);
		TimeWindow.getFirstTimeWindowTime(7094901566810L);
		
	}
	
	private int[] getNPrimeNumbers(int n) {
		int[] nums = new int[n];
		// loop through the numbers one by one
		
		for (int i = 1, x = 0; x<n; i++) {
	
			boolean isPrimeNumber = true;
	
			// check to see if the number is prime
			for (int j = 2; j < i; j++) {
				if (i % j == 0) {
					isPrimeNumber = false;
					break; // exit the inner for loop
				}
			}
			
			if (isPrimeNumber) {
				nums[x++] = i;
			}
		}
		
		return nums;
	}
	
	private void testRandomTimestamps(int advance, int size, int iterations, long maxTimestamp) {
		
		for(int x = 0; x < iterations; ++x) {
			Long timestamp = (long) (Math.random() * maxTimestamp); //1450379828
			Long window = (long) size;
			while(!(timestamp < window)){
				window += advance;
			}
			
			//Compare
			assertEquals("Failed for timestamp=" + timestamp + ", size=" + size + ", adv=" + advance, 
					window, TimeWindow.getFirstTimeWindowTime(timestamp));
		}
		
	}
}
