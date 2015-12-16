package master2015.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import master2015.structures.TimeWindow;

public class TimeWindowTest {
	
	protected String lang;
	protected Long startTs;
	protected TimeWindow tw1;
	protected TimeWindow tw2;
	protected TimeWindow tw3;
	protected TimeWindow twN;
	protected TimeWindow twN1;
	
	@Before
	public void setUp() throws Exception {
		
		lang = "es";
		startTs = 5000L;
		tw1 = new TimeWindow(lang, 10L);
		tw2 = new TimeWindow(lang, 20L);
		tw3 = new TimeWindow(lang, 30L);
		twN = new TimeWindow(lang, 40L);
		twN1 = new TimeWindow(lang, 31L);
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_FirstTs() {
		int advance = 10;
		int size = 10;
		Long startTs = 5000L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, startTs);
		assertTrue(tw1.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_TwoTsSameWindow() {
		int advance = 10;
		int size = 10;
		Long ts1 = 12000L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, startTs);
		assertTrue(tw1.equals(tw));
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
		tw = TimeWindow.getTimeWindow(lang, startTs);
		assertTrue(tw1.equals(tw));
		tw = TimeWindow.getTimeWindow(lang, ts1);
		assertTrue(tw2.equals(tw));
	}
	
	@Test
	public void GetTimeWindow_SameSizeAdvance_TsEqualWindow() {
		int advance = 10;
		int size = 10;
		Long ts1 = 25000L;
		TimeWindow tw;
		TimeWindow.configTimeWindow(size, advance);
		tw = TimeWindow.getTimeWindow(lang, startTs);
		assertTrue(tw1.equals(tw));
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
		tw = TimeWindow.getTimeWindow(lang, startTs);
		assertTrue(tw1.equals(tw));
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
		tw = TimeWindow.getTimeWindow(lang, startTs);
		assertTrue(tw1.equals(tw));
		tw = TimeWindow.getTimeWindow(lang, tsN1);
		assertTrue(twN1.equals(tw));
	}
}
