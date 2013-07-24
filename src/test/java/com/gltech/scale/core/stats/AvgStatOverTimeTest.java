package com.gltech.scale.core.stats;

import static junit.framework.Assert.*;

import org.joda.time.DateTime;
import org.junit.Test;

public class AvgStatOverTimeTest
{
	@Test
	public void testCounter() throws Exception
	{
		AvgStatOverTime stat = new AvgStatOverTime("SIZE", "");
		stat.activateCountStat("Counter", "");
		DateTime dateTime = DateTime.now().minusSeconds(1);

		stat.add(10, dateTime);
		stat.add(40, dateTime);
		stat.add(100, dateTime);

		assertEquals(3, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(3, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(3, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(3, stat.getCountStatOverTime().getCountOverSeconds(5));

		// 4 seconds back
		dateTime = dateTime.minusSeconds(1);

		stat.add(10, dateTime);
		stat.add(40, dateTime);
		stat.add(100, dateTime);

		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));

		// 3 seconds back
		dateTime = dateTime.minusSeconds(4);

		stat.add(10, dateTime);
		stat.add(40, dateTime);
		stat.add(100, dateTime);

		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));

		// 3 seconds back
		dateTime = dateTime.minusSeconds(3);

		stat.add(10, dateTime);
		stat.add(40, dateTime);
		stat.add(100, dateTime);

		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));
		assertEquals(6, stat.getCountStatOverTime().getCountOverSeconds(5));

		assertEquals(12, stat.getCountStatOverTime().getCountOverMinutes(1));
		assertEquals(12, stat.getCountStatOverTime().getCountOverHours(1));
		assertEquals(12, stat.getCountStatOverTime().getCountOverHours(2));
	}

	@Test
	public void testAverager() throws Exception
	{
		AvgStatOverTime stat = new AvgStatOverTime("SIZE", "");
		DateTime dateTime = DateTime.now();

		stat.add(10, dateTime);
		stat.add(40, dateTime);
		stat.add(100, dateTime);

		assertEquals(50, stat.getAvgOverSeconds(1).getAverage());
		assertEquals(50, stat.getAvgOverMinutes(1).getAverage());
		assertEquals(50, stat.getAvgOverHours(1).getAverage());
		assertEquals(3, stat.getAvgOverHours(1).getCount());
		assertEquals(150, stat.getAvgOverHours(1).getTotal());

		// 5 seconds back
		dateTime = dateTime.minusSeconds(5);
		stat.add(125, dateTime);
		stat.add(100, dateTime);
		stat.add(75, dateTime);

		assertEquals(50, stat.getAvgOverSeconds(1).getAverage());
		assertEquals(75, stat.getAvgOverSeconds(5).getAverage());
		assertEquals(75, stat.getAvgOverMinutes(1).getAverage());
		assertEquals(75, stat.getAvgOverHours(1).getAverage());

		// 1 minute back
		dateTime = dateTime.minusMinutes(4);
		stat.add(125, dateTime);
		stat.add(100, dateTime);
		stat.add(75, dateTime);

		assertEquals(50, stat.getAvgOverSeconds(1).getAverage());
		assertEquals(75, stat.getAvgOverSeconds(5).getAverage());
		assertEquals(75, stat.getAvgOverMinutes(1).getAverage());
		assertEquals(83, stat.getAvgOverMinutes(5).getAverage());
		assertEquals(83, stat.getAvgOverHours(1).getAverage());

		assertEquals(9, stat.getAvgOverHours(1).getCount());
		assertEquals(750, stat.getAvgOverHours(1).getTotal());
	}
}
