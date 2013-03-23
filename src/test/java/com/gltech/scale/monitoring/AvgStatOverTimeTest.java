package com.gltech.scale.monitoring;

import static junit.framework.Assert.*;

import com.gltech.scale.monitoring.AvgStatOverTime;
import org.joda.time.DateTime;
import org.junit.Test;

public class AvgStatOverTimeTest
{
	@Test
	public void testAverager() throws Exception
	{
		AvgStatOverTime stat = new AvgStatOverTime();
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
