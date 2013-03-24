package com.gltech.scale.monitoring;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.monitoring.results.AvgStat;
import com.gltech.scale.monitoring.results.GroupStats;
import com.gltech.scale.monitoring.results.OverTime;
import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

public class StatsManagerTest
{
	@Test
	public void testToBytes() throws Exception
	{
		Schema<GroupStats> groupStatsSchema = RuntimeSchema.getSchema(GroupStats.class);

		StatsManager statsManager = new StatsManager();
		AvgStatOverTime addMessageStat = statsManager.createAvgStat("Inbound Service", "Message.Size");
		addMessageStat.add(40);
		addMessageStat.add(160);
		addMessageStat.add(50);

		ByteArrayInputStream in = new ByteArrayInputStream(statsManager.toBytes());

		List<GroupStats> groupStatsList = ProtostuffIOUtil.parseListFrom(in, groupStatsSchema);

		String groupName = "";
		String statName = "";
		long avg = 0;
		long count = 0;

		for(GroupStats groupStats : groupStatsList)
		{
			groupName = groupStats.getName();
			for(OverTime<AvgStat> stat : groupStats.getAvgStats())
			{
				statName = stat.getName();
				avg = stat.getMin1().getAverage();
				count = stat.getMin1().getCount();
			}
		}

		Assert.assertEquals("Inbound Service", groupName);
		Assert.assertEquals("Message.Size", statName);
		Assert.assertEquals(83, avg);
		Assert.assertEquals(3, count);
	}
}
