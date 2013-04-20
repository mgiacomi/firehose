package com.gltech.scale.core.stats;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.stats.results.*;
import com.gltech.scale.monitoring.model.ResultsIO;
import com.gltech.scale.monitoring.model.ServerStats;
import junit.framework.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class StatsManagerTest
{
	@Test
	public void testToBytes() throws Exception
	{
		ServiceMetaData serviceMetaData = new ServiceMetaData();
		serviceMetaData.setWorkerId(UUID.randomUUID());

		RegistrationService registrationService = mock(RegistrationService.class);
		when(registrationService.getLocalServerMetaData()).thenReturn(serviceMetaData);

		ResultsIO resultsIO = new ResultsIO();

		StatsManager statsManager = new StatsManagerImpl(registrationService);
		AvgStatOverTime addMessageStat = statsManager.createAvgStat("Inbound Service", "Message.Size", "");
		addMessageStat.add(40);
		addMessageStat.add(160);
		addMessageStat.add(50);

		byte[] bytes = resultsIO.toBytes(statsManager.getServerStats());
		ServerStats serverStats = resultsIO.toServerStats(bytes);

		String groupName = "";
		String statName = "";
		long avg = 0;
		long count = 0;

		for (GroupStats groupStats : serverStats.getGroupStatsList())
		{
			groupName = groupStats.getName();
			for (OverTime<AvgStat> stat : groupStats.getAvgStats())
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
