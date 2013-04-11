package com.gltech.scale.monitoring.model;

import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.stats.StatsManagerImpl;
import junit.framework.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultsIOTest
{
	@Test
	public void testToBytesAndBack() throws Exception
	{
		ResultsIO resultsIO = new ResultsIO();

		ServiceMetaData serviceMetaData = new ServiceMetaData();
		serviceMetaData.setWorkerId(UUID.randomUUID());

		RegistrationService registrationService = mock(RegistrationService.class);
		when(registrationService.getLocalServerMetaData()).thenReturn(serviceMetaData);

		StatsManager statsManager = new StatsManagerImpl(registrationService);
		AvgStatOverTime testStat = statsManager.createAvgStat("testGroup", "Test.Size", "bytes");
		testStat.activateCountStat("Test.Count", "times");
		testStat.add(1004);

		String json = resultsIO.toJson(statsManager.getServerStats());
		byte[] bytes = resultsIO.toBytes(statsManager.getServerStats());
		ServerStats serverStats = resultsIO.toServerStats(bytes);
		Assert.assertEquals(json, resultsIO.toJson(serverStats));
	}
}
