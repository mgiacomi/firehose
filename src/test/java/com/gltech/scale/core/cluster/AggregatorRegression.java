package com.gltech.scale.core.cluster;

import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.aggregator.PrimaryBackupSet;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class AggregatorRegression
{
	private TestingServer testingServer;
	static private Props props;

	@Before
	public void setUp() throws Exception
	{
		testingServer = new TestingServer(21818);
	}

	@After
	public void tearDown() throws Exception
	{
		testingServer.stop();
	}

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");
	}

	@Test
	public void testAggregatorAssignmentOverTime() throws Exception
	{
		TimePeriodUtils timePeriodUtils = new TimePeriodUtils();

		ServiceMetaData rm0 = new ServiceMetaData(UUID.randomUUID(), "0.0.0.0", 0);
		ServiceMetaData rm1 = new ServiceMetaData(UUID.randomUUID(), "1.1.1.1", 1);
		ServiceMetaData rm2 = new ServiceMetaData(UUID.randomUUID(), "2.2.2.2", 2);
		ServiceMetaData rm3 = new ServiceMetaData(UUID.randomUUID(), "3.3.3.3", 3);
		ServiceMetaData rm4 = new ServiceMetaData(UUID.randomUUID(), "4.4.4.4", 4);
		ServiceMetaData rm5 = new ServiceMetaData(UUID.randomUUID(), "5.5.5.5", 5);
		ServiceMetaData rm6 = new ServiceMetaData(UUID.randomUUID(), "6.6.6.6", 6);
		ServiceMetaData rm7 = new ServiceMetaData(UUID.randomUUID(), "7.7.7.7", 7);
		ServiceMetaData rm8 = new ServiceMetaData(UUID.randomUUID(), "8.8.8.8", 8);
		ServiceMetaData rm9 = new ServiceMetaData(UUID.randomUUID(), "9.9.9.9", 9);

		RegistrationService registrationService = mock(RegistrationService.class);
		when(registrationService.getAggregatorMetaDataById(rm0.getWorkerId().toString())).thenReturn(rm0);
		when(registrationService.getAggregatorMetaDataById(rm1.getWorkerId().toString())).thenReturn(rm1);
		when(registrationService.getAggregatorMetaDataById(rm2.getWorkerId().toString())).thenReturn(rm2);
		when(registrationService.getAggregatorMetaDataById(rm3.getWorkerId().toString())).thenReturn(rm3);
		when(registrationService.getAggregatorMetaDataById(rm4.getWorkerId().toString())).thenReturn(rm4);
		when(registrationService.getAggregatorMetaDataById(rm5.getWorkerId().toString())).thenReturn(rm5);
		when(registrationService.getAggregatorMetaDataById(rm6.getWorkerId().toString())).thenReturn(rm6);
		when(registrationService.getAggregatorMetaDataById(rm7.getWorkerId().toString())).thenReturn(rm7);
		when(registrationService.getAggregatorMetaDataById(rm8.getWorkerId().toString())).thenReturn(rm8);
		when(registrationService.getAggregatorMetaDataById(rm9.getWorkerId().toString())).thenReturn(rm9);

		ChannelCoordinator channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());

		FakeAggregator frm0 = new FakeAggregator(rm0);
		FakeAggregator frm1 = new FakeAggregator(rm1);
		FakeAggregator frm2 = new FakeAggregator(rm2);
		FakeAggregator frm3 = new FakeAggregator(rm3);
		FakeAggregator frm4 = new FakeAggregator(rm4);
		FakeAggregator frm5 = new FakeAggregator(rm5);
		FakeAggregator frm6 = new FakeAggregator(rm6);
		FakeAggregator frm7 = new FakeAggregator(rm7);
		FakeAggregator frm8 = new FakeAggregator(rm8);
		FakeAggregator frm9 = new FakeAggregator(rm9);

		Map<String, FakeAggregator> idToAggregator = new HashMap<>();
		idToAggregator.put(rm0.getWorkerId().toString(), frm0);
		idToAggregator.put(rm1.getWorkerId().toString(), frm1);
		idToAggregator.put(rm2.getWorkerId().toString(), frm2);
		idToAggregator.put(rm3.getWorkerId().toString(), frm3);
		idToAggregator.put(rm4.getWorkerId().toString(), frm4);
		idToAggregator.put(rm5.getWorkerId().toString(), frm5);
		idToAggregator.put(rm6.getWorkerId().toString(), frm6);
		idToAggregator.put(rm7.getWorkerId().toString(), frm7);
		idToAggregator.put(rm8.getWorkerId().toString(), frm8);
		idToAggregator.put(rm9.getWorkerId().toString(), frm9);

		new Thread(frm0).start();
		new Thread(frm1).start();
		new Thread(frm2).start();
		new Thread(frm3).start();
		new Thread(frm4).start();
		new Thread(frm5).start();
		new Thread(frm6).start();
		new Thread(frm7).start();
		new Thread(frm8).start();
		new Thread(frm9).start();

		while (true)
		{
			AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now());
			if (aggregatorsByPeriod != null)
			{
				for (PrimaryBackupSet primaryBackupSet : aggregatorsByPeriod.getPrimaryBackupSets())
				{
					FakeAggregator primary = idToAggregator.get(primaryBackupSet.getPrimary().getWorkerId().toString());
					primary.assign(true, timePeriodUtils.nearestPeriodCeiling(DateTime.now()));

					if (primaryBackupSet.getBackup() != null)
					{
						FakeAggregator backup = idToAggregator.get(primaryBackupSet.getBackup().getWorkerId().toString());
						backup.assign(false, timePeriodUtils.nearestPeriodCeiling(DateTime.now()));
					}
				}
			}
//			frm0.assign(false, ZookeeperCoordinationService.nearestPeriodCeiling(DateTime.now(), 5));

			Thread.sleep(1000);
		}

	}

	class FakeAggregator implements Runnable
	{
		private DateTime nearestPeriodCeiling;
		private ChannelCoordinator channelCoordinator;
		private boolean primary;

		FakeAggregator(ServiceMetaData serviceMetaData)
		{
			RegistrationService registrationService = mock(RegistrationService.class);
			channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());
			when(registrationService.getLocalServerMetaData()).thenReturn(serviceMetaData);
		}

		public void assign(boolean primary, DateTime nearestPeriodCeiling)
		{
			this.primary = primary;
			this.nearestPeriodCeiling = nearestPeriodCeiling;
		}

		@Override
		public void run()
		{
			try
			{
				channelCoordinator.registerWeight(false, 0, 0, 0);

				int tick = 1;
				while (true)
				{
					if (nearestPeriodCeiling != null && DateTime.now().isAfter(nearestPeriodCeiling.minusSeconds(5)))
					{
						if (DateTime.now().isAfter(nearestPeriodCeiling.plusSeconds(3)))
						{
							channelCoordinator.registerWeight(false, 0, 0, 999 - (tick++));
						}
						else if (DateTime.now().isAfter(nearestPeriodCeiling))
						{
							if (primary)
							{
								channelCoordinator.registerWeight(false, 3, 2, 999);
							}
							else
							{
								channelCoordinator.registerWeight(true, 1, 3, 999);
							}
						}
						else
						{
							if (primary)
							{
								channelCoordinator.registerWeight(false, 3, 2, 999);
							}
							else
							{
								channelCoordinator.registerWeight(true, 1, 3, 999);
							}
						}
					}

					Thread.sleep(500);
				}
			}
			catch (Exception e)
			{
			}
		}
	}
}