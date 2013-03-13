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
	public void testRopeAssignmentOverTime() throws Exception
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
		when(registrationService.getRopeManagerMetaDataById(rm0.getWorkerId().toString())).thenReturn(rm0);
		when(registrationService.getRopeManagerMetaDataById(rm1.getWorkerId().toString())).thenReturn(rm1);
		when(registrationService.getRopeManagerMetaDataById(rm2.getWorkerId().toString())).thenReturn(rm2);
		when(registrationService.getRopeManagerMetaDataById(rm3.getWorkerId().toString())).thenReturn(rm3);
		when(registrationService.getRopeManagerMetaDataById(rm4.getWorkerId().toString())).thenReturn(rm4);
		when(registrationService.getRopeManagerMetaDataById(rm5.getWorkerId().toString())).thenReturn(rm5);
		when(registrationService.getRopeManagerMetaDataById(rm6.getWorkerId().toString())).thenReturn(rm6);
		when(registrationService.getRopeManagerMetaDataById(rm7.getWorkerId().toString())).thenReturn(rm7);
		when(registrationService.getRopeManagerMetaDataById(rm8.getWorkerId().toString())).thenReturn(rm8);
		when(registrationService.getRopeManagerMetaDataById(rm9.getWorkerId().toString())).thenReturn(rm9);

		ChannelCoordinator channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());

		FakeRopeManager frm0 = new FakeRopeManager(rm0);
		FakeRopeManager frm1 = new FakeRopeManager(rm1);
		FakeRopeManager frm2 = new FakeRopeManager(rm2);
		FakeRopeManager frm3 = new FakeRopeManager(rm3);
		FakeRopeManager frm4 = new FakeRopeManager(rm4);
		FakeRopeManager frm5 = new FakeRopeManager(rm5);
		FakeRopeManager frm6 = new FakeRopeManager(rm6);
		FakeRopeManager frm7 = new FakeRopeManager(rm7);
		FakeRopeManager frm8 = new FakeRopeManager(rm8);
		FakeRopeManager frm9 = new FakeRopeManager(rm9);

		Map<String, FakeRopeManager> idToRopeManager = new HashMap<>();
		idToRopeManager.put(rm0.getWorkerId().toString(), frm0);
		idToRopeManager.put(rm1.getWorkerId().toString(), frm1);
		idToRopeManager.put(rm2.getWorkerId().toString(), frm2);
		idToRopeManager.put(rm3.getWorkerId().toString(), frm3);
		idToRopeManager.put(rm4.getWorkerId().toString(), frm4);
		idToRopeManager.put(rm5.getWorkerId().toString(), frm5);
		idToRopeManager.put(rm6.getWorkerId().toString(), frm6);
		idToRopeManager.put(rm7.getWorkerId().toString(), frm7);
		idToRopeManager.put(rm8.getWorkerId().toString(), frm8);
		idToRopeManager.put(rm9.getWorkerId().toString(), frm9);

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
			AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getRopeManagerPeriodMatrix(DateTime.now());
			if (aggregatorsByPeriod != null)
			{
				for (PrimaryBackupSet primaryBackupSet : aggregatorsByPeriod.getPrimaryBackupSets())
				{
					FakeRopeManager primary = idToRopeManager.get(primaryBackupSet.getPrimary().getWorkerId().toString());
					primary.assign(true, timePeriodUtils.nearestPeriodCeiling(DateTime.now()));

					if (primaryBackupSet.getBackup() != null)
					{
						FakeRopeManager backup = idToRopeManager.get(primaryBackupSet.getBackup().getWorkerId().toString());
						backup.assign(false, timePeriodUtils.nearestPeriodCeiling(DateTime.now()));
					}
				}
			}
//			frm0.assign(false, ZookeeperCoordinationService.nearestPeriodCeiling(DateTime.now(), 5));

			Thread.sleep(1000);
		}

	}

	class FakeRopeManager implements Runnable
	{
		private DateTime nearestPeriodCeiling;
		private ChannelCoordinator channelCoordinator;
		private boolean primary;

		FakeRopeManager(ServiceMetaData serviceMetaData)
		{
			RegistrationService registrationService = mock(RegistrationService.class);
			channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());
			when(registrationService.getLocalRopeManagerMetaData()).thenReturn(serviceMetaData);
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