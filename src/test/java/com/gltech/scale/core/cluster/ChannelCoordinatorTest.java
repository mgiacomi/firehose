package com.gltech.scale.core.cluster;

import static org.junit.Assert.*;

import com.gltech.scale.core.aggregator.AggregatorsByPeriod;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ChannelCoordinatorTest
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
	public void testFutureSchedulingToFarInAdvance()
	{
		ServiceMetaData rm0 = new ServiceMetaData(UUID.randomUUID(), "0.0.0.0", 0);

		RegistrationService registrationService = mock(RegistrationService.class);
		ChannelCoordinator channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());
		when(registrationService.getAggregatorMetaDataById(rm0.getWorkerId().toString())).thenReturn(rm0);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm0);
		channelCoordinator.registerWeight(true, 3, 2, 0);

		channelCoordinator.getAggregatorPeriodMatrix(DateTime.now());
		channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().plusSeconds(5));

		boolean gotException = false;

		try
		{
			channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().plusSeconds(10));
		}
		catch (ClusterException e)
		{
			gotException = true;
		}

		assertTrue(gotException);
	}

	@Test
	public void testReregisterWeight() throws Exception
	{
		ServiceMetaData rm0 = new ServiceMetaData(UUID.randomUUID(), "0.0.0.0", 0);
		ServiceMetaData rm1 = new ServiceMetaData(UUID.randomUUID(), "1.1.1.1", 1);

		RegistrationService registrationService = mock(RegistrationService.class);
		when(registrationService.getAggregatorMetaDataById(rm0.getWorkerId().toString())).thenReturn(rm0);
		when(registrationService.getAggregatorMetaDataById(rm1.getWorkerId().toString())).thenReturn(rm1);

		ChannelCoordinator channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());

		when(registrationService.getLocalServerMetaData()).thenReturn(rm0);
		channelCoordinator.registerWeight(true, 3, 2, 0);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm1);
		channelCoordinator.registerWeight(true, 2, 2, 0);

		AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(2));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 0);

		channelCoordinator.registerWeight(true, 4, 2, 0);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(1));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 0);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 1);
	}

	@Test
	public void testGetAggregatorPeriodMatrixByPercent() throws Exception
	{
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
		ServiceMetaData rm10 = new ServiceMetaData(UUID.randomUUID(), "10.10.10.10", 10);
		ServiceMetaData rm11 = new ServiceMetaData(UUID.randomUUID(), "11.11.11.11", 11);
		ServiceMetaData rm12 = new ServiceMetaData(UUID.randomUUID(), "12.12.12.12", 12);

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
		when(registrationService.getAggregatorMetaDataById(rm10.getWorkerId().toString())).thenReturn(rm10);
		when(registrationService.getAggregatorMetaDataById(rm11.getWorkerId().toString())).thenReturn(rm11);
		when(registrationService.getAggregatorMetaDataById(rm12.getWorkerId().toString())).thenReturn(rm12);

		ChannelCoordinator channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());

		when(registrationService.getLocalServerMetaData()).thenReturn(rm1);
		channelCoordinator.registerWeight(true, 2, 2, 0);

		AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(1));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 1);
		assertNull(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup());

		when(registrationService.getLocalServerMetaData()).thenReturn(rm0);
		channelCoordinator.registerWeight(true, 3, 2, 0);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(2));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 0);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm5);
		channelCoordinator.registerWeight(false, 0, 0, 111);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(3));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 5);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 1);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm2);
		channelCoordinator.registerWeight(true, 1, 2, 0);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(4));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 5);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 2);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm3);
		channelCoordinator.registerWeight(true, 0, 1, 0);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(5));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 5);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 3);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm7);
		channelCoordinator.registerWeight(false, 0, 0, 12);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(6));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 7);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 5);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm4);
		channelCoordinator.registerWeight(true, 0, 0, 0);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(7));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 7);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 5);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm9);
		channelCoordinator.registerWeight(false, 0, 0, 1);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(8));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 7);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm6);
		channelCoordinator.registerWeight(false, 0, 0, 21);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(9));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 7);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm8);
		channelCoordinator.registerWeight(false, 0, 0, 2);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(10));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 8);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm10);
		channelCoordinator.registerWeight(false, 0, 0, 22);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(11));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 8);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm11);
		channelCoordinator.registerWeight(false, 0, 0, 23);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusHours(12));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 8);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getPrimary().getListenPort(), 7);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getBackup().getListenPort(), 6);

		when(registrationService.getLocalServerMetaData()).thenReturn(rm12);
		channelCoordinator.registerWeight(false, 0, 0, 25);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now());
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 8);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getPrimary().getListenPort(), 7);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getBackup().getListenPort(), 6);
	}

	@Test
	public void testGetAggregatorFuturePeriods() throws Exception
	{
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
		ServiceMetaData rm10 = new ServiceMetaData(UUID.randomUUID(), "10.10.10.10", 10);
		ServiceMetaData rm11 = new ServiceMetaData(UUID.randomUUID(), "11.11.11.11", 11);
		ServiceMetaData rm12 = new ServiceMetaData(UUID.randomUUID(), "12.12.12.12", 12);

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
		when(registrationService.getAggregatorMetaDataById(rm10.getWorkerId().toString())).thenReturn(rm10);
		when(registrationService.getAggregatorMetaDataById(rm11.getWorkerId().toString())).thenReturn(rm11);
		when(registrationService.getAggregatorMetaDataById(rm12.getWorkerId().toString())).thenReturn(rm12);

		ChannelCoordinator channelCoordinator = new ChannelCoordinatorImpl(registrationService, new TimePeriodUtils());

		when(registrationService.getLocalServerMetaData()).thenReturn(rm0);
		channelCoordinator.registerWeight(false, 0, 0, 1);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm1);
		channelCoordinator.registerWeight(false, 0, 0, 2);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm2);
		channelCoordinator.registerWeight(false, 0, 0, 3);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm3);
		channelCoordinator.registerWeight(false, 0, 0, 4);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm4);
		channelCoordinator.registerWeight(false, 0, 0, 5);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm5);
		channelCoordinator.registerWeight(false, 0, 0, 6);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm6);
		channelCoordinator.registerWeight(false, 0, 0, 7);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm7);
		channelCoordinator.registerWeight(false, 0, 0, 8);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm8);
		channelCoordinator.registerWeight(false, 0, 0, 9);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm9);
		channelCoordinator.registerWeight(false, 0, 0, 10);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm10);
		channelCoordinator.registerWeight(false, 0, 0, 11);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm11);
		channelCoordinator.registerWeight(false, 0, 0, 12);
		when(registrationService.getLocalServerMetaData()).thenReturn(rm12);
		channelCoordinator.registerWeight(false, 0, 0, 13);

		AggregatorsByPeriod aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusSeconds(1));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 0);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getPrimary().getListenPort(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getBackup().getListenPort(), 3);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusSeconds(2));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 4);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 5);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getPrimary().getListenPort(), 6);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getBackup().getListenPort(), 7);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusSeconds(3));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 8);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 9);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getPrimary().getListenPort(), 10);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getBackup().getListenPort(), 11);

		aggregatorsByPeriod = channelCoordinator.getAggregatorPeriodMatrix(DateTime.now().minusSeconds(4));
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().size(), 2);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getPrimary().getListenPort(), 12);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(0).getBackup().getListenPort(), 1);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getPrimary().getListenPort(), 3);
		assertEquals(aggregatorsByPeriod.getPrimaryBackupSets().get(1).getBackup().getListenPort(), 5);
	}

	@Test
	public void testWeightBreakdown() throws Exception
	{
		boolean active = true;
		int primaries = 1;
		int backups = 2;
		int atRest = 123;

		long weight = new WeightBreakdown(active, primaries, backups, atRest).toWeight();


		WeightBreakdown weightBreakdown = new WeightBreakdown(weight);

		assertEquals(weight, 2001002123L);
		assertEquals(active, weightBreakdown.isActive());
		assertEquals(primaries, weightBreakdown.getPrimaries());
		assertEquals(backups, weightBreakdown.getBackups());
		assertEquals(atRest, weightBreakdown.getAtRest());
	}
}
