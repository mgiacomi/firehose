package com.gltech.scale.core.cluster;

import com.gltech.scale.core.cluster.registration.RegistrationServiceImpl;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class ClusterServiceTest
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
	public void testNearestPeriodCeiling()
	{
		TimePeriodUtils timePeriodUtils = new TimePeriodUtils();

		assertEquals(new DateTime(2012, 9, 21, 22, 32, 15), timePeriodUtils.nearestPeriodCeiling(new DateTime(2012, 9, 21, 22, 32, 12)));
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 5), timePeriodUtils.nearestPeriodCeiling(new DateTime(2012, 9, 21, 22, 32, 4)));
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 5), timePeriodUtils.nearestPeriodCeiling(new DateTime(2012, 9, 21, 22, 32, 5)));
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 10), timePeriodUtils.nearestPeriodCeiling(new DateTime(2012, 9, 21, 22, 32, 6)));
		assertEquals(new DateTime(2012, 9, 21, 22, 32, 10), timePeriodUtils.nearestPeriodCeiling(new DateTime(2012, 9, 21, 22, 32, 10)));
		assertEquals(new DateTime(2012, 9, 21, 22, 33, 0), timePeriodUtils.nearestPeriodCeiling(new DateTime(2012, 9, 21, 22, 32, 57)));
	}

	@Test
	public void fullPathToNameOnly()
	{
		assertEquals("C|B|20121011150500", BatchPeriodMapper.nodeNameStripPath("/channel/batches/C|B|20121011150500"));
	}

	@Test
	public void testRegisterAndQueryAggregators() throws Exception
	{
		ClusterService clusterService = new ClusterServiceImpl(new RegistrationServiceImpl());

		props.set("aggregator.rest_host", "aggregator1");
		props.set("aggregator.rest_port", 8080);
		clusterService.getRegistrationService().registerAsAggregator();

		props.set("aggregator.rest_host", "aggregator2");
		props.set("aggregator.rest_port", 9090);
		clusterService.getRegistrationService().registerAsAggregator();

		Thread.sleep(1000);
		List<ServiceMetaData> servers = clusterService.getRegistrationService().getRegisteredAggregators();

		List<String> channelNames = new ArrayList<>();
		channelNames.add(servers.get(0).getListenAddress());
		channelNames.add(servers.get(1).getListenAddress());

		Collections.sort(channelNames);

		assertEquals("aggregator1", channelNames.get(0));
		assertEquals("aggregator2", channelNames.get(1));
	}
}
