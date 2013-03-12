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
		assertEquals("C|B|20121011150500", BatchPeriodMapper.nodeNameStripPath("/rope/timebuckets/C|B|20121011150500"));
	}

	@Test
	public void testRegisterAndQueryRopeManagers() throws Exception
	{
		ClusterService clusterService = new ZookeeperClusterService(new RegistrationServiceImpl());

		props.set("rope_manager.rest_host", "ropemgr1");
		props.set("rope_manager.rest_port", 8080);
		clusterService.getRegistrationService().registerAsRopeManager();

		props.set("rope_manager.rest_host", "ropemgr2");
		props.set("rope_manager.rest_port", 9090);
		clusterService.getRegistrationService().registerAsRopeManager();

		Thread.sleep(1000);
		List<ServiceMetaData> servers = clusterService.getRegistrationService().getRegisteredRopeManagers();

		List<String> ropeNames = new ArrayList<>();
		ropeNames.add(servers.get(0).getListenAddress());
		ropeNames.add(servers.get(1).getListenAddress());

		Collections.sort(ropeNames);

		assertEquals("ropemgr1", ropeNames.get(0));
		assertEquals("ropemgr2", ropeNames.get(1));
	}
}
