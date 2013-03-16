package com.gltech.scale.core.inbound;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.util.Http404Exception;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.*;
import org.junit.rules.ExpectedException;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

public class EventResourceRegression
{
/*
	@Rule
	public ExpectedException exception = ExpectedException.none();

	static private Props props;
	private TestingServer testingServer;
	private InboundRestClient inboundRestClient;
	private StorageServiceRestClient storageServiceRestClient;
	private ServiceMetaData eventService;
	private ServiceMetaData storageService;
	private ChannelMetaData bmd1;
	private TimePeriodUtils timePeriodUtils;

	@Before
	public void setUp() throws Exception
	{
		testingServer = new TestingServer(21818);

		// Pops and test classes have to be static to be used by embedded server.
		eventService = new ServiceMetaData();
		eventService.setListenAddress(props.get("inbound.rest_host", Defaults.REST_HOST));
		eventService.setListenPort(props.get("inbound.rest_port", Defaults.REST_PORT));

		storageService = new ServiceMetaData();
		storageService.setListenAddress(props.get("storage.rest_host", Defaults.REST_HOST));
		storageService.setListenPort(props.get("storage.rest_port", Defaults.REST_PORT));

		inboundRestClient = new InboundRestClient();
		storageServiceRestClient = new StorageServiceRestClient();
		timePeriodUtils = new TimePeriodUtils();

		bmd1 = createBucketMetaData("Matt", "Music");

		EmbeddedServer.start(9090);
	}

	@After
	public void tearDown() throws Exception
	{
		EmbeddedServer.stop();
		testingServer.stop();
	}

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");
	}

	@AfterClass
	public static void afterClass() throws Exception
	{
	}

	@Test
	public void createdAndGetBucketMetaData() throws Exception
	{
		storageServiceRestClient.putBucketMetaData(storageService, bmd1);
		ChannelMetaData bmd2 = inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");
		assertEquals(bmd1, bmd2);
	}

	@Test
	public void createIfDoesNotExist() throws Exception
	{
		exception.expect(Http404Exception.class);
		inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");
	}

	@Test
	public void postEvenIfBucketDoesNotExist() throws Exception
	{
		inboundRestClient.postEvent(eventService, "test1", "test2", "{}");
	}


	@Test
	public void postAndGetSomeEvents() throws Exception
	{
		storageServiceRestClient.putBucketMetaData(storageService, bmd1);

		ChannelMetaData bmd2 = inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");
		assertEquals(bmd1, bmd2);

		ChannelMetaData channelMetaData = inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");

		List<String> requests = new ArrayList<>();
		requests.add("{\"singer\":\"Metallica\",\"title\":\"Fade To Black\"}");
		requests.add("{\"singer\":\"AC/DC\",\"title\":\"Back In Black\"}");
		requests.add("{\"singer\":\"Tori Amos\",\"title\":\"Little Earthquakes\"}");

		StringBuilder sb = new StringBuilder();
		while (sb.toString().getBytes().length < 102400)
		{
			sb.append("more text more text more text more text more text more text more text more text more text more text ");
		}
		requests.add("{\"singer\":\"Winded Jonny\",\"title\":\"" + sb.toString() + "\"}");

		List<String> requests2 = new ArrayList<>();
		requests2.add("{\"singer\":\"Metallica2\",\"title\":\"Fade To Black2\"}");
		requests2.add("{\"singer\":\"AC/DC2\",\"title\":\"Back In Black2\"}");
		requests2.add("{\"singer\":\"Tori Amos2\",\"title\":\"Little Earthquakes2\"}");

		StringBuilder expectedResults1 = new StringBuilder();
		for (String json : requests)
		{
			inboundRestClient.postEvent(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), json);
			if (expectedResults1.length() > 0)
			{
				expectedResults1.append(",");
			}
			expectedResults1.append(json);
		}
		DateTime first = DateTime.now();

		Thread.sleep(5000);

		StringBuilder expectedResults2 = new StringBuilder();
		for (String json : requests2)
		{
			inboundRestClient.postEvent(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), json);
			if (expectedResults2.length() > 0)
			{
				expectedResults2.append(",");
			}
			expectedResults2.append(json);
		}
		DateTime second = DateTime.now();

		DateTime nowish = DateTime.now().withSecondOfMinute(0).withMillisOfSecond(0);

		Thread.sleep(60000);

		String firstEvents = inboundRestClient.getEvents(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), timePeriodUtils.nearestPeriodCeiling(first), TimeUnit.SECONDS);
		assertEquals("[" + expectedResults1 + "]", firstEvents);

		String secondEvents = inboundRestClient.getEvents(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), timePeriodUtils.nearestPeriodCeiling(second), TimeUnit.SECONDS);
		assertEquals("[" + expectedResults2 + "]", secondEvents);

		String allEvents = inboundRestClient.getEvents(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), nowish, TimeUnit.MINUTES);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);

		allEvents = inboundRestClient.getEvents(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), nowish, TimeUnit.HOURS);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);

		allEvents = inboundRestClient.getEvents(eventService, channelMetaData.getCustomer(), channelMetaData.getBucket(), nowish, TimeUnit.DAYS);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);
	}

	private ChannelMetaData createBucketMetaData(String customer, String bucket)
	{
		return new ChannelMetaData(customer, bucket, ChannelMetaData.BucketType.eventset, 5, MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
	}
*/
}
