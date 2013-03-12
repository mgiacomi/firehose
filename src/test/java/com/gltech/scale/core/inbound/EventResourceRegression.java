package com.gltech.scale.core.inbound;

import com.gltech.scale.core.coordination.TimePeriodUtils;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.StorageServiceRestClient;
import com.gltech.scale.core.util.Http404Exception;
import com.gltech.scale.core.util.Props;
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
	@Rule
	public ExpectedException exception = ExpectedException.none();

	static private Props props;
	private TestingServer testingServer;
	private InboundRestClient inboundRestClient;
	private StorageServiceRestClient storageServiceRestClient;
	private ServiceMetaData eventService;
	private ServiceMetaData storageService;
	private BucketMetaData bmd1;
	private TimePeriodUtils timePeriodUtils;

	@Before
	public void setUp() throws Exception
	{
		testingServer = new TestingServer(21818);

		// Pops and test classes have to be static to be used by embedded server.
		eventService = new ServiceMetaData();
		eventService.setListenAddress(props.get("event_service.rest_host", "localhost"));
		eventService.setListenPort(props.get("event_service.rest_port", 9090));

		storageService = new ServiceMetaData();
		storageService.setListenAddress(props.get("event_service.rest_host", "localhost"));
		storageService.setListenPort(props.get("event_service.rest_port", 9090));

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
		BucketMetaData bmd2 = inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");
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

		BucketMetaData bmd2 = inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");
		assertEquals(bmd1, bmd2);

		BucketMetaData bucketMetaData = inboundRestClient.getBucketMetaData(eventService, "Matt", "Music");

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
			inboundRestClient.postEvent(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), json);
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
			inboundRestClient.postEvent(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), json);
			if (expectedResults2.length() > 0)
			{
				expectedResults2.append(",");
			}
			expectedResults2.append(json);
		}
		DateTime second = DateTime.now();

		DateTime nowish = DateTime.now().withSecondOfMinute(0).withMillisOfSecond(0);

		Thread.sleep(60000);

		String firstEvents = inboundRestClient.getEvents(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), timePeriodUtils.nearestPeriodCeiling(first), TimeUnit.SECONDS);
		assertEquals("[" + expectedResults1 + "]", firstEvents);

		String secondEvents = inboundRestClient.getEvents(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), timePeriodUtils.nearestPeriodCeiling(second), TimeUnit.SECONDS);
		assertEquals("[" + expectedResults2 + "]", secondEvents);

		String allEvents = inboundRestClient.getEvents(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nowish, TimeUnit.MINUTES);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);

		allEvents = inboundRestClient.getEvents(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nowish, TimeUnit.HOURS);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);

		allEvents = inboundRestClient.getEvents(eventService, bucketMetaData.getCustomer(), bucketMetaData.getBucket(), nowish, TimeUnit.DAYS);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);
	}

	private BucketMetaData createBucketMetaData(String customer, String bucket)
	{
		return new BucketMetaData(customer, bucket, BucketMetaData.BucketType.eventset, 5, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
	}
}
