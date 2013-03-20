package com.gltech.scale.core.inbound;

import com.gltech.scale.core.cluster.TimePeriodUtils;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.core.storage.providers.MemoryStore;
import com.gltech.scale.util.ModelIO;
import com.gltech.scale.util.Props;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.netflix.curator.test.TestingServer;
import com.sun.jersey.api.NotFoundException;
import org.joda.time.DateTime;
import org.junit.*;
import org.junit.rules.ExpectedException;

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
	private ServiceMetaData inboundService;
	private ChannelMetaData bmd1;
	private TimePeriodUtils timePeriodUtils;

	@Before
	public void setUp() throws Exception
	{
		testingServer = new TestingServer(21818);

		// Pops and test classes have to be static to be used by embedded server.
		inboundService = new ServiceMetaData();
		inboundService.setListenAddress(props.get("inbound.rest_host", Defaults.REST_HOST));
		inboundService.setListenPort(props.get("inbound.rest_port", Defaults.REST_PORT));

		inboundRestClient = new InboundRestClient(new ModelIO());
		timePeriodUtils = new TimePeriodUtils();

		bmd1 = createBucketMetaData("Matt_Music");

		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(Storage.class).to(MemoryStore.class).in(Singleton.class);
			}
		});
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
		inboundRestClient.putChannelMetaData(inboundService, bmd1);
		ChannelMetaData bmd2 = inboundRestClient.getChannelMetaData(inboundService, "Matt_Music");
		assertEquals(bmd1, bmd2);
	}

	@Test
	public void createIfDoesNotExist() throws Exception
	{
		exception.expect(NotFoundException.class);
		inboundRestClient.getChannelMetaData(inboundService, "Matt_Music");
	}

	@Test
	public void postEvenIfBucketDoesNotExist() throws Exception
	{
		inboundRestClient.postMessage(inboundService, "test1_test2", "{}");
		Thread.sleep(2000);
		ChannelMetaData cmd = inboundRestClient.getChannelMetaData(inboundService, "test1_test2");
		assertEquals("test1_test2", cmd.getName());
	}

	@Test
	public void postAndGetSomeEvents() throws Exception
	{
		inboundRestClient.putChannelMetaData(inboundService, bmd1);

		ChannelMetaData channelMetaData = inboundRestClient.getChannelMetaData(inboundService, "Matt_Music");
		assertEquals(bmd1, channelMetaData);

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
			inboundRestClient.postMessage(inboundService, channelMetaData.getName(), json);
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
			inboundRestClient.postMessage(inboundService, channelMetaData.getName(), json);
			if (expectedResults2.length() > 0)
			{
				expectedResults2.append(",");
			}
			expectedResults2.append(json);
		}
		DateTime second = DateTime.now();

		DateTime nowish = DateTime.now().withSecondOfMinute(0).withMillisOfSecond(0);

		Thread.sleep(20000);

		String firstEvents = inboundRestClient.getMessages(inboundService, channelMetaData.getName(), timePeriodUtils.nearestPeriodCeiling(first), TimeUnit.SECONDS);
		assertEquals("[" + expectedResults1 + "]", firstEvents);

		String secondEvents = inboundRestClient.getMessages(inboundService, channelMetaData.getName(), timePeriodUtils.nearestPeriodCeiling(second), TimeUnit.SECONDS);
		assertEquals("[" + expectedResults2 + "]", secondEvents);

		String allEvents = inboundRestClient.getMessages(inboundService, channelMetaData.getName(), nowish, TimeUnit.MINUTES);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);

		allEvents = inboundRestClient.getMessages(inboundService, channelMetaData.getName(), nowish, TimeUnit.HOURS);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);

		allEvents = inboundRestClient.getMessages(inboundService, channelMetaData.getName(), nowish, TimeUnit.DAYS);
		assertEquals("[" + expectedResults1 + "," + expectedResults2 + "]", allEvents);
	}

	private ChannelMetaData createBucketMetaData(String channelName)
	{
		return new ChannelMetaData(channelName, ChannelMetaData.TTL_DAY, false);
	}
}
