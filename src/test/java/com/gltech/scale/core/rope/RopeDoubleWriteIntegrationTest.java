package com.gltech.scale.core.rope;

import com.gltech.scale.core.inbound.InboundRestClient;
import com.gltech.scale.core.model.Message;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.StoragePayload;
import com.gltech.scale.core.util.Props;
import com.netflix.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.*;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class RopeDoubleWriteIntegrationTest
{
	static private Props props;
	private TestingServer testingServer;

	@Before
	public void setUp() throws Exception
	{
		testingServer = new TestingServer(21818);
		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).to(VerySimpleStorage.class).in(Singleton.class);
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
		props.set("zookeeper.throw_unregister_exception", false);
	}

	@AfterClass
	public static void afterClass() throws Exception
	{
	}

	@Test
	public void testEventToRopeAndCollect() throws Exception
	{
		ServiceMetaData ropeManager = new ServiceMetaData();
		ropeManager.setListenAddress(props.get("rope_manager.rest_host", "localhost"));
		ropeManager.setListenPort(props.get("rope_manager.rest_port", 9090));

		ServiceMetaData eventService = new ServiceMetaData();
		eventService.setListenAddress(props.get("event_service.rest_host", "localhost"));
		eventService.setListenPort(props.get("event_service.rest_port", 9090));

		List<String> requests = new ArrayList<>();
		requests.add("{\"singer\":\"Metallica\",\"title\":\"Fade To Black\"}");
		requests.add("{\"singer\":\"AC/DC\",\"title\":\"Back In Black\"}");
		requests.add("{\"singer\":\"Tori Amos\",\"title\":\"Little Earthquakes\"}");

		List<String> requests2 = new ArrayList<>();
		requests2.add("{\"singer\":\"Metallica2\",\"title\":\"Fade To Black2\"}");
		requests2.add("{\"singer\":\"AC/DC2\",\"title\":\"Back In Black2\"}");
		requests2.add("{\"singer\":\"Tori Amos2\",\"title\":\"Little Earthquakes2\"}");

		InboundRestClient ecrc = new InboundRestClient();

		for (String json : requests)
		{
			ecrc.postEvent(eventService, "customer1", "bucket1", json);
		}
		DateTime first = DateTime.now();

		// Test injecting fake events to the rope manager to make sure they are collected too.
		RopeManagerRestClient rrc = new RopeManagerRestClient();
		Message backupMessage = new Message("customer1", "bucket1", "event from failed server".getBytes());
		rrc.postBackupEvent(ropeManager, backupMessage);

		Thread.sleep(5000);

		for (String json : requests2)
		{
			ecrc.postEvent(eventService, "customer1", "bucket1", json);
		}
		DateTime second = DateTime.now();

		RopeManagerRestClient ropeClient = new RopeManagerRestClient();
		List<Message> events = ropeClient.getTimeBucketEvents(ropeManager, "customer1", "bucket1", first);
		assertEquals(3, events.size());

		for (int i = 0; i < events.size(); i++)
		{
			Message message = events.get(i);
			assertEquals(requests.get(i), new String(message.getPayload()));
		}

		events = ropeClient.getTimeBucketEvents(ropeManager, "customer1", "bucket1", second);
		assertEquals(3, events.size());

		for (int i = 0; i < events.size(); i++)
		{
			Message message = events.get(i);
			assertEquals(requests2.get(i), new String(message.getPayload()));
		}

		List<Message> backupsEvents = ropeClient.getBackupTimeBucketEvents(ropeManager, "customer1", "bucket1", first);
		assertEquals(1, backupsEvents.size());
		assertEquals("event from failed server", new String(backupsEvents.get(0).getPayload()));
	}

	// Just a quick dirty inner class to verify that bind overrides are working.
	static class VerySimpleStorage implements ByteArrayStorage
	{
		@Override
		public void putBucket(BucketMetaData bucketMetaData)
		{
		}

		@Override
		public BucketMetaData getBucket(String customer, String bucket)
		{
			return new BucketMetaData("customer1", "bucket1", BucketMetaData.BucketType.eventset, 5, MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.doublewritesync);
		}

		@Override
		public void putPayload(StoragePayload storagePayload)
		{
		}

		@Override
		public StoragePayload getPayload(String customer, String bucket, String id)
		{
			throw new RuntimeException("I would love to get that for you, but I don't know how...");
		}
	}
}
