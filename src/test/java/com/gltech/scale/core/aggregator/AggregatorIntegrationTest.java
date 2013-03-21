package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.inbound.InboundRestClient;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.util.ModelIO;
import com.gltech.scale.util.StreamDelimiter;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.*;

import javax.ws.rs.core.MediaType;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class AggregatorIntegrationTest
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
				binder.bind(Storage.class).to(TestStore.class).in(Singleton.class);
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
	public void testEventToAggregatorAndCollect() throws Exception
	{
		ModelIO modelIO = new ModelIO();
		StreamDelimiter streamDelimiter = new StreamDelimiter();

		ServiceMetaData aggregator = new ServiceMetaData();
		aggregator.setListenAddress(props.get("aggregator.rest_host", Defaults.REST_HOST));
		aggregator.setListenPort(props.get("aggregator.rest_port", Defaults.REST_PORT));

		ServiceMetaData inboundService = new ServiceMetaData();
		inboundService.setListenAddress(props.get("inbound.rest_host", Defaults.REST_HOST));
		inboundService.setListenPort(props.get("inbound.rest_port", Defaults.REST_PORT));

		List<String> requests = new ArrayList<>();
		requests.add("{\"singer\":\"Metallica\",\"title\":\"Fade To Black\"}");
		requests.add("{\"singer\":\"AC/DC\",\"title\":\"Back In Black\"}");
		requests.add("{\"singer\":\"Tori Amos\",\"title\":\"Little Earthquakes\"}");

		List<String> requests2 = new ArrayList<>();
		requests2.add("{\"singer\":\"Metallica2\",\"title\":\"Fade To Black2\"}");
		requests2.add("{\"singer\":\"AC/DC2\",\"title\":\"Back In Black2\"}");
		requests2.add("{\"singer\":\"Tori Amos2\",\"title\":\"Little Earthquakes2\"}");

		InboundRestClient ecrc = new InboundRestClient(modelIO);

		for (String json : requests)
		{
			ecrc.postMessage(inboundService, "test1", json);
		}
		DateTime first = DateTime.now();

		// Test injecting fake events to the aggregator to make sure they are collected too.
		AggregatorRestClient aggregatorClient = new AggregatorRestClient(new ModelIO());
		Message backupMessage = new Message(MediaType.APPLICATION_JSON_TYPE, "event from failed server".getBytes());
		aggregatorClient.postBackupMessage(aggregator, "test1", backupMessage);

		Thread.sleep(5000);

		for (String json : requests2)
		{
			ecrc.postMessage(inboundService, "test1", json);
		}
		DateTime second = DateTime.now();

		// Get first set of messages
		InputStream inputStream = aggregatorClient.getBatchMessagesStream(aggregator, "test1", first);

		List<Message> messages = new ArrayList<>();
		while (true)
		{
			try
			{
				messages.add(modelIO.toMessage(streamDelimiter.readNext(inputStream)));
			}
			catch (EOFException e)
			{
				break;
			}
		}

		inputStream.close();

		assertEquals(3, messages.size());

		for (int i = 0; i < messages.size(); i++)
		{
			Message message = messages.get(i);
			assertEquals(requests.get(i), new String(message.getPayload()));
		}

		// Get Second set of messages
		inputStream = aggregatorClient.getBatchMessagesStream(aggregator, "test1", second);

		messages = new ArrayList<>();
		while (true)
		{
			try
			{
				messages.add(modelIO.toMessage(streamDelimiter.readNext(inputStream)));
			}
			catch (EOFException e)
			{
				break;
			}
		}

		inputStream.close();

		assertEquals(3, messages.size());

		for (int i = 0; i < messages.size(); i++)
		{
			Message message = messages.get(i);
			assertEquals(requests2.get(i), new String(message.getPayload()));
		}

		// Get backup messages
		inputStream = aggregatorClient.getBackupBatchMessagesStream(aggregator, "test", first);

		messages = new ArrayList<>();
		while (true)
		{
			try
			{
				messages.add(modelIO.toMessage(streamDelimiter.readNext(inputStream)));
			}
			catch (EOFException e)
			{
				break;
			}
		}

		inputStream.close();

		assertEquals(1, messages.size());
		assertEquals("event from failed server", new String(messages.get(0).getPayload()));
	}

	// Just a quick dirty inner class to verify that bind overrides are working.
	static class TestStore implements Storage
	{
		@Override
		public ChannelMetaData getChannelMetaData(String channelName)
		{
			return new ChannelMetaData("test", ChannelMetaData.TTL_DAY, true);
		}

		@Override
		public void putChannelMetaData(ChannelMetaData channelMetaData)
		{
		}

		public void putBytes(ChannelMetaData channelMetaData, String id, byte[] data)
		{
		}

		public byte[] getBytes(ChannelMetaData channelMetaData, String id)
		{
			return new byte[0];
		}

		public void putMessages(ChannelMetaData channelMetaData, String id, InputStream inputStream)
		{
		}

		public void getMessages(ChannelMetaData channelMetaData, String id, OutputStream outputStream)
		{
		}
	}
}
