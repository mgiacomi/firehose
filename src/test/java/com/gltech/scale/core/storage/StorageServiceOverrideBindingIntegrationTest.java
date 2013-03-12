package com.gltech.scale.core.storage;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.StoragePayload;
import com.gltech.scale.util.ClientCreator;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import static junit.framework.Assert.assertEquals;

public class StorageServiceOverrideBindingIntegrationTest
{
	static private Props props;
	static private TestingServer testingServer;
	private Client client;

	@Before
	public void setUp() throws Exception
	{
		client = ClientCreator.createCached();
	}

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");
		Props.getProps().set("zookeeper.throw_unregister_exception", false);

		testingServer = new TestingServer(21818);
		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).to(LameStorage.class).in(Singleton.class);
			}
		});
	}

	@AfterClass
	public static void afterClass() throws Exception
	{
		EmbeddedServer.stop();
		testingServer.stop();
	}

	@Test
	public void testLameStorage()
	{
		WebResource resource = client.resource("http://localhost:9090/storage/c/b");
		ClientResponse getResponse = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		assertEquals(500, getResponse.getStatus());
	}

	// Just a quick dirty inner class to verify that bind overrides are working.
	static class LameStorage implements ByteArrayStorage
	{
		@Override
		public void putBucket(BucketMetaData bucketMetaData)
		{
		}

		@Override
		public BucketMetaData getBucket(String customer, String bucket)
		{
			throw new RuntimeException("Sorry I'm just really lame.");
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
