package com.gltech.scale.util;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.server.EmbeddedServer;
import com.netflix.curator.test.TestingServer;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RetryClientFilterTest
{
	private static final int RESPONSE_TIME = 500;
	static private Props props;
	static private TestingServer testingServer;

	@BeforeClass
	public static void setUpClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");
		Props.getProps().set("zookeeper.throw_unregister_exception", false);

		testingServer = new TestingServer(21818);
		EmbeddedServer.start(2222, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(TheResource.class);
			}
		});
	}

	@AfterClass
	public static void tearDownClass() throws Exception
	{
		EmbeddedServer.stop();
		testingServer.stop();
	}

	@Before
	public void setUp() throws Exception
	{
		Props.getProps().set("RetryClientFilter.SleepMillis", 1000);
	}

	@Test
	public void testSuccess() throws Exception
	{
		ClientCreator.createCached().resource("http://localhost:2222/TheResource/").get(String.class);
		long start = System.currentTimeMillis();
		ClientCreator.createCached().resource("http://localhost:2222/TheResource/").get(String.class);
		long end = System.currentTimeMillis();
		assertTrue(RESPONSE_TIME > (end - start));
	}

	@Test
	public void testMethodNotAllowed() throws Exception
	{
		long start = System.currentTimeMillis();
		ClientResponse response = ClientCreator.createCached().resource("http://localhost:2222/TheResource/").post(ClientResponse.class, "stuff");
		assertEquals(405, response.getStatus());
		long end = System.currentTimeMillis();
		assertTrue(RESPONSE_TIME > (end - start));
	}

	@Test(expected = ClientHandlerException.class)
	public void testMediaType() throws Exception
	{
		long start = System.currentTimeMillis();
		ClientCreator.createCached().resource("http://localhost:2222/TheResource/").get(List.class);
		long end = System.currentTimeMillis();
		assertTrue(RESPONSE_TIME > (end - start));
	}

	@Test
	public void testUnknownHost() throws Exception
	{
		try
		{
			ClientCreator.createCached().resource("http://blah:2222/TheResource/").get(String.class);
		}
		catch (ClientHandlerException e)
		{
			assertEquals("java.net.UnknownHostException: blah", e.getMessage());
		}

	}

	@Test
	public void testInvalidPort() throws Exception
	{
		Props.getProps().get("RetryClientFilter.SleepMillis", 1);
		try
		{
			ClientCreator.createCached().resource("http://localhost:2223/TheResource/").get(String.class);
		}
		catch (ClientHandlerException e)
		{
			assertEquals("Connection retries limit 3 exceeded for uri http://localhost:2223/TheResource/", e.getMessage());
		}

	}

}
