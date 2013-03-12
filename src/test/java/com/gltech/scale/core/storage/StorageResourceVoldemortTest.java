package com.gltech.scale.core.storage;

import com.gltech.scale.util.VoldemortClientTest;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.TermVoldemortStorage;
import com.gltech.scale.core.storage.bytearray.ValidatingStorage;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageResourceVoldemortTest extends StorageResourceBaseTest
{
	private static final Logger logger = LoggerFactory.getLogger(StorageResourceVoldemortTest.class);
	static private Props props;
	static private TestingServer testingServer;

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

		testingServer = new TestingServer(21818);

		VoldemortClientTest.start();

		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).toInstance(
						new ValidatingStorage(new TermVoldemortStorage()));
			}
		});
	}

	@AfterClass
	public static void afterClass() throws Exception
	{
		EmbeddedServer.stop();
		VoldemortClientTest.stop();
		testingServer.stop();
	}
}
