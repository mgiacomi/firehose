package com.gltech.scale.core.storage;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.bytearray.MemoryStorage;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.ValidatingStorage;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class StorageResourceMemoryTest extends StorageResourceBaseTest
{
	static private Props props;
	static private TestingServer testingServer;

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

		testingServer = new TestingServer(21818);
		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).toInstance(new ValidatingStorage(new MemoryStorage()));
			}
		});
	}

	@AfterClass
	public static void afterClass() throws Exception
	{
		EmbeddedServer.stop();
		testingServer.stop();
	}
}
