package com.gltech.scale.load;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.bytearray.BucketOnlyStorage;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.ValidatingStorage;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;

public class LoadServer
{

	public static void main(String[] args) throws Exception
	{
		Props props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/config/load/props.properties");

		TestingServer testingServer = new TestingServer(21818);
		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).toInstance(new ValidatingStorage(new BucketOnlyStorage()));
			}
		});
	}
}
