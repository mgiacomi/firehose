package com;

import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.core.storage.providers.MemoryStore;
import com.gltech.scale.util.Props;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.netflix.curator.test.TestingServer;

public class MonitoringRegression
{
	static public class AppInstance
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/monitoring.properties");

			TestingServer testingServer = new TestingServer(21818);

			EmbeddedServer.start(8181, new Module()
			{
				public void configure(Binder binder)
				{
					binder.bind(Storage.class).to(MemoryStore.class).in(Singleton.class);
				}
			});

			while (true)
			{
				Thread.sleep(60000);
			}
		}
	}

	static public class MonitoringInstance
	{
		public static void main(String[] args) throws Exception
		{
			Props props = Props.getProps();
			props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/monitoring.properties");

			com.gltech.scale.monitoring.server.EmbeddedServer.start(9093);

			while (true)
			{
				Thread.sleep(60000);
			}
		}
	}
}
