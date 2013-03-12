package com;

import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;

public class MonitoringRegression
{
	public static void main(String[] args) throws Exception
	{
		Props props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/monitoring.properties");

		TestingServer testingServer = new TestingServer(21818);

		EmbeddedServer.start(9999);

		while (true)
		{
			Thread.sleep(60000);
		}
	}
}
