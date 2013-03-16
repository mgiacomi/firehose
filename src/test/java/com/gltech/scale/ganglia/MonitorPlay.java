package com.gltech.scale.ganglia;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.*;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;

import javax.ws.rs.core.MediaType;

public class MonitorPlay
{
/*
	static private Props props;
	static private TestingServer testingServer;

	public static void main(String[] args) throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

		ServiceMetaData storageService = new ServiceMetaData();
		storageService.setListenAddress(props.get("storage_service.rest_host", "localhost"));
		storageService.setListenPort(props.get("storage_service.rest_port", 9090));

		testingServer = new TestingServer(21818);
		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).toInstance(new ValidatingStorage(new MemoryStorage()));
			}
		});

		StorageServiceRestClient restClient = new StorageServiceRestClient();
		ChannelMetaData channelMetaData = new ChannelMetaData("MonitorPlay", "one", ChannelMetaData.BucketType.bytes, 10,
				MediaType.APPLICATION_JSON_TYPE, ChannelMetaData.LifeTime.small, ChannelMetaData.Redundancy.singlewrite);
		restClient.putBucketMetaData(storageService, channelMetaData);
		for (int i = 0; i < 180; i++)
		{
			String id = i + " ";
			restClient.put(storageService, "MonitorPlay", "one", id, id.getBytes());
			Thread.sleep(1000);
			System.out.print(i);
		}
	}
*/
}
