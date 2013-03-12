package com.gltech.scale.ganglia;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.*;
import com.gltech.scale.core.storage.bytearray.MemoryStorage;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.ValidatingStorage;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;

import javax.ws.rs.core.MediaType;

public class MonitorPlay
{
	static private Props props;
	static private TestingServer testingServer;

	public static void main(String[] args) throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");

		ServiceMetaData storageService = new ServiceMetaData();
		storageService.setListenAddress(props.get("event_service.rest_host", "localhost"));
		storageService.setListenPort(props.get("event_service.rest_port", 9090));

		testingServer = new TestingServer(21818);
		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).toInstance(new ValidatingStorage(new MemoryStorage()));
			}
		});

		StorageServiceRestClient restClient = new StorageServiceRestClient();
		BucketMetaData bucketMetaData = new BucketMetaData("MonitorPlay", "one", BucketMetaData.BucketType.bytes, 10,
				MediaType.APPLICATION_JSON_TYPE, BucketMetaData.LifeTime.small, BucketMetaData.Redundancy.singlewrite);
		restClient.putBucketMetaData(storageService, bucketMetaData);
		for (int i = 0; i < 180; i++)
		{
			String id = i + " ";
			restClient.put(storageService, "MonitorPlay", "one", id, id.getBytes());
			Thread.sleep(1000);
			System.out.print(i);
		}
	}
}
