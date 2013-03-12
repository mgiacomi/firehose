package com.gltech.scale.util;

import voldemort.client.CachingStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;

import java.util.Arrays;
import java.util.List;

public class VoldemortClient
{
	private static StoreClientFactory factory;

	public static StoreClientFactory createFactory()
	{
		if (factory != null)
		{
			return factory;
		}
		List<String> bootstrapUrls = Props.getProps().get("voldemortBootstrapUrls", Arrays.asList("tcp://localhost:6666"));

		ClientConfig clientConfig = new ClientConfig();
		clientConfig.setBootstrapUrls(bootstrapUrls);
		factory = new CachingStoreClientFactory(new SocketStoreClientFactory(clientConfig));
		return factory;
	}
}
