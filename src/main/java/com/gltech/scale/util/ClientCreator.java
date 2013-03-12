package com.gltech.scale.util;

import com.sun.jersey.api.client.Client;

public class ClientCreator
{
	private static final Client cachedClient = createNew();

	/**
	 * Creating clients is relatively expensive, so you may want to cache these locally.
	 * This Client will retry by default.
	 */
	public static Client createNew()
	{
		int connectTimeoutMillis = Props.getProps().get("ClientCreator.ConnectionTimeoutMillis", 30 * 1000);
		int readTimeoutMillis = Props.getProps().get("ClientCreator.ReadTimeoutMillis", 60 * 1000);

		Client client = Client.create();
		client.setConnectTimeout(connectTimeoutMillis);
		client.setReadTimeout(readTimeoutMillis);
		client.addFilter(new RetryClientFilter());
		client.setChunkedEncodingSize(-1);
		return client;
	}

	/**
	 * This returns a cached shared version of createNew().
	 * You should not make any changes to this client.
	 */
	public static Client createCached()
	{
		return cachedClient;
	}

}
