package com.gltech.scale.util;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

public class ZkClientCreator
{
	private static CuratorFramework cachedClient = createNew();

	/**
	 * Creating clients is relatively expensive, so you may want to cache these locally.
	 * This Client will retry by default.
	 */
	private static CuratorFramework createNew()
	{
		Props props = Props.getProps();
		String zookeeperConnectionString = props.get("zookeeper.connection_string", "localhost:2181");
		int maxRetries = props.get("zookeeper.retry_policy.max_retries", 3);
		int baseSleepTimeMs = props.get("zookeeper.retry_policy.base_sleep_time_ms", 1000);
		int sessionTimeoutMs = props.get("zookeeper.session_timeout_ms", 60000);
		int connectionTimeoutMs = props.get("zookeeper.connection_timeout_ms", 10000);

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
		client.start();

		return client;
	}

	/**
	 * This returns a cached shared version of createNew().
	 * You should not make any changes to this client.
	 */
	synchronized public static CuratorFramework createCached()
	{
		if (cachedClient == null)
		{
			cachedClient = createNew();
		}

		return cachedClient;
	}

	synchronized public static void close()
	{
		if (cachedClient != null)
		{
			cachedClient.close();
			cachedClient = null;
		}
	}

}
