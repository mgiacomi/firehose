package com.gltech.scale.core.coordination.registration;

import com.google.common.base.Throwables;
import com.gltech.scale.core.util.ZkClientCreator;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.x.discovery.ServiceCacheBuilder;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.ServiceCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ServiceAdvertiser
{
	public static final String EVENT_SERVICE = "EventService";
	public static final String COLLECTOR_SERVICE = "CollectorService";
	public static final String ROPE_SERVICE = "RopeService";
	public static final String STORAGE_SERVICE = "StorageService";

	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.coordination.registration.CollectorManagerAdvertiser");
	private final String serviceName;
	private final CuratorFramework client = ZkClientCreator.createCached();

	ServiceAdvertiser(String serviceName)
	{
		this.serviceName = serviceName;
	}

	public ServiceCache<ServiceMetaData> getServiceCache()
	{
		try
		{
			ServiceDiscovery<ServiceMetaData> discovery = getDiscovery();
			discovery.start();

			ServiceCacheBuilder<ServiceMetaData> cacheBuilder = discovery.serviceCacheBuilder();
			ServiceCache<ServiceMetaData> serviceCache = cacheBuilder.name(serviceName).build();

			discovery.close();

			return serviceCache;
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	public ServiceMetaData available(String listenAddress, int listenPort)
	{
		ServiceMetaData serviceMetaData = new ServiceMetaData(UUID.randomUUID(), listenAddress, listenPort);

		try
		{
			ServiceDiscovery<ServiceMetaData> discovery = getDiscovery();
			discovery.start();
			discovery.registerService(getInstance(serviceMetaData));
			discovery.close();
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}

		return serviceMetaData;
	}

	public void unavailable(ServiceMetaData serviceMetaData)
	{
		try
		{
			ServiceDiscovery<ServiceMetaData> discovery = getDiscovery();
			discovery.start();
			discovery.unregisterService(getInstance(serviceMetaData));
			discovery.close();
		}
		catch (IllegalStateException ise)
		{
			logger.error("Shutting down instance that was never started.");
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	private ServiceInstance<ServiceMetaData> getInstance(ServiceMetaData serviceMetaData) throws Exception
	{
		return ServiceInstance.<ServiceMetaData>builder()
				.name(serviceName)
				.address(serviceMetaData.getListenAddress())
				.port(serviceMetaData.getListenPort())
				.id(serviceMetaData.getWorkerId().toString())
				.payload(serviceMetaData)
				.build();
	}

	public ServiceDiscovery<ServiceMetaData> getDiscovery()
	{
		try
		{
			ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), "/services/" + serviceName);

			return ServiceDiscoveryBuilder.builder(ServiceMetaData.class)
					.basePath("/services/" + serviceName)
					.client(client)
					.build();
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}
}