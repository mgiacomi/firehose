package com.gltech.scale.core.cluster.registration;

import com.gltech.scale.core.cluster.ClusterException;
import com.google.common.base.Throwables;
import com.gltech.scale.util.Props;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.ServiceCache;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import com.netflix.curator.x.discovery.strategies.RoundRobinStrategy;
import com.netflix.curator.x.discovery.strategies.StickyStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RegistrationServiceImpl implements RegistrationService
{
	private static final Logger logger = LoggerFactory.getLogger(RegistrationServiceImpl.class);
	private ServiceAdvertiser eventServiceAdvertiser;
	private ServiceAdvertiser collectorManagerAdvertiser;
	private ServiceAdvertiser ropeManagerAdvertiser;
	private ServiceAdvertiser storageServiceAdvertiser;
	private ServiceMetaData localEventServiceMetaData;
	private ServiceMetaData localCollectorManagerMetaData;
	private ServiceMetaData localRopeManagerMetaData;
	private ServiceMetaData localStorageServiceMetaData;
	private ServiceCache<ServiceMetaData> eventServiceCache;
	private ServiceCache<ServiceMetaData> collectorManagerCache;
	private ServiceCache<ServiceMetaData> ropeManagerCache;
	private ServiceCache<ServiceMetaData> storageServiceCache;
	private RandomStrategy<ServiceMetaData> randomStorageStrategy = new RandomStrategy<>();
	private RoundRobinStrategy<ServiceMetaData> roundRobinStorageStrategy = new RoundRobinStrategy<>();
	private StickyStrategy<ServiceMetaData> stickyStorageStrategy = new StickyStrategy<>(randomStorageStrategy);
	private Props props = Props.getProps();

	public RegistrationServiceImpl()
	{
		eventServiceAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.EVENT_SERVICE);
		eventServiceCache = eventServiceAdvertiser.getServiceCache();
		eventServiceCache.addListener(new EventServiceCacheListener());

		collectorManagerAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.COLLECTOR_SERVICE);
		collectorManagerCache = collectorManagerAdvertiser.getServiceCache();
		collectorManagerCache.addListener(new CollectorManagerCacheListener());

		ropeManagerAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.ROPE_SERVICE);
		ropeManagerCache = ropeManagerAdvertiser.getServiceCache();
		ropeManagerCache.addListener(new RopeManagerCacheListener());

		storageServiceAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.STORAGE_SERVICE);
		storageServiceCache = storageServiceAdvertiser.getServiceCache();
		storageServiceCache.addListener(new StorageServiceCacheListener());

		try
		{
			eventServiceCache.start();
			collectorManagerCache.start();
			ropeManagerCache.start();
			storageServiceCache.start();
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	public void registerAsEventService()
	{
		String host = props.get("server_host", "localhost");
		int port = props.get("server_port", 8080);

		try
		{
			localEventServiceMetaData = eventServiceAdvertiser.available(host, port);
			logger.info("Registering EventService server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register EventService host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsEventService()
	{
		eventServiceAdvertiser.unavailable(localEventServiceMetaData);
		logger.info("Unregistered EventService server host=" + localEventServiceMetaData.getListenAddress() + " port=" + localEventServiceMetaData.getListenPort());
	}

	public ServiceMetaData getLocalCollectorManagerMetaData()
	{
		return localCollectorManagerMetaData;
	}

	public void registerAsCollectorManager()
	{
		String host = props.get("server_host", "localhost");
		int port = props.get("server_port", 8080);

		try
		{
			localCollectorManagerMetaData = collectorManagerAdvertiser.available(host, port);
			logger.info("Registering CollectorManager server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register CollectorManager host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsCollectorManager()
	{
		collectorManagerAdvertiser.unavailable(localCollectorManagerMetaData);
		logger.info("Unregistered CollectorManager server host=" + localCollectorManagerMetaData.getListenAddress() + " port=" + localCollectorManagerMetaData.getListenPort());
	}

	public void registerAsRopeManager()
	{
		String host = props.get("server_host", "localhost");
		int port = props.get("server_port", 8080);

		try
		{
			localRopeManagerMetaData = ropeManagerAdvertiser.available(host, port);
			logger.info("Registering RopeManager server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register RopeManager host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsRopeManager()
	{
		ropeManagerAdvertiser.unavailable(localRopeManagerMetaData);
		logger.info("Unregistered RopeManager server host=" + localRopeManagerMetaData.getListenAddress() + " port=" + localRopeManagerMetaData.getListenPort());
	}

	public ServiceMetaData getLocalRopeManagerMetaData()
	{
		return localRopeManagerMetaData;
	}

	public ServiceMetaData getRopeManagerMetaDataById(String id)
	{
		for (ServiceInstance<ServiceMetaData> serviceInstance : ropeManagerCache.getInstances())
		{
			if (serviceInstance.getId().equals(id))
			{
				return serviceInstance.getPayload();
			}
		}

		return null;
	}

	public void registerAsStorageService()
	{
		String host = props.get("server_host", "localhost");
		int port = props.get("server_port", 8080);

		try
		{
			localStorageServiceMetaData = storageServiceAdvertiser.available(host, port);
			logger.info("Registering StorageService server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register StorageService host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsStorageService()
	{
		storageServiceAdvertiser.unavailable(localStorageServiceMetaData);
		logger.info("Unregistered StorageService server host=" + localStorageServiceMetaData.getListenAddress() + " port=" + localStorageServiceMetaData.getListenPort());
	}

	public ServiceMetaData getStorageServiceRandom()
	{
		try
		{
			ServiceInstance<ServiceMetaData> serviceInstance = randomStorageStrategy.getInstance(storageServiceCache);
			if (serviceInstance != null)
			{
				return serviceInstance.getPayload();
			}
			return null;
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	public ServiceMetaData getStorageServiceRoundRobin()
	{
		try
		{
			ServiceInstance<ServiceMetaData> serviceInstance = roundRobinStorageStrategy.getInstance(storageServiceCache);
			if (serviceInstance != null)
			{
				return serviceInstance.getPayload();
			}
			return null;
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	public ServiceMetaData getStorageServiceSticky()
	{
		try
		{
			ServiceInstance<ServiceMetaData> serviceInstance = stickyStorageStrategy.getInstance(storageServiceCache);
			if (serviceInstance != null)
			{
				return serviceInstance.getPayload();
			}
			return null;
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	public List<ServiceMetaData> getRegisteredRopeManagers()
	{
		List<ServiceMetaData> serviceMetaDataList = new ArrayList<>();

		for (ServiceInstance<ServiceMetaData> serviceMetaData : ropeManagerCache.getInstances())
		{
			serviceMetaDataList.add(serviceMetaData.getPayload());
		}

		return serviceMetaDataList;
	}

	public void shutdown()
	{
		try
		{
			eventServiceCache.close();
			collectorManagerCache.close();
			ropeManagerCache.close();
			storageServiceCache.close();

			logger.info("Registration ServiceCache is shutdown.");
		}
		catch (IOException | IllegalStateException e)
		{
			logger.error("Failed to close the Service Cache");
		}
	}

	private class EventServiceCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered EventService list has been updated. " + eventServiceCache.getInstances().size() + " EventService(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
		}
	}

	private class CollectorManagerCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered CollectorManager list has been updated. " + collectorManagerCache.getInstances().size() + " CollectorManager(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
		}
	}

	private class RopeManagerCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered RopeManager list has been updated. " + ropeManagerCache.getInstances().size() + " RopeManager(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
		}
	}

	private class StorageServiceCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered StorageService list has been updated. " + storageServiceCache.getInstances().size() + " StorageService(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
		}
	}

}
