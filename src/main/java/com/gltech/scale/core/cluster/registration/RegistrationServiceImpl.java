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
	private ServiceAdvertiser aggregatorAdvertiser;
	private ServiceAdvertiser storageServiceAdvertiser;
	private ServiceMetaData localEventServiceMetaData;
	private ServiceMetaData localCollectorManagerMetaData;
	private ServiceMetaData localAggregatorMetaData;
	private ServiceMetaData localStorageServiceMetaData;
	private ServiceCache<ServiceMetaData> eventServiceCache;
	private ServiceCache<ServiceMetaData> collectorManagerCache;
	private ServiceCache<ServiceMetaData> aggregatorCache;
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

		aggregatorAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.AGGREGATOR_SERVICE);
		aggregatorCache = aggregatorAdvertiser.getServiceCache();
		aggregatorCache.addListener(new AggregatorCacheListener());

		storageServiceAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.STORAGE_SERVICE);
		storageServiceCache = storageServiceAdvertiser.getServiceCache();
		storageServiceCache.addListener(new StorageServiceCacheListener());

		try
		{
			eventServiceCache.start();
			collectorManagerCache.start();
			aggregatorCache.start();
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

	public void registerAsAggregator()
	{
		String host = props.get("server_host", "localhost");
		int port = props.get("server_port", 8080);

		try
		{
			localAggregatorMetaData = aggregatorAdvertiser.available(host, port);
			logger.info("Registering aggregator server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register aggregator host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsAggregator()
	{
		aggregatorAdvertiser.unavailable(localAggregatorMetaData);
		logger.info("Unregistered aggregator server host=" + localAggregatorMetaData.getListenAddress() + " port=" + localAggregatorMetaData.getListenPort());
	}

	public ServiceMetaData getLocalAggregatorMetaData()
	{
		return localAggregatorMetaData;
	}

	public ServiceMetaData getAggregatorMetaDataById(String id)
	{
		for (ServiceInstance<ServiceMetaData> serviceInstance : aggregatorCache.getInstances())
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

	public List<ServiceMetaData> getRegisteredAggregators()
	{
		List<ServiceMetaData> serviceMetaDataList = new ArrayList<>();

		for (ServiceInstance<ServiceMetaData> serviceMetaData : aggregatorCache.getInstances())
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
			aggregatorCache.close();
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

	private class AggregatorCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered RopeManager list has been updated. " + aggregatorCache.getInstances().size() + " RopeManager(s) are active.");
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
