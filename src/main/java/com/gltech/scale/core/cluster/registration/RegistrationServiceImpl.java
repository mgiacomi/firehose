package com.gltech.scale.core.cluster.registration;

import com.gltech.scale.core.cluster.ClusterException;
import com.gltech.scale.core.model.Defaults;
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
	private ServiceAdvertiser inboundServiceAdvertiser;
	private ServiceAdvertiser storageWriterAdvertiser;
	private ServiceAdvertiser aggregatorAdvertiser;
	private ServiceMetaData localInboundServiceMetaData;
	private ServiceMetaData localStorageWriterMetaData;
	private ServiceMetaData localAggregatorMetaData;
	private ServiceCache<ServiceMetaData> inboundServiceCache;
	private ServiceCache<ServiceMetaData> storageWriterCache;
	private ServiceCache<ServiceMetaData> aggregatorCache;
	private RandomStrategy<ServiceMetaData> randomStorageStrategy = new RandomStrategy<>();
	private RoundRobinStrategy<ServiceMetaData> roundRobinStorageStrategy = new RoundRobinStrategy<>();
	private StickyStrategy<ServiceMetaData> stickyStorageStrategy = new StickyStrategy<>(randomStorageStrategy);
	private Props props = Props.getProps();

	public RegistrationServiceImpl()
	{
		inboundServiceAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.INBOUND_SERVICE);
		inboundServiceCache = inboundServiceAdvertiser.getServiceCache();
		inboundServiceCache.addListener(new InboundServiceCacheListener());

		storageWriterAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.STORAGE_WRITER);
		storageWriterCache = storageWriterAdvertiser.getServiceCache();
		storageWriterCache.addListener(new StorageWriterCacheListener());

		aggregatorAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.AGGREGATOR_SERVICE);
		aggregatorCache = aggregatorAdvertiser.getServiceCache();
		aggregatorCache.addListener(new AggregatorCacheListener());

		try
		{
			inboundServiceCache.start();
			storageWriterCache.start();
			aggregatorCache.start();
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	public void registerAsInboundService()
	{
		String host = props.get("server_host", Defaults.REST_HOST);
		int port = props.get("server_port", Defaults.REST_PORT);

		try
		{
			localInboundServiceMetaData = inboundServiceAdvertiser.available(host, port);
			logger.info("Registering InboundService server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register InboundService host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsInboundService()
	{
		inboundServiceAdvertiser.unavailable(localInboundServiceMetaData);
		logger.info("Unregistered InboundService server host=" + localInboundServiceMetaData.getListenAddress() + " port=" + localInboundServiceMetaData.getListenPort());
	}

	public ServiceMetaData getLocalStorageWriterMetaData()
	{
		return localStorageWriterMetaData;
	}

	public void registerAsStorageWriter()
	{
		String host = props.get("storage_writer.rest_host", Defaults.REST_HOST);
		int port = props.get("storage_writer.rest_port", Defaults.REST_PORT);

		try
		{
			localStorageWriterMetaData = storageWriterAdvertiser.available(host, port);
			logger.info("Registering StorageWriter server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register StorageWriter host=" + host + " port=" + port, e);
		}
	}

	public void unRegisterAsStorageWriter()
	{
		storageWriterAdvertiser.unavailable(localStorageWriterMetaData);
		logger.info("Unregistered StorageWriter server host=" + localStorageWriterMetaData.getListenAddress() + " port=" + localStorageWriterMetaData.getListenPort());
	}

	public void registerAsAggregator()
	{
		String host = props.get("aggregator.rest_host", Defaults.REST_HOST);
		int port = props.get("aggregator.rest_port", Defaults.REST_PORT);

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
			inboundServiceCache.close();
			storageWriterCache.close();
			aggregatorCache.close();

			logger.info("Registration ServiceCache is shutdown.");
		}
		catch (IOException | IllegalStateException e)
		{
			logger.error("Failed to close the Service Cache");
		}
	}

	private class InboundServiceCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered InboundService list has been updated. " + inboundServiceCache.getInstances().size() + " InboundService(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
		}
	}

	private class StorageWriterCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered StorageWriter list has been updated. " + storageWriterCache.getInstances().size() + " StorageWriters(s) are active.");
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
			logger.info("Registered aggregator list has been updated. " + aggregatorCache.getInstances().size() + " aggregator(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
		}
	}
}
