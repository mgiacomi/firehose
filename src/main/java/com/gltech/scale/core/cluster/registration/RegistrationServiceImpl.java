package com.gltech.scale.core.cluster.registration;

import com.gltech.scale.core.cluster.ClusterException;
import com.gltech.scale.core.model.Defaults;
import com.google.common.base.Throwables;
import com.gltech.scale.util.Props;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceCache;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import com.netflix.curator.x.discovery.strategies.RoundRobinStrategy;
import com.netflix.curator.x.discovery.strategies.StickyStrategy;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class RegistrationServiceImpl implements RegistrationService
{
	private static final Logger logger = LoggerFactory.getLogger(RegistrationServiceImpl.class);
	private final ServiceMetaData localServerMetaData;
	private ServiceAdvertiser serverAdvertiser;
	private ServiceAdvertiser inboundServiceAdvertiser;
	private ServiceAdvertiser outboundServiceAdvertiser;
	private ServiceAdvertiser storageWriterAdvertiser;
	private ServiceAdvertiser aggregatorAdvertiser;
	private ServiceCache<ServiceMetaData> serverCache;
	private ServiceCache<ServiceMetaData> inboundServiceCache;
	private ServiceCache<ServiceMetaData> outboundServiceCache;
	private ServiceCache<ServiceMetaData> storageWriterCache;
	private ServiceCache<ServiceMetaData> aggregatorCache;
	private RandomStrategy<ServiceMetaData> randomStorageStrategy = new RandomStrategy<>();
	private RoundRobinStrategy<ServiceMetaData> roundRobinStorageStrategy = new RoundRobinStrategy<>();
	private StickyStrategy<ServiceMetaData> stickyStorageStrategy = new StickyStrategy<>(randomStorageStrategy);
	private Props props = Props.getProps();

	public RegistrationServiceImpl()
	{
		String host = props.get("server_host", Defaults.REST_HOST);
		int port = props.get("server_port", -1);

		localServerMetaData = new ServiceMetaData(UUID.randomUUID(), host, port);

		serverAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.SERVER);
		serverCache = serverAdvertiser.getServiceCache();
		serverCache.addListener(new ServerCacheListener());

		inboundServiceAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.INBOUND_SERVICE);
		inboundServiceCache = inboundServiceAdvertiser.getServiceCache();
		inboundServiceCache.addListener(new InboundServiceCacheListener());

		outboundServiceAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.OUTBOUND_SERVICE);
		outboundServiceCache = outboundServiceAdvertiser.getServiceCache();
		outboundServiceCache.addListener(new OutboundServiceCacheListener());

		storageWriterAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.STORAGE_WRITER);
		storageWriterCache = storageWriterAdvertiser.getServiceCache();
		storageWriterCache.addListener(new StorageWriterCacheListener());

		aggregatorAdvertiser = new ServiceAdvertiser(ServiceAdvertiser.AGGREGATOR_SERVICE);
		aggregatorCache = aggregatorAdvertiser.getServiceCache();
		aggregatorCache.addListener(new AggregatorCacheListener());

		try
		{
			serverCache.start();
			inboundServiceCache.start();
			outboundServiceCache.start();
			storageWriterCache.start();
			aggregatorCache.start();
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public Set<String> getRoles()
	{
		Set<String> roles = new HashSet<>();

		for (ServiceInstance<ServiceMetaData> serviceMetaData : inboundServiceCache.getInstances())
		{
			if (localServerMetaData.equals(serviceMetaData.getPayload()))
			{
				roles.add("Inbound");
			}
		}

		for (ServiceInstance<ServiceMetaData> serviceMetaData : outboundServiceCache.getInstances())
		{
			if (localServerMetaData.equals(serviceMetaData.getPayload()))
			{
				roles.add("Outbound");
			}
		}

		for (ServiceInstance<ServiceMetaData> serviceMetaData : aggregatorCache.getInstances())
		{
			if (localServerMetaData.equals(serviceMetaData.getPayload()))
			{
				roles.add("Aggregator");
			}
		}

		for (ServiceInstance<ServiceMetaData> serviceMetaData : storageWriterCache.getInstances())
		{
			if (localServerMetaData.equals(serviceMetaData.getPayload()))
			{
				roles.add("StorageWriter");
			}
		}

		return roles;
	}

	@Override
	public DateTime getLocalServerRegistrationTime()
	{
		for (ServiceInstance<ServiceMetaData> serviceMetaData : serverCache.getInstances())
		{
			if (localServerMetaData.equals(serviceMetaData.getPayload()))
			{
				return new DateTime(serviceMetaData.getRegistrationTimeUTC(), DateTimeZone.UTC).withZone(DateTimeZone.getDefault());
			}
		}

		return null;
	}

	@Override
	public ServiceMetaData getLocalServerMetaData()
	{
		return localServerMetaData;
	}

	@Override
	public void registerAsServer()
	{
		if (localServerMetaData.getListenPort() == -1)
		{
			throw new IllegalStateException("RegistrationService could not determine the port for the Server on this host.");
		}

		try
		{
			serverAdvertiser.available(localServerMetaData);
			logger.info("Registering Server server host={} port={}", localServerMetaData.getListenAddress(), localServerMetaData.getListenPort());
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register Server host=" + localServerMetaData.getListenAddress() + " port=" + localServerMetaData.getListenPort(), e);
		}
	}

	@Override
	public void unRegisterAsServer()
	{
		serverAdvertiser.unavailable(localServerMetaData);
		logger.info("Unregistered server host=" + localServerMetaData.getListenAddress() + " port=" + localServerMetaData.getListenPort());
	}

	@Override
	public List<ServiceMetaData> getRegisteredServers()
	{
		List<ServiceMetaData> serviceMetaDataList = new ArrayList<>();

		for (ServiceInstance<ServiceMetaData> serviceMetaData : serverCache.getInstances())
		{
			serviceMetaDataList.add(serviceMetaData.getPayload());
		}

		return serviceMetaDataList;
	}

	@Override
	public void registerAsInboundService()
	{
		String host = props.get("server_host", Defaults.REST_HOST);
		int port = props.get("server_port", -1);

		if (port == -1)
		{
			throw new IllegalStateException("RegistrationService could not determine the port for the InboundService on this host.");
		}

		try
		{
			inboundServiceAdvertiser.available(localServerMetaData);
			logger.info("Registering InboundService server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register InboundService host=" + host + " port=" + port, e);
		}
	}

	@Override
	public void unRegisterAsInboundService()
	{
		inboundServiceAdvertiser.unavailable(localServerMetaData);
		logger.info("Unregistered InboundService server host=" + localServerMetaData.getListenAddress() + " port=" + localServerMetaData.getListenPort());
	}

	@Override
	public void registerAsOutboundService()
	{
		String host = props.get("server_host", Defaults.REST_HOST);
		int port = props.get("server_port", -1);

		if (port == -1)
		{
			throw new IllegalStateException("RegistrationService could not determine the port for the OutboundService on this host.");
		}

		try
		{
			outboundServiceAdvertiser.available(localServerMetaData);
			logger.info("Registering OutboundService server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register OutboundService host=" + host + " port=" + port, e);
		}
	}

	@Override
	public void unRegisterAsOutboundService()
	{
		outboundServiceAdvertiser.unavailable(localServerMetaData);
		logger.info("Unregistered OutboundService server host=" + localServerMetaData.getListenAddress() + " port=" + localServerMetaData.getListenPort());
	}

	@Override
	public void registerAsStorageWriter()
	{
		String host = props.get("storage_writer.rest_host", Defaults.REST_HOST);
		int port = props.get("server_port", -1);

		if (port == -1)
		{
			throw new IllegalStateException("RegistrationService could not determine the port for the StorageWriter on this host.");
		}

		try
		{
			storageWriterAdvertiser.available(localServerMetaData);
			logger.info("Registering StorageWriter server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register StorageWriter host=" + host + " port=" + port, e);
		}
	}

	@Override
	public void unRegisterAsStorageWriter()
	{
		storageWriterAdvertiser.unavailable(localServerMetaData);
		logger.info("Unregistered StorageWriter server host=" + localServerMetaData.getListenAddress() + " port=" + localServerMetaData.getListenPort());
	}

	@Override
	public void registerAsAggregator()
	{
		String host = props.get("aggregator.rest_host", Defaults.REST_HOST);
		int port = props.get("server_port", -1);

		if (port == -1)
		{
			throw new IllegalStateException("RegistrationService could not determine the port for the Aggregator on this host.");
		}

		try
		{
			aggregatorAdvertiser.available(localServerMetaData);
			logger.info("Registering aggregator server host=" + host + " port=" + port);
		}
		catch (Exception e)
		{
			throw new ClusterException("Failed to register aggregator host=" + host + " port=" + port, e);
		}
	}

	@Override
	public void unRegisterAsAggregator()
	{
		aggregatorAdvertiser.unavailable(localServerMetaData);
		logger.info("Unregistered aggregator server host=" + localServerMetaData.getListenAddress() + " port=" + localServerMetaData.getListenPort());
	}

	@Override
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

	@Override
	public List<ServiceMetaData> getRegisteredAggregators()
	{
		List<ServiceMetaData> serviceMetaDataList = new ArrayList<>();

		for (ServiceInstance<ServiceMetaData> serviceMetaData : aggregatorCache.getInstances())
		{
			serviceMetaDataList.add(serviceMetaData.getPayload());
		}

		return serviceMetaDataList;
	}

	@Override
	public void shutdown()
	{
		try
		{
			unRegisterAsServer();

			serverAdvertiser.close();
			inboundServiceAdvertiser.close();
			outboundServiceAdvertiser.close();
			storageWriterAdvertiser.close();
			aggregatorAdvertiser.close();

			logger.info("Registration ServiceCache is shutdown.");
		}
		catch (IllegalStateException e)
		{
			logger.error("Failed to close the Service Advertisers");
		}
	}

	private class ServerCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered Server list has been updated. " + serverCache.getInstances().size() + " Server(s) are active.");
		}

		@Override
		public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
		{
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

	private class OutboundServiceCacheListener implements ServiceCacheListener
	{
		@Override
		public void cacheChanged()
		{
			logger.info("Registered OutboundService list has been updated. " + outboundServiceCache.getInstances().size() + " OutboundService(s) are active.");
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
