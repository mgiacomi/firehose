package com.gltech.scale.core.server;

import com.gltech.scale.core.cluster.*;
import com.gltech.scale.core.inbound.InboundServiceImpl;
import com.gltech.scale.core.inbound.InboundServiceStats;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.StatsResource;
import com.gltech.scale.core.storage.providers.AwsS3Store;
import com.gltech.scale.core.storage.providers.VoldemortStore;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.stats.StatsManagerImpl;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.util.Modules;
import com.gltech.scale.core.writer.*;
import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.cluster.ChannelCoordinatorImpl;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.RegistrationServiceImpl;
import com.gltech.scale.core.inbound.InboundResource;
import com.gltech.scale.core.inbound.InboundService;
import com.gltech.scale.core.aggregator.*;
import com.gltech.scale.core.storage.*;
import com.gltech.scale.util.Props;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class GuiceServletConfig extends GuiceServletContextListener
{
	private Props props = Props.getProps();
	private Module moduleOverride;
	private Injector injector;

	public GuiceServletConfig()
	{
	}

	public GuiceServletConfig(Module moduleOverride)
	{
		this.moduleOverride = moduleOverride;
	}

	@Override
	protected Injector getInjector()
	{
		if (injector == null)
		{
			JerseyServletModule jsm = new JerseyServletModule()
			{
				@Override
				protected void configureServlets()
				{
					// Global injector settings
					bind(StatsResource.class);
					bind(ClusterService.class).to(ClusterServiceImpl.class).in(Singleton.class);
					bind(RegistrationService.class).to(RegistrationServiceImpl.class).in(Singleton.class);
					bind(ChannelCoordinator.class).to(ChannelCoordinatorImpl.class).in(Singleton.class);
					bind(StatsManager.class).to(StatsManagerImpl.class).in(Singleton.class);
					bind(ChannelCache.class).to(ChannelCacheImpl.class).in(Singleton.class);

					if (props.get("enable.inbound_service", true))
					{
						bind(InboundResource.class);
						bind(InboundService.class).to(InboundServiceStats.class).in(Singleton.class);
						bind(InboundService.class).annotatedWith(Names.named(InboundServiceStats.BASE)).to(InboundServiceImpl.class).in(Singleton.class);
					}

					if (props.get("enable.aggregator", true))
					{
						bind(AggregatorResource.class);

						// The two bindings below show how to implement decorator pattern in Guice
						bind(Aggregator.class).to(AggregatorStats.class).in(Singleton.class);
						bind(Aggregator.class).annotatedWith(Names.named(AggregatorStats.BASE)).to(AggregatorImpl.class).in(Singleton.class);
					}

					if (props.get("enable.storage_writer", true))
					{
						bind(StorageWriterResource.class);

						// These should not be singletons we want to get a new one with each injector call.
						bind(StorageWriteManager.class).to(StorageWriteManagerImpl.class);
						bind(BatchWriter.class).to(BatchWriterImpl.class);
					}

					// Setup storage provider
					String storageServiceStore = props.get("storage_store", Defaults.STORAGE_STORE);

					if ("voldemort".equalsIgnoreCase(storageServiceStore))
					{
						bind(Storage.class).to(VoldemortStore.class).in(Singleton.class);
					}
					else if ("s3".equalsIgnoreCase(storageServiceStore))
					{
						bind(Storage.class).to(AwsS3Store.class).in(Singleton.class);
					}
					else
					{
						throw new RuntimeException("storage_service.store type " + storageServiceStore + " is not available.");
					}

					serve("/*").with(GuiceContainer.class);
				}
			};

			if (moduleOverride != null)
			{
				injector = Guice.createInjector(Modules.override(jsm).with(moduleOverride));
			}
			else
			{
				injector = Guice.createInjector(jsm);
			}
		}

		return injector;
	}
}