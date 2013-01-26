package com.gltech.scale.core.server;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.util.Modules;
import com.gltech.scale.core.collector.*;
import com.gltech.scale.core.coordination.CoordinationService;
import com.gltech.scale.core.coordination.RopeCoordinator;
import com.gltech.scale.core.coordination.RopeCoordinatorImpl;
import com.gltech.scale.core.coordination.ZookeeperCoordinationService;
import com.gltech.scale.core.coordination.registration.RegistrationService;
import com.gltech.scale.core.coordination.registration.RegistrationServiceImpl;
import com.gltech.scale.core.event.EventResource;
import com.gltech.scale.core.event.EventService;
import com.gltech.scale.core.event.EventServiceImpl;
import com.gltech.scale.core.rope.*;
import com.gltech.scale.core.storage.*;
import com.gltech.scale.core.storage.bytearray.*;
import com.gltech.scale.core.storage.stream.AwsS3Storage;
import com.gltech.scale.core.util.Props;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

//todo - gfm - 9/24/12 - This seems closely coupled with EmbeddedServer.  Should it be moved there?
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
					bind(CoordinationService.class).to(ZookeeperCoordinationService.class).in(Singleton.class);
					bind(RegistrationService.class).to(RegistrationServiceImpl.class).in(Singleton.class);
					bind(RopeCoordinator.class).to(RopeCoordinatorImpl.class).in(Singleton.class);

					// Service specific injector settings
					if (props.get("enable.event_service", true))
					{
						bind(EventResource.class);
						bind(EventService.class).to(EventServiceImpl.class).in(Singleton.class);
					}

					if (props.get("enable.rope_manager", true))
					{
						bind(RopeResource.class);

						// The two bindings below show how to implement decorator pattern in Guice
						bind(RopeManager.class).to(RopeManagerStats.class).in(Singleton.class);
						bind(RopeManager.class).annotatedWith(Names.named(RopeManagerStats.BASE)).to(RopeManagerImpl.class).in(Singleton.class);
					}

					if (props.get("enable.collector_manager", true))
					{
						bind(CollectorResource.class);

						// These should not be singletons we want to get a new one with each injector call.
						bind(CollectorManager.class).to(CollectorManagerWithCoordination.class);
						bind(TimeBucketCollector.class).to(TimeBucketCollectorImpl.class);
					}

					if (props.get("storage_service.local", false))
					{
						bind(StorageServiceClient.class).to(StorageServiceLocalClient.class).in(Singleton.class);
					}
					else
					{
						bind(StorageServiceClient.class).to(StorageServiceRestClient.class).in(Singleton.class);
					}

					if (props.get("enable.storage_service", true) || props.get("storage_service.local", false))
					{
						bind(StorageResource.class);

						String storageServiceStore = props.get("storage_service.store", "voldemort");

						if ("memory".equalsIgnoreCase(storageServiceStore))
						{
							bind(ByteArrayStorage.class).to(MemoryStorage.class).in(Singleton.class);
							bind(Storage.class).to(ByteArrayStorageAdapter.class).in(Singleton.class);
						}
						else if ("voldemort".equalsIgnoreCase(storageServiceStore))
						{
							bind(ByteArrayStorage.class).to(VoldemortStorage.class).in(Singleton.class);
							bind(Storage.class).to(ByteArrayStorageAdapter.class).in(Singleton.class);
						}
						else if ("bucket_only".equalsIgnoreCase(storageServiceStore))
						{
							bind(ByteArrayStorage.class).to(BucketOnlyStorage.class).in(Singleton.class);
							bind(Storage.class).to(ByteArrayStorageAdapter.class).in(Singleton.class);
						}
						else if ("s3".equalsIgnoreCase(storageServiceStore))
						{
							bind(Storage.class).to(AwsS3Storage.class).in(Singleton.class);
						}
						else
						{
							throw new RuntimeException("storage_service.store type " + storageServiceStore + " is not available.");
						}
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