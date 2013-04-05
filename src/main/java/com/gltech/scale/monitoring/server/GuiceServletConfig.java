package com.gltech.scale.monitoring.server;

import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.cluster.ClusterServiceImpl;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.cluster.registration.RegistrationServiceImpl;
import com.gltech.scale.monitoring.resources.MonitoringResource;
import com.gltech.scale.monitoring.services.ClusterStatsService;
import com.gltech.scale.monitoring.services.ClusterStatsServiceImpl;
import com.gltech.scale.util.Props;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.util.Modules;
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
					bind(MonitoringResource.class);
					bind(ClusterService.class).to(ClusterServiceImpl.class).in(Singleton.class);
					bind(RegistrationService.class).to(RegistrationServiceImpl.class).in(Singleton.class);

					// Monitoring Specific
					bind(ClusterStatsService.class).to(ClusterStatsServiceImpl.class).in(Singleton.class);

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