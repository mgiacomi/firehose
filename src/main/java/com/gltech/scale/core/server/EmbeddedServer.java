package com.gltech.scale.core.server;

import com.gltech.scale.core.cluster.ChannelCoordinator;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.StatCallBack;
import com.gltech.scale.core.stats.StatsManager;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import com.gltech.scale.core.writer.StorageWriteManager;
import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.inbound.InboundService;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.lifecycle.LifeCycleManager;
import com.gltech.scale.core.aggregator.Aggregator;
import com.gltech.scale.core.aggregator.WeightManager;
import com.gltech.scale.util.Props;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.TimeUnit;

public class EmbeddedServer
{
	private static final Logger logger = LoggerFactory.getLogger(EmbeddedServer.class);
	private static Server server;
	private static Props props = Props.getProps();
	private static Injector injector;

	public static void main(String[] args) throws Exception
	{
		start(8080);
		server.join();
	}

	public static synchronized void start(int port) throws Exception
	{
		start(port, null);
	}

	public static synchronized void start(int port, Module moduleOverride) throws Exception
	{
		props.set("server_port", port);

		if (server != null)
		{
			logger.info("server already started");
			return;
		}
		logger.info("starting server on port " + port);

		// Register ProtoStuff classes
		Defaults.registerProtoClasses();

		// Create the server.
		server = new Server(port);

		// servlet handler
		ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
		servletContextHandler.setContextPath("/");

		GuiceServletConfig guiceServletConfig = new GuiceServletConfig(moduleOverride);
		servletContextHandler.addEventListener(guiceServletConfig);
		injector = guiceServletConfig.getInjector();

		// Then add GuiceFilter and configure the server to
		// reroute all requests through this filter.
		servletContextHandler.addFilter(GuiceFilter.class, "/*", null);

		// Must add DefaultServlet for embedded Jetty.
		// Failing to do this will cause 404 errors.
		// This is not needed if web.xml is used instead.
		servletContextHandler.addServlet(DefaultServlet.class, "/");

		// Bind all resources
		HandlerCollection handlerList = new HandlerCollection();
		handlerList.setHandlers(new Handler[]{servletContextHandler});
		server.setHandler(handlerList);

		// Handle all non jersey services here
		startServices();

		// Monitor System level stats.
		monitorSystemStats();

		// Start the server
		server.start();
		logger.info("Server started on port {}", port);
	}

	private static void startServices()
	{
		// Registered CoordinationService for shutdown
		final ClusterService clusterService = injector.getInstance(ClusterService.class);
		clusterService.getRegistrationService().registerAsServer();
		LifeCycleManager.getInstance().add(clusterService, LifeCycle.Priority.FINAL);

		// Registered StatsManager for shutdown
		final StatsManager statsManager = injector.getInstance(StatsManager.class);
		statsManager.start();
		LifeCycleManager.getInstance().add(statsManager, LifeCycle.Priority.FINAL);

		if (props.get("enable.inbound_service", true))
		{
			// Registered InboundService for shutdown
			InboundService inboundService = injector.getInstance(InboundService.class);
			LifeCycleManager.getInstance().add(inboundService, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.aggregator", true))
		{
			// Registered aggregator for shutdown
			Aggregator aggregator = injector.getInstance(Aggregator.class);
			LifeCycleManager.getInstance().add(aggregator, LifeCycle.Priority.INITIAL);

			// Start WeightManager and registered it for shutdown
			WeightManager weightManager = injector.getInstance(WeightManager.class);
			weightManager.start();
			LifeCycleManager.getInstance().add(weightManager, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.storage_writer", true))
		{
			// Start the CollectorManager and register it for shutdown
			StorageWriteManager storageWriteManager = injector.getInstance(StorageWriteManager.class);
			storageWriteManager.setInjector(injector);
			storageWriteManager.start();
			LifeCycleManager.getInstance().add(storageWriteManager, LifeCycle.Priority.INITIAL);

			// Start the ChannelCoordinator and register it for shutdown
			ChannelCoordinator channelCoordinator = injector.getInstance(ChannelCoordinator.class);
			channelCoordinator.start();
			LifeCycleManager.getInstance().add(channelCoordinator, LifeCycle.Priority.INITIAL);
		}
	}

	private static void monitorSystemStats() {
		final OperatingSystemMXBean osStats = ManagementFactory.getOperatingSystemMXBean();
		StatsManager statsManager = injector.getInstance(StatsManager.class);

		statsManager.createAvgStat("System", "LoadAvg", "LoadUnits", new StatCallBack()
		{
			public long getValue()
			{
				return Math.round(osStats.getSystemLoadAverage());
			}
		});


	}

	public static synchronized void stop() throws Exception
	{
		logger.info("stopping server");
		server.stop();

		LifeCycleManager.getInstance().shutdown();

		int retryCount = 10;
		while (!server.isStopped() && retryCount > 0)
		{
			retryCount++;
			TimeUnit.MINUTES.sleep(1);
		}

		server = null;
		logger.info("server shutdown");
	}

	public static Injector getInjector()
	{
		return injector;
	}
}