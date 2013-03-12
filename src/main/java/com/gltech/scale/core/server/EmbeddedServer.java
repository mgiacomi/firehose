package com.gltech.scale.core.server;

import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import com.gltech.scale.core.collector.CollectorManager;
import com.gltech.scale.core.coordination.CoordinationService;
import com.gltech.scale.core.coordination.RopeCoordinator;
import com.gltech.scale.core.inbound.InboundService;
import com.gltech.scale.core.lifecycle.LifeCycle;
import com.gltech.scale.core.lifecycle.LifeCycleManager;
import com.gltech.scale.core.monitor.MonitoringPublisher;
import com.gltech.scale.core.monitor.StatisticsFilter;
import com.gltech.scale.core.monitor.TimerMap;
import com.gltech.scale.core.monitor.TimerMapPublishMetricGroup;
import com.gltech.scale.core.rope.RopeManager;
import com.gltech.scale.core.rope.WeightManager;
import com.gltech.scale.core.util.Props;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedServer
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.server.EmbeddedServer");
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

		// Create the server.
		server = new Server(port);

		// Create a servlet context and add the jersey servlet.
		ServletContextHandler sch = new ServletContextHandler(server, "/");

		// Add our Guice listener that includes our bindings
		GuiceServletConfig guiceServletConfig = new GuiceServletConfig(moduleOverride);
		sch.addEventListener(guiceServletConfig);
		injector = guiceServletConfig.getInjector();

		TimerMap timerMap = new TimerMap();
		MonitoringPublisher.getInstance().register(new TimerMapPublishMetricGroup("Loki REST", timerMap));
		StatisticsFilter.add(timerMap);
		StatisticsFilter.add("/events/", "Events");
		StatisticsFilter.add("/storage/", "Storage");
		StatisticsFilter.add("/ropes/", "Ropes");
		sch.addFilter(StatisticsFilter.class, "/*", null);

		// Then add GuiceFilter and configure the server to
		// reroute all requests through this filter.
		sch.addFilter(GuiceFilter.class, "/*", null);

		// Must add DefaultServlet for embedded Jetty.
		// Failing to do this will cause 404 errors.
		// This is not needed if web.xml is used instead.
		sch.addServlet(DefaultServlet.class, "/");

		// Setup Handler for html files
		ResourceHandler resource_handler = new ResourceHandler();
		resource_handler.setDirectoriesListed(true);
		resource_handler.setWelcomeFiles(new String[]{"index.html"});

		resource_handler.setResourceBase("public_html");

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[]{resource_handler, new DefaultHandler()});
		server.setHandler(handlers);

		// Handle all non jersey services here
		startServices();

		// Start the server
		server.start();
		logger.info("server started on port " + port);
	}

	private static void startServices()
	{
		// Registered CoordinationService for shutdown
		final CoordinationService coordinationService = injector.getInstance(CoordinationService.class);
		LifeCycleManager.getInstance().add(coordinationService, LifeCycle.Priority.FINAL);

		if (props.get("enable.event_service", true))
		{
			// Registered EventService for shutdown
			InboundService inboundService = injector.getInstance(InboundService.class);
			LifeCycleManager.getInstance().add(inboundService, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.rope_manager", true))
		{
			// Registered RopeManager for shutdown
			RopeManager ropeManager = injector.getInstance(RopeManager.class);
			LifeCycleManager.getInstance().add(ropeManager, LifeCycle.Priority.INITIAL);

			// Start WeightManager and registered it for shutdown
			WeightManager weightManager = injector.getInstance(WeightManager.class);
			new Thread(weightManager, "WeightManager").start();
			LifeCycleManager.getInstance().add(weightManager, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.collector_manager", true))
		{
			// Start the CollectorManager and register it for shutdown
			CollectorManager collectorManager = injector.getInstance(CollectorManager.class);
			collectorManager.setInjector(injector);
			new Thread(collectorManager, "CollectorManager").start();
			LifeCycleManager.getInstance().add(collectorManager, LifeCycle.Priority.INITIAL);

			// Start the RopeCoordinator and register it for shutdown
			RopeCoordinator ropeCoordinator = injector.getInstance(RopeCoordinator.class);
			new Thread(ropeCoordinator, "RopeCoordinator").start();
			LifeCycleManager.getInstance().add(ropeCoordinator, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.storage_service", true))
		{
			// Register the storage service with the coordination service and register for shutdown
			coordinationService.getRegistrationService().registerAsStorageService();

			LifeCycleManager.getInstance().add(new LifeCycle()
			{
				public void shutdown()
				{
					coordinationService.getRegistrationService().unRegisterAsStorageService();
				}
			}, LifeCycle.Priority.INITIAL);
		}
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
			Thread.sleep(1000);
		}

		server = null;
		logger.info("server shutdown");
	}

	public static Injector getInjector()
	{
		return injector;
	}
}