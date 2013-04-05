package com.gltech.scale.monitoring.server;

import com.gltech.scale.core.cluster.ClusterService;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.lifecycle.LifeCycleManager;
import com.gltech.scale.monitoring.services.GatheringService;
import com.gltech.scale.util.Props;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class EmbeddedServer
{
	private static final Logger logger = LoggerFactory.getLogger(EmbeddedServer.class);
	private static Server server;
	private static Props props = Props.getProps();
	private static Injector injector;

	public static void main(String[] args) throws Exception
	{
		start(9090);
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

		// static files handler
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(true);
		resourceHandler.setResourceBase("public_html");
		resourceHandler.setWelcomeFiles(new String[]{"index.html"});

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
		servletContextHandler.addServlet(DefaultServlet.class, "/aggregator/*");
		servletContextHandler.addServlet(DefaultServlet.class, "/inbound/*");
		servletContextHandler.addServlet(DefaultServlet.class, "/storagewriter/*");
		servletContextHandler.addServlet(DefaultServlet.class, "/stats/*");

		// websocket handler
//		myWebSocketHandler myWebSocketHandler = new myWebSocketHandler();

		// Bind all resources
		HandlerCollection handlerList = new HandlerCollection();
//		handlerList.setHandlers(new Handler[]{webSocketHandler,servletContextHandler,resourceHandler});
		handlerList.setHandlers(new Handler[]{servletContextHandler,resourceHandler});
		server.setHandler(handlerList);

		// Handle all non jersey services here
		startServices();

		// Start the server
		server.start();
		logger.info("server started on port " + port);
	}

	private static void startServices()
	{
		// Registered CoordinationService for shutdown
		final ClusterService clusterService = injector.getInstance(ClusterService.class);
		clusterService.getRegistrationService().registerAsServer();
		LifeCycleManager.getInstance().add(clusterService, LifeCycle.Priority.FINAL);

		// Registered StatsManager for shutdown
		final GatheringService gatheringService = injector.getInstance(GatheringService.class);
		gatheringService.start();
		LifeCycleManager.getInstance().add(gatheringService, LifeCycle.Priority.FINAL);

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