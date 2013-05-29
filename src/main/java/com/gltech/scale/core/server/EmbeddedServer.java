package com.gltech.scale.core.server;

import ch.qos.logback.classic.Level;
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
import com.sun.management.OperatingSystemMXBean;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.TimeUnit;

public class EmbeddedServer
{
	private static final Logger logger = LoggerFactory.getLogger(EmbeddedServer.class);
	private static Server server;
	private static Props props = Props.getProps();
	private static Injector injector;

	public static void main(String[] args) throws Exception
	{
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);

		if (args.length > 0)
		{
			props.loadFromFile(args[0]);
		}
		if (args.length > 1)
		{
			props.set("server_host", args[1]);
		}

		if ("exception".equals(props.get("server_host", "exception")))
		{
			throw new IllegalStateException("A 'server_host' needs to be specified either in the properties file or on the command line.");
		}

		start(props.get("server_port", 8080));
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

		MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());

		// Register ProtoStuff classes
		Defaults.registerProtoClasses();

		// Create the server.
		server = new Server(port);
		server.addBean(mbContainer);

		StatisticsHandler statsHandler = new StatisticsHandler();

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
		handlerList.setHandlers(new Handler[]{statsHandler, servletContextHandler});
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

		if (props.get("enable.inbound_service", false))
		{
			// Registered InboundService for shutdown
			InboundService inboundService = injector.getInstance(InboundService.class);
			LifeCycleManager.getInstance().add(inboundService, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.aggregator", false))
		{
			// Registered aggregator for shutdown
			Aggregator aggregator = injector.getInstance(Aggregator.class);
			LifeCycleManager.getInstance().add(aggregator, LifeCycle.Priority.INITIAL);

			// Start WeightManager and registered it for shutdown
			WeightManager weightManager = injector.getInstance(WeightManager.class);
			weightManager.start();
			LifeCycleManager.getInstance().add(weightManager, LifeCycle.Priority.INITIAL);
		}

		if (props.get("enable.storage_writer", false))
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

	private static void monitorSystemStats()
	{
		final OperatingSystemMXBean osStats = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		StatsManager statsManager = injector.getInstance(StatsManager.class);

		statsManager.createAvgStat("Common", "Request_Count", "number", new StatCallBack()
		{
			int prevRequests = -1;
			int result = -1;

			public long getValue()
			{
				try
				{
					ObjectName objectName = new ObjectName("org.eclipse.jetty.server.handler:type=statisticshandler,id=0");
					int requests = (Integer) server.getAttribute(objectName, "requests");

					if (prevRequests > -1)
					{
						result = requests - prevRequests;
					}
					prevRequests = requests;
				}
				catch (Exception e)
				{
					logger.error("Failed to query JMX Atrributes.", e);
				}
				return result;
			}
		});

		statsManager.createAvgStat("Common", "ActiveRequest_Count", "number", new StatCallBack()
		{
			public long getValue()
			{
				try
				{
					ObjectName objectName = new ObjectName("org.eclipse.jetty.server.handler:type=statisticshandler,id=0");
					return (Integer) server.getAttribute(objectName, "requestsActive");
				}
				catch (Exception e)
				{
					logger.error("Failed to query JMX Atrributes.", e);
				}
				return -1;
			}
		});

		statsManager.createAvgStat("Common", "LoadAvg", "LoadUnits", new StatCallBack()
		{
			public long getValue()
			{
				return Math.round(osStats.getSystemLoadAverage());
			}
		});

		statsManager.createAvgStat("Common", "Threads", "number", new StatCallBack()
		{
			public long getValue()
			{
				return Thread.activeCount();
			}
		});

		statsManager.createAvgStat("Common", "CPU", "%", new StatCallBack()
		{
			long prevProcessCpuTime = -1;
			long prevUpTime = 0;
			long result = 0;

			public long getValue()
			{
				long processCpuTime = osStats.getProcessCpuTime();
				long upTime = runtimeMXBean.getUptime();

				if (prevProcessCpuTime > -1)
				{
					long elapsedCpu = osStats.getProcessCpuTime() - prevProcessCpuTime;
					long elapsedTime = upTime - prevUpTime;
					// cpuUsage could go higher than 100% because elapsedTime
					// and elapsedCpu are not fetched simultaneously. Limit to
					// 99% to avoid Plotter showing a scale from 0% to 200%.
					result = (long) Math.min(100L, elapsedCpu / (elapsedTime * 10000F * osStats.getAvailableProcessors()));
				}
				prevProcessCpuTime = processCpuTime;
				prevUpTime = upTime;

				return result;
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