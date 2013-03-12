package com.gltech.scale.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;

public class VoldemortClientTest
{
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.voldemort.VoldemortTestUtil");
	private static VoldemortServer server;

	public static synchronized void start()
	{
		if (server != null)
		{
			logger.warn("server already started!");
			return;
		}
		String userDir = System.getProperty("user.dir") + "/voldemort/test/";
		logger.info("starting voldemort using dir " + userDir);
		VoldemortConfig config = VoldemortConfig.loadFromVoldemortHome(userDir);
		server = new VoldemortServer(config);
		server.start();
		logger.info("started voldemort ");
	}

	public static synchronized void stop()
	{
		if (server != null)
		{
			server.stop();
			server = null;
			logger.info("stopped voldemort ");
		}
		else
		{
			logger.info("voldemort isn't started yet.");
		}
	}
}
