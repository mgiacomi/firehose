package com.gltech.scale.load;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.util.Props;
import org.slf4j.LoggerFactory;

public class OtherService
{
	public static void main(String[] args) throws Exception
	{
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);
		Props props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/" + args[1] + ".properties");

		EmbeddedServer.start(Integer.valueOf(args[0]));
	}
}
