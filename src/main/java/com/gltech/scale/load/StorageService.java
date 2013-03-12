package com.gltech.scale.load;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.bytearray.BucketOnlyStorage;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.storage.bytearray.ValidatingStorage;
import com.gltech.scale.util.Props;
import org.slf4j.LoggerFactory;

public class StorageService
{
	public static void main(String[] args) throws Exception
	{
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);
		Props props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/storage.properties");

		EmbeddedServer.start(Integer.valueOf(args[0]), new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(ByteArrayStorage.class).toInstance(new ValidatingStorage(new BucketOnlyStorage()));
			}
		});
	}
}
