package com.gltech.scale.core.writer;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.util.Props;
import com.netflix.curator.test.TestingServer;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class StorageWriteManagerTest
{
	static private Props props;

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		props = Props.getProps();
		props.loadFromFile(System.getProperty("user.dir") + "/src/test/resources/props.properties");
	}

	@Test
	public void testStorageWriterStartAndStop() throws Exception
	{
		final StorageWriteTestManager storageWriter = new StorageWriteTestManager();

		TestingServer testingServer = new TestingServer(21818);

		EmbeddedServer.start(9090, new Module()
		{
			public void configure(Binder binder)
			{
				binder.bind(StorageWriteManager.class).toInstance(storageWriter);
			}
		});

		assertTrue(storageWriter.isGotInjector());
		assertTrue(storageWriter.isDidRun());
		assertFalse(storageWriter.isShutdown());

		EmbeddedServer.stop();

		assertTrue(storageWriter.isShutdown());
		assertTrue(storageWriter.isConfirmShutdown());

		testingServer.stop();
	}

	static class StorageWriteTestManager implements StorageWriteManager
	{
		private volatile boolean shutdown = false;
		private volatile boolean confirmShutdown = false;
		private boolean didRun = false;
		private boolean gotInjector = false;

		public void run()
		{
			try
			{
				while (!shutdown)
				{
					didRun = true;
					System.out.println("StorageWriterTest running shutdown loop.");
					Thread.sleep(1000);
				}
			}
			catch (InterruptedException e)
			{
				// If we have been inturrupted then just die.
			}
			System.out.println("ok I'm shutdown");
			confirmShutdown = true;
		}

		public void shutdown()
		{
			shutdown = true;

			int shutdownTimer = 0;
			while (!confirmShutdown && shutdownTimer < 10)
			{
				try
				{
					System.out.println("waiting to shutdown!!");
					Thread.sleep(1000);
					shutdownTimer++;
				}
				catch (InterruptedException e)
				{
					// Don't worry about it, just die.
				}
			}
		}

		public void start()
		{
		}

		public void setInjector(Injector injector)
		{
			gotInjector = true;
		}

		public boolean isShutdown()
		{
			return shutdown;
		}

		public boolean isDidRun()
		{
			return didRun;
		}

		public boolean isGotInjector()
		{
			return gotInjector;
		}

		public boolean isConfirmShutdown()
		{
			return confirmShutdown;
		}
	}

}
