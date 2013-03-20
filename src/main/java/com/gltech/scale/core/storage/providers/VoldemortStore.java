package com.gltech.scale.core.storage.providers;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.core.storage.StreamSplitter;
import com.gltech.scale.ganglia.Timer;
import com.gltech.scale.ganglia.TimerThreadPoolExecutor;
import com.gltech.scale.util.Props;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class VoldemortStore implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(VoldemortStore.class);
	private Props props = Props.getProps();
	private Timer storeTimer = new Timer();
	private final ThreadPoolExecutor threadPoolExecutor;
	private final Semaphore semaphore;

	public VoldemortStore()
	{
		int activeUploads = props.get("storage.s3.concurrent_writes", Defaults.CONCURRENT_STORE_WRITES);

		TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
		threadPoolExecutor = new TimerThreadPoolExecutor(activeUploads, activeUploads, 1, TimeUnit.MINUTES, queue, new S3UploadThreadFactory(), storeTimer);
		semaphore = new Semaphore(activeUploads);
	}

	public void put(ChannelMetaData channelMetaData)
	{
		String key = channelMetaData.getName();
	}

	public ChannelMetaData get(String channelName)
	{
		return new ChannelMetaData(channelName, ChannelMetaData.TTL_DAY, false);
	}

	public void getMessages(String channelName, String id, OutputStream outputStream)
	{
	}

	public void putMessages(String channelName, String id, InputStream inputStream, Map<String, List<String>> headers)
	{
	}

	private String keyNameWithUniquePrefix(String channelName, String id)
	{
		try
		{
			MessageDigest md = MessageDigest.getInstance("MD5");
			String key = channelName + "|" + id;
			md.update(key.getBytes(), 0, key.length());
			String md5 = new BigInteger(1, md.digest()).toString(16);
			return md5.substring(0, 2) + "|" + channelName + "|" + id;
		}
		catch (NoSuchAlgorithmException e)
		{
			throw Throwables.propagate(e);
		}
	}

	private class PartUploader implements Callable<Long>
	{
		private StreamSplitter.StreamPart streamPart;
		private String key;
		private int part;

		private PartUploader(StreamSplitter.StreamPart streamPart, String key, int part)
		{
			this.streamPart = streamPart;
			this.key = key;
			this.part = part;
		}

		public Long call() throws Exception
		{
			logger.debug("Start Voldemort Part Upload key={}, part={}", key, part);


			//LZFInputStream


			return null;
		}
	}

	private class S3UploadThreadFactory implements ThreadFactory
	{
		public Thread newThread(Runnable r)
		{
			return new Thread(r, "VoldemortUploader");
		}
	}
}
